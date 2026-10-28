//! wake/redis_queue — the redis_queue wake source (rs#19 second installment;
//! port of plaita-go pkg/wake/redisqueue.go, semantics authority Python
//! RedisQueueService).
//!
//! Two faces:
//!
//! 1. Dispatch bridge: consume `{prefix}:redis_queue:queue` — each task
//!    carries `redis_config` (the USER redis: host/port/db/password/socket
//!    timeouts) + `queue_config` {name, type: list|stream|pubsub,
//!    message_format} and starts a listener on that user queue.
//! 2. Listener: poll/subscribe the user queue and publish a `redis_message`
//!    resume event per message (the redis_queue node's fixed event
//!    contract).
//!
//! Completion semantics mirror the Python listener: the task_complete key
//! (`task_complete:{execution_id}`, 1h TTL on the user redis) short-circuits
//! the loop; after a message was processed the listener keeps consuming
//! until `max_wait_after_message` (default 10s) of idleness, then marks the
//! task complete and returns. Python additionally stops early when the flow
//! has no active subscriptions left (subscription_storage lookup); this
//! engine lacks a correlation index, so the idle window alone terminates the
//! listen — documented, strictly-lower-efficiency divergence (same external
//! behavior: the execution resumes on the first event either way).
//!
//! The queue entry stays un-acked for the whole listen (at-least-once): a
//! service crash mid-listen leaves the entry for the next start to
//! re-consume. A publish failure returns un-acked too (the whole listen
//! restarts next cycle — BLPOP/XREAD semantics mean the message itself was
//! already consumed from the user queue, same as Python).
//!
//! Dial failures retry ≤5 with exponential backoff (cap 60s); a permanent
//! dial failure publishes the `redis_error` event (success:false) and acks
//! the entry — Python `_trigger_error_event` + return-true semantics.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use uuid::Uuid;

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::events::sleep_interruptible;
use crate::keys;
use crate::wake::{env_f64, env_i64, EventSink, RedisEventSink};

pub const SOURCE_REDIS_QUEUE: &str = "redis_queue";

const MAX_CONSECUTIVE_ERRORS: usize = 5;

/// The redis_queue wake source. One instance consumes one
/// `{prefix}:redis_queue:queue`.
pub struct RedisQueueWake {
    cfg: Config,
    backend: Arc<dyn RedisBackend>,
    sink: Arc<dyn EventSink>,
    stop: Arc<AtomicBool>,

    queue_key: String,
    registry_key: String,
    start_time: String,
    instance_id: String,

    poll: Duration,
    heartbeat: Duration,
    registry_ttl_secs: i64,
    batch: i64,
    dial_max_retries: i64,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
    max_wait_after_message: Duration,

    inflight: Mutex<HashSet<String>>,
    /// Test seam: a pre-built "user redis" (production dials via
    /// `dial_user`; tests inject a FakeRedis here).
    pub user_backend: Mutex<Option<Arc<dyn RedisBackend>>>,
}

impl RedisQueueWake {
    pub fn new(cfg: Config, backend: Arc<dyn RedisBackend>, stop: Arc<AtomicBool>) -> Self {
        let sink: Arc<dyn EventSink> = Arc::new(RedisEventSink {
            backend: Arc::clone(&backend),
            cfg: cfg.clone(),
        });
        Self::with_parts(cfg, backend, sink, stop)
    }

    /// Overrides the event sink (tests inject failures).
    pub fn with_sink(
        cfg: Config,
        backend: Arc<dyn RedisBackend>,
        sink: Arc<dyn EventSink>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self::with_parts(cfg, backend, sink, stop)
    }

    fn with_parts(
        cfg: Config,
        backend: Arc<dyn RedisBackend>,
        sink: Arc<dyn EventSink>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let instance_id = cfg.instance_id.clone();
        Self {
            queue_key: keys::service_queue_key(&cfg, SOURCE_REDIS_QUEUE),
            registry_key: format!(
                "{}:registry:{}:{}",
                cfg.key_prefix, SOURCE_REDIS_QUEUE, cfg.instance_id
            ),
            start_time: now_iso(),
            poll: Duration::from_secs_f64(env_f64("PLAITA_REDIS_QUEUE_POLL_INTERVAL_SECONDS", 0.5)),
            heartbeat: Duration::from_secs_f64(env_f64("PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10.0)),
            registry_ttl_secs: env_i64("PLAITA_REGISTRY_TTL_SECONDS", 30).max(1),
            batch: env_i64("PLAITA_REDIS_QUEUE_CONSUME_BATCH", 50),
            dial_max_retries: 5,
            dial_backoff_base: Duration::from_secs(1),
            dial_backoff_max: Duration::from_secs(60),
            max_wait_after_message: Duration::from_secs_f64(env_f64(
                "PLAITA_REDIS_QUEUE_MAX_WAIT_AFTER_MESSAGE_SECONDS",
                10.0,
            )),
            cfg,
            backend,
            sink,
            stop,
            instance_id,
            inflight: Mutex::new(HashSet::new()),
            user_backend: Mutex::new(None),
        }
    }

    /// Runs until `stop` is set.
    pub fn run(self: &Arc<Self>) {
        self.registry_beat("starting", 0);
        let heartbeat_stop = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = {
            let hb = Arc::clone(self);
            let hb_stop = Arc::clone(&heartbeat_stop);
            std::thread::spawn(move || hb.registry_loop(hb_stop))
        };
        eprintln!(
            "[wake-redis-queue] started queue={} instance={}",
            self.queue_key, self.instance_id
        );
        let mut joins: Vec<std::thread::JoinHandle<()>> = Vec::new();
        while !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            self.cycle_once(&mut joins);
            joins.retain(|j| !j.is_finished());
            sleep_interruptible(&self.stop, self.poll);
        }
        for j in joins {
            let _ = j.join();
        }
        heartbeat_stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = heartbeat_handle.join();
        self.registry_shutdown();
        eprintln!("[wake-redis-queue] stopped queue={}", self.queue_key);
    }

    fn registry_loop(self: Arc<Self>, stop: Arc<AtomicBool>) {
        self.registry_beat("running", 0);
        while !stop.load(std::sync::atomic::Ordering::SeqCst) {
            if sleep_interruptible(&stop, self.heartbeat) {
                return;
            }
            let active = self.inflight.lock().map(|m| m.len()).unwrap_or(0);
            self.registry_beat("running", active as i64);
        }
    }

    fn registry_beat(&self, status: &str, active: i64) {
        let info = json!({
            "instance_id": self.instance_id,
            "service_type": SOURCE_REDIS_QUEUE,
            "host": hostname(),
            "status": status,
            "start_time": self.start_time,
            "metadata": {"engine": "plaita-rs", "key_prefix": self.cfg.key_prefix},
            "active_tasks": active,
            "last_heartbeat": now_iso(),
        });
        if let Err(e) = self.backend.set(
            &self.registry_key,
            &info.to_string(),
            Some(self.registry_ttl_secs),
        ) {
            eprintln!("[wake-redis-queue] registry beat failed: {e}");
        }
    }

    fn registry_shutdown(&self) {
        let tomb = json!({
            "instance_id": self.instance_id,
            "service_type": SOURCE_REDIS_QUEUE,
            "host": hostname(),
            "status": "stopped",
            "start_time": self.start_time,
            "last_heartbeat": now_iso(),
        });
        let _ = self
            .backend
            .set(&self.registry_key, &tomb.to_string(), Some(5));
        let _ = self.backend.del(&[&self.registry_key]);
    }

    fn cycle_once(self: &Arc<Self>, joins: &mut Vec<std::thread::JoinHandle<()>>) {
        let raws = match self.backend.lrange(&self.queue_key, 0, self.batch - 1) {
            Ok(v) => v,
            Err(e) => {
                if !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                    eprintln!("[wake-redis-queue] consume read failed: {e}");
                }
                return;
            }
        };
        for raw in raws {
            let task: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[wake-redis-queue] invalid task JSON (dropped): {e}: {:.200}",
                        raw
                    );
                    let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                    continue;
                }
            };
            let ok = str_field(&task, "node_id").is_some_and(|s| !s.is_empty())
                && str_field(&task, "event_type").is_some_and(|s| !s.is_empty())
                && task
                    .get("redis_config")
                    .map(|v| v.is_object())
                    .unwrap_or(false)
                && task
                    .get("queue_config")
                    .map(|v| v.is_object())
                    .unwrap_or(false);
            if !ok {
                eprintln!(
                    "[wake-redis-queue] task missing node_id/event_type/redis_config/queue_config (dropped): {:.200}",
                    raw
                );
                let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                continue;
            }
            {
                let mut inflight = match self.inflight.lock() {
                    Ok(m) => m,
                    Err(_) => return,
                };
                if !inflight.insert(raw.clone()) {
                    continue;
                }
            }
            let me = Arc::clone(self);
            joins.push(std::thread::spawn(move || {
                struct Guard(Arc<RedisQueueWake>, String);
                impl Drop for Guard {
                    fn drop(&mut self) {
                        if let Ok(mut m) = self.0.inflight.lock() {
                            m.remove(self.1.as_str());
                        }
                    }
                }
                let _guard = Guard(Arc::clone(&me), raw.clone());
                me.handle_listen(&raw, &task);
            }));
        }
    }

    /// One full listen (blocks its thread for the listen's life).
    fn handle_listen(&self, raw: &str, task: &Value) {
        if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            return; // shutdown: entry stays for the next start
        }
        let rc = task.get("redis_config").cloned().unwrap_or(json!({}));
        let qc = task.get("queue_config").cloned().unwrap_or(json!({}));
        let execution_id = str_field(task, "execution_id").unwrap_or_default();
        let user = match self.dial_user(&rc) {
            Ok(c) => c,
            Err(e) => {
                if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                eprintln!(
                    "[wake-redis-queue] user dial failed permanently; error event + ack (execution_id={}): {}",
                    execution_id, e
                );
                self.publish_error_event(task, &e);
                let _ = self.backend.lrem(&self.queue_key, 1, raw);
                return;
            }
        };
        let complete_key = format!("task_complete:{}", execution_id);
        let _ = user.set(&complete_key, "0", Some(3600));
        let queue_name = str_field(&qc, "name").unwrap_or_default();
        let queue_type = str_field(&qc, "type").unwrap_or_else(|| "list".to_string());
        let format = str_field(&qc, "message_format").unwrap_or_else(|| "json".to_string());
        let lc = task.get("listen_config").cloned().unwrap_or(json!({}));
        let max_wait = match lc.get("max_wait_after_message").and_then(|v| v.as_f64()) {
            Some(v) if v > 0.0 => Duration::from_secs_f64(v),
            _ => self.max_wait_after_message,
        };
        let confirmed = match queue_type.as_str() {
            "list" => self.listen_list(&user, task, &complete_key, &queue_name, &format, max_wait),
            "stream" => {
                self.listen_stream(&user, task, &complete_key, &queue_name, &format, max_wait)
            }
            "pubsub" => {
                self.listen_pubsub(&user, task, &complete_key, &queue_name, &format, max_wait)
            }
            other => {
                eprintln!(
                    "[wake-redis-queue] unsupported queue type (dropped): {}",
                    other
                );
                true
            }
        };
        if confirmed && !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = self.backend.lrem(&self.queue_key, 1, raw);
        }
    }

    /// Dials the USER redis (host/port/db/password form → URL) with ≤5
    /// exponential-backoff retries (Python _create_redis_client_with_retry).
    fn dial_user(&self, rc: &Value) -> Result<Arc<dyn RedisBackend>, String> {
        if let Ok(guard) = self.user_backend.lock() {
            if let Some(user) = guard.as_ref() {
                return Ok(Arc::clone(user));
            }
        }
        let host = str_field(rc, "host").unwrap_or_else(|| "localhost".to_string());
        let port = rc.get("port").and_then(|v| v.as_i64()).unwrap_or(6379);
        let db = rc.get("db").and_then(|v| v.as_i64()).unwrap_or(0);
        let password = str_field(rc, "password").unwrap_or_default();
        let url = if password.is_empty() {
            format!("redis://{}:{}/{}", host, port, db)
        } else {
            format!("redis://:{}@{}:{}/{}", password, host, port, db)
        };
        let mut last = String::new();
        for attempt in 0..=self.dial_max_retries {
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("shutdown".to_string());
            }
            match crate::redis_impl::RedisConnection::connect(&url) {
                Ok(c) => {
                    let boxed: Arc<dyn RedisBackend> = Arc::new(c);
                    return Ok(boxed);
                }
                Err(e) => {
                    last = e.to_string();
                    if attempt < self.dial_max_retries {
                        let delay = (self.dial_backoff_base.as_secs_f64()
                            * 2f64.powi(attempt as i32))
                        .min(self.dial_backoff_max.as_secs_f64());
                        eprintln!(
                            "[wake-redis-queue] user dial failed ({}); retrying in {:.1}s: {}",
                            attempt + 1,
                            delay,
                            last
                        );
                        sleep_interruptible(&self.stop, Duration::from_secs_f64(delay));
                    }
                }
            }
        }
        Err(last)
    }

    fn complete_flag_set(&self, user: &Arc<dyn RedisBackend>, complete_key: &str) -> bool {
        matches!(user.get(complete_key), Ok(Some(v)) if v == "1")
    }

    fn listen_list(
        &self,
        user: &Arc<dyn RedisBackend>,
        task: &Value,
        complete_key: &str,
        queue_name: &str,
        format: &str,
        max_wait: Duration,
    ) -> bool {
        let mut processed = false;
        let mut last_message = Instant::now();
        let mut consecutive_errors = 0usize;
        loop {
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                return false;
            }
            if self.complete_flag_set(user, complete_key) {
                return true;
            }
            if processed && last_message.elapsed() > max_wait {
                let _ = user.set(complete_key, "1", Some(3600));
                return true;
            }
            match user.blpop(queue_name, 2) {
                Ok(Some((_, message))) => {
                    let ev = self.build_message_event(
                        task,
                        json!({
                            "trigger_type": "redis_message",
                            "queue_type": "list",
                            "queue_name": queue_name,
                            "message": parse_message(&message, format),
                            "raw_message": message,
                            "success": true,
                        }),
                    );
                    if let Err(e) = self.publish_resume(&ev) {
                        if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                            return false;
                        }
                        eprintln!(
                            "[wake-redis-queue] resume event publish failed; entry kept (listen restarts next cycle): {e}"
                        );
                        return false;
                    }
                    processed = true;
                    last_message = Instant::now();
                    consecutive_errors = 0;
                    std::thread::sleep(Duration::from_millis(500));
                }
                Ok(None) => {
                    // Real redis blocks the full 2s; the fake pops-or-None
                    // immediately — keep the loop cadence bounded in both.
                    consecutive_errors = 0;
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                        return false;
                    }
                    consecutive_errors += 1;
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        eprintln!(
                            "[wake-redis-queue] list listener giving up after {} errors: {}",
                            consecutive_errors, e
                        );
                        return true;
                    }
                    eprintln!("[wake-redis-queue] list listener error: {e}");
                }
            }
        }
    }

    fn listen_stream(
        &self,
        user: &Arc<dyn RedisBackend>,
        task: &Value,
        complete_key: &str,
        stream_name: &str,
        format: &str,
        max_wait: Duration,
    ) -> bool {
        let mut processed = false;
        let mut last_message = Instant::now();
        let mut consecutive_errors = 0usize;
        let mut last_id = "$".to_string();
        loop {
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                return false;
            }
            if self.complete_flag_set(user, complete_key) {
                return true;
            }
            if processed && last_message.elapsed() > max_wait {
                let _ = user.set(complete_key, "1", Some(3600));
                return true;
            }
            match user.xread(stream_name, &last_id, 2000) {
                Ok(entries) if entries.is_empty() => {
                    // BLOCK 2000 in production; the fake returns instantly.
                    std::thread::sleep(Duration::from_millis(100));
                }
                Ok(entries) => {
                    for entry in entries {
                        let fields = json!(entry
                            .fields
                            .iter()
                            .cloned()
                            .collect::<Vec<(String, String)>>());
                        let ev = self.build_message_event(
                            task,
                            json!({
                                "trigger_type": "redis_message",
                                "queue_type": "stream",
                                "stream_name": stream_name,
                                "message_id": entry.id,
                                "message": parse_stream_fields(&fields, format),
                                "raw_fields": fields,
                                "success": true,
                            }),
                        );
                        if let Err(e) = self.publish_resume(&ev) {
                            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                                return false;
                            }
                            eprintln!(
                                "[wake-redis-queue] resume event publish failed; entry kept: {e}"
                            );
                            return false;
                        }
                        last_id = entry.id.clone();
                        processed = true;
                        last_message = Instant::now();
                        consecutive_errors = 0;
                        std::thread::sleep(Duration::from_millis(500));
                    }
                }
                Err(e) => {
                    if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                        return false;
                    }
                    consecutive_errors += 1;
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        eprintln!(
                            "[wake-redis-queue] stream listener giving up after {} errors: {}",
                            consecutive_errors, e
                        );
                        return true;
                    }
                    eprintln!("[wake-redis-queue] stream listener error: {e}");
                }
            }
        }
    }

    fn listen_pubsub(
        &self,
        user: &Arc<dyn RedisBackend>,
        task: &Value,
        complete_key: &str,
        channel_name: &str,
        format: &str,
        max_wait: Duration,
    ) -> bool {
        // psubscribe with a literal channel name (no wildcards) = plain
        // SUBSCRIBE; the confirmation is consumed inside psubscribe (§8.2).
        let mut sub = match user.psubscribe(channel_name) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "[wake-redis-queue] pubsub subscribe failed: {e}: channel={}",
                    channel_name
                );
                self.publish_error_event(task, &e.to_string());
                return true;
            }
        };
        let mut processed = false;
        let mut last_message = Instant::now();
        loop {
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                return false;
            }
            if self.complete_flag_set(user, complete_key) {
                return true;
            }
            if processed && last_message.elapsed() > max_wait {
                let _ = user.set(complete_key, "1", Some(3600));
                return true;
            }
            match sub.next_message(Duration::from_secs(2)) {
                Some((_, payload)) => {
                    let ev = self.build_message_event(
                        task,
                        json!({
                            "trigger_type": "redis_message",
                            "queue_type": "pubsub",
                            "channel_name": channel_name,
                            "message": parse_message(&payload, format),
                            "raw_message": payload,
                            "success": true,
                        }),
                    );
                    if let Err(e) = self.publish_resume(&ev) {
                        if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                            return false;
                        }
                        eprintln!(
                            "[wake-redis-queue] resume event publish failed; entry kept: {e}"
                        );
                        return false;
                    }
                    processed = true;
                    last_message = Instant::now();
                    std::thread::sleep(Duration::from_millis(500));
                }
                None => continue, // receive timeout — idle path
            }
        }
    }

    fn build_message_event(&self, task: &Value, mut data: Value) -> Value {
        let tenant = str_field(task, "tenant_id")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.cfg.default_tenant_id.clone());
        let execution_id = str_field(task, "execution_id").unwrap_or_default();
        if let Some(obj) = data.as_object_mut() {
            obj.insert("node_id".to_string(), task["node_id"].clone());
            obj.insert("execution_id".to_string(), json!(execution_id));
            obj.insert("flow_id".to_string(), task["flow_id"].clone());
            obj.insert("tenant_id".to_string(), json!(tenant));
            obj.insert("timestamp".to_string(), json!(now_ms()));
        }
        json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": str_field(task, "event_type").unwrap_or_default(),
            "data": data,
            "timestamp": now_ms() as f64 / 1000.0,
            "correlation_id": execution_id,
            "tenant_id": tenant,
        })
    }

    fn publish_resume(&self, ev: &Value) -> Result<(), String> {
        let tenant = ev["tenant_id"].as_str().unwrap_or_default().to_string();
        let event_type = ev["event_type"].as_str().unwrap_or_default().to_string();
        let channel = keys::business_event_channel(&self.cfg, &tenant, &event_type);
        self.sink.publish(&channel, &ev.to_string())
    }

    #[allow(clippy::unused_self)]
    fn publish_error_event(&self, task: &Value, message: &str) {
        let ev = self.build_message_event(
            task,
            json!({
                "trigger_type": "redis_error",
                "error_message": message,
                "success": false,
            }),
        );
        let tenant = ev["tenant_id"].as_str().unwrap_or_default().to_string();
        let event_type = ev["event_type"].as_str().unwrap_or_default().to_string();
        let channel = keys::business_event_channel(&self.cfg, &tenant, &event_type);
        if let Err(e) = self.sink.publish(&channel, &ev.to_string()) {
            eprintln!("[wake-redis-queue] error event publish failed: {e}");
        }
    }
}

/// json → decode (fallback raw on failure), text → string (Python
/// _parse_message).
fn parse_message(raw: &str, format: &str) -> Value {
    if format == "json" {
        if let Ok(v) = serde_json::from_str::<Value>(raw) {
            return v;
        }
    }
    json!(raw)
}

/// json format reads the `data` field; missing/unparsable → the raw fields
/// (Python _parse_stream_message). `fields` arrives as the raw
/// Vec<(String,String)> rendered as a JSON object.
fn parse_stream_fields(fields: &Value, format: &str) -> Value {
    if format == "json" {
        if let Some(data) = fields.get("data").and_then(|v| v.as_str()) {
            if let Ok(v) = serde_json::from_str::<Value>(data) {
                return v;
            }
        }
    }
    fields.clone()
}

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn hostname() -> String {
    if let Ok(h) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let h = h.trim().to_string();
        if !h.is_empty() {
            return h;
        }
    }
    for key in ["HOST", "HOSTNAME"] {
        if let Ok(h) = std::env::var(key) {
            if !h.is_empty() {
                return h;
            }
        }
    }
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;

    struct Fixture {
        wake: Arc<RedisQueueWake>,
        backend: Arc<FakeRedis>,
        user: Arc<FakeRedis>,
        events: Arc<Mutex<Vec<(String, String)>>>,
    }

    fn fixture() -> Fixture {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_KEY_PREFIX" => "plaita".to_string(),
            "PLAITA_INSTANCE_ID" => "rq-test".to_string(),
            _ => String::new(),
        });
        let backend = Arc::new(FakeRedis::new());
        let user = Arc::new(FakeRedis::new());
        let backend_dyn: Arc<dyn RedisBackend> = backend.clone();
        let user_dyn: Arc<dyn RedisBackend> = user.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        struct SeqSink(Arc<Mutex<Vec<(String, String)>>>);
        impl EventSink for SeqSink {
            fn publish(&self, channel: &str, payload: &str) -> Result<(), String> {
                self.0
                    .lock()
                    .unwrap()
                    .push((channel.to_string(), payload.to_string()));
                Ok(())
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(RedisQueueWake::with_sink(
            cfg,
            backend_dyn,
            Arc::new(SeqSink(Arc::clone(&events))),
            stop,
        ));
        *wake.user_backend.lock().unwrap() = Some(user_dyn);
        Fixture {
            wake,
            backend,
            user,
            events,
        }
    }

    fn task_raw() -> String {
        json!({
            "type": "redis_queue",
            "node_id": "listen",
            "event_type": "redis_message",
            "execution_id": "exec-rq",
            "flow_id": "flow-rq",
            "tenant_id": "default",
            "redis_config": {"host": "127.0.0.1", "port": 6379, "db": 0},
            "queue_config": {"name": "user:q", "type": "list", "message_format": "json"},
            "listen_config": {"max_wait_after_message": 0.3},
        })
        .to_string()
    }

    #[test]
    fn list_listen_publishes_and_acks_on_idle() {
        let fx = fixture();
        let raw = task_raw();
        fx.backend.rpush(&fx.wake.queue_key, &raw).unwrap();
        fx.user.rpush("user:q", r#"{"k":"v"}"#).unwrap();
        let mut joins = Vec::new();
        fx.wake.cycle_once(&mut joins);
        for j in joins {
            j.join().unwrap();
        }
        let events = fx.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].0.ends_with(":events:redis_message"));
        let ev: Value = serde_json::from_str(&events[0].1).unwrap();
        assert_eq!(ev["correlation_id"], "exec-rq");
        assert_eq!(ev["data"]["trigger_type"], "redis_message");
        assert_eq!(ev["data"]["queue_type"], "list");
        assert_eq!(ev["data"]["message"]["k"], "v");
        drop(events);
        // idle window (0.3s) elapses → complete flag + dispatch ack
        assert_eq!(fx.user.get("task_complete:exec-rq").unwrap().unwrap(), "1");
        assert!(fx
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn complete_flag_helper_reads_user_redis() {
        let fx = fixture();
        let user_dyn: Arc<dyn RedisBackend> = fx.user.clone();
        assert!(!fx.wake.complete_flag_set(&user_dyn, "task_complete:e1"));
        fx.user.set("task_complete:e1", "1", Some(3600)).unwrap();
        assert!(fx.wake.complete_flag_set(&user_dyn, "task_complete:e1"));
        // Python handle_task resets the flag to "0" at listen start — a
        // pre-set flag does NOT survive into the loop (same semantics).
        fx.user.set("task_complete:e1", "0", Some(3600)).unwrap();
        assert!(!fx.wake.complete_flag_set(&user_dyn, "task_complete:e1"));
    }

    #[test]
    fn missing_config_dropped() {
        let fx = fixture();
        let bad = json!({
            "type": "redis_queue", "node_id": "listen", "event_type": "redis_message",
            "execution_id": "exec-x",
        })
        .to_string();
        fx.backend.rpush(&fx.wake.queue_key, &bad).unwrap();
        let mut joins = Vec::new();
        fx.wake.cycle_once(&mut joins);
        for j in joins {
            j.join().unwrap();
        }
        assert!(fx
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
        assert!(fx.events.lock().unwrap().is_empty());
    }

    #[test]
    fn real_dial_failure_publishes_error_and_acks() {
        let mut fx = fixture();
        // Clear the fixture's injected user backend: the real dial to a dead
        // port must fail fast (fixture injects by default).
        *fx.wake.user_backend.lock().unwrap() = None;
        let wake_mut: &mut RedisQueueWake = Arc::get_mut(&mut fx.wake)
            .expect("fixture owns the only Arc before run() spawns anything");
        wake_mut.dial_max_retries = 0;
        wake_mut.dial_backoff_base = Duration::from_millis(1);
        let raw = json!({
            "type": "redis_queue", "node_id": "listen", "event_type": "redis_message",
            "execution_id": "exec-df", "flow_id": "flow-df", "tenant_id": "default",
            "redis_config": {"host": "127.0.0.1", "port": 1, "db": 0},
            "queue_config": {"name": "user:q", "type": "list", "message_format": "json"},
        })
        .to_string();
        fx.backend.rpush(&fx.wake.queue_key, &raw).unwrap();
        let mut joins = Vec::new();
        fx.wake.cycle_once(&mut joins);
        for j in joins {
            j.join().unwrap();
        }
        let events = fx.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].1.contains("\"trigger_type\":\"redis_error\""));
        drop(events);
        assert!(fx
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
    }
}
