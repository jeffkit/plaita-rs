//! wake — the wake services: companion processes that consume the
//! suspend-service queues (`{prefix}:{subtype}:queue`, §5.2) the worker's
//! dispatch writes and turn each due signal into a resume event.
//!
//! Port of plaita-go `pkg/wake` (rs#19 first installment: delay); semantics
//! authority Python `plaita/server/services/delay_service.py` — the C4-2
//! list→ZSET two-phase scheduling with the #35 retry/backoff/dead-letter
//! trigger-confirmation semantics:
//!
//! - convey: LRANGE list → ZADD `{queue}:scheduled` (member = raw JSON,
//!   score = due epoch ms) → LREM list. ZADD first, remove second: a crash
//!   in between re-conveys next round and ZADD is idempotent per member.
//! - trigger: ZRANGEBYSCORE due → publish resume event → ZREM only after the
//!   publish is confirmed (at-least-once). A failed publish leaves the entry
//!   scheduled, records the attempt in the `{queue}:attempts` hash and backs
//!   off `min(base·2^(n-1), max)`; past max attempts the entry moves to the
//!   `{queue}:dead` list (RPUSH before ZREM — prefer a duplicate over a
//!   loss) and a best-effort `service_error` event is broadcast.
//!
//! The resume event wire shape and the double-write storage keys
//! (`{esp}events:{id}` + `{esp}types:{type}` + PUBLISH `{ns}:events:{type}`)
//! are byte-compatible with the Python services (and the Go worker for the
//! default tenant — Go additionally routes the storage keys through the
//! tenant namespace, a documented cross-engine divergence; the resume EVENT
//! channel is tenant-routed identically everywhere, which is the face the
//! worker bridge consumes). Mixed fleets share one Redis and any side's wake
//! process serves any side's workers.
//! Timestamps: the wire `timestamp` and the type-index ZADD score are epoch
//! SECONDS (Python Event convention; the reconcile scan assumes it — the
//! data-level `*_trigger_timestamp` fields stay epoch ms, matching Python's
//! handle_task payload).
//!
//! Registry: `{prefix}:registry:delay:{instance_id}` SETEX (TTL 30s, 10s
//! heartbeat, stopped tombstone 5s → DEL) — the roll-call data source for
//! the cluster-tier acceptance gate.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::events::sleep_interruptible;
use crate::keys;

pub const SOURCE_DELAY: &str = "delay";

const SERVICE_ERROR_EVENT: &str = "service_error";
const REGISTRY_TOMBSTONE_TTL_SECS: i64 = 5;

pub(crate) fn env_f64(name: &str, def: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        // finite + bounded: `inf`/1e300 would panic Duration::from_secs_f64
        // at construction (and a day-scale poll interval is certainly a
        // misconfiguration).
        .filter(|v| v.is_finite() && *v > 0.0 && *v <= 86_400.0)
        .unwrap_or(def)
}

pub(crate) fn env_i64(name: &str, def: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(def)
}

/// Event sink seam (tests inject failures). The production sink performs the
/// §6.2 double-write + Pub/Sub notification.
pub trait EventSink: Send + Sync {
    fn publish(&self, channel: &str, payload: &str) -> Result<(), String>;
}

/// Production sink: SETEX `{esp}events:{id}` + ZADD `{esp}types:{type}`
/// (score = epoch seconds) + EXPIRE + PUBLISH `{ns}:events:{type}` — the
/// exact double-write the Go EventPublisher performs (byte-compatible keys,
/// so the reconcile回扫面 covers RS-published events the same way).
pub struct RedisEventSink {
    pub backend: Arc<dyn RedisBackend>,
    pub cfg: Config,
}

impl RedisEventSink {
    pub fn new(backend: Arc<dyn RedisBackend>, cfg: Config) -> Self {
        Self { backend, cfg }
    }
}

impl EventSink for RedisEventSink {
    fn publish(&self, channel: &str, payload: &str) -> Result<(), String> {
        let v: Value = serde_json::from_str(payload).map_err(|e| format!("payload: {e}"))?;
        let event_id = v
            .get("event_id")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let event_type = v
            .get("event_type")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let ts = v.get("timestamp").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let record_key = keys::event_storage_record_key(&self.cfg, &event_id);
        let type_key = keys::event_storage_type_index_key(&self.cfg, &event_type);
        self.backend
            .set(&record_key, payload, Some(self.cfg.event_ttl_seconds))
            .map_err(|e| e.to_string())?;
        self.backend
            .zadd(&type_key, ts, &event_id)
            .map_err(|e| e.to_string())?;
        self.backend
            .expire(&type_key, self.cfg.event_ttl_seconds)
            .map_err(|e| e.to_string())?;
        self.backend
            .publish(channel, payload)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// The delay wake source. One instance serves one `{prefix}:delay:queue`.
/// Shared by Arc across threads (the crate's sync thread model).
pub struct DelayWake {
    pub cfg: Config,
    pub backend: Arc<dyn RedisBackend>,
    pub sink: Arc<dyn EventSink>,
    pub stop: Arc<AtomicBool>,

    queue_key: String,
    scheduled_key: String,
    attempts_key: String,
    dead_key: String,
    registry_key: String,
    start_time: String,

    poll: Duration,
    heartbeat: Duration,
    registry_ttl_secs: i64,
    backoff_base: Duration,
    backoff_max: Duration,
    max_attempts: i64,
    convey_batch: i64,
    due_batch: i64,
    instance_id: String,

    inflight: Mutex<HashSet<String>>,
}

impl DelayWake {
    /// Builds the source from the worker Config + env-tunable knobs (the
    /// same env names plaita-go's plaita-wake reads, so deployments
    /// configure both identically).
    pub fn new(cfg: Config, backend: Arc<dyn RedisBackend>, stop: Arc<AtomicBool>) -> Self {
        let queue_key = match std::env::var("PLAITA_DELAY_QUEUE") {
            Ok(v) if v.trim().is_empty() => panic!(
                "PLAITA_DELAY_QUEUE: empty value is not allowed (identity/routing); unset it to use the default"
            ),
            Ok(v) => v,
            Err(_) => format!("{}:delay:queue", cfg.key_prefix),
        };
        let sink: Arc<dyn EventSink> = Arc::new(RedisEventSink {
            backend: Arc::clone(&backend),
            cfg: cfg.clone(),
        });
        Self {
            instance_id: cfg.instance_id.clone(),
            registry_key: format!(
                "{}:registry:{}:{}",
                cfg.key_prefix, SOURCE_DELAY, cfg.instance_id
            ),
            scheduled_key: format!("{queue_key}:scheduled"),
            attempts_key: format!("{queue_key}:attempts"),
            dead_key: format!("{queue_key}:dead"),
            start_time: now_iso(),
            poll: Duration::from_secs_f64(env_f64("PLAITA_DELAY_POLL_INTERVAL_SECONDS", 0.5)),
            heartbeat: Duration::from_secs_f64(env_f64("PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10.0)),
            registry_ttl_secs: env_i64("PLAITA_REGISTRY_TTL_SECONDS", 30).max(1),
            backoff_base: Duration::from_secs_f64(env_f64(
                "PLAITA_DELAY_RETRY_BACKOFF_BASE_SECONDS",
                1.0,
            )),
            backoff_max: Duration::from_secs_f64(env_f64(
                "PLAITA_DELAY_RETRY_BACKOFF_MAX_SECONDS",
                30.0,
            )),
            max_attempts: env_i64("PLAITA_DELAY_MAX_TRIGGER_ATTEMPTS", 5),
            convey_batch: env_i64("PLAITA_DELAY_CONVEY_BATCH", 200),
            due_batch: env_i64("PLAITA_DELAY_DUE_BATCH", 50),
            queue_key,
            cfg,
            backend,
            sink,
            stop,
            inflight: Mutex::new(HashSet::new()),
        }
    }

    /// Overrides the event sink (tests inject failures).
    pub fn with_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = sink;
        self
    }

    /// Runs until `stop` is set. The consume cycle is synchronous; each due
    /// task runs on its own thread with the in-flight set guarding
    /// duplicates. Returns after in-flight triggers land (or stay parked on
    /// their own shutdown check) and the registry record is tombstoned.
    pub fn run(self: &Arc<Self>) {
        self.registry_beat("starting", 0);
        let heartbeat_stop = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = {
            let hb = Arc::clone(self);
            let hb_stop = Arc::clone(&heartbeat_stop);
            std::thread::spawn(move || hb.registry_loop(hb_stop))
        };
        eprintln!(
            "[wake-delay] started queue={} scheduled={} instance={}",
            self.queue_key, self.scheduled_key, self.instance_id
        );
        let mut joins: Vec<std::thread::JoinHandle<()>> = Vec::new();
        while !self.stop.load(Ordering::SeqCst) {
            self.convey_once();
            self.submit_due_once(&mut joins);
            joins.retain(|j| !j.is_finished());
            sleep_interruptible(&self.stop, self.poll);
        }
        for j in joins {
            let _ = j.join();
        }
        // Stop the heartbeat loop and JOIN it before the tombstone: a beat
        // completing after the shutdown DEL would resurrect the registry key
        // as "running" with a full TTL — a phantom roll-call row exactly
        // when the acceptance gate is watching (Go: cancelReg(); <-regDone).
        heartbeat_stop.store(true, Ordering::SeqCst);
        let _ = heartbeat_handle.join();
        self.registry_shutdown();
        eprintln!("[wake-delay] stopped queue={}", self.queue_key);
    }

    fn registry_loop(self: Arc<Self>, stop: Arc<AtomicBool>) {
        self.registry_beat("running", 0);
        while !stop.load(Ordering::SeqCst) {
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
            "service_type": SOURCE_DELAY,
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
            eprintln!("[wake-delay] registry beat failed: {e}");
        }
    }

    fn registry_shutdown(&self) {
        let tomb = json!({
            "instance_id": self.instance_id,
            "service_type": SOURCE_DELAY,
            "host": hostname(),
            "status": "stopped",
            "start_time": self.start_time,
            "last_heartbeat": now_iso(),
        });
        let _ = self.backend.set(
            &self.registry_key,
            &tomb.to_string(),
            Some(REGISTRY_TOMBSTONE_TTL_SECS),
        );
        let _ = self.backend.del(&[&self.registry_key]);
    }

    // ---- list → ZSET convey ----

    fn convey_once(&self) {
        let raws = match self
            .backend
            .lrange(&self.queue_key, 0, self.convey_batch - 1)
        {
            Ok(v) => v,
            Err(e) => {
                if !self.stop.load(Ordering::SeqCst) {
                    eprintln!("[wake-delay] convey read failed: {e}");
                }
                return;
            }
        };
        if raws.is_empty() {
            return;
        }
        let now_ms = now_ms();
        let mut valid: Vec<(String, f64)> = Vec::new();
        for raw in raws {
            let task: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[wake-delay] invalid task JSON (dropped): {e}: {:.200}",
                        raw
                    );
                    let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                    continue;
                }
            };
            if !validate_delay_task(&task) {
                eprintln!(
                    "[wake-delay] task failed validation (dropped): {:.200}",
                    raw
                );
                let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                continue;
            }
            valid.push((raw, due_score_ms(&task, now_ms)));
        }
        if valid.is_empty() {
            return;
        }
        // ZADD everything first, LREM only after — the crash window
        // re-conveys and ZADD is idempotent per member.
        for (raw, score) in &valid {
            if let Err(e) = self.backend.zadd(&self.scheduled_key, *score, raw) {
                eprintln!(
                    "[wake-delay] convey ZADD failed; list untouched, next round re-conveys: {e}"
                );
                return;
            }
        }
        for (raw, _) in &valid {
            if let Err(e) = self.backend.lrem(&self.queue_key, 1, raw) {
                eprintln!("[wake-delay] convey LREM failed; ZADD is idempotent, next round re-conveys: {e}");
            }
        }
    }

    // ---- due poll + trigger ----

    fn submit_due_once(self: &Arc<Self>, joins: &mut Vec<std::thread::JoinHandle<()>>) {
        let now_ms = now_ms();
        let due = match self.backend.zrangebyscore(
            &self.scheduled_key,
            f64::NEG_INFINITY,
            now_ms as f64,
            self.due_batch,
        ) {
            Ok(v) => v,
            Err(e) => {
                if !self.stop.load(Ordering::SeqCst) {
                    eprintln!("[wake-delay] due poll failed: {e}");
                }
                return;
            }
        };
        // One HMGET for all due members' attempt records (Go batches the
        // same way); read failure is fail-open (retry immediately).
        let due_refs: Vec<&str> = due.iter().map(|s| s.as_str()).collect();
        let attempts = match self.backend.hmget(&self.attempts_key, &due_refs) {
            Ok(rows) => due
                .iter()
                .cloned()
                .zip(rows)
                .filter_map(|(m, raw)| raw.and_then(|r| parse_attempt(&r)).map(|rec| (m, rec)))
                .collect::<Vec<(String, AttemptRecord)>>(),
            Err(e) => {
                eprintln!("[wake-delay] attempt records read failed (fail-open): {e}");
                Vec::new()
            }
        };
        for member in due {
            if let Some((_, rec)) = attempts.iter().find(|(m, _)| *m == member) {
                if rec.next > now_ms {
                    continue; // backoff window — next round
                }
            }
            {
                let mut inflight = match self.inflight.lock() {
                    Ok(m) => m,
                    Err(_) => return,
                };
                if !inflight.insert(member.clone()) {
                    continue;
                }
            }
            let me = Arc::clone(self);
            joins.push(std::thread::spawn(move || {
                // Drop-guard: process_task panicking must not leak the
                // in-flight slot (the entry would never be resubmitted).
                struct Guard(Arc<DelayWake>, String);
                impl Drop for Guard {
                    fn drop(&mut self) {
                        if let Ok(mut m) = self.0.inflight.lock() {
                            m.remove(self.1.as_str());
                        }
                    }
                }
                let _guard = Guard(Arc::clone(&me), member.clone());
                me.process_task(&member);
            }));
        }
    }

    fn process_task(&self, member: &str) {
        let task: Value = match serde_json::from_str(member) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[wake-delay] scheduled task undecodable (removed): {e}: {:.200}",
                    member
                );
                self.finalize(member);
                return;
            }
        };
        if self.stop.load(Ordering::SeqCst) {
            return; // shutdown: the entry stays parked for the next start
        }
        match self.publish_resume(&task) {
            Ok(()) => self.finalize(member),
            Err(e) => {
                if self.stop.load(Ordering::SeqCst) {
                    return; // shutdown raced the publish: no attempt counted
                }
                self.defer(member, e);
            }
        }
    }

    fn publish_resume(&self, task: &Value) -> Result<(), String> {
        let now_ms = now_ms();
        let tenant = str_field(task, "tenant_id")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.cfg.default_tenant_id.clone());
        let event_type = str_field(task, "event_type").unwrap_or_default();
        let execution_id = str_field(task, "execution_id").unwrap_or_default();
        let data = json!({
            "node_id": task.get("node_id").cloned().unwrap_or(Value::Null),
            "execution_id": execution_id,
            "flow_id": task.get("flow_id").cloned().unwrap_or(Value::Null),
            "tenant_id": tenant,
            "trigger_type": "delay_completed",
            "delay_ms": task.get("delay_ms").cloned().unwrap_or(Value::Null),
            "actual_trigger_timestamp": now_ms,
            "planned_trigger_timestamp": task.get("trigger_timestamp").cloned().unwrap_or(Value::Null),
            "success": true,
        });
        let ev = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": event_type,
            "data": data,
            // Epoch SECONDS: Python Event wire convention; the type-index
            // ZADD score shares the unit (reconcile scans seconds windows).
            "timestamp": now_ms as f64 / 1000.0,
            "correlation_id": execution_id,
            "tenant_id": tenant,
        });
        let channel = keys::business_event_channel(&self.cfg, &tenant, &event_type);
        self.sink.publish(&channel, &ev.to_string())
    }

    fn finalize(&self, member: &str) {
        if let Err(e) = self.backend.zrem(&self.scheduled_key, member) {
            eprintln!("[wake-delay] finalize ZREM failed; a duplicate trigger may follow (event-side dedup converges): {e}");
        }
        if let Err(e) = self.backend.hdel(&self.attempts_key, member) {
            eprintln!("[wake-delay] finalize HDel failed: {e}");
        }
    }

    // ---- retry / dead letter ----

    fn defer(&self, member: &str, cause: String) {
        let attempts = self.load_attempt(member).map(|r| r.attempts).unwrap_or(0) + 1;
        if attempts >= self.max_attempts {
            // One pipelined round-trip (TxOp; Go pipeline parity). The redis
            // crate's pipe() here is a batched send, NOT MULTI/EXEC — same
            // as Go's Pipeline().Exec — but the op order (RPUSH dead first,
            // then ZREM/HDel) still prefers a duplicate over a loss if the
            // batch degrades mid-flight.
            let tx = self.backend.tx(&[
                crate::backend::TxOp::Rpush {
                    key: self.dead_key.clone(),
                    value: member.to_string(),
                },
                crate::backend::TxOp::Zrem {
                    key: self.scheduled_key.clone(),
                    member: member.to_string(),
                },
                crate::backend::TxOp::Hdel {
                    key: self.attempts_key.clone(),
                    field: member.to_string(),
                },
            ]);
            if tx.is_err() {
                eprintln!("[wake-delay] dead-letter move failed; entry stays scheduled (duplicate possible, loss impossible)");
                return;
            }
            eprintln!(
                "[wake-delay] trigger retries exhausted ({}); dead-lettered: {:.200}",
                attempts, member
            );
            self.publish_dead_letter_event(member, attempts);
            return;
        }
        let backoff_ms =
            (self.backoff_base.as_secs_f64() * 2f64.powi((attempts - 1) as i32) * 1000.0)
                .min(self.backoff_max.as_secs_f64() * 1000.0);
        let next = now_ms() + backoff_ms as i64;
        let rec = json!({"attempts": attempts, "next": next}).to_string();
        if let Err(e) = self.backend.hset(&self.attempts_key, member, &rec) {
            eprintln!(
                "[wake-delay] backoff record failed; entry stays scheduled, next round retries immediately: {e}"
            );
            return;
        }
        eprintln!(
            "[wake-delay] trigger failed ({}); kept scheduled for retry, backoff {:.0}ms: cause={}",
            attempts, backoff_ms, cause
        );
    }

    fn publish_dead_letter_event(&self, member: &str, attempts: i64) {
        let task: Value = serde_json::from_str(member).unwrap_or(json!({}));
        let tenant = str_field(&task, "tenant_id")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.cfg.default_tenant_id.clone());
        let execution_id = str_field(&task, "execution_id").unwrap_or_default();
        let ev = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": SERVICE_ERROR_EVENT,
            "data": {
                "node_id": task.get("node_id").cloned().unwrap_or(Value::Null),
                "execution_id": execution_id,
                "flow_id": task.get("flow_id").cloned().unwrap_or(Value::Null),
                "tenant_id": tenant,
                "error_type": "delay_trigger_dead_letter",
                "error_message": format!("延迟任务到期触发重试超限（{} 次），已转死信: {}", attempts, self.dead_key),
                "service_type": SOURCE_DELAY,
                "timestamp": now_ms(),
            },
            "timestamp": now_ms() as f64 / 1000.0,
            "correlation_id": execution_id,
            "tenant_id": tenant,
        });
        let channel = keys::business_event_channel(&self.cfg, &tenant, SERVICE_ERROR_EVENT);
        // Best-effort, off the trigger thread (Go bounds this with a 3s
        // context; here a detached thread keeps a hung Redis from parking
        // the trigger and its in-flight slot — the dead list itself remains
        // the discovery surface either way).
        let payload = ev.to_string();
        let sink = Arc::clone(&self.sink);
        std::thread::spawn(move || {
            if let Err(e) = sink.publish(&channel, &payload) {
                eprintln!(
                    "[wake-delay] dead-letter event broadcast failed (dead list remains): {e}"
                );
            }
        });
    }

    fn load_attempt(&self, member: &str) -> Option<AttemptRecord> {
        match self.backend.hget(&self.attempts_key, member) {
            Ok(Some(raw)) => parse_attempt(&raw),
            Ok(None) => None,
            Err(e) => {
                // fail-open: a broken counter must never park a task out of
                // reach (Python mirrors this).
                eprintln!("[wake-delay] attempt records read failed (fail-open): {e}");
                None
            }
        }
    }
}

struct AttemptRecord {
    attempts: i64,
    next: i64,
}

fn parse_attempt(raw: &str) -> Option<AttemptRecord> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .map(|v| AttemptRecord {
            attempts: v.get("attempts").and_then(|x| x.as_i64()).unwrap_or(0),
            next: v.get("next").and_then(|x| x.as_i64()).unwrap_or(0),
        })
}

/// node_id + event_type required (Python base validation); at least one of
/// delay_ms / trigger_timestamp (delay-specific).
fn validate_delay_task(task: &Value) -> bool {
    let Some(obj) = task.as_object() else {
        return false;
    };
    let nonempty = |k: &str| {
        obj.get(k)
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
    };
    if !nonempty("node_id") || !nonempty("event_type") {
        return false;
    }
    obj.contains_key("delay_ms") || obj.contains_key("trigger_timestamp")
}

/// trigger_timestamp wins, else now + delay_ms (a zero delay is due now).
fn due_score_ms(task: &Value, now_ms: i64) -> f64 {
    if let Some(ts) = task.get("trigger_timestamp").and_then(|v| v.as_f64()) {
        if ts > 0.0 {
            return ts;
        }
    }
    let delay = task.get("delay_ms").and_then(|v| v.as_f64()).unwrap_or(0.0);
    (now_ms as f64) + delay
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
    // gethostname without a libc dependency: Linux exposes the kernel
    // hostname in procfs; HOST/HOSTNAME cover macOS shells. The registry row
    // is per-instance anyway (instance_id disambiguates).
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

    struct SharedSink(Arc<RecordingSink>);
    impl EventSink for SharedSink {
        fn publish(&self, channel: &str, payload: &str) -> Result<(), String> {
            self.0.publish(channel, payload)
        }
    }

    /// Test sink: mirrors the production RedisEventSink's storage legs
    /// (record + type index) so assertions can cover the double-write, adds
    /// event recording, and fails by event type (injected trigger failures).
    struct RecordingSink {
        backend: Arc<dyn RedisBackend>,
        cfg: Config,
        events: Mutex<Vec<(String, String)>>,
        fail_types: Mutex<Vec<String>>,
    }

    impl EventSink for RecordingSink {
        fn publish(&self, channel: &str, payload: &str) -> Result<(), String> {
            let ev: Value = serde_json::from_str(payload).map_err(|e| e.to_string())?;
            let et = ev
                .get("event_type")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string();
            if self.fail_types.lock().unwrap().contains(&et) {
                return Err(format!("sink down for {et}"));
            }
            let event_id = ev
                .get("event_id")
                .and_then(|x| x.as_str())
                .unwrap_or_default();
            let ts = ev.get("timestamp").and_then(|x| x.as_f64()).unwrap_or(0.0);
            self.backend
                .set(
                    &keys::event_storage_record_key(&self.cfg, event_id),
                    payload,
                    Some(self.cfg.event_ttl_seconds),
                )
                .map_err(|e| e.to_string())?;
            self.backend
                .zadd(
                    &keys::event_storage_type_index_key(&self.cfg, &et),
                    ts,
                    event_id,
                )
                .map_err(|e| e.to_string())?;
            self.backend
                .expire(
                    &keys::event_storage_type_index_key(&self.cfg, &et),
                    self.cfg.event_ttl_seconds,
                )
                .map_err(|e| e.to_string())?;
            self.events
                .lock()
                .unwrap()
                .push((channel.to_string(), payload.to_string()));
            Ok(())
        }
    }

    struct Fixture {
        wake: Arc<DelayWake>,
        backend: Arc<FakeRedis>,
        sink: Arc<RecordingSink>,
    }

    fn fixture() -> Fixture {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_KEY_PREFIX" => "plaita".to_string(),
            "PLAITA_INSTANCE_ID" => "delay-test".to_string(),
            _ => String::new(),
        });
        let backend = Arc::new(FakeRedis::new());
        let backend_dyn: Arc<dyn RedisBackend> = backend.clone();
        let sink = Arc::new(RecordingSink {
            backend: backend_dyn.clone(),
            events: Mutex::new(Vec::new()),
            fail_types: Mutex::new(Vec::new()),
            cfg: cfg.clone(),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let mut wake = DelayWake::new(cfg, backend_dyn, stop);
        wake.sink = Arc::new(SharedSink(Arc::clone(&sink)));
        Fixture {
            wake: Arc::new(wake),
            backend,
            sink,
        }
    }

    fn task_raw(node: &str, trigger_ms: Option<i64>) -> String {
        let mut task = json!({
            "type": "delay",
            "node_id": node,
            "event_type": "delay_trigger",
            "execution_id": "exec-1",
            "flow_id": "flow-1",
            "tenant_id": "default",
            "delay_ms": 1000i64,
        });
        if let Some(ts) = trigger_ms {
            task["trigger_timestamp"] = json!(ts);
        }
        task.to_string()
    }

    fn scheduled_count(fx: &Fixture) -> usize {
        fx.wake
            .backend
            .zrangebyscore(&fx.wake.scheduled_key, -1e300, 1e300, 1000)
            .unwrap()
            .len()
    }

    #[test]
    fn convey_moves_list_to_scheduled() {
        let fx = fixture();
        let past = task_raw("past", Some(now_ms() - 1000));
        let future = task_raw("future", Some(now_ms() + 60_000));
        fx.wake.backend.rpush(&fx.wake.queue_key, &past).unwrap();
        fx.wake.backend.rpush(&fx.wake.queue_key, &future).unwrap();
        fx.wake.convey_once();
        assert!(fx
            .wake
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
        assert_eq!(scheduled_count(&fx), 2);
        // trigger_timestamp wins over delay_ms for the score
        let due_now = fx
            .wake
            .backend
            .zrangebyscore(
                &fx.wake.scheduled_key,
                (now_ms() - 2000) as f64,
                now_ms() as f64,
                10,
            )
            .unwrap();
        assert_eq!(due_now, vec![past]);
    }

    #[test]
    fn convey_drops_invalid_entries() {
        let fx = fixture();
        fx.wake
            .backend
            .rpush(&fx.wake.queue_key, "{not json")
            .unwrap();
        let no_event = json!({"node_id": "n", "delay_ms": 1}).to_string();
        fx.wake
            .backend
            .rpush(&fx.wake.queue_key, &no_event)
            .unwrap();
        let no_delay = json!({"node_id": "n", "event_type": "delay_trigger"}).to_string();
        fx.wake
            .backend
            .rpush(&fx.wake.queue_key, &no_delay)
            .unwrap();
        let good = task_raw("ok", None);
        fx.wake.backend.rpush(&fx.wake.queue_key, &good).unwrap();
        fx.wake.convey_once();
        assert_eq!(scheduled_count(&fx), 1);
        assert!(fx
            .wake
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn convey_idempotent_on_crash_window() {
        let fx = fixture();
        let raw = task_raw("n", None);
        fx.wake.backend.rpush(&fx.wake.queue_key, &raw).unwrap();
        fx.wake
            .backend
            .zadd(&fx.wake.scheduled_key, now_ms() as f64, &raw)
            .unwrap();
        fx.wake.convey_once();
        assert_eq!(scheduled_count(&fx), 1);
        assert!(fx
            .wake
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn trigger_publishes_resume_event_and_finalizes() {
        let fx = fixture();
        let raw = task_raw("wait", Some(now_ms() - 100));
        fx.wake
            .backend
            .zadd(&fx.wake.scheduled_key, (now_ms() - 100) as f64, &raw)
            .unwrap();
        let mut joins = Vec::new();
        fx.wake.submit_due_once(&mut joins);
        for j in joins {
            j.join().unwrap();
        }
        assert_eq!(scheduled_count(&fx), 0);
        assert!(fx
            .wake
            .backend
            .hget(&fx.wake.attempts_key, &raw)
            .unwrap()
            .is_none());
        let events = fx.sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        let (channel, payload) = &events[0];
        assert_eq!(channel, "plaita:events:delay_trigger");
        let ev: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(ev["correlation_id"], "exec-1");
        assert_eq!(ev["tenant_id"], "default");
        assert_eq!(ev["data"]["trigger_type"], "delay_completed");
        assert!(ev["data"]["planned_trigger_timestamp"].is_i64());
        // §6.2 double-write: storage record + type index (epoch-seconds score)
        let record_key =
            keys::event_storage_record_key(&fx.wake.cfg, ev["event_id"].as_str().unwrap());
        assert!(fx.wake.backend.get(&record_key).unwrap().is_some());
        let type_key = keys::event_storage_type_index_key(&fx.wake.cfg, "delay_trigger");
        let idx = fx
            .wake
            .backend
            .zrangebyscore(&type_key, 0.0, 1e300, 10)
            .unwrap();
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn publish_failure_defers_then_recovers() {
        let fx = fixture();
        fx.sink
            .fail_types
            .lock()
            .unwrap()
            .push("delay_trigger".to_string());
        let raw = task_raw("wait", None);
        fx.wake
            .backend
            .zadd(&fx.wake.scheduled_key, now_ms() as f64, &raw)
            .unwrap();
        fx.wake.process_task(&raw);
        assert_eq!(
            scheduled_count(&fx),
            1,
            "failed publish must stay scheduled"
        );
        let rec = fx
            .wake
            .backend
            .hget(&fx.wake.attempts_key, &raw)
            .unwrap()
            .unwrap();
        let rec: Value = serde_json::from_str(&rec).unwrap();
        assert_eq!(rec["attempts"], 1);
        assert!(rec["next"].as_i64().unwrap() > now_ms());
        // Backoff expired + publish recovered ⇒ confirmed, counter cleared.
        fx.sink.fail_types.lock().unwrap().clear();
        fx.wake
            .backend
            .hset(&fx.wake.attempts_key, &raw, r#"{"attempts":1,"next":0}"#)
            .unwrap();
        fx.wake.process_task(&raw);
        assert_eq!(scheduled_count(&fx), 0);
        assert!(fx
            .wake
            .backend
            .hget(&fx.wake.attempts_key, &raw)
            .unwrap()
            .is_none());
    }

    #[test]
    fn dead_letter_after_max_attempts() {
        let mut fx = fixture();
        // Max attempts shrunk for the test: the fixture owns the only Arc
        // (no threads spawned yet), so Arc::get_mut hands out a plain &mut.
        let wake_mut: &mut DelayWake = Arc::get_mut(&mut fx.wake)
            .expect("fixture owns the only Arc before run() spawns anything");
        wake_mut.max_attempts = 2;
        fx.sink
            .fail_types
            .lock()
            .unwrap()
            .push("delay_trigger".to_string());
        let raw = task_raw("wait", None);
        fx.wake
            .backend
            .zadd(&fx.wake.scheduled_key, now_ms() as f64, &raw)
            .unwrap();
        fx.wake.process_task(&raw); // attempt 1 → backoff
        fx.wake.process_task(&raw); // attempt 2 = max → dead letter
        let dead = fx.wake.backend.lrange(&fx.wake.dead_key, 0, -1).unwrap();
        assert_eq!(dead, vec![raw.clone()]);
        assert_eq!(scheduled_count(&fx), 0);
        assert!(fx
            .wake
            .backend
            .hget(&fx.wake.attempts_key, &raw)
            .unwrap()
            .is_none());
        // service_error broadcast went through (fail list only covers
        // delay_trigger); it runs on a detached thread — poll for it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let events = fx.sink.events.lock().unwrap();
            if events.len() == 1 {
                assert!(events[0].0.ends_with(":events:service_error"));
                assert!(events[0].1.contains("delay_trigger_dead_letter"));
                break;
            }
            drop(events);
            assert!(
                std::time::Instant::now() < deadline,
                "service_error broadcast did not arrive in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn shutdown_keeps_task_parked() {
        let fx = fixture();
        fx.wake.stop.store(true, Ordering::SeqCst);
        let raw = task_raw("wait", None);
        fx.wake
            .backend
            .zadd(&fx.wake.scheduled_key, now_ms() as f64, &raw)
            .unwrap();
        fx.wake.process_task(&raw);
        assert_eq!(scheduled_count(&fx), 1);
        assert!(fx
            .wake
            .backend
            .hget(&fx.wake.attempts_key, &raw)
            .unwrap()
            .is_none());
    }

    #[test]
    fn registry_lifecycle() {
        let fx = fixture();
        fx.wake.registry_beat("running", 2);
        let blob = fx.wake.backend.get(&fx.wake.registry_key).unwrap().unwrap();
        assert!(
            blob.contains("\"service_type\":\"delay\""),
            "record: {blob}"
        );
        assert!(blob.contains("\"status\":\"running\""));
        fx.wake.registry_shutdown();
        assert!(fx
            .wake
            .backend
            .get(&fx.wake.registry_key)
            .unwrap()
            .is_none());
    }
}
