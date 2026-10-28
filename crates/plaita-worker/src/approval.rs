//! wake/approval — the approval wake source (rs#19 second installment; port
//! of plaita-go pkg/wake/approval.go, semantics authority Python
//! ApprovalService).
//!
//! Two faces:
//!
//! 1. Intake: consume `{prefix}:approval:queue` (the worker's §5.2 dispatch)
//!    → create the shared record `{prefix}:approval:pending:{approval_id}`
//!    (TTL 7d, JSON shape byte-compatible with Python: approval_id /
//!    task_config / created_time / status:"pending" / approvals:[] /
//!    required_approvers / strategy). A duplicate dispatch whose record is
//!    already on file is acked WITHOUT rebuilding (a rebuild would wipe
//!    decisions already collected).
//! 2. Decisions: `ApprovalWake::submit_decision` — the entry surface a
//!    decision UI (Python console against the same Redis, or any host)
//!    calls. Serialized per approval by a SET NX decision lock (TTL 10s,
//!    compare-and-del release = the execution-lease primitive); the record
//!    update is a read-verify-append-write that concurrent instances would
//!    otherwise clobber. Strategy thresholds any/all/majority over the
//!    required approvers; on completion the `approval_decision` resume event
//!    publishes and the record is DELETED (terminal state lives on in the
//!    execution row).
//!
//! Publish-failure discipline (Python verbatim): the failed decision is NOT
//! persisted — persisting it would wedge the approval (the retry hits
//! 「已经审批过」 with the threshold met but no event ever published). The
//! user retries the same decision; the record re-appends and re-publishes.
//!
//! Record keys are Python-flat (`{prefix}:approval:pending:*`, no tenant
//! segment) — mixed fleets share one approval state space through the same
//! records the Python console reads and writes.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::events::sleep_interruptible;
use crate::keys;
use crate::wake::{env_f64, env_i64, EventSink, RedisEventSink};

pub const SOURCE_APPROVAL: &str = "approval";

const LOCK_TTL_SECS: i64 = 10;
const LOCK_WAIT_SECS: f64 = 2.0;
const LOCK_POLL_MS: u64 = 50;
const RECORD_TTL_SECS: i64 = 7 * 86400;

/// The approval wake source. One instance consumes one
/// `{prefix}:approval:queue`.
pub struct ApprovalWake {
    cfg: Config,
    backend: Arc<dyn RedisBackend>,
    sink: Arc<dyn EventSink>,
    stop: Arc<AtomicBool>,

    queue_key: String,
    record_prefix: String,
    lock_prefix: String,
    registry_key: String,
    start_time: String,
    instance_id: String,

    poll: Duration,
    heartbeat: Duration,
    registry_ttl_secs: i64,
    batch: i64,

    inflight: Mutex<HashSet<String>>,
}

impl ApprovalWake {
    pub fn new(cfg: Config, backend: Arc<dyn RedisBackend>, stop: Arc<AtomicBool>) -> Self {
        let sink: Arc<dyn EventSink> = Arc::new(RedisEventSink {
            backend: Arc::clone(&backend),
            cfg: cfg.clone(),
        });
        Self::with_sink_parts(cfg, backend, sink, stop)
    }

    /// Overrides the event sink (tests inject failures).
    pub fn with_sink(
        cfg: Config,
        backend: Arc<dyn RedisBackend>,
        sink: Arc<dyn EventSink>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self::with_sink_parts(cfg, backend, sink, stop)
    }

    fn with_sink_parts(
        cfg: Config,
        backend: Arc<dyn RedisBackend>,
        sink: Arc<dyn EventSink>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let instance_id = cfg.instance_id.clone();
        Self {
            queue_key: keys::service_queue_key(&cfg, SOURCE_APPROVAL),
            record_prefix: format!("{}:approval:pending:", cfg.key_prefix),
            lock_prefix: format!("{}:approval:lock:", cfg.key_prefix),
            registry_key: format!(
                "{}:registry:{}:{}",
                cfg.key_prefix, SOURCE_APPROVAL, cfg.instance_id
            ),
            start_time: now_iso(),
            poll: Duration::from_secs_f64(env_f64("PLAITA_APPROVAL_POLL_INTERVAL_SECONDS", 0.5)),
            heartbeat: Duration::from_secs_f64(env_f64("PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10.0)),
            registry_ttl_secs: env_i64("PLAITA_REGISTRY_TTL_SECONDS", 30).max(1),
            batch: env_i64("PLAITA_APPROVAL_CONSUME_BATCH", 50),
            cfg,
            backend,
            sink,
            stop,
            instance_id,
            inflight: Mutex::new(HashSet::new()),
        }
    }

    /// Runs until `stop` is set: registry heartbeats + the intake loop.
    pub fn run(self: &Arc<Self>) {
        self.registry_beat("starting", 0);
        let heartbeat_stop = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = {
            let hb = Arc::clone(self);
            let hb_stop = Arc::clone(&heartbeat_stop);
            std::thread::spawn(move || hb.registry_loop(hb_stop))
        };
        eprintln!(
            "[wake-approval] started queue={} instance={}",
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
        eprintln!("[wake-approval] stopped queue={}", self.queue_key);
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
            "service_type": SOURCE_APPROVAL,
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
            eprintln!("[wake-approval] registry beat failed: {e}");
        }
    }

    fn registry_shutdown(&self) {
        let tomb = json!({
            "instance_id": self.instance_id,
            "service_type": SOURCE_APPROVAL,
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

    // ---- intake ----

    fn cycle_once(self: &Arc<Self>, joins: &mut Vec<std::thread::JoinHandle<()>>) {
        let raws = match self.backend.lrange(&self.queue_key, 0, self.batch - 1) {
            Ok(v) => v,
            Err(e) => {
                if !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                    eprintln!("[wake-approval] consume read failed: {e}");
                }
                return;
            }
        };
        for raw in raws {
            let task: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[wake-approval] invalid task JSON (dropped): {e}: {:.200}",
                        raw
                    );
                    if let Err(e) = self.backend.lrem(&self.queue_key, 1, &raw) {
                        eprintln!("[wake-approval] bad-entry LREM failed: {e}");
                    }
                    continue;
                }
            };
            if !validate_task(&task) {
                eprintln!(
                    "[wake-approval] task failed validation (dropped): {:.200}",
                    raw
                );
                if let Err(e) = self.backend.lrem(&self.queue_key, 1, &raw) {
                    eprintln!("[wake-approval] bad-entry LREM failed: {e}");
                }
                continue;
            }
            // Duplicate-dispatch guard: an approval whose record is already
            // on file is acked without rebuilding (a rebuild would wipe the
            // decisions already collected).
            let approval_id = str_field(&task, "approval_id").unwrap_or_default();
            if !approval_id.is_empty() {
                match self.backend.get(&self.record_key(&approval_id)) {
                    Ok(Some(_)) => {
                        eprintln!(
                            "[wake-approval] task already on file (duplicate dispatch, acked): {}",
                            approval_id
                        );
                        let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("[wake-approval] record lookup failed (treating as absent): {e}");
                    }
                }
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
                struct Guard(Arc<ApprovalWake>, String);
                impl Drop for Guard {
                    fn drop(&mut self) {
                        if let Ok(mut m) = self.0.inflight.lock() {
                            m.remove(self.1.as_str());
                        }
                    }
                }
                let _guard = Guard(Arc::clone(&me), raw.clone());
                me.handle_intake(&raw, &task);
            }));
        }
    }

    fn record_key(&self, approval_id: &str) -> String {
        format!("{}{}", self.record_prefix, approval_id)
    }

    /// Intake: create the shared record. On backend failure the entry stays
    /// queued (returns without ack — at-least-once).
    fn handle_intake(&self, raw: &str, task: &Value) {
        if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            return; // shutdown: the entry stays for the next start
        }
        let approval_id = str_field(task, "approval_id").unwrap_or_default();
        if approval_id.is_empty() {
            eprintln!(
                "[wake-approval] task without approval_id (dropped): {:.200}",
                raw
            );
            let _ = self.backend.lrem(&self.queue_key, 1, raw);
            return;
        }
        let approvers = task
            .get("approver_config")
            .and_then(|c| c.get("approvers"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let strategy = str_field(
            task.get("approver_config").unwrap_or(&Value::Null),
            "strategy",
        )
        .unwrap_or_else(|| "any".to_string());
        let record = json!({
            "approval_id": approval_id,
            "task_config": task,
            "created_time": now_ms(),
            "status": "pending",
            "approvals": [],
            "required_approvers": approvers,
            "strategy": strategy,
        });
        if let Err(e) = self.backend.set(
            &self.record_key(&approval_id),
            &record.to_string(),
            Some(RECORD_TTL_SECS),
        ) {
            eprintln!("[wake-approval] record write failed; entry kept for retry: {e}");
            return;
        }
        let _ = self.backend.lrem(&self.queue_key, 1, raw);
        eprintln!(
            "[wake-approval] task created: {} approvers={:?} strategy={}",
            approval_id, approvers, strategy
        );
    }

    // ---- decisions ----

    /// Records one approver's decision and — when the strategy threshold is
    /// met — publishes the `approval_decision` resume event. Returns the
    /// Python submit_approval_decision response shape.
    pub fn submit_decision(
        &self,
        approval_id: &str,
        approver_id: &str,
        decision: &str,
        comments: &str,
    ) -> DecisionResult {
        let lock_key = format!("{}{}", self.lock_prefix, approval_id);
        let holder = format!("{}:{}", now_ms(), Uuid::new_v4());
        let deadline = std::time::Instant::now() + Duration::from_secs_f64(LOCK_WAIT_SECS);
        loop {
            match self.backend.set_nx(&lock_key, &holder, LOCK_TTL_SECS) {
                Ok(true) => break,
                _ => {
                    if std::time::Instant::now() >= deadline {
                        return DecisionResult::error("审批处理中，请稍后重试");
                    }
                    std::thread::sleep(Duration::from_millis(LOCK_POLL_MS));
                }
            }
        }
        // Compare-and-del release = the execution-lease primitive.
        struct LockGuard<'a>(&'a Arc<dyn RedisBackend>, String, String);
        impl Drop for LockGuard<'_> {
            fn drop(&mut self) {
                let _ = self.0.lease_release(&self.1, &self.2);
            }
        }
        let _lock = LockGuard(&self.backend, lock_key, holder);

        let Ok(Some(raw)) = self.backend.get(&self.record_key(approval_id)) else {
            return DecisionResult::error("审批任务不存在");
        };
        let Ok(mut record) = serde_json::from_str::<Value>(&raw) else {
            eprintln!("[wake-approval] record corrupt: {}", approval_id);
            return DecisionResult::error("审批任务不存在");
        };

        // Permission + duplicate checks (shared record ⇒ cross-instance).
        let required = record
            .get("required_approvers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if !required.iter().any(|a| a.as_str() == Some(approver_id)) {
            return DecisionResult::error("无审批权限");
        }
        let mut approvals = record
            .get("approvals")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if approvals
            .iter()
            .any(|a| a.get("approver_id").and_then(|v| v.as_str()) == Some(approver_id))
        {
            return DecisionResult::error("已经审批过");
        }

        approvals.push(json!({
            "approver_id": approver_id,
            "decision": decision,
            "comments": comments,
            "timestamp": now_ms(),
        }));
        record["approvals"] = json!(approvals);

        let final_decision = final_decision(
            str_field(&record, "strategy").unwrap_or_else(|| "any".to_string()),
            &required,
            &approvals,
        );
        if final_decision.is_none() {
            if let Err(e) = self.backend.set(
                &self.record_key(approval_id),
                &record.to_string(),
                Some(RECORD_TTL_SECS),
            ) {
                eprintln!("[wake-approval] record update failed: {e}");
                return DecisionResult::error(&e.to_string());
            }
            eprintln!(
                "[wake-approval] decision recorded (pending threshold): {} count={}",
                approval_id,
                approvals.len()
            );
            return DecisionResult {
                status: "success".to_string(),
                final_decision: String::new(),
                message: "审批已记录，等待其他审批人".to_string(),
            };
        }
        let final_decision = final_decision.unwrap();

        // Threshold met: publish the resume event; the failed decision is
        // NOT persisted (Python verbatim — persisting it wedges the
        // approval: the retry hits 已经审批过 with the threshold met but no
        // event ever published). The user retries the same decision.
        let task_cfg = record.get("task_config").cloned().unwrap_or(json!({}));
        let tenant = str_field(&task_cfg, "tenant_id")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.cfg.default_tenant_id.clone());
        let execution_id = str_field(&task_cfg, "execution_id").unwrap_or_default();
        let event_type = str_field(&task_cfg, "event_type").unwrap_or_default();
        let now_ms = now_ms();
        let ev = json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": event_type,
            "data": {
                "node_id": task_cfg.get("node_id").cloned().unwrap_or(Value::Null),
                "execution_id": execution_id,
                "flow_id": task_cfg.get("flow_id").cloned().unwrap_or(Value::Null),
                "tenant_id": tenant,
                "trigger_type": "approval_completed",
                "approval_id": approval_id,
                "final_decision": final_decision,
                "approval_details": approvals,
                "timestamp": now_ms,
                "success": true,
            },
            "timestamp": now_ms as f64 / 1000.0,
            "correlation_id": execution_id,
            "tenant_id": tenant,
        });
        let channel = keys::business_event_channel(&self.cfg, &tenant, &event_type);
        if let Err(e) = self.sink.publish(&channel, &ev.to_string()) {
            eprintln!(
                "[wake-approval] resume event publish failed; record untouched for retry: {e}"
            );
            return DecisionResult::error(&e);
        }
        // Published: mark terminal in-record (best-effort) and delete —
        // terminal state lives on in the execution row.
        record["status"] = json!(final_decision);
        record["completed_time"] = json!(now_ms);
        let _ = self.backend.del(&[self.record_key(approval_id).as_str()]);
        eprintln!(
            "[wake-approval] completed: {} final_decision={}",
            approval_id, final_decision
        );
        DecisionResult {
            status: "success".to_string(),
            final_decision,
            message: "审批完成".to_string(),
        }
    }
}

/// Python submit_approval_decision's return shape.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionResult {
    pub status: String,
    pub final_decision: String,
    pub message: String,
}

impl DecisionResult {
    fn error(message: &str) -> Self {
        DecisionResult {
            status: "error".to_string(),
            final_decision: String::new(),
            message: message.to_string(),
        }
    }
}

/// any / all / majority over the required approvers; None = threshold not
/// met yet (Python _check_approval_result).
fn final_decision(strategy: String, required: &[Value], approvals: &[Value]) -> Option<String> {
    let mut approve = 0usize;
    let mut reject = 0usize;
    for a in approvals {
        match a.get("decision").and_then(|v| v.as_str()) {
            Some("approve") => approve += 1,
            Some("reject") => reject += 1,
            _ => {}
        }
    }
    let total = required.len();
    match strategy.as_str() {
        "all" => {
            if reject > 0 {
                Some("reject".to_string())
            } else if approve == total {
                Some("approve".to_string())
            } else {
                None
            }
        }
        "majority" => {
            if reject > total / 2 {
                Some("reject".to_string())
            } else if approve > total / 2 {
                Some("approve".to_string())
            } else {
                None
            }
        }
        _ => {
            // "any"
            if approve > 0 {
                Some("approve".to_string())
            } else if reject > 0 {
                Some("reject".to_string())
            } else {
                None
            }
        }
    }
}

/// base fields (node_id/event_type) + approval_config + approver_config.
/// approvers (Python validate_task_config).
fn validate_task(task: &Value) -> bool {
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
    let has = |k: &str| obj.get(k).map(|v| v.is_object()).unwrap_or(false);
    if !has("approval_config") || !has("approver_config") {
        return false;
    }
    obj["approver_config"]
        .get("approvers")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
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
    use std::sync::atomic::Ordering;

    struct RecordingSink {
        backend: Arc<dyn RedisBackend>,
        cfg: Config,
        events: Mutex<Vec<(String, String)>>,
        fail: AtomicBool,
    }

    impl EventSink for RecordingSink {
        fn publish(&self, channel: &str, payload: &str) -> Result<(), String> {
            if self.fail.load(Ordering::SeqCst) {
                return Err("sink down".to_string());
            }
            let ev: Value = serde_json::from_str(payload).map_err(|e| e.to_string())?;
            let event_id = ev["event_id"].as_str().unwrap_or_default().to_string();
            let event_type = ev["event_type"].as_str().unwrap_or_default().to_string();
            let ts = ev["timestamp"].as_f64().unwrap_or(0.0);
            self.backend
                .set(
                    &keys::event_storage_record_key(&self.cfg, &event_id),
                    payload,
                    Some(self.cfg.event_ttl_seconds),
                )
                .map_err(|e| e.to_string())?;
            self.backend
                .zadd(
                    &keys::event_storage_type_index_key(&self.cfg, &event_type),
                    ts,
                    &event_id,
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
        wake: Arc<ApprovalWake>,
        backend: Arc<FakeRedis>,
        sink: Arc<RecordingSink>,
    }

    fn fixture() -> Fixture {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_KEY_PREFIX" => "plaita".to_string(),
            "PLAITA_INSTANCE_ID" => "approval-test".to_string(),
            _ => String::new(),
        });
        let backend = Arc::new(FakeRedis::new());
        let backend_dyn: Arc<dyn RedisBackend> = backend.clone();
        let sink = Arc::new(RecordingSink {
            backend: backend_dyn.clone(),
            events: Mutex::new(Vec::new()),
            fail: AtomicBool::new(false),
            cfg: cfg.clone(),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(ApprovalWake::with_sink(
            cfg,
            backend_dyn,
            sink.clone(),
            stop,
        ));
        Fixture {
            wake,
            backend,
            sink,
        }
    }

    fn task_raw(strategy: &str) -> String {
        json!({
            "type": "approval",
            "node_id": "sign",
            "event_type": "approval_decision",
            "execution_id": "exec-ap",
            "flow_id": "flow-ap",
            "tenant_id": "default",
            "approval_id": "ap-1",
            "approval_config": {"title": "上线审批"},
            "approver_config": {"approvers": ["alice", "bob"], "strategy": strategy},
        })
        .to_string()
    }

    #[test]
    fn intake_creates_record_with_python_shape() {
        let fx = fixture();
        let raw = task_raw("any");
        fx.backend.rpush(&fx.wake.queue_key, &raw).unwrap();
        let mut joins = Vec::new();
        fx.wake.cycle_once(&mut joins);
        for j in joins {
            j.join().unwrap();
        }
        let record = fx
            .backend
            .get(&format!("{}ap-1", fx.wake.record_prefix))
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&record).unwrap();
        assert_eq!(v["status"], "pending");
        assert_eq!(v["strategy"], "any");
        assert_eq!(v["approval_id"], "ap-1");
        assert_eq!(v["required_approvers"], json!(["alice", "bob"]));
        assert_eq!(v["approvals"], json!([]));
        assert!(v["created_time"].is_i64());
        assert!(v["task_config"].is_object());
        assert!(fx
            .backend
            .lrange(&fx.wake.queue_key, 0, -1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn intake_drops_invalid_tasks() {
        let fx = fixture();
        let no_approvers = json!({
            "type": "approval", "node_id": "n", "event_type": "approval_decision",
            "approval_id": "ap-x",
            "approval_config": {}, "approver_config": {"approvers": []},
        })
        .to_string();
        fx.backend.rpush(&fx.wake.queue_key, &no_approvers).unwrap();
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
        assert!(fx
            .backend
            .get(&format!("{}ap-x", fx.wake.record_prefix))
            .unwrap()
            .is_none());
    }

    #[test]
    fn duplicate_dispatch_skips_rebuild() {
        let fx = fixture();
        let existing = json!({
            "approval_id": "ap-1", "status": "pending", "created_time": 1,
            "approvals": [{"approver_id": "alice", "decision": "approve"}],
            "required_approvers": ["alice", "bob"], "strategy": "all",
            "task_config": {"node_id": "sign", "event_type": "approval_decision"},
        });
        fx.backend
            .set(
                &format!("{}ap-1", fx.wake.record_prefix),
                &existing.to_string(),
                Some(1000),
            )
            .unwrap();
        let raw = task_raw("all");
        fx.backend.rpush(&fx.wake.queue_key, &raw).unwrap();
        let mut joins = Vec::new();
        fx.wake.cycle_once(&mut joins);
        for j in joins {
            j.join().unwrap();
        }
        let v: Value = serde_json::from_str(
            &fx.backend
                .get(&format!("{}ap-1", fx.wake.record_prefix))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            v["approvals"][0]["decision"], "approve",
            "rebuild must not wipe decisions"
        );
    }

    #[test]
    fn any_strategy_completes() {
        let fx = fixture();
        let raw = task_raw("any");
        fx.wake
            .handle_intake(&raw, &serde_json::from_str(&raw).unwrap());
        let res = fx.wake.submit_decision("ap-1", "alice", "approve", "lgtm");
        assert_eq!(res.status, "success");
        assert_eq!(res.final_decision, "approve");
        assert!(fx
            .backend
            .get(&format!("{}ap-1", fx.wake.record_prefix))
            .unwrap()
            .is_none());
        let events = fx.sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].0.ends_with(":events:approval_decision"));
        let ev: Value = serde_json::from_str(&events[0].1).unwrap();
        assert_eq!(ev["correlation_id"], "exec-ap");
        assert_eq!(ev["data"]["trigger_type"], "approval_completed");
        assert_eq!(ev["data"]["final_decision"], "approve");
        assert!(ev["data"]["approval_details"].is_array());
    }

    #[test]
    fn all_strategy_needs_all() {
        let fx = fixture();
        let raw = task_raw("all");
        fx.wake
            .handle_intake(&raw, &serde_json::from_str(&raw).unwrap());
        let res = fx.wake.submit_decision("ap-1", "alice", "approve", "");
        assert_eq!(res.status, "success");
        assert!(res.final_decision.is_empty());
        assert!(fx.sink.events.lock().unwrap().is_empty());
        let v: Value = serde_json::from_str(
            &fx.backend
                .get(&format!("{}ap-1", fx.wake.record_prefix))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(v["approvals"].as_array().unwrap().len(), 1);
        let res = fx.wake.submit_decision("ap-1", "bob", "approve", "");
        assert_eq!(res.final_decision, "approve");
        assert_eq!(fx.sink.events.lock().unwrap().len(), 1);
    }

    #[test]
    fn majority_reject_works() {
        let fx = fixture();
        let raw = task_raw("majority");
        fx.wake
            .handle_intake(&raw, &serde_json::from_str(&raw).unwrap());
        fx.wake.submit_decision("ap-1", "alice", "reject", "");
        // majority of 2 = >1 ⇒ one reject is not yet final
        assert!(fx.sink.events.lock().unwrap().is_empty());
        fx.wake.submit_decision("ap-1", "bob", "reject", "");
        let ev = &fx.sink.events.lock().unwrap()[0];
        assert!(ev.1.contains("\"final_decision\":\"reject\""));
    }

    #[test]
    fn permission_duplicate_and_post_completion() {
        let fx = fixture();
        let raw = task_raw("any");
        fx.wake
            .handle_intake(&raw, &serde_json::from_str(&raw).unwrap());
        let res = fx.wake.submit_decision("ap-1", "mallory", "approve", "");
        assert_eq!(res.message, "无审批权限");
        assert_eq!(
            fx.wake
                .submit_decision("ap-1", "alice", "approve", "")
                .status,
            "success"
        );
        let res = fx.wake.submit_decision("ap-1", "alice", "approve", "");
        assert_eq!(
            res.message, "审批任务不存在",
            "record deleted on completion"
        );
    }

    #[test]
    fn publish_failure_leaves_record_untouched() {
        let fx = fixture();
        let raw = task_raw("any");
        fx.wake
            .handle_intake(&raw, &serde_json::from_str(&raw).unwrap());
        fx.sink.fail.store(true, Ordering::SeqCst);
        let res = fx.wake.submit_decision("ap-1", "alice", "approve", "");
        assert_eq!(res.status, "error");
        let v: Value = serde_json::from_str(
            &fx.backend
                .get(&format!("{}ap-1", fx.wake.record_prefix))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            v["approvals"].as_array().unwrap().is_empty(),
            "failed decision must NOT persist (retry would wedge on 已经审批过)"
        );
    }
}
