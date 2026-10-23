//! Queue protocol (§4, port of queue.go): one stream, one consumer group per
//! stream in this deployment, DLQ as a plain record stream, reclaim-before-
//! read with a rotating cursor and an active-lease guard.
//!
//! Wire format (§4.1): a stream message carries exactly two fields —
//! `payload` (task body JSON string) + `schema_version` (decimal string).
//! Missing = v1; unparsable = fall back to v1 with a warning; a version
//! ABOVE this build's write version → DLQ (the same `check_schema` on both
//! the first-delivery and the reclaim path).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};

use crate::backend::{PendingEntry, RedisBackend};
use crate::config::{Config, RECLAIM_WINDOW, RECONNECT_BACKOFF, TASK_SCHEMA_VERSION};
use crate::error::{ErrorKind, WorkerError};
use crate::keys;
use crate::metrics::Metrics;

/// Dead-letter reason constants (§4.4; the Go-side canonical names).
pub const REASON_INVALID_PAYLOAD: &str = "invalid_task_payload";
pub const REASON_UNSUPPORTED_SCHEMA: &str = "unsupported_schema_version";
pub const REASON_MAX_DELIVERIES: &str = "max_deliveries_exceeded";
pub const REASON_DEAD_LETTER_GUARD_FIRED: &str = "dead_letter_guard_fired";

/// The parsed task body (§4.1 field table; snake_case field names, no
/// rename_all — wire-compatible with plaita-go `Task`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Task {
    #[serde(rename = "type", default)]
    pub task_type: String,
    #[serde(default)]
    pub flow_id: String,
    #[serde(default)]
    pub execution_id: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub dedup_key: String,
    #[serde(default)]
    pub resume_type: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub target_instance_id: String,
    #[serde(default)]
    pub quota_defers: i64,
    #[serde(default)]
    pub quota_bypass: bool,

    // worker-side metadata (not part of the payload wire format)
    #[serde(skip)]
    pub delivery_count: i64,
    #[serde(skip)]
    pub envelope_schema_ver: i64,
    #[serde(skip)]
    pub message_id: String,
    #[serde(skip)]
    pub suspended_resume_chain: bool,
}

impl Task {
    /// The A47-F2 decision carriers: the only wake-up sources of a suspended
    /// execution — never acked in the lease-conflict window (§5.3).
    pub fn is_decision_carrier(&self) -> bool {
        self.task_type == "resume"
            && matches!(self.resume_type.as_str(), "event" | "timeout" | "cancel")
    }

    /// The wire dict of the task body (DLQ envelope payload; worker metadata
    /// excluded).
    pub fn wire_dict(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Object(serde_json::Map::new()))
    }
}

/// A raw stream message: the two wire fields plus parse metadata.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub message_id: String,
    pub schema_version: i64,
    pub payload_raw: String,
    pub fields: Vec<(String, String)>,
}

/// A delivery pairs an envelope with its parsed task.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub envelope: Envelope,
    pub task: Task,
}

/// Guarded dead-letter callback (§4.4 — the queue-level trait hook; the
/// worker-side guard branch table is M2, M1 leaves the default allow-all).
pub trait DeadLetterGuard: Send + Sync {
    /// Returns true to allow the dead-letter to proceed; false leaves the
    /// message pending.
    fn allow(&self, envelope: &Envelope, task: &Task, reason: &str) -> bool;
}

/// The default guard: everything dead-letters (M1 posture — the active-
/// lease guard inside reclaim already protects live executions).
pub struct AllowAllGuard;

impl DeadLetterGuard for AllowAllGuard {
    fn allow(&self, _envelope: &Envelope, _task: &Task, _reason: &str) -> bool {
        true
    }
}

/// The task-queue face over one backend + config.
pub struct TaskQueue {
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    metrics: Arc<dyn Metrics>,
    /// Rotating reclaim cursor (#196): full window → advance to the last
    /// scanned id; short/empty window → restart from the head.
    reclaim_cursor: Mutex<String>,
    dl_guard: Mutex<Arc<dyn DeadLetterGuard>>,
}

impl TaskQueue {
    pub fn new(
        backend: Arc<dyn RedisBackend>,
        cfg: Config,
        metrics: Arc<dyn Metrics>,
    ) -> TaskQueue {
        TaskQueue {
            backend,
            cfg,
            metrics,
            reclaim_cursor: Mutex::new("0".to_string()),
            dl_guard: Mutex::new(Arc::new(AllowAllGuard)),
        }
    }

    pub fn set_dead_letter_guard(&self, guard: Arc<dyn DeadLetterGuard>) {
        *self.dl_guard.lock().unwrap() = guard;
    }

    pub fn cfg(&self) -> &Config {
        &self.cfg
    }

    pub fn backend(&self) -> &Arc<dyn RedisBackend> {
        &self.backend
    }

    /// XLEN sample (O(1), the queue_depth gauge source). Sampling errors
    /// never break the loop — the caller treats Err as 0.
    pub fn depth(&self) -> i64 {
        self.backend.xlen(&keys::queue_key(&self.cfg)).unwrap_or(0)
    }

    /// Creates the consumer group idempotently (§4.2 step 1: XGROUP CREATE
    /// id=0 MKSTREAM, BUSYGROUP tolerated). Done EVERY round — a fully
    /// drained stream (XDEL everything) loses its group with the key, so it
    /// must be recreated.
    pub fn ensure_group(&self) -> Result<(), WorkerError> {
        match self.backend.xgroup_create_mkstream(
            &keys::queue_key(&self.cfg),
            &self.cfg.consumer_group,
            "0",
        ) {
            Ok(()) => Ok(()),
            Err(crate::backend::BackendError::BusyGroup(_)) => Ok(()),
            Err(e) => Err(WorkerError::transient(format!("ensure group: {e}"))),
        }
    }

    /// One read round (§4.2): reclaim first, then XREADGROUP COUNT 1 BLOCK.
    /// `Ok(None)` when nothing is available — including transient Redis
    /// failures, which warn + back off instead of failing the loop.
    /// Malformed payloads and over-version envelopes are dead-lettered
    /// inside this call.
    pub fn read(&self) -> Result<Option<Delivery>, WorkerError> {
        self.ensure_group()?;
        if let Some((envelope, count)) = self.reclaim_one() {
            return self.delivery_for(envelope, count, true);
        }
        let res = self.backend.xreadgroup(
            &self.cfg.consumer_group,
            &self.cfg.consumer_name,
            &keys::queue_key(&self.cfg),
            1,
            Some(self.cfg.read_block_ms),
        );
        let messages = match res {
            Ok(m) => m,
            Err(e) => {
                // Transient failure family (§4.2): warn + backoff, keep
                // polling — never blow up the loop.
                eprintln!("[queue] read transient failure: {e}");
                std::thread::sleep(RECONNECT_BACKOFF);
                return Ok(None);
            }
        };
        let Some(msg) = messages.into_iter().next() else {
            return Ok(None);
        };
        // First delivery: delivery_count is always 1 (§4.2) — the ceiling
        // gate never applies to it.
        let envelope = Envelope {
            message_id: msg.id,
            schema_version: 1,
            payload_raw: String::new(),
            fields: msg.fields,
        };
        self.delivery_for(envelope, 1, false)
    }

    /// Parses + schema-checks one envelope into a Delivery. `delivery_count`
    /// is the count this round carries (1 for first delivery, the pre-XCLAIM
    /// times_delivered for reclaim); the delivery ceiling gate applies on the
    /// reclaim path only (§4.2).
    fn delivery_for(
        &self,
        mut envelope: Envelope,
        delivery_count: i64,
        reclaim: bool,
    ) -> Result<Option<Delivery>, WorkerError> {
        let task = match self.parse_envelope(&mut envelope) {
            Ok(t) => t,
            Err(perr) => {
                // Malformed payload at the parse point: DLQ + ack in place.
                self.dead_letter_invalid(&envelope, &perr, delivery_count);
                return Ok(None);
            }
        };
        let mut task = task;
        task.delivery_count = delivery_count;
        if self.check_schema(&envelope, &task, delivery_count)? {
            return Ok(None); // already dead-lettered
        }
        if reclaim && self.reclaim_delivery_ceiling(&envelope, &task, delivery_count) {
            return Ok(None); // guarded dead-letter done
        }
        Ok(Some(Delivery { envelope, task }))
    }

    /// Fills envelope schema_version / payload_raw and decodes the payload
    /// into a Task. Missing schema_version = v1; an unparsable schema_version
    /// also falls back to v1 with a warning (§4.1). A payload that is not a
    /// JSON object is ErrInvalidPayload.
    fn parse_envelope(&self, envelope: &mut Envelope) -> Result<Task, WorkerError> {
        let raw = envelope
            .fields
            .iter()
            .find(|(k, _)| k == "payload")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        envelope.payload_raw = raw.clone();
        let sv_raw = envelope
            .fields
            .iter()
            .find(|(k, _)| k == "schema_version")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        if sv_raw.is_empty() {
            envelope.schema_version = 1; // missing = v1
        } else {
            match sv_raw.parse::<i64>() {
                Ok(n) => envelope.schema_version = n,
                Err(_) => {
                    eprintln!("[queue] unparsable schema_version, falling back to v1: {sv_raw:?}");
                    envelope.schema_version = 1;
                }
            }
        }
        if raw.is_empty() {
            return Err(WorkerError::new(
                ErrorKind::InvalidPayload,
                "missing payload field",
            ));
        }
        let body: Value = serde_json::from_str(&raw).map_err(|e| {
            WorkerError::new(
                ErrorKind::InvalidPayload,
                format!("payload is not JSON: {e}"),
            )
        })?;
        if !body.is_object() {
            return Err(WorkerError::new(
                ErrorKind::InvalidPayload,
                "payload is not a JSON object",
            ));
        }
        let mut task: Task = serde_json::from_value(body).map_err(|e| {
            WorkerError::new(ErrorKind::InvalidPayload, format!("payload decode: {e}"))
        })?;
        task.envelope_schema_ver = envelope.schema_version;
        task.message_id = envelope.message_id.clone();
        task.suspended_resume_chain = task.is_decision_carrier();
        Ok(task)
    }

    /// Rejects envelopes above this build's write version into the DLQ
    /// (§4.1 — first-delivery and reclaim paths make the SAME call).
    /// Returns true when the message was dead-lettered.
    fn check_schema(
        &self,
        envelope: &Envelope,
        task: &Task,
        delivery_count: i64,
    ) -> Result<bool, WorkerError> {
        if envelope.schema_version <= TASK_SCHEMA_VERSION {
            return Ok(false);
        }
        self.dead_letter(
            envelope,
            Some(task),
            delivery_count,
            REASON_UNSUPPORTED_SCHEMA,
            None,
        );
        Ok(true)
    }

    /// The reclaim pass (§4.2 step 2): scan the PEL from the rotating
    /// cursor, apply idle threshold + active-lease guard, XCLAIM single
    /// messages. Returns the envelope and its pre-XCLAIM times_delivered.
    fn reclaim_one(&self) -> Option<(Envelope, i64)> {
        let start = {
            let cursor = self.reclaim_cursor.lock().unwrap();
            if cursor.is_empty() || cursor.as_str() == "0" {
                "-".to_string()
            } else {
                // Inclusive boundary scan: a re-scanned boundary message is
                // harmless — the idle threshold gates it (queue.go scanStart).
                cursor.clone()
            }
        };
        let pending = self
            .backend
            .xpending_ext(
                &keys::queue_key(&self.cfg),
                &self.cfg.consumer_group,
                &start,
                "+",
                RECLAIM_WINDOW,
            )
            .unwrap_or_else(|e| {
                if !matches!(e, crate::backend::BackendError::BusyGroup(_)) {
                    eprintln!("[queue] reclaim scan failed: {e}");
                }
                Vec::<PendingEntry>::new()
            });
        // Empty PEL or a failed scan both restart from the head.
        if pending.is_empty() {
            *self.reclaim_cursor.lock().unwrap() = "0".to_string();
            return None;
        }
        if (pending.len() as i64) < RECLAIM_WINDOW {
            // Short window: next round starts over from the head.
            *self.reclaim_cursor.lock().unwrap() = "0".to_string();
        } else {
            *self.reclaim_cursor.lock().unwrap() = pending[pending.len() - 1].id.clone();
        }
        for p in pending {
            if p.idle_ms < self.cfg.claim_min_idle_ms {
                continue;
            }
            if let Some((envelope, count)) = self.reclaim_message(&p) {
                return Some((envelope, count));
            }
        }
        None
    }

    /// Active-lease guard + XCLAIM for one pending message (§4.2 #47):
    /// read the body without side effects and skip when the execution lease
    /// is still held; probe failures conservatively skip. times_delivered is
    /// the count BEFORE the XCLAIM.
    fn reclaim_message(&self, p: &PendingEntry) -> Option<(Envelope, i64)> {
        let msgs = self
            .backend
            .xrange(&keys::queue_key(&self.cfg), &p.id, &p.id, Some(1))
            .unwrap_or_else(|e| {
                eprintln!("[queue] reclaim probe failed, skipping {}: {e}", p.id);
                Vec::new()
            });
        let msg = msgs.into_iter().next()?;
        let mut envelope = Envelope {
            message_id: msg.id,
            schema_version: 1,
            payload_raw: String::new(),
            fields: msg.fields,
        };
        if let Ok(task) = self.parse_envelope(&mut envelope) {
            if !task.execution_id.is_empty() {
                match crate::lease::get_holder(
                    self.backend.as_ref(),
                    &self.cfg,
                    &task.tenant_id,
                    &task.execution_id,
                ) {
                    Err(e) => {
                        eprintln!("[queue] reclaim lease probe failed, skipping {}: {e}", p.id);
                        return None;
                    }
                    Ok(Some(_)) => return None, // still held: not reclaimable yet
                    Ok(None) => {}
                }
            }
        }
        let claimed = self
            .backend
            .xclaim(
                &keys::queue_key(&self.cfg),
                &self.cfg.consumer_group,
                &self.cfg.consumer_name,
                self.cfg.claim_min_idle_ms,
                &[&p.id],
            )
            .unwrap_or_else(|e| {
                eprintln!("[queue] reclaim xclaim failed {}: {e}", p.id);
                Vec::new()
            });
        claimed.into_iter().next().map(|m| {
            (
                Envelope {
                    message_id: m.id,
                    schema_version: 1,
                    payload_raw: String::new(),
                    fields: m.fields,
                },
                p.delivery_count,
            )
        })
    }

    /// The delivery-budget gate (§4.2): only the reclaim path applies it
    /// (first deliveries always process). Returns true when handled.
    fn reclaim_delivery_ceiling(
        &self,
        envelope: &Envelope,
        task: &Task,
        delivery_count: i64,
    ) -> bool {
        if delivery_count < self.cfg.max_deliveries {
            return false;
        }
        self.dead_letter_guarded(envelope, task, delivery_count, REASON_MAX_DELIVERIES);
        true
    }

    /// XACKs the message then best-effort XDELs the entry (§4.3: every
    /// stream in this deployment carries exactly one group; a failed XDEL
    /// never breaks the ack semantics). An ack matching 0 entries warns.
    pub fn ack(&self, message_id: &str) -> Result<(), WorkerError> {
        let n = self
            .backend
            .xack(
                &keys::queue_key(&self.cfg),
                &self.cfg.consumer_group,
                &[message_id],
            )
            .map_err(|e| WorkerError::transient(format!("ack: {e}")))?;
        if n == 0 {
            eprintln!("[queue] ack matched no pending message: {message_id}");
        }
        let _ = self
            .backend
            .xdel(&keys::queue_key(&self.cfg), &[message_id]);
        Ok(())
    }

    /// Writes the DLQ envelope (§4.4): XADD → XTRIM (exact) → ack. The task
    /// dict is embedded when available (the parsed raw payload value — every
    /// wire field survives, unknown fields included), otherwise the raw
    /// payload string. Malformed messages additionally carry raw_fields
    /// (values truncated to 500 chars) + truncated.
    pub fn dead_letter(
        &self,
        envelope: &Envelope,
        task: Option<&Task>,
        delivery_count: i64,
        reason: &str,
        raw_fields: Option<Vec<(String, String)>>,
    ) {
        self.metrics.inc_counter(
            crate::metrics::METRIC_DLQ_TOTAL,
            &[("reason", reason), ("queue", &self.cfg.queue_name.clone())],
        );
        let mut entry = serde_json::json!({
            "reason": reason,
            "source_stream": self.cfg.queue_name,
            "source_id": envelope.message_id,
            "delivery_count": delivery_count,
            "dead_lettered_at": crate::config::float_epoch(),
            // A44: the audit/replay side reads the SOURCE envelope version,
            // never this build's write version.
            "schema_version": envelope.schema_version.max(1),
        });
        match task {
            Some(t) => entry["payload"] = t.wire_dict(),
            None => entry["payload"] = Value::String(envelope.payload_raw.clone()),
        }
        if let Some(fields) = raw_fields {
            let (map, truncated) = truncated_raw_fields(&fields);
            entry["raw_fields"] = map;
            entry["truncated"] = Value::Bool(truncated);
        }
        let source_schema = envelope.schema_version.max(1);
        if let Err(e) = self.backend.xadd(
            &keys::dlq_key(&self.cfg),
            &[
                ("payload".to_string(), entry.to_string()),
                ("schema_version".to_string(), source_schema.to_string()),
            ],
        ) {
            eprintln!("[queue] dead letter XADD failed (reason={reason}): {e}");
            return;
        }
        if let Err(e) = self
            .backend
            .xtrim_maxlen_exact(&keys::dlq_key(&self.cfg), self.cfg.dlq_max_len)
        {
            eprintln!("[queue] dead letter XTRIM failed: {e}");
        }
        if let Err(e) = self.ack(&envelope.message_id) {
            // §4.4: an ack failure after a successful DLQ write is a named,
            // loud failure — the message stays pending and will dead-letter
            // again rather than vanish.
            eprintln!(
                "[queue] dead letter ACK failed (id={}, reason={reason}): {e}",
                envelope.message_id
            );
        }
    }

    /// Malformed payloads: DLQ envelope carries the truncated raw fields +
    /// truncated flag (§4.4).
    fn dead_letter_invalid(&self, envelope: &Envelope, perr: &WorkerError, delivery_count: i64) {
        eprintln!(
            "[queue] invalid task payload dead-lettered (id={}): {perr}",
            envelope.message_id
        );
        self.dead_letter(
            envelope,
            None,
            delivery_count,
            REASON_INVALID_PAYLOAD,
            Some(envelope.fields.clone()),
        );
    }

    /// Runs the dead-letter guard first (§4.4); a skipped guard leaves the
    /// message pending.
    fn dead_letter_guarded(
        &self,
        envelope: &Envelope,
        task: &Task,
        delivery_count: i64,
        reason: &str,
    ) {
        let guard = self.dl_guard.lock().unwrap().clone();
        if !guard.allow(envelope, task, reason) {
            eprintln!(
                "[queue] dead letter guard skipped dead-lettering (id={}, reason={reason})",
                envelope.message_id
            );
            return;
        }
        self.dead_letter(envelope, Some(task), delivery_count, reason, None);
    }

    /// Appends a task body to the main queue stream (worker-as-producer
    /// paths: normalized continue, recovery clone, event/timeout carriers).
    /// The envelope schema version is INHERITED from the source carrier
    /// (§4.1 / A44-02): a copy written by this build must never masquerade
    /// as a newer schema than the message it reproduces.
    pub fn enqueue(&self, body: &Value, schema_version: i64) -> Result<String, WorkerError> {
        self.enqueue_raw(&body.to_string(), schema_version)
    }

    /// The raw-payload form of [`TaskQueue::enqueue`] — the dead-letter
    /// guard's recovery clone and the quota defer preserve unknown payload
    /// fields byte-for-byte (worker.go EnqueueRaw / deferOverQuotaStart).
    pub fn enqueue_raw(&self, payload: &str, schema_version: i64) -> Result<String, WorkerError> {
        let sv = if schema_version <= 0 {
            TASK_SCHEMA_VERSION
        } else {
            schema_version
        };
        self.backend
            .xadd(
                &keys::queue_key(&self.cfg),
                &[
                    ("payload".to_string(), payload.to_string()),
                    ("schema_version".to_string(), sv.to_string()),
                ],
            )
            .map_err(|e| WorkerError::transient(format!("enqueue: {e}")))
    }

    /// Deletes entries with id <= group last-delivered-id that are not in
    /// the PEL (§4.3 fallback sweep; bounded scan: an oversized PEL skips the
    /// round rather than risk deleting a pending message off a partial set).
    pub fn sweep_acked_residue(&self) -> Result<(), WorkerError> {
        let queue = keys::queue_key(&self.cfg);
        let groups = self
            .backend
            .xinfo_groups(&queue)
            .map_err(|e| WorkerError::transient(format!("sweep groups: {e}")))?;
        let last_delivered = groups
            .iter()
            .find(|g| g.name == self.cfg.consumer_group)
            .map(|g| g.last_delivered_id.clone())
            .unwrap_or_default();
        if last_delivered.is_empty() || last_delivered == "0" || last_delivered == "0-0" {
            return Ok(());
        }
        let pel = self
            .backend
            .xpending_ext(&queue, &self.cfg.consumer_group, "-", "+", 1000)
            .map_err(|e| WorkerError::transient(format!("sweep pel: {e}")))?;
        if pel.len() >= 1000 {
            eprintln!(
                "[queue] sweep skipped: PEL too large for bounded scan (size={})",
                pel.len()
            );
            return Ok(());
        }
        let pel_ids: std::collections::HashSet<&str> = pel.iter().map(|p| p.id.as_str()).collect();
        let msgs = self
            .backend
            .xrange(&queue, "-", &last_delivered, None)
            .map_err(|e| WorkerError::transient(format!("sweep range: {e}")))?;
        let residue: Vec<&str> = msgs
            .iter()
            .map(|m| m.id.as_str())
            .filter(|id| !pel_ids.contains(id))
            .collect();
        if !residue.is_empty() {
            self.backend
                .xdel(&queue, &residue)
                .map_err(|e| WorkerError::transient(format!("sweep xdel: {e}")))?;
        }
        Ok(())
    }
}

/// Truncates every stream field value to 500 chars and reports whether
/// truncation happened (§4.4).
fn truncated_raw_fields(fields: &[(String, String)]) -> (Value, bool) {
    const MAX: usize = 500;
    let mut map = serde_json::Map::new();
    let mut truncated = false;
    for (k, v) in fields {
        if v.len() > MAX {
            let mut cut = MAX;
            while !v.is_char_boundary(cut) {
                cut -= 1;
            }
            map.insert(k.clone(), Value::String(v[..cut].to_string()));
            truncated = true;
            continue;
        }
        map.insert(k.clone(), Value::String(v.clone()));
    }
    map.insert("truncated".to_string(), Value::Bool(truncated));
    (Value::Object(map), truncated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;
    use crate::metrics::{NoopMetrics, RecordingMetrics};
    use serde_json::json;
    use std::sync::Arc as StdArc;

    fn cfg(extra: &dyn Fn(&str) -> String) -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                return "q-test".to_string();
            }
            extra(k)
        })
    }

    fn queue_with(cfg: Config) -> (StdArc<FakeRedis>, TaskQueue) {
        let backend: StdArc<FakeRedis> = StdArc::new(FakeRedis::new());
        let q = TaskQueue::new(
            backend.clone() as StdArc<dyn RedisBackend>,
            cfg.clone(),
            StdArc::new(NoopMetrics),
        );
        (backend, q)
    }

    fn seed_stream(backend: &FakeRedis, cfg: &Config, payload: &Value, sv: Option<&str>) -> String {
        let mut fields = vec![("payload".to_string(), payload.to_string())];
        if let Some(sv) = sv {
            fields.push(("schema_version".to_string(), sv.to_string()));
        }
        backend.xadd(&keys::queue_key(cfg), &fields).unwrap()
    }

    #[test]
    fn wire_format_and_first_delivery() {
        let cfg = cfg(&|_| String::new());
        let (backend, q) = queue_with(cfg.clone());
        // missing schema_version = v1
        let body = json!({"type": "start", "flow_id": "f", "execution_id": "e1"});
        seed_stream(&backend, &cfg, &body, None);
        let d = q.read().unwrap().expect("delivery");
        assert_eq!(d.task.task_type, "start");
        assert_eq!(d.envelope.schema_version, 1);
        assert_eq!(d.task.delivery_count, 1);
        assert!(!d.task.suspended_resume_chain);
        q.ack(&d.envelope.message_id).unwrap();
        // the best-effort XDEL removed the stream entry
        assert_eq!(backend.xlen(&keys::queue_key(&cfg)).unwrap(), 0);
    }

    #[test]
    fn decision_carrier_flag() {
        let mut t = Task {
            task_type: "resume".into(),
            resume_type: "event".into(),
            ..Default::default()
        };
        assert!(t.is_decision_carrier());
        t.resume_type = "timeout".into();
        assert!(t.is_decision_carrier());
        t.resume_type = "cancel".into();
        assert!(t.is_decision_carrier());
        t.resume_type = "continue".into();
        assert!(!t.is_decision_carrier());
        t.task_type = "start".into();
        t.resume_type = "event".into();
        assert!(!t.is_decision_carrier());
    }

    #[test]
    fn malformed_payload_and_overschema_dead_letter() {
        let metrics: StdArc<RecordingMetrics> = StdArc::new(RecordingMetrics::default());
        let cfg = cfg(&|_| String::new());
        let backend = StdArc::new(FakeRedis::new());
        let q = TaskQueue::new(
            backend.clone() as StdArc<dyn RedisBackend>,
            cfg.clone(),
            metrics.clone() as StdArc<dyn Metrics>,
        );
        // malformed: payload not an object
        backend
            .xadd(
                &keys::queue_key(&cfg),
                &[("payload".to_string(), "[1,2]".to_string())],
            )
            .unwrap();
        assert!(q.read().unwrap().is_none());
        // overschema
        seed_stream(&backend, &cfg, &json!({"type": "start"}), Some("2"));
        assert!(q.read().unwrap().is_none());
        // missing payload field
        backend.xadd(&keys::queue_key(&cfg), &[]).unwrap();
        assert!(q.read().unwrap().is_none());
        let dlq = backend
            .xrange(&keys::dlq_key(&cfg), "-", "+", None)
            .unwrap();
        assert_eq!(dlq.len(), 3);
        let first: Value = serde_json::from_str(dlq[0].field("payload").unwrap()).unwrap();
        assert_eq!(first["reason"], REASON_INVALID_PAYLOAD);
        assert_eq!(first["schema_version"], 1);
        assert_eq!(first["raw_fields"]["truncated"], Value::Bool(false));
        assert!(first["raw_fields"]["payload"].is_string());
        let second: Value = serde_json::from_str(dlq[1].field("payload").unwrap()).unwrap();
        assert_eq!(second["reason"], REASON_UNSUPPORTED_SCHEMA);
        // the source envelope version is preserved (A44)
        assert_eq!(second["schema_version"], 2);
        // all three acked + removed
        assert_eq!(backend.xlen(&keys::queue_key(&cfg)).unwrap(), 0);
        assert_eq!(
            metrics.counter_of(
                crate::metrics::METRIC_DLQ_TOTAL,
                &[
                    ("reason", REASON_UNSUPPORTED_SCHEMA),
                    ("queue", "q-test:flow:queue")
                ]
            ),
            1
        );
    }

    #[test]
    fn enqueue_inherits_source_schema() {
        let cfg = cfg(&|_| String::new());
        let (_backend, q) = queue_with(cfg.clone());
        let body = json!({"type": "resume", "resume_type": "continue"});
        let id = q.enqueue(&body, 3).unwrap();
        let entry = q
            .backend()
            .xrange(&keys::queue_key(&cfg), &id, &id, Some(1))
            .unwrap()
            .pop()
            .unwrap();
        // A44-02: the copy must NOT masquerade as a newer schema than its
        // source — it carries the SOURCE version.
        assert_eq!(entry.field("schema_version"), Some("3"));
        // sv <= 0 normalizes to the build's write version
        let id2 = q.enqueue(&body, 0).unwrap();
        let entry2 = q
            .backend()
            .xrange(&keys::queue_key(&cfg), &id2, &id2, Some(1))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(entry2.field("schema_version"), Some("1"));
    }

    #[test]
    fn reclaim_redelivers_aged_pending_and_respects_lease_guard() {
        let cfg = cfg(&|k| {
            if k == "PLAITA_CLAIM_MIN_IDLE_MS" {
                "0".to_string() // reclaim everything immediately (test speed)
            } else {
                String::new()
            }
        });
        let (backend, q) = queue_with(cfg.clone());
        // (1) held-lease guard: a pending resume carrier for a leased
        // execution is skipped (never reclaimed while held).
        let holder =
            crate::lease::acquire(backend.as_ref(), &cfg, "", "held", "start:i:aa", 120).unwrap();
        let body = json!({"type": "resume", "flow_id": "f", "execution_id": "held", "resume_type": "event"});
        seed_stream(&backend, &cfg, &body, Some("1"));
        let d1 = q.read().unwrap().unwrap();
        assert_eq!(d1.task.execution_id, "held");
        assert_eq!(d1.task.delivery_count, 1);
        // no ack — the message stays pending; reclaim sees the held lease
        assert!(q.read().unwrap().is_none());
        // (2) after release the aged entry is reclaimed carrying the
        // PRE-XCLAIM times_delivered (§4.2)…
        crate::lease::release_with(backend.as_ref(), &cfg, &holder).unwrap();
        let d2 = q.read().unwrap().unwrap();
        assert_eq!(d2.envelope.message_id, d1.envelope.message_id);
        assert_eq!(
            d2.task.delivery_count, 1,
            "reclaim carries pre-XCLAIM times_delivered"
        );
        q.ack(&d2.envelope.message_id).unwrap();
    }

    #[test]
    fn reclaim_delivery_ceiling_dead_letters() {
        let cfg = cfg(&|k| match k {
            "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
            "PLAITA_MAX_DELIVERIES" => "2".to_string(),
            _ => String::new(),
        });
        let (backend, q) = queue_with(cfg.clone());
        let body = json!({"type": "start", "flow_id": "f", "execution_id": "ex", "params": {}});
        seed_stream(&backend, &cfg, &body, Some("1"));
        let _d1 = q.read().unwrap().unwrap(); // first delivery — ceiling never applies
        let d2 = q.read().unwrap().unwrap(); // reclaim: pre-claim count 1 < 2 → deliver
        assert_eq!(d2.task.delivery_count, 1);
        // the next reclaim sees times_delivered 2 >= max_deliveries 2 →
        // guarded dead-letter, no delivery
        assert!(q.read().unwrap().is_none());
        let dlq = backend
            .xrange(&keys::dlq_key(&cfg), "-", "+", None)
            .unwrap();
        assert_eq!(dlq.len(), 1);
        let entry: Value = serde_json::from_str(dlq[0].field("payload").unwrap()).unwrap();
        assert_eq!(entry["reason"], REASON_MAX_DELIVERIES);
        assert_eq!(entry["delivery_count"], 2);
        assert_eq!(entry["payload"]["execution_id"], "ex");
        // the original message was acked + removed
        assert_eq!(backend.xlen(&keys::queue_key(&cfg)).unwrap(), 0);
        assert_eq!(
            backend
                .xpending_ext(
                    &keys::queue_key(&cfg),
                    &q.cfg().consumer_group,
                    "-",
                    "+",
                    10
                )
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn sweep_acked_residue_removes_only_non_pel_entries() {
        let cfg = cfg(&|_| String::new());
        let (backend, q) = queue_with(cfg.clone());
        // consume two messages: ack one, leave one pending
        seed_stream(&backend, &cfg, &json!({"n": 1}), None);
        seed_stream(&backend, &cfg, &json!({"n": 2}), None);
        let d1 = q.read().unwrap().unwrap();
        let d2 = q.read().unwrap().unwrap();
        q.ack(&d1.envelope.message_id).unwrap();
        q.sweep_acked_residue().unwrap();
        // the acked one was already XDEL'd; the pending one survives
        let left = backend
            .xrange(&keys::queue_key(&cfg), "-", "+", None)
            .unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, d2.envelope.message_id);
        // true residue: an entry acked (XACK) but NOT XDEL'd sits below
        // last-delivered-id outside the PEL — the sweep removes it while the
        // pending survivor survives
        let residue_id = d1.envelope.message_id.clone();
        // re-create the entry under its original id (the queue ack XDEL'd
        // it); the fake's test hook writes it back byte-for-byte
        backend
            .xadd_with_id(
                &keys::queue_key(&cfg),
                &residue_id,
                &[("payload".to_string(), "ghost".to_string())],
            )
            .unwrap();
        q.sweep_acked_residue().unwrap();
        let left = backend
            .xrange(&keys::queue_key(&cfg), "-", "+", None)
            .unwrap();
        assert_eq!(left.len(), 1, "residue swept, pending survivor kept");
        assert_eq!(left[0].id, d2.envelope.message_id);
    }
}
