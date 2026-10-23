//! Node-level retry budgets, backoff and carrier conversion (rs-worker-design
//! §9.1 / §11.2; port of plaita-go `pkg/worker/retry.go` + the guard branch
//! table of `worker.go:2063-2153`).
//!
//! Three Redis counters, all sliding 7-day TTLs — deliberately NOT the
//! message delivery_count (whose reclaim semantics cannot express "retries of
//! THIS node"):
//!
//! - `{ns}:execution:noderetry:{id}:{node}` — per-node budget = max_deliveries
//! - `{ns}:execution:noderetry:{id}` — per-execution fallback when the failed
//!   node id is unknown
//! - `{ns}:execution:noderefetch:{id}` — total ceiling = 100
//! - `{ns}:execution:nofail:{id}` — deterministic-failure ceiling = 12
//!
//! Any successful node advance clears its counter (§9.1: the counters express
//! *consecutive* failures); a fully successful run also clears the two
//! consecutive-failure ceilings (`reset_consecutive`).
//!
//! fail-closed 总则：载体转换的墓碑/状态探针失败一律留 pending（retry.go
//! 「绝不 fail-open 重排」）；死信守卫的探针失败一律 skip。

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config, DEFAULT_NODE_ABORT_CODE};
use crate::error::{terminal_error_state, ErrorKind, WorkerError};
use crate::keys;
use crate::queue::{DeadLetterGuard, Task, REASON_UNSUPPORTED_SCHEMA};
use crate::state::{self, ExecutionState};
use crate::worker::Worker;

/// Fallback total retry ceiling per execution (§9.1 layer 2,
/// NODE_RETRY_TOTAL_MAX=100).
pub const NODE_RETRY_TOTAL_MAX: i64 = 100;

/// Consecutive deterministic-node-failure ceiling (§9.1 layer 3,
/// DETERMINISTIC_FAILURE_MAX=12).
pub const DETERMINISTIC_FAILURE_MAX: i64 = 12;

/// Sliding TTL of every worker-side counter key (§8.3: retry counters live
/// 7 days).
const COUNTER_TTL_SECS: i64 = 7 * 86400;

/// Backoff base override for tests (ms; 0 = protocol default 1000ms). Go uses
/// a package variable for the same reason (retry.go:176).
static NODE_RETRY_BACKOFF_BASE_MS: AtomicU64 = AtomicU64::new(0);

const NODE_RETRY_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(30);

/// Test hook: collapse the retry backoff (protocol default = 1s base).
pub fn set_retry_backoff_base_ms(ms: u64) {
    NODE_RETRY_BACKOFF_BASE_MS.store(ms, Ordering::SeqCst);
}

/// §9.1 backoff: min(30s, 1s * 2^(attempt-1)).
pub fn node_retry_backoff(attempt: i64) -> std::time::Duration {
    let base_ms = {
        let v = NODE_RETRY_BACKOFF_BASE_MS.load(Ordering::SeqCst);
        if v == 0 {
            1000u64
        } else {
            v
        }
    };
    if attempt < 1 || base_ms == 0 {
        return std::time::Duration::ZERO;
    }
    // Shift with saturating u64 math: attempt 64+ would overflow the shift.
    let d_ms = base_ms.saturating_mul(1u64 << (attempt - 1).min(63));
    let d = std::time::Duration::from_millis(d_ms);
    if d.is_zero() || d > NODE_RETRY_BACKOFF_CAP {
        NODE_RETRY_BACKOFF_CAP
    } else {
        d
    }
}

/// The three-counter bookkeeping face (retry.go RetryBudgets).
pub struct RetryBudgets {
    backend: std::sync::Arc<dyn RedisBackend>,
    cfg: Config,
}

impl RetryBudgets {
    pub(crate) fn new(backend: std::sync::Arc<dyn RedisBackend>, cfg: Config) -> RetryBudgets {
        RetryBudgets { backend, cfg }
    }

    /// Test-support constructor (the behavior tests drive the guard/
    /// conversion branches against the shared fake backend directly).
    pub fn with_backend(backend: std::sync::Arc<dyn RedisBackend>, cfg: Config) -> RetryBudgets {
        RetryBudgets { backend, cfg }
    }

    /// INCR + sliding TTL refresh in one transaction (§9.1).
    fn incr_counter(&self, key: &str) -> Result<i64, WorkerError> {
        let n = self
            .backend
            .incr(key)
            .map_err(|e| WorkerError::transient(format!("retry counter increment: {e}")))?;
        let _ = self.backend.expire(key, COUNTER_TTL_SECS);
        Ok(n)
    }

    /// The per-node attempt count including this failure.
    pub(crate) fn record_node_retry(
        &self,
        tenant: &str,
        execution_id: &str,
        node_id: &str,
    ) -> Result<i64, WorkerError> {
        self.incr_counter(&keys::noderetry_key(
            &self.cfg,
            tenant,
            execution_id,
            node_id,
        ))
    }

    /// The fallback total attempt count.
    pub(crate) fn record_total(
        &self,
        tenant: &str,
        execution_id: &str,
    ) -> Result<i64, WorkerError> {
        self.incr_counter(&keys::noderefetch_key(&self.cfg, tenant, execution_id))
    }

    /// The deterministic-failure count (the only convergence signal for
    /// non-retryable node failures, §9.1 layer 3).
    pub(crate) fn record_deterministic(
        &self,
        tenant: &str,
        execution_id: &str,
    ) -> Result<i64, WorkerError> {
        self.incr_counter(&keys::nofail_key(&self.cfg, tenant, execution_id))
    }

    fn read_counter(&self, key: &str) -> i64 {
        match self.backend.get(key) {
            Ok(Some(raw)) => raw.trim().parse().unwrap_or(0),
            _ => 0,
        }
    }

    /// The dead-letter guard's view: the max across the per-node keys and the
    /// execution-level fallback key (§9.2 — the guard cannot know which node
    /// failed; any node at budget means the execution is stuck).
    pub(crate) fn max_node_retry(&self, tenant: &str, execution_id: &str) -> i64 {
        let base = keys::noderetry_key(&self.cfg, tenant, execution_id, "");
        let mut best = self.read_counter(&base);
        match self.backend.scan_match(&format!("{base}:*")) {
            Ok(ks) => {
                for k in ks {
                    let n = self.read_counter(&k);
                    if n > best {
                        best = n;
                    }
                }
            }
            Err(e) => {
                eprintln!("[retry] node retry counter scan failed (using what was read): {e}");
            }
        }
        best
    }

    /// The 100-retry fallback ceiling reached?
    pub(crate) fn total_exhausted(&self, tenant: &str, execution_id: &str) -> bool {
        self.read_counter(&keys::noderefetch_key(&self.cfg, tenant, execution_id))
            >= NODE_RETRY_TOTAL_MAX
    }

    /// Clears the counters of nodes that completed successfully (§9.1: 任一节
    /// 点成功推进即清零). Counter reset failures only warn — the counters keep
    /// accumulating, which degrades to earlier budget exhaustion, never data
    /// damage.
    pub(crate) fn reset_on_progress(&self, tenant: &str, execution_id: &str, succeeded: &[String]) {
        if succeeded.is_empty() {
            return;
        }
        let ks: Vec<String> = succeeded
            .iter()
            .map(|n| keys::noderetry_key(&self.cfg, tenant, execution_id, n))
            .collect();
        let refs: Vec<&str> = ks.iter().map(|s| s.as_str()).collect();
        if let Err(e) = self.backend.del(&refs) {
            eprintln!(
                "[retry] retry counter reset failed (harmless: counters keep accumulating) \
                 (execution_id={execution_id}): {e}"
            );
        }
    }

    /// Clears the fallback total + deterministic-failure counters (a fully
    /// successful run resets the consecutive-failure ceilings — the replayed
    /// start node must NOT clear them on a retry, only a full run does).
    pub(crate) fn reset_consecutive(&self, tenant: &str, execution_id: &str) {
        let ks = [
            keys::noderefetch_key(&self.cfg, tenant, execution_id),
            keys::nofail_key(&self.cfg, tenant, execution_id),
        ];
        let refs: Vec<&str> = ks.iter().map(|s| s.as_str()).collect();
        if let Err(e) = self.backend.del(&refs) {
            eprintln!("[retry] consecutive counter reset failed: {e}");
        }
    }

    /// Deletes every retry counter of an execution — the start of a fresh
    /// execution carrying a recycled id must not inherit the old budgets
    /// (§9.3 fresh-id cleanup).
    pub(crate) fn reset_all(&self, tenant: &str, execution_id: &str) {
        let base = keys::noderetry_key(&self.cfg, tenant, execution_id, "");
        let mut ks = vec![
            base.clone(),
            keys::noderefetch_key(&self.cfg, tenant, execution_id),
            keys::nofail_key(&self.cfg, tenant, execution_id),
        ];
        if let Ok(scan) = self.backend.scan_match(&format!("{base}:*")) {
            ks.extend(scan);
        }
        let refs: Vec<&str> = ks.iter().map(|s| s.as_str()).collect();
        if let Err(e) = self.backend.del(&refs) {
            eprintln!("[retry] retry counter reset failed: {e}");
        }
    }
}

/// §9.1 retryability (retry.go isRetryableNodeFailure ↔ the engine's own
/// `NodeError::is_retryable`): a Node-family failure whose error class is
/// transient and whose code is not the deterministic 1001. Cancel/timeout/
/// suspend errors are separate variants in rs and never reach this arm.
pub(crate) fn retryable_failed_node(err: &plaita_engine::error::FlowError) -> Option<String> {
    if let plaita_engine::error::FlowError::Node { node_id, error, .. } = err {
        if error.is_retryable() {
            return Some(node_id.clone());
        }
    }
    None
}

/// The §8.3 platform-root deletion tombstone probe (retry.go
/// executionTombstoned): the tombstone value is `{tenant: …}`; a missing or
/// empty tenant normalizes to the default tenant, so cross-tenant same ids
/// are NOT treated as deleted. A corrupt tombstone reads as absent — the
/// state row is the authority. Shared by the carrier conversion, the
/// dead-letter guard and the DLQ replay.
pub(crate) fn execution_tombstoned(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
) -> Result<bool, WorkerError> {
    let raw = backend
        .get(&keys::tombstone_key(cfg, execution_id))
        .map_err(|e| WorkerError::transient(format!("tombstone probe: {e}")))?;
    let Some(raw) = raw else {
        return Ok(false);
    };
    let Ok(v) = serde_json::from_str::<Value>(&raw) else {
        return Ok(false);
    };
    let mut tomb_tenant = v
        .get("tenant")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    if tomb_tenant.is_empty() {
        tomb_tenant = cfg.default_tenant_id.clone();
    }
    let carrier_tenant = if tenant.is_empty() {
        cfg.default_tenant_id.as_str()
    } else {
        tenant
    };
    Ok(tomb_tenant == carrier_tenant)
}

/// The worker-judged budget-exhausted terminal (§5.4 fixed error_type +
/// DEFAULT_NODE_ABORT_CODE, retry.go terminalizeNodeRetryExhausted).
pub(crate) fn node_retry_exhausted_error(
    node_id: &str,
    attempt: i64,
    budget: i64,
    total_exhausted: bool,
    total: i64,
    cause: &str,
) -> Value {
    let message = if total_exhausted {
        format!(
            "execution retried {total} times (fallback ceiling {NODE_RETRY_TOTAL_MAX}); \
             node {node_id} keeps failing: {cause}"
        )
    } else {
        format!("node {node_id} failed {attempt}/{budget} times; retry budget exhausted: {cause}")
    };
    let mut v = terminal_error_state(
        "node_retry_budget_exhausted",
        DEFAULT_NODE_ABORT_CODE,
        &message,
        None,
        Some(node_id),
    );
    v["node_retries"] = json!(attempt);
    v
}

impl Worker {
    /// The §8.3 platform-root deletion tombstone probe (shared helper).
    pub(crate) fn execution_tombstoned(
        &self,
        tenant: &str,
        execution_id: &str,
    ) -> Result<bool, WorkerError> {
        execution_tombstoned(self.backend.as_ref(), &self.cfg, tenant, execution_id)
    }

    /// The unchanged start copy (fresh-start semantics; retry.go
    /// startCarrierBody).
    fn start_carrier_body(task: &Task) -> Value {
        let mut body = json!({
            "type": "start",
            "flow_id": task.flow_id,
            "execution_id": task.execution_id,
            "timestamp": now_iso(),
        });
        if !task.tenant_id.is_empty() {
            body["tenant_id"] = json!(task.tenant_id);
        }
        if !task.version.is_empty() {
            body["version"] = json!(task.version);
        }
        if task.params.is_object() && !task.params.as_object().unwrap().is_empty() {
            body["params"] = task.params.clone();
        }
        body
    }

    /// §9.1 载体转换 (retry.go retryCarrierBody, the `_requeue_body_for_retry`
    /// port): before re-enqueueing, a start carrier is checked against the
    /// state row and converted to a resume/continue carrier whenever a row
    /// exists, so the redelivered copy continues the SAME execution.
    ///
    /// Branch table (Python docstring order):
    /// - non-start task → pass through unchanged;
    /// - no execution_id → start copy;
    /// - deletion tombstone → Ok(None): the caller must ack the original and
    ///   NOT re-enqueue (A51-01 — a copy would resurrect the deleted
    ///   execution);
    /// - tombstone/state probe failure → Err: fail closed, the original stays
    ///   pending (绝不 fail-open 重排; #29 — a degraded start copy would be
    ///   idempotently-acked by the start gate against the existing row and
    ///   the retry would be lost);
    /// - no state row (crash before the first persist) → start copy;
    /// - a row exists → resume/continue with params passed through (#122),
    ///   REGARDLESS of flow_id (#52: a mismatched non-empty id is
    ///   poison-acked downstream by the continuation gate; an empty one
    ///   resolves from the stored row). The version is deliberately dropped —
    ///   the resume path resolves the definition from the row's flow_version.
    pub(crate) fn retry_carrier_body(&self, task: &Task) -> Result<Option<Value>, WorkerError> {
        if task.task_type != "start" {
            return Ok(Some(task.wire_dict()));
        }
        if task.execution_id.is_empty() {
            return Ok(Some(Self::start_carrier_body(task)));
        }
        if self.execution_tombstoned(&task.tenant_id, &task.execution_id)? {
            eprintln!(
                "[retry] execution deleted (tombstone), retry re-enqueue dropped: the original \
                 will be acked (execution_id={}, tenant_id={})",
                task.execution_id, task.tenant_id
            );
            return Ok(None);
        }
        let row = state::load(
            self.backend.as_ref(),
            &self.cfg,
            &task.tenant_id,
            &task.execution_id,
        )
        .map_err(|e| {
            eprintln!(
                "[retry] retry carrier state probe failed; original left pending \
                 (execution_id={}): {e}",
                task.execution_id
            );
            e
        })?;
        if row.is_none() {
            return Ok(Some(Self::start_carrier_body(task)));
        }
        let mut body = json!({
            "type": "resume",
            "flow_id": task.flow_id,
            "execution_id": task.execution_id,
            "resume_type": "continue",
            "timestamp": now_iso(),
        });
        if !task.tenant_id.is_empty() {
            body["tenant_id"] = json!(task.tenant_id);
        }
        if task.params.is_object() && !task.params.as_object().unwrap().is_empty() {
            body["params"] = task.params.clone();
        }
        Ok(Some(body))
    }

    /// §9.1's explicit re-enqueue (retry.go requeueRetryableFailure): backoff
    /// (interruptible by D2), enqueue a fresh carrier, then ack the original.
    /// The enqueue→ack order is load-bearing: a crash in between duplicates
    /// (the lease serializes), never loses. `Ok(())` = the original was
    /// acked (or deliberately dropped); Err = leave pending.
    pub(crate) fn requeue_retryable_failure(
        &self,
        task: &Task,
        failed_node: &str,
        attempt: i64,
        budget: i64,
    ) -> Result<(), WorkerError> {
        let delay = node_retry_backoff(attempt);
        if !delay.is_zero() {
            eprintln!(
                "[retry] node retryable failure: backing off before re-enqueue \
                 (execution_id={}, node={failed_node}, attempt={attempt}, budget={budget}, \
                 delay={}ms)",
                task.execution_id,
                delay.as_millis()
            );
            if crate::events::sleep_interruptible(&self.stop, delay) {
                eprintln!(
                    "[retry] retry backoff aborted by drain force-stop; not re-enqueueing, \
                     message left pending (execution_id={}, attempt={attempt})",
                    task.execution_id
                );
                return Err(WorkerError::new(
                    ErrorKind::DrainAborted,
                    "retry backoff aborted by drain force-stop",
                ));
            }
        }
        let body = match self.retry_carrier_body(task)? {
            Some(b) => b,
            None => {
                // Deleted execution (tombstone): ack and drop — a re-enqueued
                // copy would resurrect it (§9.1 / A51-01).
                eprintln!(
                    "[retry] retryable failure on a deleted execution; original acked without a \
                     re-enqueue (execution_id={}, node={failed_node})",
                    task.execution_id
                );
                return self.queue.ack(&task.message_id);
            }
        };
        if let Err(e) = self.queue.enqueue(&body, task.envelope_schema_ver) {
            eprintln!(
                "[retry] retry carrier enqueue failed; original left pending for reclaim \
                 (execution_id={}): {e}",
                task.execution_id
            );
            return Err(WorkerError::transient(format!(
                "retry carrier enqueue: {e}"
            )));
        }
        if let Err(e) = self.queue.ack(&task.message_id) {
            eprintln!(
                "[retry] retry carrier enqueued but the original ack failed (duplicate \
                 delivery, lease-serialized) (execution_id={}): {e}",
                task.execution_id
            );
            return Ok(());
        }
        self.metrics.inc_counter(
            crate::metrics::METRIC_RETRY_TOTAL,
            &[
                ("tenant", task.tenant_id.as_str()),
                ("flow", task.flow_id.as_str()),
                ("node", failed_node),
            ],
        );
        eprintln!(
            "[retry] retry carrier re-enqueued and original acked (execution_id={}, \
             node={failed_node}, attempt={attempt}, budget={budget})",
            task.execution_id
        );
        Ok(())
    }
}

/// The §4.4 guarded dead-letter branch table (go worker.go:2063-2153
/// deadLetterGuard):
///
/// ```text
/// schema rejection                 → allow, never clone (A49-01)
/// no execution_id                  → allow
/// state load failure               → skip (conservative)
/// state missing / terminal         → allow
/// lease probe failure              → skip (conservative)
/// lease active                     → skip (a live worker owns it)
/// deleted execution (tombstone)    → allow, never clone (A51-01)
/// running + retry budget exhausted → terminalize error, then allow
/// non-terminal + lease empty       → re-enqueue a recovery clone, then allow
/// ```
///
/// The clone inherits the SOURCE envelope version (§4.1 / A44-02): a copy
/// written by this build must not masquerade as newer than its source.
pub struct GuardedDeadLetter {
    pub(crate) backend: std::sync::Arc<dyn RedisBackend>,
    pub(crate) cfg: Config,
    pub(crate) retries: std::sync::Arc<RetryBudgets>,
    pub(crate) quota: crate::quota::TenantQuota,
}

impl GuardedDeadLetter {
    /// Test/standalone constructor (Worker::new wires the guard itself).
    pub fn with_backend(
        backend: std::sync::Arc<dyn RedisBackend>,
        cfg: Config,
    ) -> GuardedDeadLetter {
        GuardedDeadLetter {
            retries: std::sync::Arc::new(RetryBudgets::new(backend.clone(), cfg.clone())),
            quota: crate::quota::TenantQuota::new(backend.clone(), cfg.clone()),
            backend,
            cfg,
        }
    }

    /// Terminalizes a stuck RUNNING execution before its dead-letter
    /// (plaita#242: only a running execution is "stuck"; suspended waits for
    /// an external decision and must never be terminalized here). The write
    /// uses fence_gen=0 — safe because the lease was probed EMPTY two lines
    /// up (§6.2: no live holder can race a fenced write past us).
    fn terminalize_stuck_running(&self, tenant: &str, row: &ExecutionState, reason: &str) -> bool {
        let mut row = row.clone();
        let retry_count = self.retries.max_node_retry(tenant, &row.execution_id);
        let total_exhausted = self.retries.total_exhausted(tenant, &row.execution_id);
        if retry_count < self.cfg.max_deliveries && !total_exhausted {
            return true; // budget left: plain dead-letter path
        }
        eprintln!(
            "[guard] dead letter guard: retry budget exhausted, terminalizing before the dead \
             letter (execution_id={}, node_retries={retry_count}, total_exhausted=\
             {total_exhausted})",
            row.execution_id
        );
        let now = now_iso();
        row.status = state::STATUS_ERROR.to_string();
        row.end_time = now.clone();
        row.last_update_time = now;
        row.error = Some(json!({
            "message": format!(
                "node execution kept failing ({retry_count} retries); retry budget exhausted"
            ),
            "node_retries": retry_count,
            "code": DEFAULT_NODE_ABORT_CODE,
            "error_type": "node_retry_budget_exhausted",
            "failed_reason": reason,
        }));
        if let Err(e) = state::save(self.backend.as_ref(), &self.cfg, tenant, &row, 0) {
            eprintln!("[guard] dead letter guard: terminalization failed, skipping: {e}");
            return false;
        }
        // #51: the checkpoint has no TTL — the terminal delete is its only
        // reclamation. This terminal write bypasses persist_error_terminal,
        // so it deletes on its own.
        if let Err(e) =
            crate::checkpoint::delete(self.backend.as_ref(), &self.cfg, tenant, &row.execution_id)
        {
            eprintln!(
                "[guard] dead letter guard: checkpoint cleanup failed (execution_id={}): {e}",
                row.execution_id
            );
        }
        // the terminal write returns the tenant's start slot (quota.rs 收口)
        self.quota.release(tenant, &row.execution_id);
        true
    }
}

impl DeadLetterGuard for GuardedDeadLetter {
    fn allow(&self, envelope: &crate::queue::Envelope, task: &Task, reason: &str) -> bool {
        if reason == REASON_UNSUPPORTED_SCHEMA {
            return true; // A49-01: never clone a schema-rejected payload
        }
        if task.execution_id.is_empty() {
            return true;
        }
        let tenant = task.tenant_id.as_str();
        let row = match state::load(self.backend.as_ref(), &self.cfg, tenant, &task.execution_id) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[guard] dead letter guard: state load failed, skipping: {e}");
                return false;
            }
        };
        let Some(row) = row else {
            return true;
        };
        if state::is_terminal(&row.status) {
            return true;
        }
        match crate::lease::get_holder(self.backend.as_ref(), &self.cfg, tenant, &task.execution_id)
        {
            Err(e) => {
                eprintln!("[guard] dead letter guard: lease probe failed, skipping: {e}");
                return false;
            }
            Ok(Some(_)) => {
                eprintln!(
                    "[guard] dead letter guard: lease active, skipping (execution_id={})",
                    task.execution_id
                );
                return false;
            }
            Ok(None) => {}
        }
        // The state snapshot may predate a deletion; check the tombstone
        // after the lease probe and before cloning/terminalizing (A51-01).
        match execution_tombstoned(self.backend.as_ref(), &self.cfg, tenant, &task.execution_id) {
            Err(e) => {
                eprintln!("[guard] dead letter guard: tombstone check failed, skipping: {e}");
                return false;
            }
            Ok(true) => {
                eprintln!(
                    "[guard] dead letter guard: execution deleted, allowing the dead letter \
                     without a recovery clone (execution_id={})",
                    task.execution_id
                );
                return true;
            }
            Ok(false) => {}
        }
        if row.status == state::STATUS_RUNNING
            && !self.terminalize_stuck_running(tenant, &row, reason)
        {
            return false;
        }
        // Non-terminal + no lease: the only recovery carrier is this message.
        // Re-enqueue a fresh copy (delivery restarts at 1) before allowing the
        // original into the DLQ.
        if envelope.payload_raw.is_empty() {
            eprintln!("[guard] dead letter guard: no raw payload to clone, skipping");
            return false;
        }
        match self
            .backend
            .xadd(
                &keys::queue_key(&self.cfg),
                &[
                    ("payload".to_string(), envelope.payload_raw.clone()),
                    (
                        "schema_version".to_string(),
                        task.envelope_schema_ver.max(1).to_string(),
                    ),
                ],
            )
            .map_err(WorkerError::from)
        {
            Ok(_) => {
                eprintln!(
                    "[guard] dead letter guard: recovery carrier re-enqueued; original may now \
                     dead-letter (execution_id={})",
                    task.execution_id
                );
                true
            }
            Err(e) => {
                eprintln!(
                    "[guard] dead letter guard: recovery clone enqueue failed, skipping: {e}"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        set_retry_backoff_base_ms(0);
        assert_eq!(node_retry_backoff(0), std::time::Duration::ZERO);
        assert_eq!(node_retry_backoff(1), std::time::Duration::from_secs(1));
        assert_eq!(node_retry_backoff(2), std::time::Duration::from_secs(2));
        assert_eq!(node_retry_backoff(5), std::time::Duration::from_secs(16));
        assert_eq!(node_retry_backoff(6), NODE_RETRY_BACKOFF_CAP);
        assert_eq!(node_retry_backoff(40), NODE_RETRY_BACKOFF_CAP);
        set_retry_backoff_base_ms(1);
        assert_eq!(node_retry_backoff(3), std::time::Duration::from_millis(4));
        set_retry_backoff_base_ms(0);
    }
}
