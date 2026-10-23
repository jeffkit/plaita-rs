//! Per-tenant start admission quota (rs-worker-design §9.3 闸序的 [M2] 租户
//! 配额 admit; port of plaita-go `pkg/worker/quota.go`, issue #30, Python
//! plaita server/tenant_quota.py 最小公平性原语). One ZSET semaphore per
//! tenant:
//!
//! - `{prefix}:tenant_quota` — HASH tenant_id → max concurrent starts
//! - `{prefix}:tenant_running:{t}` — ZSET member=execution_id, score=expiry
//!
//! Both are platform mechanism keys under the namespace root (NOT partitioned
//! per tenant) so every worker instance enforces the same view; the shadow
//! prefix keeps test/shadow runs off production keys.
//!
//! Semantics (quota.go 同款):
//!
//! - **DEFAULT IS UNLIMITED**: no configured cap → the gate is bypassed
//!   entirely (no Redis writes). Enforcement requires PLAITA_TENANT_MAX_
//!   RUNNING or a per-tenant HASH entry (Python fail-open matrix: ≤0 /
//!   unparsable / missing / Redis failure = unlimited).
//! - **ADMIT** runs one atomic Lua (purge expired members — crash self-heal;
//!   idempotently refresh an existing member — message redelivery safe;
//!   ZADD + key EXPIRE below the cap; deny when full).
//! - **RELEASE** happens at the single terminal-write choke point
//!   (`Worker::release_quota_slot`): completed/error/cancelled ZREM the
//!   slot; suspended KEEPS it (the execution is still inside its admitted
//!   lifecycle; the resume carrier reuses the slot). A crash between admit
//!   and terminal self-heals via the entry score TTL
//!   (PLAITA_TENANT_RUNNING_TTL_SECONDS, default 86400).
//! - **OVER-LIMIT starts are deferred, never executed, never dead-lettered**:
//!   the carrier is re-enqueued at the stream TAIL (FIFO fairness — other
//!   tenants' earlier messages get priority) with a quota_defers counter and
//!   the original is acked (enqueue→ack; a crash in between only duplicates,
//!   the lease + idempotency gate serialize). After QuotaDeferMax defers the
//!   copy carries quota_bypass=true and is admitted over the cap — liveness
//!   wins over the soft quota. Only START carriers are gated; resume is
//!   rescue traffic and never deferred.
//! - Rollback: PLAITA_DISABLE_TENANT_QUOTA=1 bypasses the whole gate.
//!
//! **粒度差异声明**：go 的闸按租户（tenant）粒度；rs 同为单 Redis 多租户面
//! （cfg.namespace 租户路由），无「仓/flow 粒度」概念需要降级——本实现与 go
//! 逐字段对齐（键面/脚本/处置），无粒度分歧。

use std::sync::Arc;

use serde_json::{json, Value};

use crate::backend::RedisBackend;
use crate::config::{float_epoch, Config};
use crate::error::WorkerError;
use crate::keys;
use crate::queue::Task;
use crate::worker::Worker;

/// TenantQuota owns the per-tenant start semaphore.
pub struct TenantQuota {
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
}

impl TenantQuota {
    pub fn new(backend: Arc<dyn RedisBackend>, cfg: Config) -> TenantQuota {
        TenantQuota { backend, cfg }
    }

    fn quota_key(&self) -> String {
        keys::tenant_quota_key(&self.cfg)
    }

    fn running_key(&self, tenant: &str) -> String {
        keys::tenant_running_key(&self.cfg, tenant)
    }

    fn running_tenant<'a>(&'a self, tenant: &'a str) -> &'a str {
        if tenant.is_empty() {
            &self.cfg.default_tenant_id
        } else {
            tenant
        }
    }

    /// Resolves the tenant's max concurrent starts; `None` = unlimited (no
    /// cap configured / rollback switch / lookup failure — all fail-open).
    /// Priority: per-tenant HASH entry > global env default.
    fn resolve_cap(&self, tenant: &str) -> Option<i64> {
        if self.cfg.disable_tenant_quota {
            return None;
        }
        let raw = match self
            .backend
            .hget(&self.quota_key(), self.running_tenant(tenant))
        {
            Ok(r) => r,
            Err(e) => {
                // A quota-gate outage must not amplify into a global rejection.
                eprintln!(
                    "[quota] tenant quota lookup failed, treating as unlimited \
                     (tenant={}): {e}",
                    self.running_tenant(tenant)
                );
                return None;
            }
        };
        if let Some(raw) = raw {
            return match raw.trim().parse::<i64>() {
                Ok(n) if n > 0 => Some(n),
                // recorded but ≤0/garbage = unlimited (0=全拒 is
                // tenant_status's job, not the worker's)
                _ => None,
            };
        }
        if self.cfg.tenant_max_running > 0 {
            return Some(self.cfg.tenant_max_running);
        }
        None
    }

    /// Takes one concurrent-start slot for the tenant. `allowed=false` means
    /// the tenant's quota is full. Unconfigured/failing gates admit
    /// everything (fail-open), so the default deployment sees zero behavior
    /// change. `running` is the post-admit occupancy for the quota gauge
    /// (None when unlimited).
    pub fn admit_start(&self, tenant: &str, execution_id: &str) -> (bool, Option<i64>) {
        if execution_id.is_empty() {
            // untrackable legacy carrier: nothing to semaphore on
            return (true, None);
        }
        let Some(cap) = self.resolve_cap(tenant) else {
            return (true, None);
        };
        let now = float_epoch();
        let ttl = self.cfg.tenant_running_ttl_secs as f64;
        match self.backend.tenant_quota_admit(
            &self.running_key(tenant),
            now,
            ttl,
            cap,
            execution_id,
        ) {
            Ok((allowed, running)) => (allowed == 1, Some(running)),
            Err(e) => {
                eprintln!(
                    "[quota] tenant start admission failed (fail-open admit) \
                     (tenant={}, execution_id={execution_id}): {e}",
                    self.running_tenant(tenant)
                );
                (true, None)
            }
        }
    }

    /// Returns one start slot (terminal executions; idempotent, best-effort —
    /// the entry TTL is the leak backstop).
    pub fn release(&self, tenant: &str, execution_id: &str) {
        if execution_id.is_empty() {
            return;
        }
        if let Err(e) = self.backend.zrem(&self.running_key(tenant), execution_id) {
            eprintln!(
                "[quota] tenant start slot release failed (entry TTL backstop) \
                 (tenant={}, execution_id={execution_id}): {e}",
                self.running_tenant(tenant)
            );
        }
    }
}

impl Worker {
    /// The terminal-write choke point (§9.3/go ExecutionStore.
    /// SetOnTerminalSave): completed/error/cancelled rows return the slot;
    /// suspended keeps it. Best-effort — a failure never fails the save.
    pub(crate) fn release_quota_slot(&self, tenant: &str, execution_id: &str) {
        self.quota.release(tenant, execution_id);
    }

    /// The over-quota disposition (go deferOverQuotaStart): re-enqueue the
    /// carrier at the stream TAIL with a quota_defers counter, then ack the
    /// original — enqueue→ack order is load-bearing (a crash in between
    /// duplicates; the lease + the idempotency gate serialize, never lose).
    /// Unknown payload fields are preserved byte-for-byte from the raw
    /// envelope when available. `Ok(())` = handled (original acked or
    /// deliberately left); Err = the re-enqueue failed, the original stays
    /// pending for reclaim.
    pub(crate) fn defer_over_quota_start(
        &self,
        task: &Task,
        raw_payload: Option<&str>,
    ) -> Result<(), WorkerError> {
        let mut body: Value = match raw_payload {
            Some(raw) => serde_json::from_str(raw).unwrap_or_else(|_| task.wire_dict()),
            None => task.wire_dict(),
        };
        if !body.is_object() {
            body = task.wire_dict();
        }
        let defers = task.quota_defers + 1;
        body["quota_defers"] = json!(defers);
        if defers > self.cfg.quota_defer_max {
            body["quota_bypass"] = json!(true);
            self.metrics.inc_counter(
                crate::metrics::METRIC_QUOTA_BYPASSED_TOTAL,
                &[("tenant", task.tenant_id.as_str())],
            );
            eprintln!(
                "[quota] tenant start quota still full after the defer budget; admitting with \
                 quota_bypass (liveness over soft quota) (tenant={}, execution_id={}, \
                 defers={})",
                task.tenant_id,
                task.execution_id,
                defers - 1
            );
        } else {
            self.metrics.inc_counter(
                crate::metrics::METRIC_QUOTA_DEFERRED_TOTAL,
                &[("tenant", task.tenant_id.as_str())],
            );
            eprintln!(
                "[quota] tenant start quota full; carrier re-enqueued at the stream tail \
                 (tenant={}, execution_id={}, defers={defers}, max={})",
                task.tenant_id, task.execution_id, self.cfg.quota_defer_max,
            );
        }
        if let Err(e) = self.queue.enqueue(&body, task.envelope_schema_ver) {
            eprintln!(
                "[quota] quota defer re-enqueue failed; original left pending for reclaim \
                 (execution_id={}): {e}",
                task.execution_id
            );
            return Err(WorkerError::transient(format!("quota defer enqueue: {e}")));
        }
        if let Err(e) = self.queue.ack(&task.message_id) {
            eprintln!(
                "[quota] quota defer enqueued but the original ack failed (duplicate \
                 delivery, gate-serialized) (execution_id={}): {e}",
                task.execution_id
            );
        }
        Ok(())
    }
}
