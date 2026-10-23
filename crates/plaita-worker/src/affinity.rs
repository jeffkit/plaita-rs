//! 亲和让渡（§9.2 TaskNotForThisWorker；go `handleForeignTarget`
//! worker.go:2232-2334 + `terminalizeHandoverExhausted` worker.go:2340-2443
//! + 台账 `ledger.go` handover 决策记录的逐分支移植）。
//!
//! 容量路由的载体带 `target_instance_id`（dispatcher 写）；非目标 worker
//! 读到时**永不 DLQ**，走有界让渡协议：
//!
//! 1. `LookupStatus(target)` 探针失败 → fail-closed 原条留 pending（台账在
//!    册——「探针失败」本身要可审计）；
//! 2. 目标不在（无注册键/draining/损坏）→ **剥 target 广播**（A46-02：
//!    死目标不保留，否则每个非目标 worker 都让渡一次直到计数打穿一个本可
//!    运行的任务）；
//! 3. 目标活着 → 计数键 INCR（滑 TTL 7d）后定向转投（enqueue→ack 序）；
//!    **计数是让渡环唯一的终止条件**（新副本 delivery 恒为 1，够不着
//!    max_deliveries）——计数不可用 = 界失效 = 拒绝让渡（fail-closed；
//!    Python `WRONGTYPE` 键污染事故的部署阻断级教训，此处与其余计数面的
//!    「保守 return 1」刻意不一致）；
//! 4. 计数超 `AffinityHandoverMax`(3) → 有界终态 `AffinityHandoverExhausted`
//!    ——「没有宿主肯收」必须可观测，且终态写**绝不裸写**（#53：探活→
//!    Acquire 新 fence 代→fenced save；活租约/挂起执行一概不写，原条留
//!    pending 给真正的主人/决策载体）。
//!
//! 台账（§6.3 handover 决策记录）：每个决策点 XADD 一条到
//! `{prefix}:dispatch:ledger`（payload JSON + schema_version），**台账写
//! 失败从不回滚让渡**（先入队后记账；与 go `recordHandoverLedger` 同序同
//! 语义）。条目字段与 go `LedgerEntry` 同名（worker 侧只写 handover 面：
//! decided_by=instance_id、actor="worker:{instance_id}"；dispatcher 侧的
//! EnqueueWithLedger 面不在 worker 范围）。

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha1::{Digest, Sha1};

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config, DEFAULT_NODE_ABORT_CODE};
use crate::error::WorkerError;
use crate::keys;
use crate::lease;
use crate::queue::Task;
use crate::state::{self, ExecutionState};
use crate::worker::Worker;

/// 每执行让渡次数上界（go config.go:67-69 `AffinityHandoverMax`）。
pub const AFFINITY_HANDOVER_MAX: i64 = 3;

/// 让渡计数滑 TTL（7 天；必须远长于回收周期 claim_min_idle 60s——TTL 短于
/// 回收周期时计数同一任务绕一圈回来前过期，界形同虚设；go
/// counterTTLSeconds 同值）。
pub const HANDOVER_COUNTER_TTL_SECS: i64 = 7 * 86400;

/// 台账条目 schema 版本（go ledgerSchemaVersion）。
pub const LEDGER_SCHEMA_VERSION: i64 = 1;

/// 台账决策值（§6.3 DECISION_TARGETED / DECISION_BROADCAST）。
pub const DECISION_TARGETED: &str = "targeted";
pub const DECISION_BROADCAST: &str = "broadcast";

/// 台账让渡原因（go ledger.go:30-37 逐字同值——混合部署两侧台账可互读）。
pub const REASON_HANDOVER: &str = "TaskNotForThisWorker: handover";
pub const REASON_TARGET_UNAVAILABLE: &str = "TaskNotForThisWorker: target unavailable, broadcast";
pub const REASON_HANDOVER_EXHAUSTED: &str = "TaskNotForThisWorker: handover budget exhausted";
pub const REASON_HANDOVER_UNBOUNDED: &str = "TaskNotForThisWorker: handover counter unavailable";
pub const REASON_HANDOVER_PROBE_FAILURE: &str = "TaskNotForThisWorker: target probe failed";
pub const REASON_HANDOVER_LIVE_HOLDER: &str =
    "TaskNotForThisWorker: live lease holder owns the execution, terminal write refused";

/// §6.3 台账条目（go `LedgerEntry` 同名字段；worker 侧只填 handover 面，
/// 其余字段保持省略——`skip_serializing_if` 对齐 go 的 omitempty）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LedgerEntry {
    pub ledger_schema_version: i64,
    pub decided_at: String,
    pub decided_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub issue_id: String,
    pub queue: String,
    pub decision: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target_instance_id: String,
    pub enqueued: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor: String,
}

/// XADD 一条台账（`payload` + `schema_version` 两字段——与 go
/// `DispatchLedger.Record` 同一 wire 形状）。失败只记日志（台账写失败
/// 从不吞掉已派发的任务），由调用方决定是否继续。
fn record_ledger(backend: &std::sync::Arc<dyn RedisBackend>, cfg: &Config, entry: &LedgerEntry) {
    let blob = match serde_json::to_string(entry) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[affinity] ledger entry marshal failed (not recorded): {e}");
            return;
        }
    };
    if let Err(e) = backend.xadd(
        &keys::dispatch_ledger_key(cfg),
        &[
            ("payload".to_string(), blob),
            (
                "schema_version".to_string(),
                LEDGER_SCHEMA_VERSION.to_string(),
            ),
        ],
    ) {
        eprintln!(
            "[affinity] dispatch ledger write failed (dispatch not rolled back; \
             queue={}, decision={}): {e}",
            entry.queue, entry.decision
        );
    }
}

/// worker 让渡台账行（go `handoverEntry`）：decided_by/actor = 本实例。
fn handover_entry(
    cfg: &Config,
    task: &Task,
    decision: &str,
    reason: &str,
    message_id: &str,
) -> LedgerEntry {
    LedgerEntry {
        ledger_schema_version: LEDGER_SCHEMA_VERSION,
        decided_at: now_iso(),
        decided_by: cfg.instance_id.clone(),
        issue_id: String::new(),
        queue: cfg.queue_name.clone(),
        decision: decision.to_string(),
        reason: reason.to_string(),
        target_instance_id: task.target_instance_id.clone(),
        enqueued: !message_id.is_empty(),
        message_id: message_id.to_string(),
        tenant_id: task.tenant_id.clone(),
        actor: format!("worker:{}", cfg.instance_id),
    }
}

fn record_handover_ledger(w: &Worker, task: &Task, decision: &str, reason: &str, message_id: &str) {
    record_ledger(
        &w.backend,
        &w.cfg,
        &handover_entry(&w.cfg, task, decision, reason, message_id),
    );
}

/// 让渡计数键：execution_id 维度；无 id 时退 body 规范化指纹 sha1 hex
/// （go `handoverKey`——Go marshal map 键序排序与 Python sort_keys 一致，
/// rs 的载荷原文即规范化 JSON，直接指纹）。
fn handover_key(cfg: &Config, task: &Task, payload_raw: &str) -> String {
    if !task.execution_id.is_empty() {
        return keys::affinity_handover_execution_key(cfg, &task.tenant_id, &task.execution_id);
    }
    let mut hasher = Sha1::new();
    hasher.update(payload_raw.as_bytes());
    let digest: Vec<String> = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    keys::affinity_handover_hash_key(cfg, &digest.join(""))
}

/// INCR + 滑 TTL（TxPipeline 原子对；返回 None = 计数不可用——界失效，
/// 调用方拒绝让渡）。
fn record_handover(backend: &std::sync::Arc<dyn RedisBackend>, key: &str) -> Option<i64> {
    let count = backend.incr(key).ok()?;
    let _ = backend.expire(key, HANDOVER_COUNTER_TTL_SECS);
    Some(count)
}

/// TaskNotForThisWorker 入口（process_one 的亲和闸；go worker.go:386-388
/// 同位：drain post-read gate 之后、年龄闸之前）。返回 Ok = 载体已终局
/// （process_one 照常 ack）；Err = 原条留 pending（fail-closed 家族）。
pub fn handle_foreign_target(
    w: &Worker,
    task: &Task,
    payload_raw: &str,
) -> Result<(), WorkerError> {
    let status = w
        .registry
        .lookup_status(&task.target_instance_id)
        .inspect_err(|_| {
            record_handover_ledger(
                w,
                task,
                DECISION_TARGETED,
                REASON_HANDOVER_PROBE_FAILURE,
                "",
            );
        })?;

    // 原载荷优先（信封原文反解保真——未知字段不丢）；损坏退 wire_dict。
    let body: Value = serde_json::from_str::<Value>(payload_raw)
        .ok()
        .filter(|v| v.is_object())
        .unwrap_or_else(|| task.wire_dict());

    // A46-02：目标不在（无注册键/draining/stopping/stopped）→ 剥 target
    // 广播——死目标不保留，否则每个非目标 worker 都让渡一次。
    if status.as_deref() != Some(crate::registry::REG_STATUS_RUNNING) {
        let mut broadcast = body.clone();
        if let Value::Object(map) = &mut broadcast {
            map.remove("target_instance_id");
        }
        let new_id = w
            .queue
            .enqueue(&broadcast, task.envelope_schema_ver)
            .map_err(|e| {
                record_handover_ledger(w, task, DECISION_BROADCAST, REASON_TARGET_UNAVAILABLE, "");
                WorkerError::transient(format!("broadcast handover enqueue: {e}"))
            })?;
        record_handover_ledger(
            w,
            task,
            DECISION_BROADCAST,
            REASON_TARGET_UNAVAILABLE,
            &new_id,
        );
        eprintln!(
            "[affinity] dispatch target unavailable: task re-enqueued as broadcast \
             (target={}, status={:?}, message_id={new_id})",
            task.target_instance_id, status
        );
        return Ok(());
    }

    // 目标活着：计数（唯一的终止条件）→ 超界有界终态 / 定向转投。
    let key = handover_key(&w.cfg, task, payload_raw);
    let Some(count) = record_handover(&w.backend, &key) else {
        // fail-closed：没有界的让渡是无界风暴（旧布尔标记事故的同款洞）。
        record_handover_ledger(w, task, DECISION_TARGETED, REASON_HANDOVER_UNBOUNDED, "");
        eprintln!(
            "[affinity] handover counter unavailable, refusing to hand over \
             (execution_id={}, key={key})",
            task.execution_id
        );
        return Err(WorkerError::transient(
            "affinity handover counter unavailable",
        ));
    };
    if count > AFFINITY_HANDOVER_MAX {
        record_handover_ledger(w, task, DECISION_TARGETED, REASON_HANDOVER_EXHAUSTED, "");
        eprintln!(
            "[affinity] handover budget exhausted (execution_id={}, count={count}, \
             max={AFFINITY_HANDOVER_MAX})",
            task.execution_id
        );
        return terminalize_handover_exhausted(w, task, count);
    }

    let new_id = w
        .queue
        .enqueue(&body, task.envelope_schema_ver)
        .map_err(|e| {
            record_handover_ledger(w, task, DECISION_TARGETED, REASON_HANDOVER, "");
            WorkerError::transient(format!("handover enqueue: {e}"))
        })?;
    record_handover_ledger(w, task, DECISION_TARGETED, REASON_HANDOVER, &new_id);
    eprintln!(
        "[affinity] task handed over to its targeted worker (target={}, count={count}, \
         message_id={new_id})",
        task.target_instance_id
    );
    Ok(())
}

/// 有界终态（go `terminalizeHandoverExhausted`）：让渡环的收敛出口——
/// 「没有宿主肯收」必须落成可观测的执行终态，但终态写绝不裸写（#53）：
/// 活租约的主人拥有终态权（本 worker 拒写、原条留 pending）；挂起执行在
/// 等外部决策（弹跳的决策载体必须保持可投递，永不在此终态化）；其余经
/// Acquire 新 fence 代后 fenced save（探针与落写之间被抢 → CAS 输而非
/// clobber）。
fn terminalize_handover_exhausted(w: &Worker, task: &Task, count: i64) -> Result<(), WorkerError> {
    if task.execution_id.is_empty() {
        return Err(WorkerError::transient(
            "affinity handover counter unavailable (no execution id to terminalize)",
        ));
    }
    let tenant = &task.tenant_id;
    let execution_id = &task.execution_id;
    let state_row: Option<ExecutionState> =
        state::load(w.backend.as_ref(), &w.cfg, tenant, execution_id)?;
    let Some(mut row) = state_row else {
        // 执行已消失：无东西可终态化，照常 ack 止住回收噪声。
        return Ok(());
    };
    if state::is_terminal(&row.status) {
        eprintln!(
            "[affinity] handover exhausted on an already-terminal execution: message \
             acked to stop the reclaim noise (execution_id={execution_id})"
        );
        return Ok(());
    }
    if row.status == state::STATUS_SUSPENDED {
        eprintln!(
            "[affinity] handover exhausted on a suspended execution: never terminalized, \
             message left pending for the decision carrier (execution_id={execution_id})"
        );
        return Err(WorkerError::new(
            crate::error::ErrorKind::LeaseConflict,
            format!("handover exhausted on a suspended execution ({execution_id})"),
        ));
    }
    let holder = lease::get_holder(w.backend.as_ref(), &w.cfg, tenant, execution_id)?;
    if holder.is_some() {
        // 有活租约：主人拥有终态权——拒写留 pending。
        record_handover_ledger(w, task, DECISION_TARGETED, REASON_HANDOVER_LIVE_HOLDER, "");
        eprintln!(
            "[affinity] handover exhausted on a live-leased execution: terminal write refused, \
             message left pending (execution_id={execution_id}, holder_count={count})"
        );
        return Err(WorkerError::new(
            crate::error::ErrorKind::LeaseConflict,
            format!("handover exhausted: live holder owns the execution ({execution_id})"),
        ));
    }
    // 无活租约（探针答案=空）：Acquire 新 fence 代，fenced 终态写（探针与
    // 落写之间的并发持有者把我们变成 CAS 输，不是 clobber）。
    let holder_token = crate::lease::new_holder_token("handover", &w.cfg.instance_id);
    let handle = match lease::acquire(
        w.backend.as_ref(),
        &w.cfg,
        tenant,
        execution_id,
        &holder_token,
        w.cfg.lease_ttl_seconds,
    ) {
        Ok(h) => h,
        Err(e) => {
            record_handover_ledger(w, task, DECISION_TARGETED, REASON_HANDOVER_LIVE_HOLDER, "");
            eprintln!(
                "[affinity] handover exhausted: execution claimed concurrently, terminal \
                 write abandoned (execution_id={execution_id}): {e}"
            );
            return Err(WorkerError::new(
                crate::error::ErrorKind::LeaseConflict,
                format!("handover exhausted: claimed concurrently ({execution_id})"),
            ));
        }
    };
    let now = now_iso();
    row.status = state::STATUS_ERROR.to_string();
    row.end_time = now.clone();
    row.last_update_time = now;
    row.error = Some(json!({
        "message": format!("no worker accepted this task after {count} handovers"),
        "code": DEFAULT_NODE_ABORT_CODE,
        "error_type": "AffinityHandoverExhausted",
        "details": {
            "affinity_handovers": count,
            "affinity_reason": format!(
                "target {} never consumed the carrier",
                task.target_instance_id
            ),
        },
    }));
    if let Err(e) = state::save(w.backend.as_ref(), &w.cfg, tenant, &row, handle.fence_token) {
        let _ = lease::release_with(w.backend.as_ref(), &w.cfg, &handle);
        return Err(WorkerError::transient(format!(
            "handover-exhausted terminal persist: {e}"
        )));
    }
    // #51 同款：终态写后回收 checkpoint（键无 TTL，终态删除是唯一回收点）。
    w.delete_checkpoint(tenant, execution_id);
    let _ = lease::release_with(w.backend.as_ref(), &w.cfg, &handle);
    eprintln!(
        "[affinity] execution terminalized AffinityHandoverExhausted \
         (execution_id={execution_id}, handovers={count})"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;

    fn test_cfg(name: &str) -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                format!("af-{name}")
            } else {
                String::new()
            }
        })
    }

    /// 计数键双形态：有 execution_id 走执行维度（租户命名空间）；无 id 退
    /// 载荷 sha1 指纹（同载荷同键——go handoverKey 双腿对齐）。
    #[test]
    fn handover_key_has_execution_and_hash_legs() {
        let cfg = test_cfg("keys");
        let mut task = Task {
            execution_id: "exec-1".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };
        assert_eq!(
            handover_key(&cfg, &task, "{}"),
            format!("{}:acme:execution:affinityhandover:exec-1", cfg.key_prefix)
        );
        task.execution_id.clear();
        let a = handover_key(&cfg, &task, r#"{"type":"start"}"#);
        let b = handover_key(&cfg, &task, r#"{"type":"start"}"#);
        assert_eq!(a, b, "same payload = same key");
        assert!(a.starts_with(&format!("{}:affinity:handover:", cfg.key_prefix)));
        let c = handover_key(&cfg, &task, r#"{"type":"resume"}"#);
        assert_ne!(a, c, "different payload = different key");
    }

    /// 台账条目 wire 形状：worker 侧字段与 go LedgerEntry 同名，空值省略。
    #[test]
    fn ledger_entry_serializes_with_go_field_names() {
        let cfg = test_cfg("ledger");
        let task = Task {
            target_instance_id: "worker-b".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };
        let entry = handover_entry(&cfg, &task, DECISION_TARGETED, REASON_HANDOVER, "msg-1");
        let v: Value = serde_json::to_value(&entry).unwrap();
        assert_eq!(v["ledger_schema_version"], 1);
        assert_eq!(v["decided_by"], cfg.instance_id);
        assert_eq!(v["queue"], cfg.queue_name);
        assert_eq!(v["decision"], "targeted");
        assert_eq!(v["reason"], REASON_HANDOVER);
        assert_eq!(v["target_instance_id"], "worker-b");
        assert_eq!(v["enqueued"], true);
        assert_eq!(v["message_id"], "msg-1");
        assert_eq!(v["tenant_id"], "acme");
        assert_eq!(v["actor"], format!("worker:{}", cfg.instance_id));
        assert!(v.get("issue_id").is_none(), "omitempty fields stay absent");
    }

    /// 计数键 TTL：INCR 后滑 TTL 7d（键存在即可——fake 无 TTL 读面，
    /// 行为面由 affinity_e2e 的存活断言覆盖）。
    #[test]
    fn counter_uses_the_platform_key_shape() {
        let cfg = test_cfg("ttl");
        let backend: std::sync::Arc<dyn crate::backend::RedisBackend> =
            std::sync::Arc::new(FakeRedis::new());
        let key = keys::affinity_handover_execution_key(&cfg, "", "exec-t");
        assert_eq!(record_handover(&backend, &key), Some(1));
        assert_eq!(record_handover(&backend, &key), Some(2));
    }
}
