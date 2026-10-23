//! The Redis key space (rs-worker-design §3, byte-compatible with
//! plaita-go): every key derivation lives here so the real backend, the
//! in-memory fake and the tests agree by construction.
//!
//! `{ns}` = `cfg.namespace(tenant)` (default/empty tenant = `KeyPrefix`,
//! otherwise `KeyPrefix:{tenant}`); `{prefix}` = `cfg.key_prefix` (the plain,
//! un-tenant-routed root — the timeout index / dedup keys / service queues
//! are namespace-wide, exactly as plaita-go derives them).

use crate::config::Config;

/// `{prefix}:registry:flow_worker:{instance_id}` — worker registry entry
/// (ServiceInfo JSON; SETEX TTL 30s, heartbeat 10s — §8.1, plaita-go
/// registry.go 同形)。`{prefix}` 派生（namespace-wide 服务面）。
pub fn registry_worker_key(cfg: &Config) -> String {
    registry_worker_key_of(cfg, &cfg.instance_id)
}

/// 同上，但实例 id 由调用方给出（LookupStatus 查询面读他实例的注册键）。
pub fn registry_worker_key_of(cfg: &Config, instance_id: &str) -> String {
    format!("{}:registry:flow_worker:{}", cfg.key_prefix, instance_id)
}

/// `{prefix}:flow:queue` — task main queue (stream).
pub fn queue_key(cfg: &Config) -> String {
    cfg.queue_name.clone()
}

/// `{prefix}:flow:queue:dlq` — dead-letter stream.
pub fn dlq_key(cfg: &Config) -> String {
    cfg.dlq_key.clone()
}

/// `{ns}:execution:{id}` — execution state row (string JSON).
pub fn execution_state_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!("{}:execution:{}", cfg.namespace(tenant), execution_id)
}

/// `{ns}:execution:index` — execution list (ZSET, score = start epoch).
pub fn execution_index_key(cfg: &Config, tenant: &str) -> String {
    format!("{}:execution:index", cfg.namespace(tenant))
}

/// `{ns}:execution:lease:{id}` — lease (`{holder}:{gen}`, TTL = lease ttl).
pub fn lease_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!("{}:execution:lease:{}", cfg.namespace(tenant), execution_id)
}

/// `{ns}:execution:fence:{id}` — current fence generation (TTL 7d).
pub fn fence_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!("{}:execution:fence:{}", cfg.namespace(tenant), execution_id)
}

/// `{ns}:execution:fencegen:{id}` — generation counter (never deleted, no
/// TTL — the ABA anchor).
pub fn fencegen_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!(
        "{}:execution:fencegen:{}",
        cfg.namespace(tenant),
        execution_id
    )
}

/// `{ns}:execution:noderetry:{id}[:{node}]` — per-node retry budget counter
/// (sliding TTL 7d; without the node segment = execution-level fallback,
/// §9.1 / retry.go:15-27).
pub fn noderetry_key(cfg: &Config, tenant: &str, execution_id: &str, node_id: &str) -> String {
    let base = format!(
        "{}:execution:noderetry:{}",
        cfg.namespace(tenant),
        execution_id
    );
    if node_id.is_empty() {
        base
    } else {
        format!("{base}:{node_id}")
    }
}

/// `{ns}:execution:noderefetch:{id}` — total retry ceiling counter (100).
pub fn noderefetch_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!(
        "{}:execution:noderefetch:{}",
        cfg.namespace(tenant),
        execution_id
    )
}

/// `{ns}:execution:nofail:{id}` — deterministic-failure ceiling counter (12).
pub fn nofail_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!(
        "{}:execution:nofail:{}",
        cfg.namespace(tenant),
        execution_id
    )
}

/// `{prefix}:tenant_quota` — HASH tenant_id → max concurrent starts (platform
/// mechanism key, NOT tenant-routed: every worker enforces the same view;
/// §3 键表 / quota.go:17-22).
pub fn tenant_quota_key(cfg: &Config) -> String {
    format!("{}:tenant_quota", cfg.key_prefix)
}

/// `{prefix}:tenant_running:{t}` — ZSET member=execution_id, score=expiry
/// (the start-slot semaphore; empty tenant normalizes to default).
pub fn tenant_running_key(cfg: &Config, tenant: &str) -> String {
    let t = if tenant.is_empty() {
        cfg.default_tenant_id.as_str()
    } else {
        tenant
    };
    format!("{}:tenant_running:{}", cfg.key_prefix, t)
}

/// `{prefix}:exec_tombstone:{id}` — deletion tombstone (platform-root key,
/// NOT tenant-routed; the console deletion API writes it — §8.3). A tombstone
/// forbids re-enqueueing carriers for the deleted execution (A51-01).
pub fn tombstone_key(cfg: &Config, execution_id: &str) -> String {
    format!("{}:exec_tombstone:{}", cfg.key_prefix, execution_id)
}

/// `{ns}:execution:cancel:{id}` — cancel intent flag (value = ISO time,
/// TTL 7d; worker reads, never deletes).
pub fn cancel_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!(
        "{}:execution:cancel:{}",
        cfg.namespace(tenant),
        execution_id
    )
}

/// `{ns}:execution:checkpoint:{id}` — suspend checkpoint (no TTL while
/// suspended; the terminal delete is the lifecycle bound).
pub fn checkpoint_key(cfg: &Config, tenant: &str, execution_id: &str) -> String {
    format!(
        "{}:execution:checkpoint:{}",
        cfg.namespace(tenant),
        execution_id
    )
}

/// `{ns}:event:subscriptions:{sub_id}` — subscription record (TTL = event
/// TTL).
pub fn subscription_key(cfg: &Config, tenant: &str, subscription_id: &str) -> String {
    format!(
        "{}:event:subscriptions:{}",
        cfg.namespace(tenant),
        subscription_id
    )
}

/// `{ns}:event:subscriptions:type:{event_type}` — by-type index (SET).
pub fn subscription_type_key(cfg: &Config, tenant: &str, event_type: &str) -> String {
    format!(
        "{}:event:subscriptions:type:{}",
        cfg.namespace(tenant),
        event_type
    )
}

/// `{prefix}:event:subscriptions:timeouts` — due-time index (ZSET,
/// member = subscription key, score = due epoch). Namespace-wide: the
/// timeout checker scans one index for the whole deployment.
pub fn subscription_timeouts_key(cfg: &Config) -> String {
    format!("{}:event:subscriptions:timeouts", cfg.key_prefix)
}

/// `{prefix}:event:subscriptions:timeout_attempts` — per-member requeue
/// attempt counters for the timeout sweep (#50 one-shot due queue).
pub fn subscription_timeout_attempts_key(cfg: &Config) -> String {
    format!("{}:event:subscriptions:timeout_attempts", cfg.key_prefix)
}

/// `{prefix}:event_filter:dedup:{event_id}:{sub_id}` — event dedup (NX, 1h).
pub fn event_dedup_key(cfg: &Config, event_id: &str, subscription_id: &str) -> String {
    format!(
        "{}:event_filter:dedup:{}:{}",
        cfg.key_prefix, event_id, subscription_id
    )
}

/// `{prefix}:event_filter:timeout:{sub_id}` — timeout dedup (NX, 1h).
pub fn timeout_dedup_key(cfg: &Config, subscription_id: &str) -> String {
    format!(
        "{}:event_filter:timeout:{}",
        cfg.key_prefix, subscription_id
    )
}

/// `{ns}:flow:{flow_id}:{version}` — flow IR storage (console engine_sync is
/// the production writer).
pub fn flow_key(cfg: &Config, tenant: &str, flow_id: &str, version: &str) -> String {
    format!("{}:flow:{}:{}", cfg.namespace(tenant), flow_id, version)
}

/// `{ns}:flow_versions:{flow_id}` — SET of versions.
pub fn flow_versions_key(cfg: &Config, tenant: &str, flow_id: &str) -> String {
    format!("{}:flow_versions:{}", cfg.namespace(tenant), flow_id)
}

/// `{ns}:flow_list` — SET of flow ids.
pub fn flow_list_key(cfg: &Config, tenant: &str) -> String {
    format!("{}:flow_list", cfg.namespace(tenant))
}

/// `{prefix}:{subtype}:queue` — suspend service dispatch (RPUSH; delay/
/// approval/…). KeyPrefix-derived (the rs/go convention; Python hardcodes
/// `plaita:{subtype}:queue` — equal under the default prefix, documented
/// divergence otherwise).
pub fn service_queue_key(cfg: &Config, subtype: &str) -> String {
    format!("{}:{}:queue", cfg.key_prefix, subtype)
}

/// Business event notification channel `{ns}:events:{event_type}` (Pub/Sub;
/// the bridge subscribes to `{prefix}*:events:*`).
pub fn business_event_channel(cfg: &Config, tenant: &str, event_type: &str) -> String {
    format!("{}:events:{}", cfg.namespace(tenant), event_type)
}

/// Execution timeline live channel `{prefix}:execution:events:{id}` — the
/// shape the event bridge must NOT consume (§8.2).
pub fn execution_events_channel(cfg: &Config, execution_id: &str) -> String {
    format!("{}:execution:events:{}", cfg.key_prefix, execution_id)
}

/// Pub/Sub pattern the event bridge subscribes to.
pub fn event_bridge_pattern(cfg: &Config) -> String {
    format!("{}*:events:*", cfg.key_prefix)
}

// -- 事件存储对账（§8.2 回扫器，Python event_reconcile.py 对应物）--------

/// `{esp}events:{event_id}` — 业务事件记录（string，Python Event JSON；
/// 发布端 store_event_sync 写，TTL 7d）。`{esp}` = `cfg.event_storage_prefix`
/// （发布方的存储命名空间，默认 `plaita:event:`，不从 worker KeyPrefix 派生
/// ——与 Python 发布端逐字同键，混合部署共用一份存储）。
pub fn event_storage_record_key(cfg: &Config, event_id: &str) -> String {
    format!("{}events:{}", cfg.event_storage_prefix, event_id)
}

/// `{esp}types:{event_type}` — 事件类型索引（ZSET，member=event_id，
/// score=event.timestamp；同 store_event_sync）。
pub fn event_storage_type_index_key(cfg: &Config, event_type: &str) -> String {
    format!("{}types:{}", cfg.event_storage_prefix, event_type)
}

/// `{prefix}:event_reconcile:cursor` — 回扫游标（复合 `<ts>|<event_id>`，
/// TTL 7d）。默认前缀下与 Python 固定键 `plaita:event_reconcile:cursor`
/// 逐字同键；从 worker 前缀派生是为影子隔离与多命名空间私有书签
/// （Python 侧多对账进程共享一个全局游标——有意分歧，见 reconcile.rs）。
pub fn event_reconcile_cursor_key(cfg: &Config) -> String {
    format!("{}:event_reconcile:cursor", cfg.key_prefix)
}
