//! Worker configuration (port of plaita-go config.go, M1 knob set): every
//! field is overridable through environment variables so a shadow deployment
//! can rewrite the full key space without touching production keys. Derived
//! defaults are filled by [`Config::normalize`] — KeyPrefix first, then the
//! queue/group/DLQ names derive from it.

/// This build's envelope write version (§4.1 TASK_SCHEMA_VERSION=1).
pub const TASK_SCHEMA_VERSION: i64 = 1;

/// Default key namespace root (§3: `{prefix}` = plaita).
pub const DEFAULT_KEY_PREFIX: &str = "plaita";

/// Shadow namespace root (§3 ApplyShadow): a shadow run never touches
/// production keys.
pub const SHADOW_KEY_PREFIX: &str = "plaita-rs-shadow";

/// Fenced fence-key TTL in seconds (§3 FENCE_KEY_TTL_SECONDS, 7d).
pub const FENCE_KEY_TTL_SECONDS: i64 = 7 * 86400;

/// Worker-judged node-abort code (plaita DEFAULT_NODE_ABORT_CODE).
pub const DEFAULT_NODE_ABORT_CODE: i64 = -520;

/// Reclaim scan window (#196 rotating window, task_queue.py:534).
pub const RECLAIM_WINDOW: i64 = 8;

/// Transient Redis failure backoff before the next consume round (§4.2).
pub const RECONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// Engine marker written into the state row (`engine_version`).
pub fn engine_version() -> String {
    format!("plaita-rs {}", env!("CARGO_PKG_VERSION"))
}

/// Every M1 knob of the worker.
#[derive(Debug, Clone)]
pub struct Config {
    /// PLAITA_REDIS_URL / REDIS_URL (default redis://127.0.0.1:6379/0).
    pub redis_url: String,
    /// PLAITA_QUEUE_NAME (default `<KeyPrefix>:flow:queue`).
    pub queue_name: String,
    /// PLAITA_CONSUMER_GROUP (default `plaita-workers`, or
    /// `<KeyPrefix>-workers` when KeyPrefix is overridden — shadow consumers
    /// never join the production group).
    pub consumer_group: String,
    /// PLAITA_CONSUMER_NAME (default instance id).
    pub consumer_name: String,
    /// PLAITA_INSTANCE_ID (default `worker-<pid>`); embedded in lease holder
    /// tokens.
    pub instance_id: String,
    /// PLAITA_CLAIM_MIN_IDLE_MS (default 60000) — pending-message XCLAIM
    /// reclaim threshold.
    pub claim_min_idle_ms: i64,
    /// PLAITA_LEASE_TTL_SECONDS (default 120).
    pub lease_ttl_seconds: i64,
    /// PLAITA_MAX_DELIVERIES (default 5) — delivery ceiling on the reclaim
    /// path before the (guarded) dead-letter.
    pub max_deliveries: i64,
    /// PLAITA_DLQ_KEY (default `<queue>:dlq`).
    pub dlq_key: String,
    /// PLAITA_DLQ_MAX_LEN (default 1000) — DLQ XTRIM retention (exact).
    pub dlq_max_len: i64,
    /// PLAITA_READ_BLOCK_MS — XREADGROUP BLOCK cap (default 1000, floor 100;
    /// the ≤1s slice is the shutdown-latency bound, flow_worker.py:7126).
    pub read_block_ms: i64,
    /// PLAITA_KEY_PREFIX — namespace root for every derived key.
    pub key_prefix: String,
    /// PLAITA_DEFAULT_TENANT_ID (default "default"); the plain prefix is the
    /// namespace of the default/empty tenant.
    pub default_tenant_id: String,
    /// PLAITA_EVENT_TTL_SECONDS (default 7d) — subscription record TTL.
    pub event_ttl_seconds: i64,
    /// PLAITA_EXECUTION_STATE_TTL_DAYS (default 30) — terminal state TTL.
    pub state_ttl_days: i64,
    /// PLAITA_SWEEP_INTERVAL_SECONDS (default 300) — acked-residue sweep.
    pub sweep_interval_secs: i64,
    /// PLAITA_WORKER_DRAIN_TIMEOUT (default 30s) — D1→D2 escalation timer.
    pub drain_timeout_secs: f64,
    /// PLAITA_FLOW_CACHE_SIZE (default 100) / PLAITA_FLOW_CACHE_TTL_SECONDS
    /// (default 300) — flow definition TTL cache.
    pub flow_cache_size: usize,
    pub flow_cache_ttl_secs: i64,
    /// PLAITA_HEARTBEAT_INTERVAL_SECONDS (default 10) — registry heartbeat
    /// period (§8.1 layer 1).
    pub heartbeat_interval_secs: i64,
    /// PLAITA_REGISTRY_TTL_SECONDS (default 30) — registry entry SETEX TTL.
    pub registry_ttl_secs: i64,
    /// console 读穿透六键（PLAITA_CONSOLE_FLOW_SOURCE_URL / _API_KEY /
    /// _TIMEOUT_SECONDS / _NEGATIVE_TTL_SECONDS / _ATTEMPTS / _WRITEBACK，
    /// 另 PLAITA_DEFAULT_TENANT_ID 租户缺省）——键面/缺省/告警口径与引擎
    /// `FlowSourceConfig::from_env` 逐字一致；URL 空 = 特性关闭（本地语义）。
    pub flow_source: plaita_engine::flowsource::FlowSourceConfig,
    /// PLAITA_EXECUTION_DEADLINE_SECONDS（#35/plaita#267，默认 1800）——
    /// worker 级执行预算：单次引擎调用占据单消费者的墙钟上限（挂起等待不
    /// 占预算——resume/continue 载体拿新预算）。env 非法/≤0 回默认 + 具名
    /// WARNING（绝不静默无界，config.go:483-498 同口径）；**显式负值仅
    /// Config 结构可表达**（关闸是部署决策，env 永远关不掉）。
    pub execution_deadline_secs: f64,
    /// PLAITA_CANCEL_POLL_MS（默认 1000）——在途执行的取消标志轮询间隔。
    pub cancel_poll_ms: i64,
    /// PLAITA_TENANT_MAX_RUNNING（默认 0 = 不限）——全局缺省租户并发 start
    /// 上限（quota.rs；逐租户 HASH 项优先）。
    pub tenant_max_running: i64,
    /// PLAITA_TENANT_RUNNING_TTL_SECONDS（默认 86400）——start 槽位条目的
    /// 崩溃自愈 TTL（quota.go DefaultTenantRunningTTLSeconds）。
    pub tenant_running_ttl_secs: i64,
    /// PLAITA_TENANT_QUOTA_DEFER_MAX（默认 64）——超限载体重排预算；尽后
    /// quota_bypass=true 放行（活性 > 软配额）。
    pub quota_defer_max: i64,
    /// PLAITA_DISABLE_TENANT_QUOTA=1 ——回滚开关（整闸旁路）。
    pub disable_tenant_quota: bool,
    /// PLAITA_TASK_MAX_AGE_SECONDS（#29，默认 30 天）——载体年龄闸地平线；
    /// 0/未设回默认，负值=整闸关闭（agegate.rs）。
    pub task_max_age_secs: i64,
    /// PLAITA_EVENT_RECONCILE_INTERVAL（§8.2 回扫器，默认 300s）——事件存储
    /// 对账周期；非法/≤0 回默认 + 具名 WARNING（0 是紧环不是「关闭」，关闸
    /// 走 PLAITA_DISABLE_EVENT_RECONCILE=1；Python B2R31-03 同口径）。
    pub event_reconcile_interval_secs: f64,
    /// PLAITA_EVENT_RECONCILE_BACKFILL（默认 3600s）——启动/游标丢失时的
    /// 回扫窗口；负值按 0（只扫当轮）。
    pub event_reconcile_backfill_secs: f64,
    /// PLAITA_DISABLE_EVENT_RECONCILE=1 ——回扫器回滚开关（整面旁路）。
    pub disable_event_reconcile: bool,
    /// PLAITA_EVENT_STORAGE_PREFIX（默认 `plaita:event:`）——业务事件存储
    /// 键前缀（Python 发布端 store_event_sync 的 `key_prefix`：记录
    /// `{esp}events:{id}` + 类型索引 `{esp}types:{type}`）。这是**发布方的
    /// 存储**命名空间，不从 worker KeyPrefix 派生；apply_shadow 在等于默认
    /// 值时改写（影子部署绝不回扫生产事件存储）。
    pub event_storage_prefix: String,
}

impl Default for Config {
    fn default() -> Self {
        Config::from_env()
    }
}

impl Config {
    /// Protocol-default configuration from the process environment.
    pub fn from_env() -> Config {
        Config::from_getenv(|k| std::env::var(k).unwrap_or_default())
    }

    /// Testable environment reader (empty string = unset).
    pub fn from_getenv(getenv: impl Fn(&str) -> String) -> Config {
        let mut cfg = Config {
            redis_url: getenv2(&getenv, &["PLAITA_REDIS_URL", "REDIS_URL"]),
            queue_name: getenv("PLAITA_QUEUE_NAME"),
            consumer_group: getenv("PLAITA_CONSUMER_GROUP"),
            consumer_name: getenv("PLAITA_CONSUMER_NAME"),
            instance_id: getenv("PLAITA_INSTANCE_ID"),
            claim_min_idle_ms: env_i64(&getenv, "PLAITA_CLAIM_MIN_IDLE_MS", 60_000),
            lease_ttl_seconds: env_i64(&getenv, "PLAITA_LEASE_TTL_SECONDS", 120),
            max_deliveries: env_i64(&getenv, "PLAITA_MAX_DELIVERIES", 5),
            dlq_key: getenv("PLAITA_DLQ_KEY"),
            dlq_max_len: env_i64(&getenv, "PLAITA_DLQ_MAX_LEN", 1000),
            read_block_ms: env_i64(&getenv, "PLAITA_READ_BLOCK_MS", 1000),
            key_prefix: getenv("PLAITA_KEY_PREFIX"),
            default_tenant_id: getenv("PLAITA_DEFAULT_TENANT_ID"),
            event_ttl_seconds: env_i64(&getenv, "PLAITA_EVENT_TTL_SECONDS", 7 * 86400),
            state_ttl_days: env_i64(&getenv, "PLAITA_EXECUTION_STATE_TTL_DAYS", 30),
            sweep_interval_secs: env_i64(&getenv, "PLAITA_SWEEP_INTERVAL_SECONDS", 300),
            drain_timeout_secs: env_f64(&getenv, "PLAITA_WORKER_DRAIN_TIMEOUT", 30.0),
            flow_cache_size: env_i64(&getenv, "PLAITA_FLOW_CACHE_SIZE", 100).max(0) as usize,
            flow_cache_ttl_secs: env_i64(&getenv, "PLAITA_FLOW_CACHE_TTL_SECONDS", 300),
            heartbeat_interval_secs: env_i64(&getenv, "PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10),
            registry_ttl_secs: env_i64(&getenv, "PLAITA_REGISTRY_TTL_SECONDS", 30),
            execution_deadline_secs: env_deadline_seconds(&getenv),
            cancel_poll_ms: env_i64(&getenv, "PLAITA_CANCEL_POLL_MS", 1000),
            tenant_max_running: env_i64(&getenv, "PLAITA_TENANT_MAX_RUNNING", 0),
            tenant_running_ttl_secs: env_i64(&getenv, "PLAITA_TENANT_RUNNING_TTL_SECONDS", 86_400),
            quota_defer_max: env_i64(&getenv, "PLAITA_TENANT_QUOTA_DEFER_MAX", 64),
            disable_tenant_quota: getenv("PLAITA_DISABLE_TENANT_QUOTA") == "1",
            task_max_age_secs: env_i64(&getenv, "PLAITA_TASK_MAX_AGE_SECONDS", 30 * 86_400),
            event_reconcile_interval_secs: env_reconcile_interval(&getenv),
            event_reconcile_backfill_secs: env_reconcile_backfill(&getenv),
            disable_event_reconcile: getenv("PLAITA_DISABLE_EVENT_RECONCILE") == "1",
            event_storage_prefix: getenv("PLAITA_EVENT_STORAGE_PREFIX"),
            flow_source: plaita_engine::flowsource::FlowSourceConfig::from_getenv(&getenv),
        };
        cfg.normalize();
        cfg
    }

    /// Fills the derived defaults. Order matters: KeyPrefix first, then the
    /// queue/group/DLQ names derive from it.
    pub fn normalize(&mut self) {
        if self.key_prefix.is_empty() {
            self.key_prefix = DEFAULT_KEY_PREFIX.to_string();
        }
        if self.redis_url.is_empty() {
            self.redis_url = "redis://127.0.0.1:6379/0".to_string();
        }
        if self.queue_name.is_empty() {
            self.queue_name = format!("{}:flow:queue", self.key_prefix);
        }
        if self.consumer_group.is_empty() {
            self.consumer_group = if self.key_prefix == DEFAULT_KEY_PREFIX {
                "plaita-workers".to_string()
            } else {
                format!("{}-workers", self.key_prefix)
            };
        }
        if self.instance_id.is_empty() {
            self.instance_id = format!("worker-{}", std::process::id());
        }
        if self.consumer_name.is_empty() {
            self.consumer_name = self.instance_id.clone();
        }
        if self.dlq_key.is_empty() {
            self.dlq_key = format!("{}:dlq", self.queue_name);
        }
        if self.default_tenant_id.is_empty() {
            self.default_tenant_id = "default".to_string();
        }
        if self.claim_min_idle_ms < 0 {
            self.claim_min_idle_ms = 60_000;
        }
        if self.lease_ttl_seconds <= 0 {
            self.lease_ttl_seconds = 120;
        }
        if self.max_deliveries <= 0 {
            self.max_deliveries = 5;
        }
        if self.dlq_max_len <= 0 {
            self.dlq_max_len = 1000;
        }
        if self.read_block_ms < 100 {
            // §4.2 read-block floor: the BLOCK slice bounds shutdown latency.
            self.read_block_ms = 100;
        }
        if self.event_ttl_seconds <= 0 {
            self.event_ttl_seconds = 7 * 86400;
        }
        if self.state_ttl_days <= 0 {
            self.state_ttl_days = 30;
        }
        if self.sweep_interval_secs <= 0 {
            self.sweep_interval_secs = 300;
        }
        if self.drain_timeout_secs <= 0.0 {
            self.drain_timeout_secs = 30.0;
        }
        if self.flow_cache_size == 0 {
            self.flow_cache_size = 100;
        }
        if self.flow_cache_ttl_secs <= 0 {
            self.flow_cache_ttl_secs = 300;
        }
        if self.heartbeat_interval_secs <= 0 {
            self.heartbeat_interval_secs = 10;
        }
        if self.registry_ttl_secs <= 0 {
            self.registry_ttl_secs = 30;
        }
        if self.cancel_poll_ms <= 0 {
            self.cancel_poll_ms = 1000;
        }
        if self.tenant_running_ttl_secs <= 0 {
            self.tenant_running_ttl_secs = 86_400;
        }
        if self.quota_defer_max <= 0 {
            self.quota_defer_max = 64;
        }
        // task_max_age_secs: 0/unset → the 30-day default; a NEGATIVE value
        // (explicitly configured) disables the gate (agegate.go:108-111).
        if self.task_max_age_secs == 0 {
            self.task_max_age_secs = 30 * 86_400;
        }
        if self.event_storage_prefix.is_empty() {
            self.event_storage_prefix = DEFAULT_EVENT_STORAGE_PREFIX.to_string();
        }
        // execution_deadline_secs: env 解析已按「非法/≤0 回默认 + WARNING」
        // 处理；这里只兜结构体直构的 NaN。
        if self.execution_deadline_secs.is_nan() {
            self.execution_deadline_secs = DEFAULT_EXECUTION_DEADLINE_SECS;
        }
    }

    /// The tenant-routed key namespace (§3): the plain prefix for the
    /// default/empty tenant, `prefix:{tenant}` otherwise.
    pub fn namespace(&self, tenant_id: &str) -> String {
        if tenant_id.is_empty() || tenant_id == self.default_tenant_id {
            self.key_prefix.clone()
        } else {
            format!("{}:{}", self.key_prefix, tenant_id)
        }
    }

    /// Rewrites the namespace root into the shadow prefix. Explicit per-key
    /// overrides win; everything derived is recomputed so no key can fall
    /// back to production naming.
    pub fn apply_shadow(&mut self) {
        self.key_prefix = SHADOW_KEY_PREFIX.to_string();
        // 事件存储前缀：等于默认（未显式配置）时改写——影子部署的回扫器
        // 绝不消费生产事件存储；显式配置的覆盖保持调用方意图。
        if self.event_storage_prefix == DEFAULT_EVENT_STORAGE_PREFIX {
            self.event_storage_prefix = SHADOW_EVENT_STORAGE_PREFIX.to_string();
        }
        let default_queue = format!("{DEFAULT_KEY_PREFIX}:flow:queue");
        let shadow_queue = format!("{SHADOW_KEY_PREFIX}:flow:queue");
        if self.queue_name == default_queue || self.queue_name == shadow_queue {
            self.queue_name = shadow_queue;
        }
        let default_group = "plaita-workers".to_string();
        let shadow_group = format!("{SHADOW_KEY_PREFIX}-workers");
        if self.consumer_group == default_group || self.consumer_group == shadow_group {
            self.consumer_group = shadow_group;
        }
        let derived_dlq = format!("{}:dlq", self.queue_name);
        let default_dlq = format!("{default_queue}:dlq");
        if self.dlq_key == derived_dlq || self.dlq_key == default_dlq {
            self.dlq_key = derived_dlq;
        }
    }
}

fn getenv2(getenv: &impl Fn(&str) -> String, keys: &[&str]) -> String {
    for k in keys {
        let v = getenv(k);
        if !v.is_empty() {
            return v;
        }
    }
    String::new()
}

fn env_i64(getenv: &impl Fn(&str) -> String, key: &str, default: i64) -> i64 {
    let v = getenv(key);
    if v.is_empty() {
        return default;
    }
    v.trim().parse().unwrap_or(default)
}

fn env_f64(getenv: &impl Fn(&str) -> String, key: &str, default: f64) -> f64 {
    let v = getenv(key);
    if v.is_empty() {
        return default;
    }
    v.trim().parse().unwrap_or(default)
}

/// Worker-level execution budget default (§11.3: 1800s, the same value the
/// engine's budget layer uses — the double layer composes to the stricter
/// one naturally).
pub const DEFAULT_EXECUTION_DEADLINE_SECS: f64 = 1800.0;

/// 业务事件存储默认键前缀（Python `DEFAULT_EVENT_KEY_PREFIX`，redis.py:670
/// ——记录 `{esp}events:{id}` + 类型索引 `{esp}types:{type}`）。
pub const DEFAULT_EVENT_STORAGE_PREFIX: &str = "plaita:event:";

/// 影子部署的事件存储前缀（apply_shadow 改写默认值后的落点）。
pub const SHADOW_EVENT_STORAGE_PREFIX: &str = "plaita-rs-shadow:event:";

/// 事件回扫器默认周期/回填窗口（Python event_reconcile.py 同值：300s /
/// 3600s）。
pub const DEFAULT_RECONCILE_INTERVAL_SECS: f64 = 300.0;
pub const DEFAULT_RECONCILE_BACKFILL_SECS: f64 = 3600.0;

/// PLAITA_EVENT_RECONCILE_INTERVAL：非法/≤0 → 默认 + 具名 WARNING（0 是
/// 紧环不是「关闭」——关闸是 PLAITA_DISABLE_EVENT_RECONCILE=1 的决策，
/// env 永远关不掉；Python B2R31-03 同口径）。
fn env_reconcile_interval(getenv: &impl Fn(&str) -> String) -> f64 {
    let v = getenv("PLAITA_EVENT_RECONCILE_INTERVAL");
    if v.is_empty() {
        return DEFAULT_RECONCILE_INTERVAL_SECS;
    }
    match v.trim().parse::<f64>() {
        Ok(n) if n.is_finite() && n > 0.0 => n,
        other => {
            eprintln!(
                "[config] invalid PLAITA_EVENT_RECONCILE_INTERVAL={v:?} (parsed {other:?}); \
                 falling back to the default {DEFAULT_RECONCILE_INTERVAL_SECS}s — set a \
                 positive value to tune it, PLAITA_DISABLE_EVENT_RECONCILE=1 to disable"
            );
            DEFAULT_RECONCILE_INTERVAL_SECS
        }
    }
}

/// PLAITA_EVENT_RECONCILE_BACKFILL：非法 → 默认 + WARNING；负值按 0（只扫
/// 当轮，不回填——回填窗口是补偿上界，不是扫面前置）。
fn env_reconcile_backfill(getenv: &impl Fn(&str) -> String) -> f64 {
    let v = getenv("PLAITA_EVENT_RECONCILE_BACKFILL");
    if v.is_empty() {
        return DEFAULT_RECONCILE_BACKFILL_SECS;
    }
    match v.trim().parse::<f64>() {
        Ok(n) if n.is_finite() => n.max(0.0),
        other => {
            eprintln!(
                "[config] invalid PLAITA_EVENT_RECONCILE_BACKFILL={v:?} (parsed {other:?}); \
                 falling back to the default {DEFAULT_RECONCILE_BACKFILL_SECS}s"
            );
            DEFAULT_RECONCILE_BACKFILL_SECS
        }
    }
}

/// PLAITA_EXECUTION_DEADLINE_SECONDS (config.go:483-498 同口径): unset →
/// default; non-numeric / non-finite / ≤0 → default + named WARNING (never a
/// silent unbounded run). A NEGATIVE budget is only expressible by assigning
/// the Config struct directly (disabling the budget is a deployment
/// decision, never an env accident).
fn env_deadline_seconds(getenv: &impl Fn(&str) -> String) -> f64 {
    let v = getenv("PLAITA_EXECUTION_DEADLINE_SECONDS");
    if v.is_empty() {
        return DEFAULT_EXECUTION_DEADLINE_SECS;
    }
    match v.trim().parse::<f64>() {
        Ok(n) if n.is_finite() && n > 0.0 => n,
        other => {
            eprintln!(
                "[config] invalid {PLAITA_DEADLINE_ENV}={v:?} (parsed {other:?}); falling back \
                 to the default {DEFAULT_EXECUTION_DEADLINE_SECS}s — the budget is never \
                 silently unbounded (set a positive value to configure it)"
            );
            DEFAULT_EXECUTION_DEADLINE_SECS
        }
    }
}

const PLAITA_DEADLINE_ENV: &str = "PLAITA_EXECUTION_DEADLINE_SECONDS";

/// Naive local ISO timestamp the Python side writes
/// (`datetime.now().isoformat()` — no timezone suffix, §6.1).
pub fn now_iso() -> String {
    chrono::Local::now()
        .format("%Y-%m-%dT%H:%M:%S%.6f")
        .to_string()
}

/// Float epoch seconds (UnixNano/1e9口径, queue DLQ envelopes + timeout
/// scores).
pub fn float_epoch() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Mints a uuid4-hex execution id (§4.1: uuid4 hex form).
pub fn new_execution_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_protocol_defaults() {
        let cfg = Config::from_getenv(|_| String::new());
        assert_eq!(cfg.key_prefix, "plaita");
        assert_eq!(cfg.queue_name, "plaita:flow:queue");
        assert_eq!(cfg.consumer_group, "plaita-workers");
        assert_eq!(cfg.dlq_key, "plaita:flow:queue:dlq");
        assert_eq!(cfg.default_tenant_id, "default");
        assert_eq!(cfg.namespace(""), "plaita");
        assert_eq!(cfg.namespace("default"), "plaita");
        assert_eq!(cfg.namespace("acme"), "plaita:acme");
        assert_eq!(cfg.read_block_ms, 1000);
        assert!(cfg.instance_id.starts_with("worker-"));
        assert_eq!(cfg.redis_url, "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn shadow_rewrite_moves_every_derived_key() {
        let mut cfg = Config::from_getenv(|_| String::new());
        cfg.apply_shadow();
        assert_eq!(cfg.key_prefix, SHADOW_KEY_PREFIX);
        assert_eq!(cfg.queue_name, "plaita-rs-shadow:flow:queue");
        assert_eq!(cfg.consumer_group, "plaita-rs-shadow-workers");
        assert_eq!(cfg.dlq_key, "plaita-rs-shadow:flow:queue:dlq");
    }

    #[test]
    fn overridden_prefix_moves_default_group() {
        let cfg = Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                "tenant-a".to_string()
            } else {
                String::new()
            }
        });
        assert_eq!(cfg.queue_name, "tenant-a:flow:queue");
        assert_eq!(cfg.consumer_group, "tenant-a-workers");
    }

    #[test]
    fn read_block_floor_and_invalid_fallbacks() {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_READ_BLOCK_MS" => "10".to_string(),
            "PLAITA_LEASE_TTL_SECONDS" => "not-a-number".to_string(),
            _ => String::new(),
        });
        assert_eq!(cfg.read_block_ms, 100);
        assert_eq!(cfg.lease_ttl_seconds, 120);
    }
}
