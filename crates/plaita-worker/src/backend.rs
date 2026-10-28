//! The Redis command surface the worker needs, as a trait.
//!
//! Why a typed trait instead of raw RESP passthrough: the worker's behavior
//! tests must run without a Redis daemon (rs-worker-design §13.3) while the
//! protocol semantics (streams + PEL + idle times, TTLs, the four Lua
//! scripts) stay pinned. The trait is small enough to fake faithfully
//! (`fake` implements each command's protocol semantics natively, including
//! stream PEL/delivery-count bookkeeping) and the production backend
//! (`redis_impl`) maps each method onto the exact redis-rs command / the
//! verbatim Lua text.
//!
//! Byte-compatibility constraint (§0): key names, field names and script
//! semantics are decided in the shared layer (`keys` + the named script
//! methods) — both backends agree by construction; only the transport
//! differs.

use std::time::Duration;

/// Backend-level failure. `BusyGroup` is a named case because the queue's
/// EnsureGroup tolerates exactly that error string (§4.2 step 1).
#[derive(Debug, Clone)]
pub enum BackendError {
    Io(String),
    BusyGroup(String),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::Io(m) => write!(f, "redis: {m}"),
            BackendError::BusyGroup(m) => write!(f, "redis: {m}"),
        }
    }
}

impl std::error::Error for BackendError {}

pub type BackendResult<T> = Result<T, BackendError>;

/// One stream message: id + ordered field pairs (wire order preserved).
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

impl StreamEntry {
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// One XPENDING EXT row.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: String,
    pub consumer: String,
    pub idle_ms: i64,
    pub delivery_count: i64,
}

/// One XINFO GROUPS row (the fields the residue sweep needs).
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub name: String,
    pub last_delivered_id: String,
}

/// One transactional pipeline step (subscription register/unregister write
/// sets). The real backend runs these atomically (TxPipeline); the fake
/// applies them sequentially — the write set is idempotent, so sequential
/// application only weakens crash-atomicity, never correctness.
#[derive(Debug, Clone)]
pub enum TxOp {
    Set {
        key: String,
        value: String,
        ttl_secs: Option<i64>,
    },
    Sadd {
        key: String,
        member: String,
    },
    Srem {
        key: String,
        member: String,
    },
    Expire {
        key: String,
        ttl_secs: i64,
    },
    Zadd {
        key: String,
        score: f64,
        member: String,
    },
    Zrem {
        key: String,
        member: String,
    },
    Rpush {
        key: String,
        value: String,
    },
    Hdel {
        key: String,
        field: String,
    },
    Del {
        key: String,
    },
}

/// A Pub/Sub subscription stream (dedicated connection in the real backend;
/// an in-process queue in the fake).
pub trait PsubStream: Send {
    /// Waits up to `timeout` for the next message; `None` = timeout (the
    /// caller re-checks its stop flag and re-waits — a blocking receive
    /// cannot be interrupted in the sync model).
    fn next_message(&mut self, timeout: Duration) -> Option<(String, String)>;
}

/// The typed Redis command face (sync).
pub trait RedisBackend: Send + Sync {
    // -- strings ----------------------------------------------------------
    fn get(&self, key: &str) -> BackendResult<Option<String>>;
    fn set(&self, key: &str, value: &str, ttl_secs: Option<i64>) -> BackendResult<()>;
    fn set_nx(&self, key: &str, value: &str, ttl_secs: i64) -> BackendResult<bool>;
    fn del(&self, keys: &[&str]) -> BackendResult<i64>;
    fn expire(&self, key: &str, ttl_secs: i64) -> BackendResult<bool>;
    fn incr(&self, key: &str) -> BackendResult<i64>;
    fn incr_by(&self, key: &str, delta: i64) -> BackendResult<i64>;
    fn hincr_by(&self, key: &str, field: &str, delta: i64) -> BackendResult<i64>;
    fn hdel(&self, key: &str, field: &str) -> BackendResult<i64>;
    /// HGET one field (None = missing field/key).
    fn hget(&self, key: &str, field: &str) -> BackendResult<Option<String>>;
    /// HSET one field (test/config writers; the quota gate only reads).
    fn hset(&self, key: &str, field: &str, value: &str) -> BackendResult<i64>;

    // -- sets / zsets / lists ---------------------------------------------
    fn sadd(&self, key: &str, member: &str) -> BackendResult<()>;
    fn srem(&self, key: &str, member: &str) -> BackendResult<()>;
    fn smembers(&self, key: &str) -> BackendResult<Vec<String>>;
    /// SCAN with a MATCH glob (COUNT 100 per page, iterated to cursor 0).
    /// The retry budget's per-node key scan uses it (retry.go MaxNodeRetry);
    /// production-unsafe KEYS is never used.
    fn scan_match(&self, pattern: &str) -> BackendResult<Vec<String>>;
    fn zadd(&self, key: &str, score: f64, member: &str) -> BackendResult<()>;
    fn zrem(&self, key: &str, member: &str) -> BackendResult<()>;
    /// ZRANGEBYSCORE min max (inclusive bounds) limited to `count`.
    fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        count: i64,
    ) -> BackendResult<Vec<String>>;
    fn rpush(&self, key: &str, value: &str) -> BackendResult<()>;
    fn lrange(&self, key: &str, start: i64, stop: i64) -> BackendResult<Vec<String>>;
    /// LREM count value (wake convey/ack dequeues with count=1; count 0 =
    /// all occurrences, negative = tail→head).
    fn lrem(&self, key: &str, count: i64, value: &str) -> BackendResult<i64>;

    // -- pub/sub -----------------------------------------------------------
    fn publish(&self, channel: &str, payload: &str) -> BackendResult<i64>;
    fn psubscribe(&self, pattern: &str) -> BackendResult<Box<dyn PsubStream>>;

    // -- streams -----------------------------------------------------------
    fn xadd(&self, key: &str, fields: &[(String, String)]) -> BackendResult<String>;
    /// Inclusive XRANGE; `"-"`/`"+"` bounds supported.
    fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> BackendResult<Vec<StreamEntry>>;
    fn xlen(&self, key: &str) -> BackendResult<i64>;
    /// XGROUP CREATE … id MKSTREAM. `BusyGroup` when the group exists.
    fn xgroup_create_mkstream(&self, key: &str, group: &str, id: &str) -> BackendResult<()>;
    /// XREADGROUP `>` with COUNT and optional BLOCK ms. Empty = nothing
    /// available (including the redis.Nil case).
    fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        key: &str,
        count: i64,
        block_ms: Option<i64>,
    ) -> BackendResult<Vec<StreamEntry>>;
    /// XPENDING EXT (idle ms + delivery count per row).
    fn xpending_ext(
        &self,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: i64,
    ) -> BackendResult<Vec<PendingEntry>>;
    /// XCLAIM with min-idle-time; claimed entries come back with fields.
    fn xclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        ids: &[&str],
    ) -> BackendResult<Vec<StreamEntry>>;
    fn xack(&self, key: &str, group: &str, ids: &[&str]) -> BackendResult<i64>;
    fn xdel(&self, key: &str, ids: &[&str]) -> BackendResult<i64>;
    /// XTRIM MAXLEN ~ exact retention.
    fn xtrim_maxlen_exact(&self, key: &str, maxlen: i64) -> BackendResult<()>;
    fn xinfo_groups(&self, key: &str) -> BackendResult<Vec<GroupInfo>>;

    // -- named Lua scripts (verbatim texts live in `redis_impl`; the fake
    //    implements the same semantics natively) ---------------------------
    /// Fenced acquire (execution_lease.py:68-81). Returns the fence
    /// generation (>0) or 0 = already held.
    fn lease_acquire(
        &self,
        lease_key: &str,
        fence_key: &str,
        fencegen_key: &str,
        holder: &str,
        ttl_secs: i64,
        fence_ttl_secs: i64,
    ) -> BackendResult<i64>;
    /// Compare-and-delete the ENTIRE lease value; 1 = released, 0 = no-op.
    fn lease_release(&self, lease_key: &str, lease_value: &str) -> BackendResult<i64>;
    /// Re-EXPIRE only when the full value still matches; 0 = lost lease.
    fn lease_renew(&self, lease_key: &str, lease_value: &str, ttl_secs: i64) -> BackendResult<i64>;
    /// Fenced CAS state save (state.go luaFencedStateSave): 1 = written,
    /// 0 = fence mismatch, -1 = fence/fencegen divergence. (The arg list IS
    /// the Lua KEYS/ARGV mapping — kept positional, clippy-allowed.)
    #[allow(clippy::too_many_arguments)]
    fn fenced_state_save(
        &self,
        state_key: &str,
        fence_key: &str,
        index_key: &str,
        fencegen_key: &str,
        state_json: &str,
        gen: i64,
        ttl_secs: i64,
        score: f64,
    ) -> BackendResult<i64>;

    /// Per-tenant start admission (quota.go luaAdmitTenantStart — verbatim
    /// semantics): purge expired members, idempotently refresh an existing
    /// member (message-redelivery safe), ZADD + key EXPIRE when below the
    /// cap, deny when full. Returns (allowed, running) for the quota gauge.
    fn tenant_quota_admit(
        &self,
        running_key: &str,
        now: f64,
        ttl_secs: f64,
        cap: i64,
        member: &str,
    ) -> BackendResult<(i64, i64)>;

    /// Transactional pipeline (atomic on the real backend).
    fn tx(&self, ops: &[TxOp]) -> BackendResult<()>;
}
