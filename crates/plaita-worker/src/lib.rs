//! plaita-worker — the worker runtime for the plaita-rs engine: Redis
//! Streams queue consumption (XREADGROUP + reclaim-before-read + DLQ),
//! fenced execution leases, checkpoint persistence, the wake-up subscription
//! store with its timeout checker and event bridge, and the start/resume/
//! cancel worker loop with suspend-service dispatch (M1 skeleton; the port
//! target is plaita-go `pkg/worker`, semantics authority plaita
//! `plaita/server/flow_worker.py` + `task_queue.py` / `execution_lease.py` /
//! `event_filter.py`).
//!
//! Protocol authority (rs-worker-design §0–§3): key names, envelope fields,
//! Lua scripts and payload shapes are byte-compatible with plaita-go — mixed
//! deployments share one Redis and interoperate directly. Every degradation
//! is observable (named log markers, no silent swallows) and every probe
//! failure takes the most conservative branch (fail-closed).
//!
//! Design invariants (rs-worker-design §0):
//! 1. sync thread model (std::thread + Arc/RwLock) — no tokio, mirroring the
//!    engine;
//! 2. Redis protocol byte-compatibility with plaita-go (`keys` module owns
//!    every key derivation; the Lua scripts live verbatim in `redis_impl`);
//! 3. fail-closed + named observability (`error` carries the disposition
//!    family; parking/acking/dead-lettering decisions are logged).

pub mod backend;
pub mod config;
pub mod error;
pub mod fake;
pub mod keys;
pub mod metrics;

pub use backend::{BackendError, PendingEntry, PsubStream, RedisBackend, StreamEntry, TxOp};
pub use config::{now_iso, Config, DEFAULT_KEY_PREFIX, SHADOW_KEY_PREFIX, TASK_SCHEMA_VERSION};
pub use error::{ErrorKind, WorkerError, FLOW_ERROR_CODE};
pub use metrics::{Metrics, NoopMetrics, METRIC_DLQ_TOTAL, METRIC_EXEC_RESULT, METRIC_QUEUE_DEPTH};
