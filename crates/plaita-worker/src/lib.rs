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
//!
//! The engine boundary (rs-worker-design §7.2): the worker persists "flow
//! document reference + context snapshot + suspended node + subscription
//! info"; recovery = `ir::load` + `RuntimeCtx::from_snapshot_json` +
//! `SuspendedExecution::from_parts` + the engine's own `resume` chain. The
//! worker never forks recovery logic.

pub mod backend;
pub mod checkpoint;
pub mod config;
pub mod error;
pub mod events;
pub mod fake;
pub mod flowsource;
pub mod keys;
pub mod lease;
pub mod metrics;
pub mod queue;
pub mod quota;
pub mod redis_impl;
pub mod registry;
pub mod retry;
pub mod state;
pub mod subscriptions;
mod watchdog;
pub mod worker;

pub use backend::{BackendError, PendingEntry, PsubStream, RedisBackend, StreamEntry, TxOp};
pub use checkpoint::{
    CheckpointRecord, CheckpointStore, RedisCheckpointStore, CHECKPOINT_KIND_SUSPEND,
    CHECKPOINT_SCHEMA_VERSION, CONTEXT_FLAVOR,
};
pub use config::{now_iso, Config, DEFAULT_KEY_PREFIX, SHADOW_KEY_PREFIX, TASK_SCHEMA_VERSION};
pub use error::{ErrorKind, WorkerError, FLOW_ERROR_CODE};
pub use events::{handle_event, sweep_event_timeouts, Event};
pub use flowsource::{
    canonical_flow_definition_json, flow_definition_hash, get_flow_definition,
    preset_flow_definition, FlowDefinition, RedisFlowStore, FLOW_HASH_ALGO, LATEST_VERSION,
};
pub use lease::{holder_instance_id, new_holder_token, LeaseHandle};
pub use metrics::{Metrics, NoopMetrics, METRIC_DLQ_TOTAL, METRIC_EXEC_RESULT, METRIC_QUEUE_DEPTH};
pub use queue::{Delivery, Task, TaskQueue};
pub use redis_impl::RedisConnection;
pub use registry::{
    Registry, REG_STATUS_DRAINING, REG_STATUS_RUNNING, REG_STATUS_STARTING, REG_STATUS_STOPPED,
    REG_STATUS_STOPPING,
};
pub use retry::{
    node_retry_backoff, set_retry_backoff_base_ms, GuardedDeadLetter, DETERMINISTIC_FAILURE_MAX,
    NODE_RETRY_TOTAL_MAX,
};
pub use state::{ExecutionState, STATUS_SUSPENDED};
pub use subscriptions::{EventSubscription, RedisSubscriptions};
pub use worker::Worker;
