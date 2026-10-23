//! Execution state persistence (§6, port of plaita-go state.go): the full
//! state-row field set (snake_case, exactly as the Python model_dump writes
//! them), the fenced CAS write (fence key AND never-cleared fencegen
//! counter — #54/A2R25-05), the index ZADD, the cancel flag and the read
//! contract ("present but corrupt is an error, never silently missing").

use serde_json::{json, Map, Value};

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::error::{terminal_error_state, ErrorKind, WorkerError};
use crate::keys;

pub const STATUS_QUEUED: &str = "queued";
pub const STATUS_RUNNING: &str = "running";
pub const STATUS_SUSPENDED: &str = "suspended";
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_ERROR: &str = "error";
pub const STATUS_CANCELLED: &str = "cancelled";

/// Terminal statuses (storage/redis.py TERMINAL_EXECUTION_STATUSES).
pub fn is_terminal(status: &str) -> bool {
    matches!(status, STATUS_COMPLETED | STATUS_ERROR | STATUS_CANCELLED)
}

/// The execution state row (§6.1 field table).
#[derive(Debug, Clone, Default)]
pub struct ExecutionState {
    pub execution_id: String,
    pub flow_id: String,
    pub flow_name: String,
    pub flow_version: String,
    pub flow_hash: String,
    pub flow_hash_algo: String,
    pub engine_version: String,
    pub tenant_id: String,
    /// rs shape: the engine snapshot object `{input,nodes,errors,env,global,
    /// last_node,last_branch,flow_id,execution_id}` (§6.1 形状分歧记录:
    /// Go writes `$INPUT/$NODE/…`; the shadow reconciliation diffs shapes).
    pub context: Value,
    pub status: String,
    pub start_time: String,
    pub last_update_time: String,
    pub end_time: String,
    pub error: Option<Value>,
    pub invoker: String,
    pub node_timings: Option<Value>,
    pub timeline: Option<Value>,
    pub usage: Option<Value>,
    pub dispatch_pending: Option<Value>,
}

impl ExecutionState {
    /// Seeds the running row for a start task (§6.1).
    pub fn new_running(
        _cfg: &Config,
        task_flow_id: &str,
        task_params: &Value,
        execution_id: &str,
        tenant: &str,
    ) -> ExecutionState {
        let now = now_iso();
        ExecutionState {
            execution_id: execution_id.to_string(),
            flow_id: task_flow_id.to_string(),
            tenant_id: tenant.to_string(),
            engine_version: crate::config::engine_version(),
            context: json!({
                "input": task_params,
                "nodes": {},
                "errors": {},
                "env": {},
                "global": {},
                "last_node": null,
                "last_branch": null,
                "flow_id": task_flow_id,
                "execution_id": execution_id,
            }),
            status: STATUS_RUNNING.to_string(),
            start_time: now.clone(),
            last_update_time: now,
            invoker: "worker".to_string(),
            ..Default::default()
        }
    }

    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("execution_id".into(), json!(self.execution_id));
        if !self.flow_id.is_empty() {
            m.insert("flow_id".into(), json!(self.flow_id));
        }
        if !self.flow_name.is_empty() {
            m.insert("flow_name".into(), json!(self.flow_name));
        }
        if !self.flow_version.is_empty() {
            m.insert("flow_version".into(), json!(self.flow_version));
        }
        if !self.flow_hash.is_empty() {
            m.insert("flow_hash".into(), json!(self.flow_hash));
        }
        if !self.flow_hash_algo.is_empty() {
            m.insert("flow_hash_algo".into(), json!(self.flow_hash_algo));
        }
        if !self.engine_version.is_empty() {
            m.insert("engine_version".into(), json!(self.engine_version));
        }
        if !self.tenant_id.is_empty() {
            m.insert("tenant_id".into(), json!(self.tenant_id));
        }
        m.insert("context".into(), self.context.clone());
        m.insert("status".into(), json!(self.status));
        m.insert("start_time".into(), json!(self.start_time));
        m.insert("last_update_time".into(), json!(self.last_update_time));
        if !self.end_time.is_empty() {
            m.insert("end_time".into(), json!(self.end_time));
        }
        if let Some(e) = &self.error {
            m.insert("error".into(), e.clone());
        }
        if !self.invoker.is_empty() {
            m.insert("invoker".into(), json!(self.invoker));
        }
        if let Some(v) = &self.node_timings {
            m.insert("node_timings".into(), v.clone());
        }
        if let Some(v) = &self.timeline {
            m.insert("timeline".into(), v.clone());
        }
        if let Some(v) = &self.usage {
            m.insert("usage".into(), v.clone());
        }
        if let Some(v) = &self.dispatch_pending {
            m.insert("dispatch_pending".into(), v.clone());
        }
        Value::Object(m)
    }

    pub fn from_json(v: &Value) -> Result<ExecutionState, WorkerError> {
        let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
        Ok(ExecutionState {
            execution_id: s("execution_id"),
            flow_id: s("flow_id"),
            flow_name: s("flow_name"),
            flow_version: s("flow_version"),
            flow_hash: s("flow_hash"),
            flow_hash_algo: s("flow_hash_algo"),
            engine_version: s("engine_version"),
            tenant_id: s("tenant_id"),
            context: v.get("context").cloned().unwrap_or(Value::Null),
            status: s("status"),
            start_time: s("start_time"),
            last_update_time: s("last_update_time"),
            end_time: s("end_time"),
            error: v.get("error").cloned().filter(|e| !e.is_null()),
            invoker: s("invoker"),
            node_timings: v.get("node_timings").cloned().filter(|e| !e.is_null()),
            timeline: v.get("timeline").cloned().filter(|e| !e.is_null()),
            usage: v.get("usage").cloned().filter(|e| !e.is_null()),
            dispatch_pending: v.get("dispatch_pending").cloned().filter(|e| !e.is_null()),
        })
    }
}

/// Persists the state row (§6.2): `fence_gen > 0` routes through the fenced
/// CAS Lua (0 = fence mismatch → ErrLeaseLost family "do not ack"; -1 =
/// fence/fencegen divergence → named partial-restore suspect); terminal rows
/// get the state TTL. fence_gen = 0 is ONLY for the no-lease deterministic
/// cancels (entry cancel on a row that does not exist yet).
pub fn save(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    state: &ExecutionState,
    fence_gen: i64,
) -> Result<(), WorkerError> {
    let blob = state.to_json().to_string();
    let state_key = keys::execution_state_key(cfg, tenant, &state.execution_id);
    let index_key = keys::execution_index_key(cfg, tenant);
    let score = start_time_score(&state.start_time);
    let terminal_ttl_secs = if is_terminal(&state.status) {
        cfg.state_ttl_days * 86400
    } else {
        0
    };
    if fence_gen > 0 {
        let n = backend
            .fenced_state_save(
                &state_key,
                &keys::fence_key(cfg, tenant, &state.execution_id),
                &index_key,
                &keys::fencegen_key(cfg, tenant, &state.execution_id),
                &blob,
                fence_gen,
                terminal_ttl_secs,
                score,
            )
            .map_err(|e| WorkerError::new(ErrorKind::StatePersist, e.to_string()))?;
        if n == -1 {
            return Err(WorkerError::new(
                ErrorKind::FenceGenerationInconsistent,
                format!(
                    "gen={fence_gen}; state write refused — fence matched but the fencegen \
                     counter disagrees (partial restore suspected)"
                ),
            ));
        }
        if n == 0 {
            return Err(WorkerError::new(
                ErrorKind::LeaseLost,
                format!("fence generation mismatch (gen={fence_gen})"),
            ));
        }
        return Ok(());
    }
    // Plain write (fence_gen == 0 — no-lease deterministic terminal only).
    backend
        .set(&state_key, &blob, Some(terminal_ttl_secs.max(1)))
        .map_err(|e| WorkerError::new(ErrorKind::StatePersist, e.to_string()))?;
    backend
        .zadd(&index_key, score, &state_key)
        .map_err(|e| WorkerError::new(ErrorKind::StatePersist, e.to_string()))?;
    Ok(())
}

/// Reads the state row. Missing → Ok(None); a present-but-corrupt row or a
/// Redis failure → Err(StateLoad), never swallowed as "missing" (§6.2).
pub fn load(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
) -> Result<Option<ExecutionState>, WorkerError> {
    let raw = backend
        .get(&keys::execution_state_key(cfg, tenant, execution_id))
        .map_err(|e| WorkerError::new(ErrorKind::StateLoad, e.to_string()))?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let v: Value = serde_json::from_str(&raw).map_err(|e| {
        WorkerError::new(
            ErrorKind::StateLoad,
            format!("corrupt state json for {execution_id}: {e}"),
        )
    })?;
    ExecutionState::from_json(&v)
        .map(Some)
        .map_err(|e| WorkerError::new(ErrorKind::StateLoad, e.to_string()))
}

/// Reads the cancel flag; Some(value) carries the flag (ISO cancel time).
pub fn is_cancel_requested(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
) -> Result<Option<String>, WorkerError> {
    backend
        .get(&keys::cancel_key(cfg, tenant, execution_id))
        .map_err(|e| WorkerError::transient(format!("cancel probe: {e}")))
}

/// start_time ISO → epoch-seconds score for the index ZSET (§6.2 documented
/// ambiguity: the ZADD needs a number, so the epoch is written).
fn start_time_score(start_time: &str) -> f64 {
    chrono::NaiveDateTime::parse_from_str(start_time, "%Y-%m-%dT%H:%M:%S%.f")
        .map(|t| t.and_utc().timestamp_millis() as f64 / 1000.0)
        .unwrap_or_else(|_| crate::config::float_epoch())
}

/// Builds the worker-judged terminal error row value.
pub fn worker_terminal_error(
    error_type: &str,
    code: i64,
    message: &str,
    details: Option<Value>,
) -> Value {
    terminal_error_state(error_type, code, message, details, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;

    fn cfg() -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                "state-test".to_string()
            } else {
                String::new()
            }
        })
    }

    #[test]
    fn fenced_save_and_load_roundtrip() {
        let cfg = cfg();
        let backend = FakeRedis::new();
        let lease = crate::lease::acquire(&backend, &cfg, "", "e9", "start:i:aa", 120).unwrap();
        let mut state = ExecutionState::new_running(&cfg, "f1", &json!({"a": 1}), "e9", "");
        state.flow_hash = "abc".into();
        state.flow_hash_algo = "algo".into();
        save(&backend, &cfg, "", &state, lease.fence_token).unwrap();
        let loaded = load(&backend, &cfg, "", "e9").unwrap().unwrap();
        assert_eq!(loaded.status, STATUS_RUNNING);
        assert_eq!(loaded.flow_hash, "abc");
        assert_eq!(loaded.context["input"]["a"], 1);
        assert!(loaded.end_time.is_empty());
        // missing row is None; corrupt row is a named error
        assert!(load(&backend, &cfg, "", "nope").unwrap().is_none());
        backend
            .set(
                &keys::execution_state_key(&cfg, "", "bad"),
                "{not json",
                None,
            )
            .unwrap();
        let err = load(&backend, &cfg, "", "bad").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::StateLoad);
    }

    #[test]
    fn fence_mismatch_refuses_write() {
        let cfg = cfg();
        let backend = FakeRedis::new();
        // the fence keys are PER EXECUTION: a stale token arises when the
        // SAME execution is re-acquired (the fencegen counter is never
        // cleared) — the old holder's write must be refused
        let lease1 = crate::lease::acquire(&backend, &cfg, "", "e1", "start:i:aa", 120).unwrap();
        crate::lease::release_with(&backend, &cfg, &lease1).unwrap();
        let lease2 = crate::lease::acquire(&backend, &cfg, "", "e1", "resume:i:bb", 120).unwrap();
        assert_eq!(lease2.fence_token, lease1.fence_token + 1);
        let state = ExecutionState::new_running(&cfg, "f", &json!({}), "e1", "");
        // the LIVE holder writes fine…
        save(&backend, &cfg, "", &state, lease2.fence_token).unwrap();
        // …the stale token is refused (fence key moved on)
        let stale_state =
            ExecutionState::new_running(&cfg, "f", &json!({"clobber": true}), "e1", "");
        let err = save(&backend, &cfg, "", &stale_state, lease1.fence_token).unwrap_err();
        assert!(
            matches!(
                err.kind(),
                ErrorKind::LeaseLost | ErrorKind::FenceGenerationInconsistent
            ),
            "stale write must be refused, got {err:?}"
        );
    }

    #[test]
    fn cancel_flag_probe() {
        let cfg = cfg();
        let backend = FakeRedis::new();
        assert!(is_cancel_requested(&backend, &cfg, "", "e1")
            .unwrap()
            .is_none());
        backend
            .set(
                &keys::cancel_key(&cfg, "", "e1"),
                "2026-10-23T00:00:00",
                None,
            )
            .unwrap();
        assert!(is_cancel_requested(&backend, &cfg, "", "e1")
            .unwrap()
            .is_some());
    }
}
