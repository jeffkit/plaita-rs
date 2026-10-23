//! Checkpoint persistence (§7, port of checkpointstore.go + the rs
//! serialization boundary §7.2):
//!
//! Key: `{ns}:execution:checkpoint:{id}`; no TTL while suspended ("a human
//! approval can wait arbitrarily long; the terminal delete is what bounds
//! the key's life").
//!
//! The worker persists the four-tuple "flow document reference + context
//! snapshot + suspended node + subscription info" — NOT Rust objects.
//! Recovery rebuilds through `ir::load` + `RuntimeCtx::from_snapshot_json` +
//! `SuspendedExecution::from_parts`. The `context_flavor` field is the
//! fail-closed gate: a checkpoint written by another engine (Go's
//! `$INPUT/$NODE` context shape) is named-rejected on load, never silently
//! rebuilt into an empty context "resuming from the wrong place".

use serde_json::Value;

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::error::{ErrorKind, WorkerError};
use crate::keys;

/// The checkpoint envelope version this build writes; loaders reject
/// unknown/newer versions (fail-closed).
pub const CHECKPOINT_SCHEMA_VERSION: i64 = 1;

/// `kind` — M1–M3 always `suspend`; `per-step` is Go #32's reserved word and
/// is never written by this build.
pub const CHECKPOINT_KIND_SUSPEND: &str = "suspend";

/// The fail-closed context marker: only snapshots in THIS shape (the
/// engine's nine-root snapshot) may be rebuilt.
pub const CONTEXT_FLAVOR: &str = "plaita-rs-snapshot-v1";

/// The checkpoint record (§7.2; envelope fields match Go
/// `workflow.Checkpoint` one-for-one, `context` carries the rs snapshot +
/// flavor marker).
#[derive(Debug, Clone)]
pub struct CheckpointRecord {
    pub schema_version: i64,
    pub kind: String,
    pub execution_id: String,
    pub tenant_id: String,
    pub flow_id: String,
    pub suspended_node: String,
    pub next_node: String,
    /// The suspend boundary fixes this to "event" (Go legacy snapshot
    /// shape).
    pub resume_type: String,
    pub context_flavor: String,
    /// The nine-root engine snapshot (plaita-rs-snapshot-v1).
    pub context: Value,
    pub created_at: String,
}

impl CheckpointRecord {
    /// Builds a fresh suspend checkpoint from a suspended execution.
    pub fn suspend(
        execution_id: &str,
        tenant: &str,
        flow_id: &str,
        suspended_node: &str,
        context: Value,
    ) -> CheckpointRecord {
        CheckpointRecord {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            kind: CHECKPOINT_KIND_SUSPEND.to_string(),
            execution_id: execution_id.to_string(),
            tenant_id: tenant.to_string(),
            flow_id: flow_id.to_string(),
            suspended_node: suspended_node.to_string(),
            next_node: String::new(),
            resume_type: "event".to_string(),
            context_flavor: CONTEXT_FLAVOR.to_string(),
            context,
            created_at: now_iso(),
        }
    }

    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "schema_version": self.schema_version,
            "kind": self.kind,
            "execution_id": self.execution_id,
            "tenant_id": self.tenant_id,
            "flow_id": self.flow_id,
            "suspended_node": self.suspended_node,
            "next_node": self.next_node,
            "resume_type": self.resume_type,
            "context_flavor": self.context_flavor,
            "context": self.context,
            "created_at": self.created_at,
        })
    }

    pub fn from_json(v: &Value) -> Result<CheckpointRecord, WorkerError> {
        let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
        let schema_version = v
            .get("schema_version")
            .and_then(|x| x.as_i64())
            .unwrap_or(0);
        if schema_version <= 0 || schema_version > CHECKPOINT_SCHEMA_VERSION {
            return Err(WorkerError::new(
                ErrorKind::StateLoad,
                format!(
                    "checkpoint schema version {schema_version} not supported (engine supports \
                     1..={CHECKPOINT_SCHEMA_VERSION})"
                ),
            ));
        }
        Ok(CheckpointRecord {
            schema_version,
            kind: s("kind"),
            execution_id: s("execution_id"),
            tenant_id: s("tenant_id"),
            flow_id: s("flow_id"),
            suspended_node: s("suspended_node"),
            next_node: s("next_node"),
            resume_type: s("resume_type"),
            context_flavor: s("context_flavor"),
            context: v.get("context").cloned().unwrap_or(Value::Null),
            created_at: s("created_at"),
        })
    }
}

/// Saves the checkpoint (overwrites a previous boundary). Missing execution
/// id is a persist error.
pub fn save(
    backend: &dyn RedisBackend,
    cfg: &Config,
    cp: &CheckpointRecord,
) -> Result<(), WorkerError> {
    if cp.execution_id.is_empty() {
        return Err(WorkerError::new(
            ErrorKind::StatePersist,
            "checkpoint has no execution id",
        ));
    }
    backend
        .set(
            &keys::checkpoint_key(cfg, &cp.tenant_id, &cp.execution_id),
            &cp.to_json().to_string(),
            None,
        )
        .map_err(|e| WorkerError::new(ErrorKind::StatePersist, format!("checkpoint set: {e}")))
}

/// Reads the checkpoint. Missing → Ok(None); corrupt JSON, a newer schema
/// or an UNKNOWN context flavor → Err(StateLoad) — never silently "missing"
/// ("otherwise resume would start from the wrong place").
pub fn load(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
) -> Result<Option<CheckpointRecord>, WorkerError> {
    let raw = backend
        .get(&keys::checkpoint_key(cfg, tenant, execution_id))
        .map_err(|e| WorkerError::new(ErrorKind::StateLoad, format!("checkpoint get: {e}")))?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let v: Value = serde_json::from_str(&raw).map_err(|e| {
        WorkerError::new(
            ErrorKind::StateLoad,
            format!("corrupt checkpoint json for {execution_id}: {e}"),
        )
    })?;
    let cp = CheckpointRecord::from_json(&v)?;
    if cp.context_flavor != CONTEXT_FLAVOR {
        // Named cross-engine rejection (§7.2): a Go-written checkpoint
        // (context is `$INPUT/$NODE` shaped, no flavor) must not be rebuilt
        // by this engine.
        return Err(WorkerError::new(
            ErrorKind::StateLoad,
            format!(
                "checkpoint_context_flavor_unknown: execution {execution_id} carries context \
                 flavor {:?}, this engine only rebuilds {CONTEXT_FLAVOR:?} (cross-engine \
                 resume is rejected by design — the flow fingerprint gate would refuse it \
                 anyway)",
                cp.context_flavor
            ),
        ));
    }
    Ok(Some(cp))
}

/// Removes the checkpoint (idempotent; terminal cleanup).
pub fn delete(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
) -> Result<(), WorkerError> {
    if execution_id.is_empty() {
        return Ok(());
    }
    backend
        .del(&[&keys::checkpoint_key(cfg, tenant, execution_id)])
        .map_err(|e| WorkerError::transient(format!("checkpoint delete: {e}")))?;
    Ok(())
}

/// The §7.3 store trait (host-embedding face; the worker paths use the same
/// key logic through the free functions).
pub trait CheckpointStore: Send + Sync {
    fn save(&self, cp: &CheckpointRecord) -> Result<(), WorkerError>;
    fn load(
        &self,
        tenant_id: &str,
        execution_id: &str,
    ) -> Result<Option<CheckpointRecord>, WorkerError>;
    fn delete(&self, tenant_id: &str, execution_id: &str) -> Result<(), WorkerError>;
}

/// The Redis implementation (checkpointstore.go 的直译外壳).
pub struct RedisCheckpointStore {
    backend: std::sync::Arc<dyn crate::backend::RedisBackend>,
    cfg: Config,
}

impl RedisCheckpointStore {
    pub fn new(
        backend: std::sync::Arc<dyn crate::backend::RedisBackend>,
        cfg: Config,
    ) -> RedisCheckpointStore {
        RedisCheckpointStore { backend, cfg }
    }
}

impl CheckpointStore for RedisCheckpointStore {
    fn save(&self, cp: &CheckpointRecord) -> Result<(), WorkerError> {
        save(self.backend.as_ref(), &self.cfg, cp)
    }

    fn load(
        &self,
        tenant_id: &str,
        execution_id: &str,
    ) -> Result<Option<CheckpointRecord>, WorkerError> {
        load(self.backend.as_ref(), &self.cfg, tenant_id, execution_id)
    }

    fn delete(&self, tenant_id: &str, execution_id: &str) -> Result<(), WorkerError> {
        delete(self.backend.as_ref(), &self.cfg, tenant_id, execution_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;
    use serde_json::json;

    fn cfg() -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                "cp-test".to_string()
            } else {
                String::new()
            }
        })
    }

    #[test]
    fn save_load_delete_roundtrip_and_no_ttl() {
        let cfg = cfg();
        let backend = FakeRedis::new();
        let cp = CheckpointRecord::suspend(
            "e1",
            "",
            "f1",
            "wait",
            json!({"input": {}, "nodes": {}, "errors": {}, "env": {}, "global": {},
                  "last_node": null, "last_branch": null, "flow_id": "f1", "execution_id": "e1"}),
        );
        save(&backend, &cfg, &cp).unwrap();
        let loaded = load(&backend, &cfg, "", "e1").unwrap().unwrap();
        assert_eq!(loaded.kind, "suspend");
        assert_eq!(loaded.resume_type, "event");
        assert_eq!(loaded.context_flavor, CONTEXT_FLAVOR);
        assert_eq!(loaded.context["execution_id"], "e1");
        // suspended keys carry NO TTL
        assert_eq!(
            backend
                .ttl_ms(&keys::checkpoint_key(&cfg, "", "e1"))
                .unwrap(),
            None,
            "no TTL while suspended"
        );
        delete(&backend, &cfg, "", "e1").unwrap();
        assert!(load(&backend, &cfg, "", "e1").unwrap().is_none());
        // delete is idempotent
        delete(&backend, &cfg, "", "e1").unwrap();
    }

    #[test]
    fn foreign_flavor_is_named_rejected() {
        let cfg = cfg();
        let backend = FakeRedis::new();
        // a Go-shaped checkpoint: no context_flavor, $INPUT/$NODE context
        backend
            .set(
                &keys::checkpoint_key(&cfg, "", "e2"),
                &json!({
                    "schema_version": 1,
                    "kind": "suspend",
                    "execution_id": "e2",
                    "suspended_node": "wait",
                    "resume_type": "event",
                    "context": {"$INPUT": {}, "$NODE": {}, "$LAST_NODE": "s"},
                    "created_at": "2026-10-23T00:00:00"
                })
                .to_string(),
                None,
            )
            .unwrap();
        let err = load(&backend, &cfg, "", "e2").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::StateLoad);
        assert!(
            err.to_string()
                .contains("checkpoint_context_flavor_unknown"),
            "named flavor rejection expected, got: {err}"
        );
        // newer schema → named rejection, never "missing"
        backend
            .set(
                &keys::checkpoint_key(&cfg, "", "e3"),
                &json!({"schema_version": 99, "context": {}}).to_string(),
                None,
            )
            .unwrap();
        assert_eq!(
            load(&backend, &cfg, "", "e3").unwrap_err().kind(),
            ErrorKind::StateLoad
        );
        // missing key is Ok(None)
        assert!(load(&backend, &cfg, "", "none").unwrap().is_none());
    }
}
