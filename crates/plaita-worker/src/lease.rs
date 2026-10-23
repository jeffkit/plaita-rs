//! Execution lease protocol (§5, port of plaita-go lease.go): the three
//! Lua scripts (fenced acquire / CAS release / CAS renew) run through the
//! backend's named script methods — the verbatim script texts live in
//! `redis_impl`; their semantics must not be "improved".

use crate::backend::RedisBackend;
use crate::config::{Config, FENCE_KEY_TTL_SECONDS};
use crate::error::{ErrorKind, WorkerError};
use crate::keys;

/// Everything the worker needs to release/renew/CAS-write under a held
/// lease.
#[derive(Debug, Clone)]
pub struct LeaseHandle {
    pub execution_id: String,
    pub tenant_id: String,
    pub holder: String,
    /// The full `{holder}:{gen}` string stored in Redis — the compare token
    /// for release/renew (§5.1).
    pub lease_value: String,
    /// The acquired fence generation (>0).
    pub fence_token: i64,
}

/// Fenced acquire. `Err(LeaseConflict)` when another holder owns the lease.
pub fn acquire(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
    holder: &str,
    ttl_seconds: i64,
) -> Result<LeaseHandle, WorkerError> {
    let gen = backend
        .lease_acquire(
            &keys::lease_key(cfg, tenant, execution_id),
            &keys::fence_key(cfg, tenant, execution_id),
            &keys::fencegen_key(cfg, tenant, execution_id),
            holder,
            ttl_seconds,
            FENCE_KEY_TTL_SECONDS,
        )
        .map_err(WorkerError::from)
        .map_err(|e| WorkerError::transient(format!("lease acquire {execution_id}: {e}")))?;
    if gen == 0 {
        return Err(WorkerError::new(
            ErrorKind::LeaseConflict,
            format!("execution lease held by another worker ({execution_id})"),
        ));
    }
    Ok(LeaseHandle {
        execution_id: execution_id.to_string(),
        tenant_id: tenant.to_string(),
        holder: holder.to_string(),
        lease_value: format!("{}:{}", holder, gen),
        fence_token: gen,
    })
}

/// Deletes the lease only when the full value matches (no-op → 0); the key
/// derivation needs the namespace, so release is cfg-carrying.
pub fn release_with(
    backend: &dyn RedisBackend,
    cfg: &Config,
    handle: &LeaseHandle,
) -> Result<(), WorkerError> {
    backend
        .lease_release(
            &keys::lease_key(cfg, &handle.tenant_id, &handle.execution_id),
            &handle.lease_value,
        )
        .map_err(|e| WorkerError::transient(format!("lease release: {e}")))?;
    Ok(())
}

/// Refreshes the lease TTL when the value still matches; `false` is the
/// lost-lease signal (§5.3 watchdog — M2 wires the watchdog loop).
pub fn renew(
    backend: &dyn RedisBackend,
    cfg: &Config,
    handle: &LeaseHandle,
    ttl_seconds: i64,
) -> Result<bool, WorkerError> {
    let n = backend
        .lease_renew(
            &keys::lease_key(cfg, &handle.tenant_id, &handle.execution_id),
            &handle.lease_value,
            ttl_seconds,
        )
        .map_err(|e| WorkerError::transient(format!("lease renew: {e}")))?;
    Ok(n == 1)
}

/// Returns the current full lease value, or None when absent.
pub fn get_holder(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    execution_id: &str,
) -> Result<Option<String>, WorkerError> {
    backend
        .get(&keys::lease_key(cfg, tenant, execution_id))
        .map_err(|e| WorkerError::transient(format!("lease probe: {e}")))
}

/// Builds the lease holder token (§5.2): `{prefix}:{instance_id}:` +
/// 16 random hex chars — the last segment is ALWAYS exactly 16 hex chars
/// (wire format for holder_instance_id parsing, #155). Prefix ∈
/// {start, resume, worker, dispatch, handover}.
pub fn new_holder_token(prefix: &str, instance_id: &str) -> String {
    if instance_id.is_empty() {
        return format!("{prefix}:{}", random_hex16());
    }
    format!("{prefix}:{instance_id}:{}", random_hex16())
}

fn random_hex16() -> String {
    let id = uuid::Uuid::new_v4();
    let hex = id.simple().to_string();
    hex[..16].to_string()
}

/// Reverse-parses a lease value back to the holder's registry instance id
/// (§5.2): 3 segments with the first in (start, resume, worker), the last
/// exactly 16 hex; a 4th trailing segment (the fence generation) is
/// dropped. Unparsable = (None, false) — liveness unknown, conservative
/// park.
pub fn holder_instance_id(lease_value: &str) -> Option<String> {
    if lease_value.is_empty() {
        return None;
    }
    let mut parts: Vec<&str> = lease_value.split(':').collect();
    if parts.len() == 4 {
        parts.truncate(3); // drop the trailing generation
    }
    if parts.len() != 3 {
        return None;
    }
    match parts[0] {
        "start" | "resume" | "worker" => {}
        _ => return None,
    }
    if parts[1].is_empty() {
        return None;
    }
    let last = parts[2];
    if last.len() != 16 || !last.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(parts[1].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;

    #[test]
    fn holder_token_wire_format() {
        let token = new_holder_token("start", "worker-7");
        let parts: Vec<&str> = token.split(':').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "start");
        assert_eq!(parts[1], "worker-7");
        assert_eq!(parts[2].len(), 16);
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(holder_instance_id(&token).as_deref(), Some("worker-7"));
        // fenced value ({holder}:{gen}) keeps parsing — trailing gen dropped
        assert_eq!(
            holder_instance_id(&format!("{token}:42")).as_deref(),
            Some("worker-7")
        );
        // legacy {holder}:{gen} (numeric last segment) is NOT parseable
        assert_eq!(holder_instance_id("resume:12345"), None);
        assert_eq!(holder_instance_id("resume:worker-7:zz"), None);
        assert_eq!(holder_instance_id(""), None);
    }

    #[test]
    fn acquire_release_renew_roundtrip() {
        let cfg = Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                "lease-test".to_string()
            } else {
                String::new()
            }
        });
        let backend = FakeRedis::new();
        let h1 = acquire(&backend, &cfg, "", "e1", "start:i:a", 120).unwrap();
        // conflict for a second acquirer
        let err = acquire(&backend, &cfg, "", "e1", "start:i:b", 120).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::LeaseConflict);
        assert!(renew(&backend, &cfg, &h1, 120).unwrap());
        release_with(&backend, &cfg, &h1).unwrap();
        // after release the other holder wins
        let h2 = acquire(&backend, &cfg, "", "e1", "resume:i:b", 120).unwrap();
        // the released handle can no longer renew (value mismatch → 0)
        assert!(!renew(&backend, &cfg, &h1, 120).unwrap());
        release_with(&backend, &cfg, &h2).unwrap();
    }
}
