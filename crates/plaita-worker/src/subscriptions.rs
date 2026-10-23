//! Wake-up subscription storage (§8.1, port of eventbus.go's
//! EventSubscriptions): the engine-side `SubscriptionRegistry` trait,
//! backed by Redis so ANY worker (and any process restart) can resolve a
//! suspension.
//!
//! Keys (§3):
//! ```text
//! {ns}:event:subscriptions:{subscription_id}       = subscription JSON (TTL 7d)
//! {ns}:event:subscriptions:type:{event_type}       = SET of subscription ids
//! {prefix}:event:subscriptions:timeouts            = ZSET sub-key by due time
//! ```
//! Record JSON fields match plaita-go `EventSubscription` one-for-one —
//! a subscription written by any engine is fully readable by the others'
//! event bridges.

use std::sync::Arc;

use plaita_engine::exec::{SubscriptionRegistry, SubscriptionRequest};
use serde_json::{json, Value};

use crate::backend::{RedisBackend, TxOp};
use crate::config::{float_epoch, now_iso, Config};
use crate::error::{ErrorKind, WorkerError};
use crate::keys;

const SUBSCRIPTION_SCHEMA_VERSION: i64 = 1;

/// The persisted subscription record (fields = plaita-go EventSubscription).
#[derive(Debug, Clone)]
pub struct EventSubscription {
    pub schema_version: i64,
    pub subscription_id: String,
    pub event_type: String,
    pub filter: Value,
    pub correlation_id: String,
    pub flow_id: String,
    pub node_id: String,
    pub tenant_id: String,
    pub created_at: String,
    pub timeout: f64,
    pub timeout_at: f64,
}

impl EventSubscription {
    fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("schema_version".into(), json!(self.schema_version));
        m.insert("subscription_id".into(), json!(self.subscription_id));
        m.insert("event_type".into(), json!(self.event_type));
        if let Value::Object(o) = &self.filter {
            if !o.is_empty() {
                m.insert("filter".into(), self.filter.clone());
            }
        }
        if !self.correlation_id.is_empty() {
            m.insert("correlation_id".into(), json!(self.correlation_id));
        }
        if !self.flow_id.is_empty() {
            m.insert("flow_id".into(), json!(self.flow_id));
        }
        if !self.node_id.is_empty() {
            m.insert("node_id".into(), json!(self.node_id));
        }
        if !self.tenant_id.is_empty() {
            m.insert("tenant_id".into(), json!(self.tenant_id));
        }
        m.insert("created_at".into(), json!(self.created_at));
        if self.timeout > 0.0 {
            m.insert("timeout".into(), json!(self.timeout));
        }
        if self.timeout_at > 0.0 {
            m.insert("timeout_at".into(), json!(self.timeout_at));
        }
        Value::Object(m)
    }

    pub(crate) fn from_json(v: &Value) -> Option<EventSubscription> {
        Some(EventSubscription {
            schema_version: v.get("schema_version")?.as_i64()?,
            subscription_id: v.get("subscription_id")?.as_str()?.to_string(),
            event_type: v.get("event_type")?.as_str()?.to_string(),
            filter: v.get("filter").cloned().unwrap_or(Value::Null),
            correlation_id: v
                .get("correlation_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            flow_id: v
                .get("flow_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            node_id: v
                .get("node_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            tenant_id: v
                .get("tenant_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            created_at: v
                .get("created_at")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            timeout: v.get("timeout").and_then(|x| x.as_f64()).unwrap_or(0.0),
            timeout_at: v.get("timeout_at").and_then(|x| x.as_f64()).unwrap_or(0.0),
        })
    }
}

/// The Redis-backed registry. Also used directly by the event dispatch
/// (§8.2) and the timeout checker (§8.3).
pub struct RedisSubscriptions {
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
}

impl RedisSubscriptions {
    pub fn new(backend: Arc<dyn RedisBackend>, cfg: Config) -> RedisSubscriptions {
        RedisSubscriptions { backend, cfg }
    }

    /// Returns one subscription, or None when absent. Corrupt = named
    /// error, never "missing" (§8.1).
    pub fn get(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<EventSubscription>, WorkerError> {
        let raw = self
            .backend
            .get(&keys::subscription_key(&self.cfg, tenant, subscription_id))
            .map_err(|e| WorkerError::transient(format!("subscription get: {e}")))?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let v: Value = serde_json::from_str(&raw).map_err(|e| {
            WorkerError::new(
                ErrorKind::StateLoad,
                format!("corrupt event subscription json ({subscription_id}): {e}"),
            )
        })?;
        EventSubscription::from_json(&v).map(Some).ok_or_else(|| {
            WorkerError::new(
                ErrorKind::StateLoad,
                format!("corrupt event subscription json ({subscription_id})"),
            )
        })
    }

    /// Lists the subscription ids registered for an event type.
    pub fn subscription_ids(
        &self,
        tenant: &str,
        event_type: &str,
    ) -> Result<Vec<String>, WorkerError> {
        self.backend
            .smembers(&keys::subscription_type_key(&self.cfg, tenant, event_type))
            .map_err(|e| WorkerError::transient(format!("subscription lookup: {e}")))
    }

    /// The timeouts-index key (namespace-wide).
    pub fn timeouts_key(&self) -> String {
        keys::subscription_timeouts_key(&self.cfg)
    }

    /// Drops one member from the timeout index together with its requeue
    /// attempts bookkeeping (#50; best effort).
    pub fn drop_timeout_index_member(&self, sub_key: &str) {
        let _ = self.backend.tx(&[TxOp::Zrem {
            key: self.timeouts_key(),
            member: sub_key.to_string(),
        }]);
        let _ = self
            .backend
            .hdel(&keys::subscription_timeout_attempts_key(&self.cfg), sub_key);
    }
}

impl SubscriptionRegistry for RedisSubscriptions {
    /// Persists a subscription and (when timeout_seconds > 0) arms the
    /// timeout index. The subscription id is minted here when the engine
    /// left it empty; the engine copies the assigned id into the pending
    /// node state so a resume carrier can be bound to it.
    fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String> {
        if req.event_type.trim().is_empty() {
            return Err("event subscription requires a non-empty event type".to_string());
        }
        if req.subscription_id.is_empty() {
            req.subscription_id = crate::config::new_execution_id();
        }
        let now = float_epoch();
        let sub = EventSubscription {
            schema_version: SUBSCRIPTION_SCHEMA_VERSION,
            subscription_id: req.subscription_id.clone(),
            event_type: req.event_type.clone(),
            filter: req.filter.clone(),
            correlation_id: req.correlation_id.clone(),
            flow_id: req.flow_id.clone(),
            node_id: req.node_id.clone(),
            tenant_id: req.tenant_id.clone(),
            created_at: now_iso(),
            timeout: req.timeout_seconds,
            timeout_at: if req.timeout_seconds > 0.0 {
                now + req.timeout_seconds
            } else {
                0.0
            },
        };
        let ttl = self.cfg.event_ttl_seconds;
        let type_key = keys::subscription_type_key(&self.cfg, &sub.tenant_id, &sub.event_type);
        let mut ops = vec![
            TxOp::Set {
                key: keys::subscription_key(&self.cfg, &sub.tenant_id, &sub.subscription_id),
                value: sub.to_json().to_string(),
                ttl_secs: Some(ttl),
            },
            TxOp::Sadd {
                key: type_key.clone(),
                member: sub.subscription_id.clone(),
            },
            TxOp::Expire {
                key: type_key,
                ttl_secs: ttl,
            },
        ];
        if sub.timeout_at > 0.0 {
            ops.push(TxOp::Zadd {
                key: self.timeouts_key(),
                score: sub.timeout_at,
                member: keys::subscription_key(&self.cfg, &sub.tenant_id, &sub.subscription_id),
            });
        }
        self.backend
            .tx(&ops)
            .map_err(|e| format!("event subscription persist: {e}"))
    }

    /// Removes a subscription (idempotent). tenant is required to address
    /// the namespaced key.
    fn unregister(&self, subscription_id: &str, tenant: &str) -> Result<(), String> {
        if subscription_id.is_empty() {
            return Ok(());
        }
        let sub_key = keys::subscription_key(&self.cfg, tenant, subscription_id);
        let raw = self.backend.get(&sub_key).map_err(|e| e.to_string())?;
        let mut ops = vec![
            TxOp::Del {
                key: sub_key.clone(),
            },
            TxOp::Zrem {
                key: self.timeouts_key(),
                member: sub_key,
            },
        ];
        if let Some(raw) = raw {
            if let Some(sub) = serde_json::from_str::<Value>(&raw)
                .ok()
                .and_then(|v| EventSubscription::from_json(&v))
            {
                if !sub.event_type.is_empty() {
                    ops.push(TxOp::Srem {
                        key: keys::subscription_type_key(
                            &self.cfg,
                            &sub.tenant_id,
                            &sub.event_type,
                        ),
                        member: subscription_id.to_string(),
                    });
                }
            }
        }
        self.backend.tx(&ops).map_err(|e| e.to_string())
    }
}
