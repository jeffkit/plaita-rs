//! Event → resume-carrier dispatch (§8.2/§8.3, port of eventbus.go +
//! plaita's event_filter.py semantics) and the Pub/Sub bridge.
//!
//! Tenant discipline: the tenant comes from the trusted top-level
//! `event.tenant_id` (bound by the publisher) — NEVER from `event.data`
//! ("把能给谁注入 resume 的开关交给事件内容" is the historical defect this
//! closes).
//!
//! Timeout sweep: the due index is consumed as a ONE-SHOT queue (#50) —
//! every exit either drops the member or parks it outside the due window
//! under an attempts ceiling, so a stuck member can never pin the page.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use plaita_engine::exec::SubscriptionRegistry;
use serde_json::{json, Value};

use crate::backend::RedisBackend;
use crate::config::{float_epoch, now_iso, Config};
use crate::error::{ErrorKind, WorkerError};
use crate::keys;
use crate::queue::TaskQueue;
use crate::state;
use crate::subscriptions::{EventSubscription, RedisSubscriptions};

/// A business event (Pub/Sub payload shape).
#[derive(Debug, Clone, Default)]
pub struct Event {
    pub event_id: String,
    pub event_type: String,
    pub correlation_id: String,
    pub tenant_id: String,
    pub data: Value,
}

/// Timeout sweep tuning (#50): a member whose execution state is momentarily
/// unreadable is parked past the due window (delay) and may come back at
/// most `timeout_requeue_attempts` sweeps before it is dropped with a
/// warning.
const TIMEOUT_REQUEUE_DELAY_SECS: f64 = 30.0;
const TIMEOUT_SWEEP_PAGE_SIZE: i64 = 100;
const TIMEOUT_SWEEP_MAX_PASSES: i64 = 100;
const TIMEOUT_REQUEUE_ATTEMPTS: i64 = 5;

/// Resolves one business event into resume carriers (§8.2 EventFilter
/// semantics). Errors from Redis surfaces are returned; per-subscription
/// failures warn + skip.
pub fn handle_event(
    backend: &Arc<dyn RedisBackend>,
    cfg: &Config,
    queue: &TaskQueue,
    subs: &RedisSubscriptions,
    ev: &Event,
) -> Result<(), WorkerError> {
    if ev.event_type.trim().is_empty() || ev.event_id.is_empty() {
        return Err(WorkerError::new(
            ErrorKind::InvalidPayload,
            format!(
                "event requires event_id and event_type (got id={:?} type={:?})",
                ev.event_id, ev.event_type
            ),
        ));
    }
    if ev.correlation_id.is_empty() {
        // No execution to wake — same skip as EventFilter.
        return Ok(());
    }
    let tenant = &ev.tenant_id;
    let ids = subs
        .subscription_ids(tenant, &ev.event_type)
        .map_err(|e| WorkerError::transient(format!("event subscription lookup: {e}")))?;
    if ids.is_empty() {
        return Ok(());
    }
    for subscription_id in ids {
        let sub = match subs.get(tenant, &subscription_id) {
            Ok(Some(s)) => s,
            Ok(None) => continue,
            Err(e) => {
                eprintln!(
                    "[events] subscription read failed, skipping (subscription_id={subscription_id}): {e}"
                );
                continue;
            }
        };
        if !sub.correlation_id.is_empty() && sub.correlation_id != ev.correlation_id {
            continue;
        }
        if !subscription_filter_matches(&sub.filter, &ev.data) {
            continue;
        }
        let state_row = match state::load(backend.as_ref(), cfg, tenant, &ev.correlation_id) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "[events] state load failed, skipping (execution_id={}): {e}",
                    ev.correlation_id
                );
                continue;
            }
        };
        let Some(row) = state_row else {
            continue;
        };
        if !same_tenant(&row.tenant_id, tenant, &cfg.default_tenant_id) {
            // Depth defense (§8.2): the state row must belong to the event's
            // trusted tenant — mismatch is dropped loudly, never routed.
            eprintln!(
                "[events] event tenant does not match execution state, dropping \
                 (event_id={}, tenant={}, state_tenant={}, execution_id={})",
                ev.event_id, tenant, row.tenant_id, ev.correlation_id
            );
            continue;
        }
        if state::is_terminal(&row.status) {
            // Terminal executions must not accumulate dead subscriptions.
            let _ = subs.unregister(&subscription_id, tenant);
            continue;
        }
        let dedup_key = keys::event_dedup_key(cfg, &ev.event_id, &subscription_id);
        let first = backend
            .set_nx(&dedup_key, "1", 3600)
            .map_err(|e| WorkerError::transient(format!("event dedup claim: {e}")))?;
        if !first {
            continue;
        }
        let mut body = json!({
            "type": "resume",
            "flow_id": sub.flow_id,
            "execution_id": ev.correlation_id,
            "resume_type": "event",
            "timestamp": now_iso(),
            "data": {
                "event_id": ev.event_id,
                "event_type": ev.event_type,
                "event_data": ev.data,
                "subscription_id": subscription_id,
            },
        });
        if !tenant.is_empty() {
            body["tenant_id"] = json!(tenant);
        }
        if let Err(e) = queue.enqueue(&body, crate::config::TASK_SCHEMA_VERSION) {
            // #87: a claimed dedup key with no queued carrier would suppress
            // the event for an hour; release it so a retry can work.
            let _ = backend.del(&[&dedup_key]);
            return Err(WorkerError::transient(format!("event resume enqueue: {e}")));
        }
        eprintln!(
            "[events] event matched, resume carrier enqueued (event_id={}, event_type={}, \
             execution_id={}, subscription_id={})",
            ev.event_id, ev.event_type, ev.correlation_id, subscription_id
        );
    }
    Ok(())
}

/// Mirrors SubscriptionTimeoutChecker (§8.3): subscriptions whose deadline
/// elapsed enqueue a resume_type=timeout carrier (deduped 1h); terminal
/// executions get their leftover subscriptions reclaimed; the due index is
/// a one-shot queue (#50).
pub fn sweep_event_timeouts(
    backend: &Arc<dyn RedisBackend>,
    cfg: &Config,
    queue: &TaskQueue,
    subs: &RedisSubscriptions,
) -> Result<(), WorkerError> {
    let now = float_epoch();
    let attempts_key = keys::subscription_timeout_attempts_key(cfg);
    for _pass in 0..TIMEOUT_SWEEP_MAX_PASSES {
        let sub_keys = backend
            .zrangebyscore(&subs.timeouts_key(), 0.0, now, TIMEOUT_SWEEP_PAGE_SIZE)
            .map_err(|e| WorkerError::transient(format!("event timeout sweep: {e}")))?;
        if sub_keys.is_empty() {
            return Ok(());
        }
        for sub_key in sub_keys {
            let raw = match backend.get(&sub_key) {
                Ok(r) => r,
                Err(e) => {
                    return Err(WorkerError::transient(format!(
                        "event timeout subscription read: {e}"
                    )))
                }
            };
            let Some(raw) = raw else {
                // Record TTL expired — the member can never fire again.
                subs.drop_timeout_index_member(&sub_key);
                continue;
            };
            let subscription: Option<EventSubscription> = serde_json::from_str::<Value>(&raw)
                .ok()
                .and_then(|v| EventSubscription::from_json(&v));
            let Some(subscription) = subscription else {
                eprintln!(
                    "[events] corrupt subscription, dropping from timeout index (key={sub_key})"
                );
                subs.drop_timeout_index_member(&sub_key);
                continue;
            };
            if subscription.correlation_id.is_empty() {
                subs.drop_timeout_index_member(&sub_key);
                continue;
            }
            let state_row = state::load(
                backend.as_ref(),
                cfg,
                &subscription.tenant_id,
                &subscription.correlation_id,
            );
            let row = match state_row {
                Ok(r) => r,
                Err(e) => {
                    // #50: momentarily unreadable — park past the due window
                    // under an attempts ceiling instead of leaving it pinned
                    // at the front of the page.
                    let attempts = backend.hincr_by(&attempts_key, &sub_key, 1).map_err(|e| {
                        WorkerError::transient(format!("event timeout attempts bookkeeping: {e}"))
                    })?;
                    let _ = backend.expire(&attempts_key, 3600);
                    if attempts > TIMEOUT_REQUEUE_ATTEMPTS {
                        eprintln!(
                            "[events] state load keeps failing, dropping from timeout index \
                             (key={sub_key}, execution_id={}, attempts={attempts}): {e}",
                            subscription.correlation_id
                        );
                        subs.drop_timeout_index_member(&sub_key);
                    } else {
                        eprintln!(
                            "[events] state load failed, requeued for a later sweep \
                             (key={sub_key}, execution_id={}, attempts={attempts}): {e}",
                            subscription.correlation_id
                        );
                        let _ = backend.zadd(
                            &subs.timeouts_key(),
                            float_epoch() + TIMEOUT_REQUEUE_DELAY_SECS,
                            &sub_key,
                        );
                    }
                    continue;
                }
            };
            let Some(row) = row else {
                // The execution is gone — the subscription can never wake.
                subs.drop_timeout_index_member(&sub_key);
                continue;
            };
            if state::is_terminal(&row.status) {
                subs.drop_timeout_index_member(&sub_key);
                let _ = subs.unregister(&subscription.subscription_id, &subscription.tenant_id);
                continue;
            }
            let dedup_key = keys::timeout_dedup_key(cfg, &subscription.subscription_id);
            let first = backend
                .set_nx(&dedup_key, "1", 3600)
                .map_err(|e| WorkerError::transient(format!("event timeout dedup claim: {e}")))?;
            if !first {
                // The carrier already went out within the dedup window — the
                // member has served its purpose (#50).
                subs.drop_timeout_index_member(&sub_key);
                continue;
            }
            let mut body = json!({
                "type": "resume",
                "flow_id": subscription.flow_id,
                "execution_id": subscription.correlation_id,
                "resume_type": "timeout",
                "timestamp": now_iso(),
                "data": {
                    "subscription_id": subscription.subscription_id,
                    "event_type": subscription.event_type,
                    "node_id": subscription.node_id,
                },
            });
            if !subscription.tenant_id.is_empty() {
                body["tenant_id"] = json!(subscription.tenant_id);
            }
            if let Err(e) = queue.enqueue(&body, crate::config::TASK_SCHEMA_VERSION) {
                let _ = backend.del(&[&dedup_key]);
                return Err(WorkerError::transient(format!(
                    "event timeout resume enqueue: {e}"
                )));
            }
            // Drop only after the enqueue landed (#50): a failed enqueue
            // must leave the member in the index for the next sweep.
            subs.drop_timeout_index_member(&sub_key);
            eprintln!(
                "[events] subscription timed out, resume carrier enqueued \
                 (subscription_id={}, execution_id={})",
                subscription.subscription_id, subscription.correlation_id
            );
        }
    }
    Ok(())
}

/// One Pub/Sub bridge session → HandleEvent. Runs until `stop` flips or the
/// subscription fails; a failed session retries after the reconnect backoff
/// (a Redis restart must not permanently disable wake-ups). The window
/// between PSubscribe and the confirmation is closed INSIDE the backend
/// (real impl waits for the confirmation before returning the stream).
pub fn event_bridge_loop(
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    queue: Arc<TaskQueue>,
    subs: Arc<RedisSubscriptions>,
    stop: Arc<AtomicBool>,
) {
    let pattern = keys::event_bridge_pattern(&cfg);
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let mut stream = match backend.psubscribe(&pattern) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[events] bridge subscription failed (will retry): {e}");
                if sleep_interruptible(&stop, crate::config::RECONNECT_BACKOFF) {
                    return;
                }
                continue;
            }
        };
        loop {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            match stream.next_message(Duration::from_secs(1)) {
                None => continue, // timeout slice: re-check the stop flag
                Some((channel, payload)) => {
                    // The execution timeline channel shares the ":events:"
                    // shape with business events; it is NOT an EventBus
                    // channel and must never be parsed as one (§8.2).
                    if channel.contains(":execution:events:") {
                        continue;
                    }
                    let parsed: Option<Event> =
                        serde_json::from_str::<Value>(&payload).ok().map(|v| Event {
                            event_id: v
                                .get("event_id")
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .to_string(),
                            event_type: v
                                .get("event_type")
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .to_string(),
                            correlation_id: v
                                .get("correlation_id")
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .to_string(),
                            tenant_id: v
                                .get("tenant_id")
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .to_string(),
                            data: v.get("data").cloned().unwrap_or(Value::Null),
                        });
                    let Some(ev) = parsed else {
                        eprintln!("[events] bridge: undecodable event payload on {channel}");
                        continue;
                    };
                    if let Err(e) = handle_event(&backend, &cfg, &queue, &subs, &ev) {
                        eprintln!("[events] bridge: dispatch failed on {channel}: {e}");
                    }
                }
            }
        }
    }
}

/// Sleeps in shards so a stop flag interrupts within ~200ms (§11.1 D2
/// backoff discipline). Returns true when interrupted.
pub fn sleep_interruptible(stop: &AtomicBool, mut remaining: Duration) -> bool {
    let shard = Duration::from_millis(200);
    while remaining > Duration::ZERO {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        let d = remaining.min(shard);
        std::thread::sleep(d);
        remaining -= d;
    }
    stop.load(Ordering::SeqCst)
}

/// Applies plaita's dot-path filter to an event payload. An empty filter
/// matches.
pub fn subscription_filter_matches(filter: &Value, data: &Value) -> bool {
    let Some(map) = filter.as_object() else {
        return true;
    };
    if map.is_empty() {
        return true;
    }
    for (key, want) in map {
        let mut current = data;
        for segment in key.split('.') {
            let Some(next) = current.get(segment) else {
                return false;
            };
            current = next;
        }
        if !subscription_values_equal(current, want) {
            return false;
        }
    }
    true
}

fn subscription_values_equal(a: &Value, b: &Value) -> bool {
    if let (Some(af), Some(bf)) = (as_number(a), as_number(b)) {
        return af == bf;
    }
    value_text(a) == value_text(b)
}

fn as_number(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn same_tenant(a: &str, b: &str, default_tenant: &str) -> bool {
    fn norm<'a>(v: &'a str, default_tenant: &'a str) -> &'a str {
        if v.is_empty() {
            default_tenant
        } else {
            v
        }
    }
    norm(a, default_tenant) == norm(b, default_tenant)
}
