//! Task age gate (rs-worker-design §4.2/#29, Python B52-02/A69-04; port of
//! plaita-go `pkg/worker/agegate.go`): the main task stream has no time TTL,
//! so a carrier can survive arbitrarily long in backlog/PEL. Past the
//! configured horizon (PLAITA_TASK_MAX_AGE_SECONDS, default 30 days; a
//! negative config disables) such carriers are acked WITHOUT executing and
//! the drop is made observable (Warn log + task_expired_total + a live event
//! on `{prefix}:execution:events:{id}`).
//!
//! Age is the MAX of two sources (Python `_start_carrier_too_old`):
//!
//! - the payload `timestamp` (producer enqueue wall clock) — a re-enqueued
//!   copy inherits it, so requeues cannot wash the original age;
//! - the Stream ID millisecond prefix — a fresh re-enqueue gets a fresh ID,
//!   so a future/forged timestamp cannot whitewash an old Stream ID either.
//!
//! When NEITHER source is determinable the gate FAILS CLOSED (drop + count):
//! admitting an age-unknown carrier would defeat the horizon entirely. In
//! practice this only happens for manually XADDed entries with non-numeric
//! ids; the payload timestamp alone also covers legacy producers that never
//! carried one.

use serde_json::json;

use crate::config::Config;
use crate::queue::Task;

/// Parses the producer timestamp (naive local wall clock or RFC3339, §6.1)
/// into an age in seconds. Unparsable → not reported.
pub(crate) fn task_age_from_timestamp(ts: &str) -> Option<f64> {
    if ts.is_empty() {
        return None;
    }
    let t = parse_producer_timestamp(ts)?;
    Some(age_seconds_from(t))
}

/// Naive-local / RFC3339 producer timestamp → epoch millis. Mirrors the
/// queue-wait parser (worker.rs queue_wait_millis).
pub(crate) fn parse_producer_timestamp(ts: &str) -> Option<i64> {
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(naive.and_utc().timestamp_millis());
    }
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc).timestamp_millis())
}

fn age_seconds_from(epoch_millis: i64) -> f64 {
    let now = epoch_millis_now();
    (now - epoch_millis) as f64 / 1000.0
}

fn epoch_millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parses a Redis stream id ("<ms>-<seq>") into its write time (epoch ms).
/// Only purely numeric ids with a POSITIVE millisecond prefix carry a usable
/// write time (Python timestamp_from_stream_id 口径); "0-1" and non-numeric
/// ids report None.
pub(crate) fn stream_id_time(message_id: &str) -> Option<i64> {
    let (ms, _) = message_id.split_once('-')?;
    let ms: i64 = ms.parse().ok()?;
    if ms <= 0 {
        return None;
    }
    Some(ms)
}

/// The dual-source max age (payload timestamp vs Stream ID). Every parsable
/// source participates in the max — including negative ages (Python
/// max(ages) 口径): a negative max means the worker clock is behind the
/// producer clock, i.e. the carrier is fresher than "now" — that is
/// freshness, clamped to 0 ("just enqueued"), NOT an unknown age (#59: the
/// old sentinel merged "both unparsable" and "both future-dated" into one
/// known=false verdict and fail-closed dropped fresh carriers).
/// known=false only when NEITHER source is parsable.
pub(crate) fn carrier_age_seconds(task: &Task, message_id: &str) -> (f64, bool) {
    let mut best = f64::NEG_INFINITY;
    let mut known = false;
    if let Some(a) = task_age_from_timestamp(&task.timestamp) {
        known = true;
        if a > best {
            best = a;
        }
    }
    if let Some(ms) = stream_id_time(message_id) {
        known = true;
        let a = age_seconds_from(ms);
        if a > best {
            best = a;
        }
    }
    if !known {
        return (0.0, false);
    }
    if best < 0.0 {
        best = 0.0;
    }
    (best, true)
}

/// The dual-source max age for logs/events (0 when undeterminable — the
/// fail-closed drop is counted separately).
pub(crate) fn task_age_of(task: &Task, message_id: &str) -> f64 {
    carrier_age_seconds(task, message_id).0
}

/// Reports whether a start/resume carrier is past the age horizon and must
/// be dropped (acked without executing). max_age < 0 disables the gate
/// entirely (normalize maps 0/unset to the 30-day default).
pub(crate) fn carrier_too_old(cfg: &Config, task: &Task, message_id: &str) -> bool {
    let max_age = cfg.task_max_age_secs as f64;
    if max_age < 0.0 {
        return false;
    }
    let (age, known) = carrier_age_seconds(task, message_id);
    if !known {
        // Fail closed: an undeterminable carrier could have been backing up
        // since before the horizon; admitting it would void the gate. Name
        // each source's reason (#59): the old canned message asserted "no
        // timestamp, non-numeric stream id" even when both sources were
        // present (the future-dated shape, since fixed) — it sent debugging
        // toward the producer format.
        eprintln!(
            "[agegate] task age undeterminable ({}); dropping fail-closed (type={}, id=\
             {message_id}, timestamp={:?})",
            carrier_age_unknown_reason(task, message_id),
            task.task_type,
            task.timestamp
        );
        return true;
    }
    age > max_age
}

/// Names why each age source is unusable, so an operator can tell a legacy
/// producer (no timestamp) from a forged/manual XADD (non-numeric stream id).
pub(crate) fn carrier_age_unknown_reason(task: &Task, message_id: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    if task.timestamp.is_empty() {
        parts.push("no timestamp".to_string());
    } else if parse_producer_timestamp(&task.timestamp).is_none() {
        parts.push("unparsable timestamp".to_string());
    }
    if stream_id_time(message_id).is_none() {
        parts.push("non-numeric stream id".to_string());
    }
    parts.join(", ")
}

/// The #29 live drop event published on the execution channel (best-effort;
/// the caller owns the publish failure handling).
pub(crate) fn task_expired_event(task: &Task, message_id: &str) -> serde_json::Value {
    json!({
        "type": "task_expired",
        "at": crate::config::now_iso(),
        "age_seconds": task_age_of(task, message_id),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::queue::Task;

    fn cfg(max_age: &str) -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_TASK_MAX_AGE_SECONDS" {
                max_age.to_string()
            } else {
                String::new()
            }
        })
    }

    fn task_with_ts(ts: &str) -> Task {
        Task {
            task_type: "start".into(),
            timestamp: ts.into(),
            ..Default::default()
        }
    }

    fn days_ago_iso(days: f64) -> String {
        let ms = epoch_millis_now() - (days * 86400.0 * 1000.0) as i64;
        let t = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms).unwrap();
        t.with_timezone(&chrono::Local)
            .format("%Y-%m-%dT%H:%M:%S%.6f")
            .to_string()
    }

    #[test]
    fn horizon_exceeded_dual_source_max() {
        let c = cfg("");
        // fresh stream id, old timestamp → the timestamp drives the max
        let old = task_with_ts(&days_ago_iso(40.0));
        assert!(carrier_too_old(&c, &old, "1750000000000-0"));
        // fresh timestamp, old stream id → the stream id drives the max
        let fresh_ts = task_with_ts(&days_ago_iso(0.0));
        assert!(carrier_too_old(&c, &fresh_ts, "1750000000000-0"));
        // both fresh → pass
        let fresh_id = format!("{}-0", epoch_millis_now());
        assert!(!carrier_too_old(&c, &fresh_ts, &fresh_id));
    }

    #[test]
    fn unknown_age_fails_closed_but_negative_max_disables() {
        let c = cfg("");
        // neither source usable: no timestamp + non-numeric id
        let t = task_with_ts("");
        assert!(carrier_too_old(&c, &t, "not-a-stream-id"));
        assert_eq!(
            carrier_age_unknown_reason(&t, "not-a-stream-id"),
            "no timestamp, non-numeric stream id"
        );
        // unparsable timestamp names itself
        let t2 = task_with_ts("yesterday");
        assert_eq!(
            carrier_age_unknown_reason(&t2, "not-a-stream-id"),
            "unparsable timestamp, non-numeric stream id"
        );
        // negative max disables the gate entirely
        let off = cfg("-1");
        assert!(!carrier_too_old(&off, &t, "not-a-stream-id"));
    }

    #[test]
    fn future_dated_carrier_is_fresh_not_unknown() {
        // #59: a negative max age (future-dated by BOTH sources) is
        // freshness clamped to 0, not an unknown age
        let c = cfg("");
        let future = epoch_millis_now() + 3_600_000;
        let t = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(future).unwrap();
        let ts = t
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%dT%H:%M:%S%.6f")
            .to_string();
        let task = task_with_ts(&ts);
        let (age, known) = carrier_age_seconds(&task, &format!("{future}-0"));
        assert!(known, "future-dated both sources is KNOWN freshness");
        assert_eq!(age, 0.0);
        assert!(!carrier_too_old(&c, &task, &format!("{future}-0")));
    }
}
