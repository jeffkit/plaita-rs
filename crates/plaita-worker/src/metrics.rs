//! Metrics face (§10.3): a minimal host-injectable trait with a no-op
//! default. Metric-name stability contract (metrics.go:35-57): names below
//! are part of the wire contract with the ops surface; labels are
//! low-cardinality dimensions only (tenant/flow/status/reason/node/
//! resume_type/type/queue) — execution_id is NEVER a label.
//!
//! M1 wires queue_depth / exec_result / dlq_total; the rest land with their
//! features in M2/M3.

/// Low-cardinality metric labels.
pub type Labels<'a> = &'a [(&'a str, &'a str)];

/// Host-injectable metrics sink. All methods must be cheap and non-blocking.
pub trait Metrics: Send + Sync {
    fn inc_counter(&self, name: &str, labels: Labels);
    fn set_gauge(&self, name: &str, value: f64, labels: Labels);
    fn observe_duration(&self, name: &str, d: std::time::Duration, labels: Labels);
}

/// The default sink: observes nothing, breaks nothing.
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn inc_counter(&self, _name: &str, _labels: Labels) {}
    fn set_gauge(&self, _name: &str, _value: f64, _labels: Labels) {}
    fn observe_duration(&self, _name: &str, _d: std::time::Duration, _labels: Labels) {}
}

/// XLEN sample of the task stream, taken once per consume round (O(1)).
pub const METRIC_QUEUE_DEPTH: &str = "queue_depth";
/// Terminal classification of one engine invocation (`status` label:
/// completed/failed/suspended/cancelled).
pub const METRIC_EXEC_RESULT: &str = "exec_result";
/// Dead-letter counter with the low-cardinality `reason` label (§4.4).
pub const METRIC_DLQ_TOTAL: &str = "dlq_total";
/// One retry carrier actually went out (requeue, not budget exhaust;
/// labels tenant/flow/node — §10.3 metrics.go MetricRetryTotal).
pub const METRIC_RETRY_TOTAL: &str = "retry_total";
/// Duplicate-start idempotent acks / self-feed escapes (labels tenant/reason).
pub const METRIC_TASK_DEDUP_HIT_TOTAL: &str = "task_dedup_hit_total";

/// A recording sink for tests (labels joined into a stable key).
#[derive(Default)]
pub struct RecordingMetrics {
    pub counters: std::sync::Mutex<Vec<(String, i64)>>,
}

impl RecordingMetrics {
    pub fn counter_of(&self, name: &str, labels: &[(&str, &str)]) -> i64 {
        let key = metric_key(name, labels);
        self.counters
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == key)
            .map(|(_, v)| *v)
            .sum()
    }
}

impl Metrics for RecordingMetrics {
    fn inc_counter(&self, name: &str, labels: Labels) {
        let key = metric_key(name, labels);
        let mut counters = self.counters.lock().unwrap();
        match counters.iter_mut().find(|(k, _)| *k == key) {
            Some((_, v)) => *v += 1,
            None => counters.push((key, 1)),
        }
    }

    fn set_gauge(&self, _name: &str, _value: f64, _labels: Labels) {}
    fn observe_duration(&self, _name: &str, _d: std::time::Duration, _labels: Labels) {}
}

fn metric_key(name: &str, labels: &[(&str, &str)]) -> String {
    let mut parts: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    parts.sort();
    format!("{name}{{{}}}", parts.join(","))
}
