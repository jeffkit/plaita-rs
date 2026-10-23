//! M2 验收行为测试 · #29 载体年龄闸（agegate.go:9-110 对拍）：超龄载体 ack
//! 不执行 + task_expired_total + 直播事件；双源取大（新 stream id + 旧
//! timestamp 或反之都拦）；新鲜载体放行；负地平线=整闸关闭。

use std::sync::Arc;

use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::state;
use plaita_worker::{RedisBackend, Worker};
use serde_json::json;

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    metrics: Arc<RecordingMetrics>,
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("age-{name}"),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        _ => extra(k),
    });
    let backend = Arc::new(FakeRedis::new());
    let metrics = Arc::new(RecordingMetrics::default());
    let worker = Arc::new(Worker::new(
        backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
        cfg.clone(),
        metrics.clone() as Arc<dyn plaita_worker::Metrics>,
    ));
    Harness {
        backend,
        cfg,
        worker,
        metrics,
    }
}

fn harness(name: &str) -> Harness {
    harness_with(name, &|_| String::new())
}

fn epoch_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn iso_days_ago(days: f64) -> String {
    let ms = epoch_ms_now() - (days * 86400.0 * 1000.0) as i64;
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .unwrap()
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%dT%H:%M:%S%.6f")
        .to_string()
}

impl Harness {
    fn preset_flow(&self) {
        plaita_worker::flowsource::preset_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            "",
            "okwf",
            "v1",
            br#"{"flow_id":"okwf","nodes":[
                {"type":"start","id":"s","next":"end"},
                {"type":"end","id":"end","output":"done","resultType":"success"}
            ]}"#,
        )
        .unwrap();
    }

    fn seed_carrier(&self, id: &str, timestamp: &str, execution_id: &str) {
        self.backend
            .xadd_with_id(
                &keys::queue_key(&self.cfg),
                id,
                &[
                    (
                        "payload".to_string(),
                        json!({
                            "type": "start", "flow_id": "okwf",
                            "execution_id": execution_id,
                            "params": {}, "timestamp": timestamp
                        })
                        .to_string(),
                    ),
                    ("schema_version".to_string(), "1".to_string()),
                ],
            )
            .unwrap();
    }

    fn row(&self, execution_id: &str) -> Option<state::ExecutionState> {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id).unwrap()
    }
}

/// 验收 9：超龄载体 ack 不执行 + task_expired_total；双源取大——新 stream
/// id 洗不白旧 timestamp。
#[test]
fn aged_carrier_acked_without_executing() {
    let h = harness("old-ts");
    h.preset_flow();
    h.seed_carrier(
        &format!("{}-0", epoch_ms_now()),
        &iso_days_ago(40.0),
        "exec-old",
    );
    h.worker.process_one().unwrap();
    // acked + removed, NEVER executed, never dead-lettered
    assert!(h.row("exec-old").is_none());
    assert_eq!(h.backend.xlen(&keys::queue_key(&h.cfg)).unwrap(), 0);
    let pel = h
        .backend
        .xpending_ext(
            &keys::queue_key(&h.cfg),
            &h.cfg.consumer_group,
            "-",
            "+",
            10,
        )
        .unwrap();
    assert!(pel.is_empty());
    assert_eq!(h.backend.xlen(&keys::dlq_key(&h.cfg)).unwrap(), 0);
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_TASK_EXPIRED_TOTAL,
            &[("tenant", ""), ("type", "start")]
        ),
        1
    );
}

/// 双源取大：新鲜 timestamp 洗不白旧 stream id（伪造时间戳不能漂白积压）。
#[test]
fn aged_stream_id_drops_even_with_fresh_timestamp() {
    let h = harness("old-id");
    h.preset_flow();
    h.seed_carrier("1750000000000-0", &now_iso(), "exec-old-id");
    h.worker.process_one().unwrap();
    assert!(h.row("exec-old-id").is_none());
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_TASK_EXPIRED_TOTAL,
            &[("tenant", ""), ("type", "start")]
        ),
        1
    );
}

/// 新鲜载体正常放行执行。
#[test]
fn fresh_carrier_passes_and_executes() {
    let h = harness("fresh");
    h.preset_flow();
    h.seed_carrier(&format!("{}-0", epoch_ms_now()), &now_iso(), "exec-fresh");
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-fresh").unwrap().status, state::STATUS_COMPLETED);
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_TASK_EXPIRED_TOTAL,
            &[("tenant", ""), ("type", "start")]
        ),
        0
    );
}

/// 负地平线 = 整闸关闭：超龄载体照常执行。
#[test]
fn negative_horizon_disables_the_gate() {
    let h = harness_with("disabled", &|k| {
        if k == "PLAITA_TASK_MAX_AGE_SECONDS" {
            "-1".to_string()
        } else {
            String::new()
        }
    });
    h.preset_flow();
    h.seed_carrier(
        &format!("{}-0", epoch_ms_now()),
        &iso_days_ago(40.0),
        "exec-old2",
    );
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-old2").unwrap().status, state::STATUS_COMPLETED);
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_TASK_EXPIRED_TOTAL,
            &[("tenant", ""), ("type", "start")]
        ),
        0
    );
}
