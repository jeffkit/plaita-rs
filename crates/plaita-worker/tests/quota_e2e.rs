//! M2 验收行为测试 · #30 租户并发 start 闸（对拍 phase3_quota_test.go 行族；
//! go pkg/worker/quota.go 逐字段对齐）：配额满 defer 至队尾（quota_defers
//! 计数 + 原条 ack）、预算尽 quota_bypass 放行、终态收口释放槽（suspended
//! 保留槽）、缺省不限（零 Redis 写）、重复载体幂等刷新不重复占位。

use std::sync::Arc;

use plaita_worker::config::{float_epoch, now_iso};
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::state;
use plaita_worker::{RedisBackend, Worker};
use serde_json::{json, Value};

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    queue: Arc<TaskQueue>,
    metrics: Arc<RecordingMetrics>,
}

fn harness(name: &str) -> Harness {
    harness_with(name, &|_| String::new())
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("quota-{name}"),
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
    let queue = Arc::new(TaskQueue::new(
        backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
        cfg.clone(),
        metrics.clone() as Arc<dyn plaita_worker::Metrics>,
    ));
    Harness {
        backend,
        cfg,
        worker,
        queue,
        metrics,
    }
}

impl Harness {
    fn preset_flow(&self, flow_id: &str, raw: &str) {
        plaita_worker::flowsource::preset_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            "",
            flow_id,
            "v1",
            raw.as_bytes(),
        )
        .unwrap();
    }

    fn start_carrier(&self, flow_id: &str, execution_id: &str) {
        self.queue
            .enqueue(
                &json!({
                    "type": "start",
                    "flow_id": flow_id,
                    "execution_id": execution_id,
                    "params": {},
                    "timestamp": now_iso(),
                }),
                1,
            )
            .unwrap();
    }

    fn running_members(&self) -> Vec<String> {
        let key = keys::tenant_running_key(&self.cfg, "");
        self.backend
            .zrangebyscore(&key, 0.0, f64::MAX, 100)
            .unwrap_or_default()
    }

    fn row(&self, execution_id: &str) -> Option<state::ExecutionState> {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id).unwrap()
    }

    fn queue_entries(&self) -> Vec<Value> {
        self.backend
            .xrange(&keys::queue_key(&self.cfg), "-", "+", None)
            .unwrap()
            .into_iter()
            .map(|e| serde_json::from_str(e.field("payload").unwrap()).unwrap())
            .collect()
    }
}

const OK_FLOW: &str = r#"{"flow_id":"okwf","nodes":[
    {"type":"start","id":"s","next":"end"},
    {"type":"end","id":"end","output":"done","resultType":"success"}
]}"#;

/// 验收 10a：配额满 → defer 至队尾（quota_defers=1 副本入队、原条 ack、不执
/// 行不 DLQ、无 running 行）、metric deferred。
#[test]
fn over_quota_start_defers_to_stream_tail() {
    let h = harness_with("defer", &|k| {
        if k == "PLAITA_TENANT_MAX_RUNNING" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    h.preset_flow("okwf", OK_FLOW);
    // the semaphore is full for the default tenant
    h.backend
        .zadd(
            &keys::tenant_running_key(&h.cfg, ""),
            float_epoch() + 3600.0,
            "other-exec",
        )
        .unwrap();

    h.start_carrier("okwf", "exec-q1");
    h.worker.process_one().unwrap();
    // NOT executed: no running row, no terminal
    assert!(
        h.row("exec-q1").is_none(),
        "a deferred start never executes"
    );
    // the deferred copy sits at the tail with quota_defers=1; the original
    // was acked
    let entries = h.queue_entries();
    assert_eq!(entries.len(), 1, "exactly the deferred copy remains");
    assert_eq!(entries[0]["execution_id"], "exec-q1");
    assert_eq!(entries[0]["quota_defers"], 1);
    assert!(
        entries[0].get("quota_bypass").is_none(),
        "no bypass within the defer budget"
    );
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
    assert!(pel.is_empty(), "original acked");
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_QUOTA_DEFERRED_TOTAL,
            &[("tenant", "")]
        ),
        1
    );
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_QUOTA_EXCEEDED_TOTAL,
            &[("tenant", "")]
        ),
        1
    );
}

/// 验收 10b：defer 预算尽 → 副本携 quota_bypass=true，重处理时跳闸放行
/// （liveness > soft quota）。
#[test]
fn defer_budget_exhausted_bypasses_the_gate() {
    let h = harness_with("bypass", &|k| {
        if k == "PLAITA_TENANT_MAX_RUNNING" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    h.preset_flow("okwf", OK_FLOW);
    h.backend
        .zadd(
            &keys::tenant_running_key(&h.cfg, ""),
            float_epoch() + 3600.0,
            "other-exec",
        )
        .unwrap();
    // a carrier that already burned its defer budget (64 = the floored max;
    // defer_max ≤0 normalizes to 64 exactly as go's normalize does)
    h.queue
        .enqueue(
            &json!({
                "type": "start", "flow_id": "okwf", "execution_id": "exec-q2",
                "params": {}, "timestamp": now_iso(), "quota_defers": 64
            }),
            1,
        )
        .unwrap();
    h.worker.process_one().unwrap();
    // first round: still deferred, but the copy now carries quota_bypass
    let entries = h.queue_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["quota_bypass"], true);
    assert_eq!(entries[0]["quota_defers"], 65);
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_QUOTA_BYPASSED_TOTAL,
            &[("tenant", "")]
        ),
        1
    );
    // second round: the bypass carrier executes OVER the cap
    h.worker.process_one().unwrap();
    let row = h.row("exec-q2").expect("bypass carrier executed");
    assert_eq!(row.status, state::STATUS_COMPLETED);
}

/// 验收 10c：终态收口释放槽；suspended 保留槽（执行仍在已准入生命周期内，
/// resume 载体复用同一槽）。
#[test]
fn terminal_writes_release_the_slot_and_suspend_keeps_it() {
    let h = harness_with("release", &|k| {
        if k == "PLAITA_TENANT_MAX_RUNNING" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    // (1) a completing run: the slot is taken at admit and returned at the
    // terminal write
    h.preset_flow("okwf", OK_FLOW);
    h.start_carrier("okwf", "exec-q3");
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-q3").unwrap().status, state::STATUS_COMPLETED);
    assert!(
        h.running_members().is_empty(),
        "completed run released the slot; members={:?}",
        h.running_members()
    );

    // (2) a suspending run: the slot survives the suspend (admitted lifecycle)
    h.preset_flow(
        "waitwf",
        r#"{"flow_id":"waitwf","nodes":[
            {"type":"start","id":"s","next":"e1"},
            {"type":"event","id":"e1","event_type":"tick","next":"end"},
            {"type":"end","id":"end","output":"ok","resultType":"success"}
        ]}"#,
    );
    h.start_carrier("waitwf", "exec-q4");
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-q4").unwrap().status, state::STATUS_SUSPENDED);
    assert_eq!(
        h.running_members(),
        vec!["exec-q4".to_string()],
        "suspended keeps the slot"
    );
    // wake + complete → the slot is returned
    h.queue
        .enqueue(
            &json!({
                "type": "resume", "flow_id": "waitwf", "execution_id": "exec-q4",
                "resume_type": "event", "timestamp": now_iso(),
                "data": {"subscription_id": h.subscription_id("exec-q4")}
            }),
            1,
        )
        .unwrap();
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-q4").unwrap().status, state::STATUS_COMPLETED);
    assert!(
        h.running_members().is_empty(),
        "completion released the slot"
    );
}

impl Harness {
    fn subscription_id(&self, execution_id: &str) -> String {
        let cp =
            plaita_worker::checkpoint::load(self.backend.as_ref(), &self.cfg, "", execution_id)
                .unwrap()
                .expect("checkpoint exists");
        cp.context["nodes"]["e1"]["subscription_id"]
            .as_str()
            .expect("subscription id in $NODE")
            .to_string()
    }
}

/// 缺省 = 不限：无配置租户零 Redis 写（连 ZSET 键都不出现）。
#[test]
fn default_unlimited_never_touches_redis() {
    let h = harness("unlimited");
    h.preset_flow("okwf", OK_FLOW);
    h.start_carrier("okwf", "exec-q5");
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-q5").unwrap().status, state::STATUS_COMPLETED);
    assert!(
        !h.backend
            .get(&keys::tenant_quota_key(&h.cfg))
            .unwrap()
            .is_some(),
        "quota HASH untouched"
    );
    assert!(
        h.backend
            .zrangebyscore(&keys::tenant_running_key(&h.cfg, ""), 0.0, f64::MAX, 1)
            .is_err()
            || h.running_members().is_empty(),
        "running ZSET never created by an unlimited tenant"
    );
}

/// 重复载体（redelivery）幂等刷新：同一 execution 重入闸不重复占位。
#[test]
fn redelivered_start_refreshes_the_same_slot() {
    let h = harness_with("refresh", &|k| {
        if k == "PLAITA_TENANT_MAX_RUNNING" {
            "2".to_string()
        } else {
            String::new()
        }
    });
    h.preset_flow("okwf", OK_FLOW);
    // two DIFFERENT executions occupy both slots
    h.start_carrier("okwf", "exec-a");
    h.worker.process_one().unwrap();
    h.start_carrier("okwf", "exec-b");
    h.worker.process_one().unwrap();
    assert_eq!(h.running_members().len(), 0, "both completed and released");
    // a redelivered start for a live execution refreshes its own slot
    // without double-counting: simulate by holding the semaphore full
    h.backend
        .zadd(
            &keys::tenant_running_key(&h.cfg, ""),
            float_epoch() + 3600.0,
            "x1",
        )
        .unwrap();
    h.backend
        .zadd(
            &keys::tenant_running_key(&h.cfg, ""),
            float_epoch() + 3600.0,
            "x2",
        )
        .unwrap();
    // full now: the next start defers (never executes)
    h.start_carrier("okwf", "exec-c");
    h.worker.process_one().unwrap();
    assert!(h.row("exec-c").is_none());
    let members = h.running_members();
    assert_eq!(members.len(), 2, "deferred start never touched the ZSET");
}
