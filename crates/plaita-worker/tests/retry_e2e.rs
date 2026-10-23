//! M2 验收行为测试 · §9.1 retry 面（对拍 phase2_retry_test.go 行族）：
//! 可重试节点失败的计数 INCR/退避后新载体入队/原条 ack、预算尽 -520 终态、
//! 成功推进清计数、载体转换机器（running 行 + start 载体 → continue/墓碑
//! 不复活）、§4.4 守卫死信分支表（活租约跳过/终态放行/非终态无租约先克隆/
//! 墓碑不克隆/running 且预算尽先终态化）。

use std::sync::Arc;

use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::state;
use plaita_worker::{lease, retry, RedisBackend, Worker};
use serde_json::{json, Value};

use plaita_worker::queue::DeadLetterGuard;

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    queue: Arc<TaskQueue>,
    metrics: Arc<RecordingMetrics>,
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("retry-{name}"),
        // reclaim everything immediately so redelivery paths run fast
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

fn harness(name: &str) -> Harness {
    // collapse the retry backoff so re-enqueue paths run fast
    harness_with(name, &|k| {
        if k == "PLAITA_TEST_RETRY_BACKOFF_MS" {
            "1".to_string()
        } else {
            String::new()
        }
    })
}

impl Harness {
    fn preset_flow(&self, flow_id: &str, version: &str, raw: &str) {
        plaita_worker::flowsource::preset_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            "",
            flow_id,
            version,
            raw.as_bytes(),
        )
        .unwrap();
    }

    fn enqueue_body(&self, body: &Value) {
        self.queue.enqueue(body, 1).unwrap();
    }

    fn start_carrier(&self, flow_id: &str, execution_id: &str) {
        self.enqueue_body(&json!({
            "type": "start",
            "flow_id": flow_id,
            "execution_id": execution_id,
            "params": {},
            "timestamp": now_iso(),
        }));
    }

    fn state_row(&self, execution_id: &str) -> Option<state::ExecutionState> {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id).unwrap()
    }

    fn queue_len(&self) -> i64 {
        self.backend.xlen(&keys::queue_key(&self.cfg)).unwrap()
    }

    fn pel_len(&self) -> usize {
        self.backend
            .xpending_ext(
                &keys::queue_key(&self.cfg),
                &self.cfg.consumer_group,
                "-",
                "+",
                100,
            )
            .unwrap()
            .len()
    }

    fn dlq_len(&self) -> i64 {
        self.backend.xlen(&keys::dlq_key(&self.cfg)).unwrap()
    }

    fn counter(&self, key: String) -> i64 {
        self.backend
            .get(&key)
            .unwrap()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }
}

/// A flow whose `boom` assignment fails with an unknown expression function —
/// the engine classifies it transient (code 10000 ≠ 1001) → §9.1 retryable.
const FAIL_FLOW: &str = r#"{"flow_id":"failwf","nodes":[
    {"type":"start","id":"s","next":"boom"},
    {"type":"assignment","id":"boom","output":{"v":"$NOPE(1)"},"next":"end"},
    {"type":"end","id":"end","output":"ok","resultType":"success"}
]}"#;

/// 验收 2（phase2_retry_test.go 行族）：可重试节点失败低于预算 → 计数 INCR、
/// 退避后新 continue 载体入队、原条 ack、running 行不动；metric retry_total。
#[test]
fn retryable_node_failure_requeues_converted_carrier_and_acks_original() {
    retry::set_retry_backoff_base_ms(1);
    let h = harness("below-budget");
    h.preset_flow("failwf", "v1", FAIL_FLOW);
    h.start_carrier("failwf", "exec-retry-1");
    let handled = h.worker.process_one().unwrap();
    assert!(handled);
    // No terminal, no suspend: the row stays running and the failure is
    // carried by the NEW carrier.
    let row = h.state_row("exec-retry-1").unwrap();
    assert_eq!(row.status, state::STATUS_RUNNING);
    assert!(row.error.is_none());
    // per-node + total counters INCR'd (sliding TTL set)
    assert_eq!(
        h.counter(keys::noderetry_key(&h.cfg, "", "exec-retry-1", "boom")),
        1
    );
    assert_eq!(
        h.counter(keys::noderefetch_key(&h.cfg, "", "exec-retry-1")),
        1
    );
    // exactly one NEW continue carrier, original acked + removed
    assert_eq!(h.queue_len(), 1);
    assert_eq!(h.pel_len(), 0);
    let entry = h
        .backend
        .xrange(&keys::queue_key(&h.cfg), "-", "+", None)
        .unwrap()
        .pop()
        .unwrap();
    let body: Value = serde_json::from_str(entry.field("payload").unwrap()).unwrap();
    assert_eq!(body["type"], "resume");
    assert_eq!(body["resume_type"], "continue");
    assert_eq!(body["execution_id"], "exec-retry-1");
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_RETRY_TOTAL,
            &[("tenant", ""), ("flow", "failwf"), ("node", "boom")]
        ),
        1
    );
    // The converted carrier replays (fresh-run parity) and fails again —
    // attempt 2 lands in the counters.
    h.worker.process_one().unwrap();
    assert_eq!(
        h.counter(keys::noderetry_key(&h.cfg, "", "exec-retry-1", "boom")),
        2
    );
    retry::set_retry_backoff_base_ms(0);
}

/// 验收 2b：预算尽（attempt >= max_deliveries）→ -520 node_retry_budget_
/// exhausted 终态（node_retries/failed_node 明细）、无新载体、原条 ack。
#[test]
fn retry_budget_exhausted_writes_520_terminal() {
    retry::set_retry_backoff_base_ms(1);
    let h = harness_with("budget-out", &|k| {
        if k == "PLAITA_MAX_DELIVERIES" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    h.preset_flow("failwf", "v1", FAIL_FLOW);
    h.start_carrier("failwf", "exec-retry-2");
    h.worker.process_one().unwrap();
    let row = h.state_row("exec-retry-2").unwrap();
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "node_retry_budget_exhausted");
    assert_eq!(err["code"], -520);
    assert_eq!(err["node_retries"], 1);
    assert_eq!(err["failed_node"], "boom");
    // no new carrier; the original was acked; lease released
    assert_eq!(h.queue_len(), 0);
    assert_eq!(h.pel_len(), 0);
    assert!(
        lease::get_holder(h.backend.as_ref(), &h.cfg, "", "exec-retry-2")
            .unwrap()
            .is_none()
    );
    retry::set_retry_backoff_base_ms(0);
}

/// 验收 2c：成功推进清计数——先手工置计数，跑到 completed 后三计数清零
/// （§9.1 任一节点成功推进即清零 + 全程成功清连续失败顶）。
#[test]
fn successful_run_clears_retry_counters() {
    let h = harness("clear-on-success");
    h.preset_flow(
        "okwf",
        "v1",
        r#"{"flow_id":"okwf","nodes":[
            {"type":"start","id":"s","next":"end"},
            {"type":"end","id":"end","output":"done","resultType":"success"}
        ]}"#,
    );
    let backend = h.backend.clone();
    let cfg = h.cfg.clone();
    // seed all three counters as a previous failed life left them
    backend
        .incr(&keys::noderetry_key(&cfg, "", "exec-clear", "s"))
        .unwrap();
    backend
        .incr(&keys::noderefetch_key(&cfg, "", "exec-clear"))
        .unwrap();
    backend
        .incr(&keys::nofail_key(&cfg, "", "exec-clear"))
        .unwrap();
    h.start_carrier("okwf", "exec-clear");
    h.worker.process_one().unwrap();
    let row = h.state_row("exec-clear").unwrap();
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(
        h.counter(keys::noderetry_key(&cfg, "", "exec-clear", "s")),
        0
    );
    assert_eq!(h.counter(keys::noderefetch_key(&cfg, "", "exec-clear")), 0);
    assert_eq!(h.counter(keys::nofail_key(&cfg, "", "exec-clear")), 0);
}

/// §9.3 fresh-id cleanup: a brand-new id must not inherit the previous
/// life's retry budgets.
#[test]
fn fresh_start_resets_inherited_counters() {
    let h = harness("fresh-id");
    h.preset_flow(
        "okwf",
        "v1",
        r#"{"flow_id":"okwf","nodes":[
            {"type":"start","id":"s","next":"end"},
            {"type":"end","id":"end","output":"done","resultType":"success"}
        ]}"#,
    );
    let backend = h.backend.clone();
    let cfg = h.cfg.clone();
    backend
        .incr(&keys::noderetry_key(&cfg, "", "exec-fresh", "s"))
        .unwrap();
    h.start_carrier("okwf", "exec-fresh");
    h.worker.process_one().unwrap();
    assert_eq!(
        h.counter(keys::noderetry_key(&cfg, "", "exec-fresh", "s")),
        0
    );
}

/// 载体转换机器（retryCarrierBody 分支表，经 handle_start 的 running 分支）：
/// running 行上的重复 start → continue 载体（#47 崩溃恢复）；墓碑 → ack 不
/// 重排（A51-01）。
#[test]
fn start_on_running_row_converts_and_tombstone_drops() {
    let h = harness("convert-running");
    h.preset_flow(
        "okwf",
        "v1",
        r#"{"flow_id":"okwf","nodes":[
            {"type":"start","id":"s","next":"end"},
            {"type":"end","id":"end","output":"done","resultType":"success"}
        ]}"#,
    );
    // craft the crash residue: a running row + a start carrier in the stream
    let mut row = state::ExecutionState::new_running(&h.cfg, "okwf", &json!({}), "exec-conv", "");
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    h.start_carrier("okwf", "exec-conv");
    h.worker.process_one().unwrap();
    assert_eq!(
        h.state_row("exec-conv").unwrap().status,
        state::STATUS_RUNNING
    );
    assert_eq!(h.queue_len(), 1, "converted continue carrier enqueued");
    assert_eq!(h.pel_len(), 0, "original acked");
    row = h.state_row("exec-conv").unwrap();

    // tombstone branch: the same residue now drops without a carrier (the
    // phase-1 converted carrier is cleared first so the assertions below are
    // about the phase-2 path only)
    h.backend.del(&[&keys::queue_key(&h.cfg)]).unwrap();
    h.backend
        .set(
            &keys::tombstone_key(&h.cfg, "exec-conv"),
            &json!({"tenant": ""}).to_string(),
            None,
        )
        .unwrap();
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    h.start_carrier("okwf", "exec-conv");
    h.worker.process_one().unwrap();
    assert_eq!(h.queue_len(), 0, "tombstoned execution is not resurrected");
    assert_eq!(h.pel_len(), 0, "original acked");
}

/// 验收 5（worker.go:2054-2131 分支逐条）·活租约跳过：死信守卫在租约活跃时
/// 不放行——无 DLQ 条目、行不动（reclaim ceiling 的队列层接线已有 M1 测试
/// 覆盖（queue.rs reclaim_delivery_ceiling_dead_letters），这里断言守卫分支
/// 本身）。
#[test]
fn dead_letter_guard_skips_active_lease() {
    let h = harness("guard-lease");
    let row = state::ExecutionState::new_running(&h.cfg, "okwf", &json!({}), "exec-gl", "");
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    let _handle = lease::acquire(
        h.backend.as_ref(),
        &h.cfg,
        "",
        "exec-gl",
        "start:i:aaaa1111bbbb2222",
        120,
    )
    .unwrap();
    let guard = plaita_worker::retry::GuardedDeadLetter::with_backend(
        h.backend.clone() as Arc<dyn RedisBackend>,
        h.cfg.clone(),
    );
    let env = plaita_worker::queue::Envelope {
        message_id: "manual-1".to_string(),
        schema_version: 1,
        payload_raw: json!({
            "type": "start", "flow_id": "okwf", "execution_id": "exec-gl"
        })
        .to_string(),
        fields: vec![],
    };
    let task: plaita_worker::queue::Task = serde_json::from_value(json!({
        "type": "start", "flow_id": "okwf", "execution_id": "exec-gl"
    }))
    .unwrap();
    assert!(
        !guard.allow(&env, &task, "max_deliveries_exceeded"),
        "active lease must skip the dead letter"
    );
    assert_eq!(h.dlq_len(), 0, "no DLQ entry while the lease is live");
    assert_eq!(
        h.queue_len(),
        0,
        "no recovery clone while the lease is live"
    );
    assert_eq!(
        h.state_row("exec-gl").unwrap().status,
        state::STATUS_RUNNING
    );
}

/// 验收 5b：终态行放行。
#[test]
fn dead_letter_guard_allows_terminal_row() {
    let h = harness("guard-terminal");
    let mut row = state::ExecutionState::new_running(&h.cfg, "okwf", &json!({}), "exec-gt", "");
    row.status = state::STATUS_ERROR.to_string();
    row.end_time = now_iso();
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    let guard = plaita_worker::retry::GuardedDeadLetter::with_backend(
        h.backend.clone() as Arc<dyn RedisBackend>,
        h.cfg.clone(),
    );
    let env = plaita_worker::queue::Envelope {
        message_id: "m".to_string(),
        schema_version: 1,
        payload_raw: json!({"type": "start", "execution_id": "exec-gt"}).to_string(),
        fields: vec![],
    };
    let task: plaita_worker::queue::Task =
        serde_json::from_value(json!({"type": "start", "execution_id": "exec-gt"})).unwrap();
    assert!(guard.allow(&env, &task, "max_deliveries_exceeded"));
}

/// 验收 5c：非终态 + 无租约 → 先克隆恢复载体（继承源信封版本）再放行。
#[test]
fn dead_letter_guard_clones_non_terminal_without_lease() {
    let h = harness("guard-clone");
    let row = state::ExecutionState::new_running(&h.cfg, "okwf", &json!({}), "exec-gc", "");
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    let guard = plaita_worker::retry::GuardedDeadLetter::with_backend(
        h.backend.clone() as Arc<dyn RedisBackend>,
        h.cfg.clone(),
    );
    let payload = json!({"type": "start", "flow_id": "okwf", "execution_id": "exec-gc"});
    let env = plaita_worker::queue::Envelope {
        message_id: "m".to_string(),
        schema_version: 3,
        payload_raw: payload.to_string(),
        fields: vec![],
    };
    let task: plaita_worker::queue::Task = serde_json::from_value(payload.clone()).unwrap();
    let mut task = task;
    task.envelope_schema_ver = 3;
    assert!(guard.allow(&env, &task, "max_deliveries_exceeded"));
    // the clone carries the SOURCE envelope version (A44-02)
    let entry = h
        .backend
        .xrange(&keys::queue_key(&h.cfg), "-", "+", None)
        .unwrap()
        .pop()
        .expect("recovery clone enqueued");
    assert_eq!(entry.field("schema_version"), Some("3"));
    let body: Value = serde_json::from_str(entry.field("payload").unwrap()).unwrap();
    assert_eq!(body["execution_id"], "exec-gc");
}

/// 验收 5d：墓碑放行不克隆（A51-01）。
#[test]
fn dead_letter_guard_allows_tombstone_without_clone() {
    let h = harness("guard-tombstone");
    let row = state::ExecutionState::new_running(&h.cfg, "okwf", &json!({}), "exec-gb", "");
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    h.backend
        .set(
            &keys::tombstone_key(&h.cfg, "exec-gb"),
            &json!({"tenant": ""}).to_string(),
            None,
        )
        .unwrap();
    let guard = plaita_worker::retry::GuardedDeadLetter::with_backend(
        h.backend.clone() as Arc<dyn RedisBackend>,
        h.cfg.clone(),
    );
    let env = plaita_worker::queue::Envelope {
        message_id: "m".to_string(),
        schema_version: 1,
        payload_raw: json!({"type": "start", "execution_id": "exec-gb"}).to_string(),
        fields: vec![],
    };
    let task: plaita_worker::queue::Task =
        serde_json::from_value(json!({"type": "start", "execution_id": "exec-gb"})).unwrap();
    assert!(guard.allow(&env, &task, "max_deliveries_exceeded"));
    assert_eq!(h.queue_len(), 0, "tombstone: no recovery clone");
}

/// 验收 5e：running 且重试预算尽 → 先终态化（fence_gen=0 写安全——租约已
/// 探空）+ 删 checkpoint 再放行（plaita#242）。
#[test]
fn dead_letter_guard_terminalizes_stuck_running_before_dead_letter() {
    let h = harness_with("guard-stuck", &|k| {
        if k == "PLAITA_MAX_DELIVERIES" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    let row = state::ExecutionState::new_running(&h.cfg, "okwf", &json!({}), "exec-gs", "");
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    // a checkpoint exists — the terminalization must delete it (#51)
    let cp = plaita_worker::CheckpointRecord::suspend("exec-gs", "", "okwf", "s", json!({}));
    plaita_worker::checkpoint::save(h.backend.as_ref(), &h.cfg, &cp).unwrap();
    // node retry budget exhausted (counter at max_deliveries)
    h.backend
        .incr(&keys::noderetry_key(&h.cfg, "", "exec-gs", "boom"))
        .unwrap();
    let guard = plaita_worker::retry::GuardedDeadLetter::with_backend(
        h.backend.clone() as Arc<dyn RedisBackend>,
        h.cfg.clone(),
    );
    let env = plaita_worker::queue::Envelope {
        message_id: "m".to_string(),
        schema_version: 1,
        payload_raw: json!({"type": "start", "execution_id": "exec-gs"}).to_string(),
        fields: vec![],
    };
    let task: plaita_worker::queue::Task =
        serde_json::from_value(json!({"type": "start", "execution_id": "exec-gs"})).unwrap();
    assert!(guard.allow(&env, &task, "max_deliveries_exceeded"));
    let row = h.state_row("exec-gs").unwrap();
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "node_retry_budget_exhausted");
    assert_eq!(err["code"], -520);
    assert_eq!(err["failed_reason"], "max_deliveries_exceeded");
    assert!(h.checkpoint_missing("exec-gs"));
}

impl Harness {
    fn checkpoint_missing(&self, execution_id: &str) -> bool {
        plaita_worker::checkpoint::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .is_none()
    }
}
