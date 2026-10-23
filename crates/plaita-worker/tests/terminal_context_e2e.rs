//! 终态 context 采集 · worker 验收行为测试（rs#17 WK-M3；rs-worker-design
//! §7.4 宿主钩子增量的消费面）：终态行（completed / error）的 context =
//! 引擎根执行 FlowEnd 附带的精确九根快照——与进程内 `snapshot_json` 的九根
//! 一致，不再走 M1 的「种子 + $NODE 事件流」重建降级；挂起→唤醒→恢复的
//! 终态 context 继承 checkpoint 现场 + 续跑新节点。引擎侧形状面在
//! plaita-engine `tests/terminal_context.rs`（根帧唯一 Some）。

use std::sync::Arc;

use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::state;
use plaita_worker::subscriptions::RedisSubscriptions;
use plaita_worker::{checkpoint, events, flowsource, Worker};
use serde_json::{json, Value};

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    queue: Arc<TaskQueue>,
    subs: Arc<RedisSubscriptions>,
}

fn harness(name: &str) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("tctx-{name}"),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        _ => String::new(),
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
    let subs = Arc::new(RedisSubscriptions::new(
        backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
        cfg.clone(),
    ));
    Harness {
        backend,
        cfg,
        worker,
        queue,
        subs,
    }
}

impl Harness {
    fn preset_flow(&self, flow_id: &str, version: &str, raw: &str) {
        flowsource::preset_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            "",
            flow_id,
            version,
            raw.as_bytes(),
        )
        .unwrap();
    }

    fn start_carrier(&self, flow_id: &str, execution_id: &str, params: Value) {
        self.queue
            .enqueue(
                &json!({
                    "type": "start",
                    "flow_id": flow_id,
                    "execution_id": execution_id,
                    "params": params,
                    "timestamp": now_iso(),
                }),
                1,
            )
            .unwrap();
    }

    fn row(&self, execution_id: &str) -> state::ExecutionState {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .unwrap_or_else(|| panic!("state row for {execution_id} missing"))
    }

    fn checkpoint(&self, execution_id: &str) -> checkpoint::CheckpointRecord {
        checkpoint::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .unwrap_or_else(|| panic!("checkpoint for {execution_id} missing"))
    }
}

const ASSIGN_FLOW: &str = r#"{"flow_id":"assign9","nodes":[
    {"type":"start","id":"s","next":"a"},
    {"type":"assignment","id":"a","output":{"greeting":"$INPUT.who","doubled":"$INPUT.n"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.a.greeting","resultType":"success"}
]}"#;

const NINE_ROOTS: [&str; 9] = [
    "input",
    "nodes",
    "errors",
    "env",
    "global",
    "last_node",
    "last_branch",
    "flow_id",
    "execution_id",
];

/// 进程内对照物：同一 flow、同一 host 注入在引擎内直接执行，抓根帧
/// FlowEnd 的快照（= RuntimeCtx::snapshot_json 的九根）。
fn in_process_snapshot(flow_raw: &str, execution_id: &str, params: Value) -> Value {
    use plaita_engine::exec::{Event, Host};
    use std::sync::Mutex;
    let flow = plaita_engine::ir::load(flow_raw.as_bytes()).unwrap();
    let seen: Arc<Mutex<Vec<Option<Value>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let host =
        Host::with_execution_id(execution_id.to_string()).listen(Arc::new(move |ev: &Event| {
            if let Event::FlowEnd { context, .. } = ev {
                sink.lock().unwrap().push(context.clone());
            }
        }));
    flow.execute(&params, &host).unwrap();
    let mut ends = seen.lock().unwrap().clone();
    ends.pop()
        .flatten()
        .expect("root FlowEnd carries the snapshot")
}

/// 验收：completed 终态行的 context = 进程内 snapshot_json 的九根（逐根
/// 相等），不再有「种子 params 顶层 + $NODE 结果折叠」的重建形状。
#[test]
fn completed_row_context_equals_in_process_snapshot_nine_roots() {
    let h = harness("completed");
    h.preset_flow("assign9", "v1", ASSIGN_FLOW);
    let params = json!({"who": "m3", "n": 21});
    let snapshot = in_process_snapshot(ASSIGN_FLOW, "exec-tctx-1", params.clone());

    h.start_carrier("assign9", "exec-tctx-1", params);
    h.worker.process_one().unwrap();
    let row = h.row("exec-tctx-1");
    assert_eq!(row.status, state::STATUS_COMPLETED);

    // 九根逐根在册且与进程内快照一致。
    for root in NINE_ROOTS {
        assert_eq!(
            row.context.get(root),
            snapshot.get(root),
            "root {root:?} must equal the in-process snapshot"
        );
    }
    assert_eq!(row.context["input"], json!({"who": "m3", "n": 21}));
    assert_eq!(row.context["nodes"]["a"]["greeting"], "m3");
    assert_eq!(row.context["nodes"]["a"]["doubled"], 21);
    assert_eq!(row.context["last_node"], "a");
    assert_eq!(row.context["flow_id"], "assign9");
    assert_eq!(row.context["execution_id"], "exec-tctx-1");
    // 重建形状不得残留：M1 的降级实现把 params 顶层键直接铺在 context 上
    //（种子形状）；快照形状下 context 恰为九根对象。
    let keys: Vec<String> = row.context.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys.len(), 9, "context is exactly the nine-root object");
}

/// 验收：挂起→事件唤醒→恢复的 completed 终态 context 继承 checkpoint
/// 现场（挂起前节点结果）+ 续跑新节点结果，九根齐全——与恢复链的
/// snapshot_json 一致（引擎侧 tests/terminal_context.rs 同形状钉）。
#[test]
fn resumed_completion_context_inherits_checkpoint_and_new_nodes() {
    let h = harness("resumed");
    h.preset_flow(
        "wf-resume",
        "v1",
        r#"{"flow_id":"wf-resume","nodes":[
            {"type":"start","id":"s","next":"pre"},
            {"type":"assignment","id":"pre","output":{"a":"first"},"next":"ev"},
            {"type":"event","id":"ev","event_type":"tick","next":"post"},
            {"type":"assignment","id":"post","output":{"b":"second"},"next":"end"},
            {"type":"end","id":"end","output":"$NODE.post.b","resultType":"success"}
        ]}"#,
    );
    h.start_carrier("wf-resume", "exec-tctx-2", json!({}));
    h.worker.process_one().unwrap();
    assert_eq!(h.row("exec-tctx-2").status, state::STATUS_SUSPENDED);

    // 挂起行的 context 仍是精确快照（既有 M1 面，对照锚）。
    let cp = h.checkpoint("exec-tctx-2");
    assert_eq!(cp.context["nodes"]["pre"], json!({"a": "first"}));

    let ev = events::Event {
        event_id: "evt-tctx-2".into(),
        event_type: "tick".into(),
        correlation_id: "exec-tctx-2".into(),
        tenant_id: String::new(),
        data: json!({"status": "completed"}),
    };
    events::handle_event(
        &(h.backend.clone() as Arc<dyn plaita_worker::RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
        &ev,
    )
    .unwrap();
    h.worker.process_one().unwrap();

    let row = h.row("exec-tctx-2");
    assert_eq!(row.status, state::STATUS_COMPLETED);
    for root in NINE_ROOTS {
        assert!(
            row.context.get(root).is_some(),
            "root {root:?} present after resume"
        );
    }
    assert_eq!(row.context["nodes"]["pre"], json!({"a": "first"}));
    assert_eq!(row.context["nodes"]["post"], json!({"b": "second"}));
    assert_eq!(row.context["last_node"], "post");
    assert_eq!(row.context["execution_id"], "exec-tctx-2");
}

/// 验收：error 终态行的 context 同样是九根快照——终态面三种落点
/// （completed/cancelled/error）全走根快照。节点失败按 §9.1 可重试分类
/// 走重排，预算（PLAITA_MAX_DELIVERIES=1）立刻打穿 → -520 终态。
#[test]
fn error_row_context_is_nine_root_snapshot() {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => "tctx-errorrow".to_string(),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        "PLAITA_MAX_DELIVERIES" => "1".to_string(),
        _ => String::new(),
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
    // condition 节点引用缺失根路径的表达式 → 节点失败（可重试 Node 错误）。
    flowsource::preset_flow_definition(
        backend.as_ref(),
        &cfg,
        "",
        "wf-err",
        "v1",
        r#"{"flow_id":"wf-err","nodes":[
            {"type":"start","id":"s","next":"boom"},
            {"type":"assignment","id":"boom","output":{"x":"$INPUT.deep[0].missing"},"next":"end"},
            {"type":"end","id":"end","output":"done","resultType":"success"}
        ]}"#
        .as_bytes(),
    )
    .unwrap();
    queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": "wf-err",
                "execution_id": "exec-tctx-3",
                "params": {},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &cfg, "", "exec-tctx-3")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_ERROR, "budget-exhausted terminal");
    assert_eq!(
        row.error.as_ref().unwrap()["error_type"],
        "node_retry_budget_exhausted"
    );
    for root in NINE_ROOTS {
        assert!(
            row.context.get(root).is_some(),
            "error row context root {root:?} present: {:?}",
            row.context
        );
    }
    assert_eq!(row.context["flow_id"], "wf-err");
    assert_eq!(row.context["execution_id"], "exec-tctx-3");
}
