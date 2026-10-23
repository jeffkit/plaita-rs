//! 终态 context 采集面（rs#17 WK-M3；rs-worker-design §7.4 宿主钩子增量）：
//! 根执行帧的 FlowEnd 附「精确九根快照」（`RuntimeCtx::snapshot_json` 形状），
//! 子流/分支/迭代帧一律 `context: None`——worker 终态行直写该快照，不再用
//! 种子 + `$NODE` 事件流重建。go 对照面：OnFlowEnd 以 `flow == run.rootFlow`
//! 对象同一性抓根运行时（worker.go:229-247），buildCheckpoint 读根上下文
//! （worker.go:2446-2474）；本面把等价快照搬到事件载荷上。

use std::sync::Arc;
use std::sync::Mutex;

use plaita_engine::exec::{Event, Host};
use plaita_engine::{ir, FlowError};
use serde_json::{json, Value};

fn load(name: &str) -> plaita_engine::Flow {
    let data = std::fs::read(format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR")))
        .unwrap_or_else(|e| panic!("read {name}: {e}"));
    ir::load(&data).unwrap_or_else(|e| panic!("load {name}: {e}"))
}

/// 收集一次执行的 FlowEnd 载荷。
fn flow_ends(
    flow: &plaita_engine::Flow,
    input: Value,
    host: Host,
) -> Vec<(String, Option<Value>, Option<String>, Option<Value>)> {
    let seen: Arc<Mutex<Vec<(String, Option<Value>, Option<String>, Option<Value>)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let host = host.listen(Arc::new(move |ev: &Event| {
        if let Event::FlowEnd {
            flow_id,
            result,
            error,
            context,
        } = ev
        {
            sink.lock().unwrap().push((
                flow_id.clone(),
                result.clone(),
                error.clone(),
                context.clone(),
            ));
        }
    }));
    let _ = flow.execute(&input, &host);
    let out = seen.lock().unwrap().clone();
    out
}

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

/// 根执行 FlowEnd 附九根快照：形状逐根在册、input/nodes 内容与执行一致、
/// host 注入的 execution_id 随行。
#[test]
fn root_flow_end_carries_exact_nine_root_snapshot() {
    let flow = load("hello_world.json");
    let ends = flow_ends(
        &flow,
        json!({"k": "v"}),
        Host::with_execution_id("exec-ctx-1"),
    );
    assert!(!ends.is_empty(), "at least one FlowEnd");
    let with_ctx: Vec<_> = ends.iter().filter(|(_, _, _, c)| c.is_some()).collect();
    assert_eq!(
        with_ctx.len(),
        1,
        "exactly ONE FlowEnd carries the root snapshot: {ends:?}"
    );
    let (flow_id, result, error, ctx) = with_ctx[0];
    assert_eq!(flow_id.as_str(), flow.id.as_str());
    assert_eq!(error, &None, "successful run has no error text");
    assert_eq!(result, &Some(json!("Hello, world!")));
    let ctx = ctx.as_ref().unwrap();
    for root in NINE_ROOTS {
        assert!(ctx.get(root).is_some(), "snapshot root {root:?} missing");
    }
    assert_eq!(ctx["flow_id"], flow.id.as_str());
    assert_eq!(ctx["execution_id"], "exec-ctx-1");
    assert_eq!(ctx["input"], json!({"k": "v"}));
    // nodes 根 = 已执行节点结果映射（go $NODE 同义）——start 节点在册。
    assert!(
        ctx["nodes"]
            .as_object()
            .map(|m| !m.is_empty())
            .unwrap_or(false),
        "nodes root records executed results: {ctx}"
    );
}

/// 子流帧的 FlowEnd 不带快照（context=None）：嵌套 child 执行里只有根帧
/// 一份 Some——worker 侧「最后一份 Some 即根终态」的选取面永不被子流污染。
#[test]
fn child_flow_end_carries_no_snapshot() {
    let flow = load("child_flow.json");
    let ends = flow_ends(&flow, json!({"name": "plaita"}), Host::new());
    // greet_go（根）→ child 节点 c → greet_cf（子）：两个 FlowEnd。
    assert!(
        ends.len() >= 2,
        "nested child emits its own FlowEnd: {ends:?}"
    );
    let some: Vec<_> = ends.iter().filter(|(_, _, _, c)| c.is_some()).collect();
    assert_eq!(some.len(), 1, "only the ROOT frame attaches a snapshot");
    let (flow_id, _, _, ctx) = some[0];
    assert_eq!(flow_id, "greet_go", "the snapshot belongs to the root flow");
    assert_eq!(ctx.as_ref().unwrap()["flow_id"], "greet_go");
    for (flow_id, _, _, ctx) in &ends {
        if flow_id == "greet_cf" {
            assert!(ctx.is_none(), "child frame must not attach a snapshot");
        }
    }
}

/// 挂起路径：根帧 FlowEnd（context=Some）在挂起时也发射（nil result/
/// nil error）——快照反映挂起瞬间的上下文，worker 的挂起行不用它（挂起行
/// 用 checkpoint 的精确快照），但事件面形状一致。
#[test]
fn suspended_root_flow_end_still_carries_snapshot() {
    struct AcceptAll;
    impl plaita_engine::exec::SubscriptionRegistry for AcceptAll {
        fn register(
            &self,
            req: &mut plaita_engine::exec::SubscriptionRequest,
        ) -> Result<(), String> {
            req.subscription_id = "sub-1".to_string();
            Ok(())
        }
        fn unregister(&self, _id: &str, _tenant: &str) -> Result<(), String> {
            Ok(())
        }
    }
    // 事件节点挂起：挂起需要活跃唤醒源（无注册表 = SuspendRejected 具名拒绝，
    // 不产生挂起）。
    let data = serde_json::json!({
        "flow_id": "suspend-probe",
        "nodes": [
            {"type": "start", "id": "s", "next": "ev"},
            {"type": "event", "id": "ev", "event_type": "probe", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "done"}
        ]
    });
    let flow = ir::load(data.to_string().as_bytes()).unwrap();
    let host = Host::new().with_subscriptions(Arc::new(AcceptAll));
    let ends = flow_ends(&flow, json!({}), host);
    let some: Vec<_> = ends.iter().filter(|(_, _, _, c)| c.is_some()).collect();
    assert_eq!(some.len(), 1, "root FlowEnd carries the snapshot: {ends:?}");
    let (_, result, error, _) = some[0];
    assert_eq!(result, &None, "suspension reports nil result");
    assert!(error.is_none(), "suspension reports no error");
}

/// 恢复会话（from_snapshot_json → from_parts → resume）的根帧 FlowEnd
/// 同样附快照，且快照继承 checkpoint 上下文 + 新节点结果——终态行九根
/// 与恢复链一致（M3 行为测试的引擎侧地基）。
#[test]
fn resumed_session_root_flow_end_carries_inherited_snapshot() {
    struct AcceptAll;
    impl plaita_engine::exec::SubscriptionRegistry for AcceptAll {
        fn register(
            &self,
            req: &mut plaita_engine::exec::SubscriptionRequest,
        ) -> Result<(), String> {
            req.subscription_id = "sub-1".to_string();
            Ok(())
        }
        fn unregister(&self, _id: &str, _tenant: &str) -> Result<(), String> {
            Ok(())
        }
    }
    let data = serde_json::json!({
        "flow_id": "resume-probe",
        "nodes": [
            {"type": "start", "id": "s", "next": "set"},
            {"type": "assignment", "id": "set", "output": {"a": "first"}, "next": "ev"},
            {"type": "event", "id": "ev", "event_type": "probe", "next": "fin"},
            {"type": "assignment", "id": "fin", "output": {"b": "second"}, "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE"}
        ]
    });
    let flow = ir::load(data.to_string().as_bytes()).unwrap();
    let host = Host::with_execution_id("exec-resume-1").with_subscriptions(Arc::new(AcceptAll));
    // 第一会话：推进到 event 节点挂起。
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let suspended = match err {
        FlowError::Suspended(s) => s,
        other => panic!("expected suspension, got {other:?}"),
    };
    let ctx_snapshot = suspended.checkpoint_json();
    // 第二会话：重建 → 绑定 decision 载荷 → 续跑至完成。（监听器必须在
    // from_parts 之前挂上——Host::listen 是 builder，返回新 Host。）
    let seen: Arc<Mutex<Vec<Option<Value>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let host = host.listen(Arc::new(move |ev: &Event| {
        if let Event::FlowEnd { context, .. } = ev {
            sink.lock().unwrap().push(context.clone());
        }
    }));
    let rc = plaita_engine::RuntimeCtx::from_snapshot_json(&ctx_snapshot)
        .unwrap()
        .shared();
    let suspended_exec = plaita_engine::exec::SuspendedExecution::from_parts(
        flow.clone(),
        rc,
        host.clone(),
        suspended.suspended_node.clone(),
    );
    let ends = {
        let result = suspended_exec.resume(
            plaita_engine::exec::ResumeKind::Event,
            json!({"ok": true, plaita_engine::exec::RESUME_BINDING_KEY: "sub-1"}),
        );
        let out = seen.lock().unwrap().clone();
        assert!(result.is_ok(), "resume must complete: {result:?}");
        out
    };
    let some: Vec<_> = ends.iter().filter(|c| c.is_some()).collect();
    assert_eq!(
        some.len(),
        1,
        "exactly one root snapshot in the resume session"
    );
    let ctx = some[0].as_ref().unwrap();
    // 继承断言：第一会话的 $NODE.set 在、第二会话的 $NODE.fin 也在。
    assert_eq!(ctx["nodes"]["set"], json!({"a": "first"}));
    assert_eq!(ctx["nodes"]["fin"], json!({"b": "second"}));
    assert_eq!(ctx["last_node"], json!("fin"));
    assert_eq!(ctx["execution_id"], "exec-resume-1");
}
