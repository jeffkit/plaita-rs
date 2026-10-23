//! E3 快照往返性质测试（rs-worker-design §7.4 E3，worker M1 的引擎侧前置）：
//! `snapshot_json → from_snapshot_json → snapshot_json` 恒等；重建后的
//! `node_state()`/`subscription_id()` 与挂起时一致；重建现场的 `resume`
//! 准入行为（含 `RESUME_BINDING_KEY` 绑定校验、陈旧载体具名拒绝）与进程内
//! 挂起完全一致——worker 层的 checkpoint 恢复因此可以安全走
//! `from_parts + resume` 这一条链（严禁 fork 恢复逻辑）。

use std::sync::{Arc, Mutex};

use plaita_engine::context::RuntimeCtx;
use plaita_engine::error::FlowError;
use plaita_engine::exec::{
    Host, ResumeKind, SubscriptionRegistry, SubscriptionRequest, RESUME_BINDING_KEY,
};
use plaita_engine::ir;
use plaita_engine::model::Flow;
use serde_json::{json, Value};

#[derive(Default)]
struct RecordingRegistry {
    seq: Mutex<usize>,
    registered: Mutex<Vec<SubscriptionRequest>>,
    unregistered: Mutex<Vec<String>>,
}

impl SubscriptionRegistry for RecordingRegistry {
    fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String> {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        req.subscription_id = format!("sub-{}", seq);
        self.registered.lock().unwrap().push(req.clone());
        Ok(())
    }

    fn unregister(&self, subscription_id: &str, _tenant_id: &str) -> Result<(), String> {
        self.unregistered
            .lock()
            .unwrap()
            .push(subscription_id.to_string());
        Ok(())
    }
}

fn event_flow() -> String {
    r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"e1"},
        {"type":"event","id":"e1","event_type":"tick","next":"mid"},
        {"type":"assignment","id":"mid","output":{"seen":"$NODE.e1.status"},"next":"end"},
        {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
    ]}"#
    .to_string()
}

fn host_with_registry() -> (Host, Arc<RecordingRegistry>) {
    let registry = Arc::new(RecordingRegistry::default());
    let host = Host::with_execution_id("exec-rt-1")
        .with_tenant_id("tenant-rt")
        .with_subscriptions(registry.clone());
    (host, registry)
}

fn suspend_of(result: Result<Value, FlowError>) -> plaita_engine::exec::SuspendedExecution {
    match result {
        Err(FlowError::Suspended(s)) => *s,
        other => panic!("expected suspension, got {:?}", other.err()),
    }
}

/// 挂起 → 快照 → 重建（E1+E2）→ 快照：恒等。
#[test]
fn snapshot_rebuild_snapshot_is_identity() {
    let flow = ir::load(event_flow().as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({"who": "rt"}), &host));
    let snap1 = suspended.checkpoint_json();
    let node_state1 = suspended.node_state();
    let sub_id1 = suspended.subscription_id();

    // 从快照重建：ir::load 重建 Flow + from_snapshot_json 重建 rc + from_parts。
    let flow2: Flow = ir::load(event_flow().as_bytes()).unwrap();
    let rc2 = RuntimeCtx::from_snapshot_json(&snap1)
        .expect("nine-root snapshot must rebuild")
        .shared();
    let (host2, _registry2) = host_with_registry();
    let rebuilt = plaita_engine::exec::SuspendedExecution::from_parts(
        flow2,
        rc2,
        host2,
        suspended.suspended_node.clone(),
    );

    assert_eq!(
        rebuilt.checkpoint_json(),
        snap1,
        "snapshot roundtrip identity"
    );
    assert_eq!(rebuilt.node_state(), node_state1);
    assert_eq!(rebuilt.subscription_id(), sub_id1);
    assert_eq!(rebuilt.suspended_node, suspended.suspended_node);
}

/// 重建现场的 resume 准入与进程内挂起一致：陈旧载体具名拒绝（ResumeGuard，
/// 执行保持挂起语义），正确绑定完成续跑且输出与进程内 resume 相同、订阅
/// 注销发生、$LAST_NODE 推进。
#[test]
fn rebuilt_resume_admission_matches_in_process() {
    // 进程内基准：绑定不符 → ResumeGuard；绑定正确 → 完成。
    let flow = ir::load(event_flow().as_bytes()).unwrap();
    let (host, registry_a) = host_with_registry();
    let in_process = suspend_of(flow.execute(&json!({"who": "rt"}), &host));
    let guard_err = in_process
        .resume(ResumeKind::Event, json!({"subscription_id": "sub-99"}))
        .unwrap_err();
    assert!(matches!(guard_err, FlowError::ResumeGuard { .. }));
    let in_process_output = {
        let flow = ir::load(event_flow().as_bytes()).unwrap();
        let (host, _) = host_with_registry();
        let s = suspend_of(flow.execute(&json!({"who": "rt"}), &host));
        let payload = json!({
            RESUME_BINDING_KEY: s.node_state()["subscription_id"],
            "event_type": "tick",
            "status": "completed",
        });
        s.resume(ResumeKind::Event, payload).unwrap()
    };

    // 重建现场：同样的陈旧载体必须被同一条准入链拒绝（错误面一致）。
    let flow = ir::load(event_flow().as_bytes()).unwrap();
    let (host, registry_b) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({"who": "rt"}), &host));
    let snap = suspended.checkpoint_json();
    let rebuilt = {
        let flow2 = ir::load(event_flow().as_bytes()).unwrap();
        let rc2 = RuntimeCtx::from_snapshot_json(&snap).unwrap().shared();
        let (host2, _) = host_with_registry();
        plaita_engine::exec::SuspendedExecution::from_parts(
            flow2,
            rc2,
            host2,
            suspended.suspended_node.clone(),
        )
    };
    let rebuilt_err = rebuilt
        .resume(ResumeKind::Event, json!({"subscription_id": "sub-99"}))
        .unwrap_err();
    assert_eq!(rebuilt_err.to_string(), guard_err.to_string());

    // 正确绑定（私有键形态，由 worker 层注入）→ 与进程内 resume 同输出。
    let flow = ir::load(event_flow().as_bytes()).unwrap();
    let (host, registry_c) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({"who": "rt"}), &host));
    let snap = suspended.checkpoint_json();
    let sub_id = suspended.subscription_id().unwrap();
    let rebuilt = {
        let flow2 = ir::load(event_flow().as_bytes()).unwrap();
        let rc2 = RuntimeCtx::from_snapshot_json(&snap).unwrap().shared();
        let (host2, registry_rebuilt) = host_with_registry();
        let rebuilt = plaita_engine::exec::SuspendedExecution::from_parts(
            flow2,
            rc2,
            host2,
            suspended.suspended_node.clone(),
        );
        (rebuilt, registry_rebuilt)
    };
    let (rebuilt, registry_rebuilt) = rebuilt;
    let payload = json!({
        RESUME_BINDING_KEY: sub_id,
        "event_type": "tick",
        "status": "completed",
    });
    let output = rebuilt.resume(ResumeKind::Event, payload).unwrap();
    assert_eq!(output, in_process_output);
    assert_eq!(output["seen"], "completed");
    // 恢复会话注销订阅：resume 链消费的是重建 host 上登记的注册表。
    assert_eq!(*registry_rebuilt.unregistered.lock().unwrap(), [sub_id]);
    // 原挂起 host 的注册表不受重建恢复影响；注册表 id 互不串扰。
    assert_eq!(registry_c.unregistered.lock().unwrap().len(), 0);
    assert_eq!(registry_b.unregistered.lock().unwrap().len(), 0);
    let _ = registry_a;
}

/// 缺键/错型一律具名错误，静默缺省禁止（fail-closed）。
#[test]
fn missing_or_mistyped_roots_are_named_errors() {
    let full = json!({
        "input": {}, "nodes": {}, "errors": {}, "env": {}, "global": {},
        "last_node": null, "last_branch": null, "flow_id": "f", "execution_id": "e",
    });
    for root in [
        "input",
        "nodes",
        "errors",
        "env",
        "global",
        "last_node",
        "last_branch",
        "flow_id",
        "execution_id",
    ] {
        let mut broken = full.clone();
        broken.as_object_mut().unwrap().remove(root);
        let err = RuntimeCtx::from_snapshot_json(&broken).unwrap_err();
        assert!(
            err.contains(&format!("missing root {root:?}")),
            "named missing-root error expected for {root:?}, got: {err}"
        );
    }
    // 错型：nodes 非对象 / last_node 非空非串 / flow_id 非串 / 非对象快照。
    let mut bad = full.clone();
    bad["nodes"] = json!([1]);
    assert!(RuntimeCtx::from_snapshot_json(&bad)
        .unwrap_err()
        .contains("nodes"));
    let mut bad = full.clone();
    bad["last_node"] = json!(7);
    assert!(RuntimeCtx::from_snapshot_json(&bad)
        .unwrap_err()
        .contains("last_node"));
    let mut bad = full.clone();
    bad["flow_id"] = json!(null);
    assert!(RuntimeCtx::from_snapshot_json(&bad)
        .unwrap_err()
        .contains("flow_id"));
    assert!(RuntimeCtx::from_snapshot_json(&json!([1, 2]))
        .unwrap_err()
        .contains("JSON object"));

    // 重建出的根执行上下文可正常求值（九根齐全 → 表达式根可用）。
    let ctx = RuntimeCtx::from_snapshot_json(&full).unwrap();
    assert_eq!(ctx.flow_id(), "f");
    assert_eq!(ctx.execution_id(), "e");
    assert!(ctx.is_root_execution(), "rebuilt snapshot has no parent");
}
