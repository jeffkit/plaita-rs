//! 嵌套挂起 v1（rs#16）专项测试：child/reference 调用链的挂起穿透、
//! 沿栈恢复（决策步→返回步）、恢复中再挂起的栈合并、深位订阅绑定。

use std::sync::{Arc, Mutex};

use plaita_engine::error::FlowError;
use plaita_engine::exec::{
    Host, ResumeKind, SubscriptionRegistry, SubscriptionRequest, RESUME_BINDING_KEY,
};
use plaita_engine::ir;
use serde_json::{json, Value};

#[derive(Default)]
struct FakeRegistry {
    seq: Mutex<usize>,
}

impl SubscriptionRegistry for FakeRegistry {
    fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String> {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        req.subscription_id = format!("sub-{}", seq);
        Ok(())
    }

    fn unregister(&self, _subscription_id: &str, _tenant_id: &str) -> Result<(), String> {
        Ok(())
    }
}

fn host() -> Host {
    Host::with_execution_id("exec-nested")
        .with_tenant_id("t1")
        .with_subscriptions(Arc::new(FakeRegistry::default()))
}

fn suspend_of(result: Result<Value, FlowError>) -> Box<plaita_engine::exec::SuspendedExecution> {
    match result {
        Err(FlowError::Suspended(s)) => s,
        other => panic!("expected suspension, got {:?}", other.err()),
    }
}

fn bound(
    suspended: Box<plaita_engine::exec::SuspendedExecution>,
    kind: ResumeKind,
    payload: Value,
) -> Result<Value, FlowError> {
    let mut payload = payload;
    if let Value::Object(map) = &mut payload {
        map.entry(RESUME_BINDING_KEY.to_string())
            .or_insert_with(|| suspended.node_state()["subscription_id"].clone());
    }
    suspended.resume(kind, payload)
}

#[test]
fn grandchild_chain_parks_and_resumes_through_two_frames() {
    // root → child c1 → child c2 → event：两层调用栈。
    let doc = r#"{"flow_id":"root","nodes":[
        {"type":"start","id":"s","next":"c1"},
        {"type":"child","id":"c1","next":"o","childFlow":{"nodes":[
            {"type":"start","id":"a","next":"c2"},
            {"type":"child","id":"c2","next":"aend","childFlow":{"nodes":[
                {"type":"start","id":"b","next":"ev"},
                {"type":"event","id":"ev","event_type":"deep","next":"bend"},
                {"type":"end","id":"bend","output":{"from":"deep","answer":"$NODE.ev"},"resultType":"success"}
            ]}},
            {"type":"end","id":"aend","output":"$NODE.c2","resultType":"success"}
        ]}},
        {"type":"end","id":"o","output":"$NODE.c1","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let suspended = suspend_of(flow.execute(&json!({}), &host()));

    // 挂起在最内层 event；调用栈 = [c1 帧(root), c2 帧(c1.childFlow)]。
    assert_eq!(suspended.suspended_node, "ev");
    let path = suspended.call_stack_path();
    assert_eq!(path.len(), 2);
    assert_eq!(path[0], ("c1.childFlow".to_string(), "c2".to_string()));
    assert_eq!(path[1], ("root".to_string(), "c1".to_string()));
    // checkpoint 载路径（观测面）。
    assert_eq!(
        suspended.checkpoint_json()["call_stack"],
        json!([
            {"flow_id": "c1.childFlow", "node_id": "c2"},
            {"flow_id": "root", "node_id": "c1"},
        ])
    );

    // 恢复：决策步 → c2 内层续跑到 bend（返回步回 c1 帧）→ c1 续跑到 aend
    // （返回步回 root 帧）→ root 续跑到 o。
    let out = bound(suspended, ResumeKind::Event, json!({"k": "v"})).unwrap();
    // 根 end 输出 $NODE.c1 → $NODE.c2 → 内层 end 的字面输出；$NODE.ev 是
    // 完整恢复态（挂起态全量保留 + status 翻转 + event_data）。
    assert_eq!(out["from"], "deep");
    let ev = &out["answer"];
    assert_eq!(ev["status"], "completed");
    assert_eq!(ev["event_type"], "deep");
    assert_eq!(ev["event_data"], json!({"k": "v"}));
    assert_eq!(ev["subscription_tenant"], "t1");
    assert!(ev["event_id"].as_str().unwrap().starts_with("event_ev_"));
    // 完整状态键面（全量保留的钉子；插入序不定，按键集比对）。
    let mut keys: Vec<&str> = ev.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "event_data",
            "event_filter",
            "event_id",
            "event_type",
            "is_async",
            "status",
            "subscription_id",
            "subscription_tenant",
        ]
    );
}

#[test]
fn reference_chain_isolation_preserved_through_suspension() {
    // reference 帧（隔离上下文）内的挂起：穿透 + 隔离不破坏。
    let flow_doc = json!({
        "flow_id": "rootf",
        "nodes": [
            {"type": "start", "id": "s", "next": "r"},
            {"flow_id": "sub", "id": "r", "input": {"n": "$INPUT.name"}, "next": "o", "type": "reference"},
            {"type": "end", "id": "o", "output": "$NODE.r", "resultType": "success"},
        ],
    });
    let flow =
        ir::load_with_flow_provider(&serde_json::to_vec(&flow_doc).unwrap(), &Provider).unwrap();

    let suspended = suspend_of(flow.execute(&json!({"name": "kong"}), &host()));
    assert_eq!(suspended.suspended_node, "ev");
    assert_eq!(
        suspended.call_stack_path(),
        vec![("rootf".to_string(), "r".to_string())]
    );
    // 深位决策步的订阅绑定准入照常生效。
    let out = bound(suspended, ResumeKind::Event, json!({"x": 1})).unwrap();
    // 隔离保持：$PARENT.n → null（reference 隔离），$INPUT 是注入的 {n}。
    // 内层 end 输出 = 字面对象（$NODE.ev 是事件状态，不直接出现在 end 输出）。
    assert_eq!(out, json!({"got": {"n": "kong"}, "parent_leak": null}));
}

struct Provider;
impl plaita_engine::ir::FlowProvider for Provider {
    fn load_flow_json(&self, flow_id: &str, _v: Option<&str>) -> Result<Value, String> {
        match flow_id {
            "sub" => Ok(json!({
                "flow_id": "sub",
                "nodes": [
                    {"type": "start", "id": "a", "next": "ev"},
                    {"type": "event", "id": "ev", "event_type": "sub-event", "next": "b"},
                    {"type": "end", "id": "b", "output": {"got": "$INPUT", "parent_leak": "$PARENT.n"}, "resultType": "success"},
                ],
            })),
            other => Err(format!("flow {other:?} not in registry")),
        }
    }
}

#[test]
fn resume_continuation_re_suspension_merges_call_stacks() {
    // root → child c1 → [event A → child c2 → event B → …]：
    // 第一次恢复 A 后内层继续走到 c2 内的 B → 再次挂起，栈合并。
    let doc = r#"{"flow_id":"root","nodes":[
        {"type":"start","id":"s","next":"c1"},
        {"type":"child","id":"c1","next":"o","childFlow":{"nodes":[
            {"type":"start","id":"a","next":"evA"},
            {"type":"event","id":"evA","event_type":"first","next":"c2"},
            {"type":"child","id":"c2","next":"aend","childFlow":{"nodes":[
                {"type":"start","id":"b","next":"evB"},
                {"type":"event","id":"evB","event_type":"second","next":"bend"},
                {"type":"end","id":"bend","output":"done","resultType":"success"}
            ]}},
            {"type":"end","id":"aend","output":"$NODE.c2","resultType":"success"}
        ]}},
        {"type":"end","id":"o","output":"$NODE.c1","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let host = host();
    let first = suspend_of(flow.execute(&json!({}), &host));
    assert_eq!(first.suspended_node, "evA");
    assert_eq!(first.call_stack_path().len(), 1);

    // 第一次恢复：决策步后内层继续 → c2 → evB 再次挂起。
    let second = match bound(first, ResumeKind::Event, json!({})).unwrap_err() {
        FlowError::Suspended(inner) => inner,
        other => panic!("expected re-suspension, got {:?}", other),
    };
    assert_eq!(second.suspended_node, "evB");
    // 栈合并：新现场（c2 帧）在前，剩余（c1 帧）在后。
    let path = second.call_stack_path();
    assert_eq!(path.len(), 2);
    assert_eq!(path[0], ("c1.childFlow".to_string(), "c2".to_string()));
    assert_eq!(path[1], ("root".to_string(), "c1".to_string()));

    // 第二次恢复：一路返回步到根。
    let out = bound(second, ResumeKind::Event, json!({})).unwrap();
    assert_eq!(out, json!("done"));
}

#[test]
fn deep_binding_enforcement_and_timeout_at_depth() {
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"c"},
        {"type":"child","id":"c","next":"o","childFlow":{"nodes":[
            {"type":"start","id":"a","next":"ev"},
            {"type":"event","id":"ev","event_type":"t","next":"b"},
            {"type":"end","id":"b","output":"$NODE.ev","resultType":"success"}
        ]}},
        {"type":"end","id":"o","output":"$NODE.c","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();

    // 错误绑定 → ResumeGuard（深位照常准入）。
    let suspended = suspend_of(flow.execute(&json!({}), &host()));
    let err = suspended
        .resume(ResumeKind::Event, json!({"subscription_id": "wrong"}))
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("not bound to the suspended subscription"));

    // timeout 载体在深位：内层节点状态翻 timeout，返回步带回父帧。
    let flow = ir::load(doc.as_bytes()).unwrap();
    let suspended = suspend_of(flow.execute(&json!({}), &host()));
    let out = bound(suspended, ResumeKind::Timeout, json!({})).unwrap();
    assert_eq!(out["status"], "timeout");
}

#[test]
fn loop_reference_bypass_is_named_rejected_without_orphan_subscription() {
    // 验证报告 Q4 旁路组合：loop → reference → event——reference 的 ambient
    // 父链遮蔽 frame_chain_blocker，闸放行并注册订阅；传播转换点（主闸）必须
    // 具名拒绝**并注销该订阅**（无孤儿）。
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingRegistry {
        registered: AtomicUsize,
        unregistered: AtomicUsize,
    }
    impl plaita_engine::exec::SubscriptionRegistry for CountingRegistry {
        fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String> {
            let n = self.registered.fetch_add(1, Ordering::SeqCst) + 1;
            req.subscription_id = format!("sub-{n}");
            Ok(())
        }
        fn unregister(&self, _id: &str, _t: &str) -> Result<(), String> {
            self.unregistered.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let registry = Arc::new(CountingRegistry {
        registered: AtomicUsize::new(0),
        unregistered: AtomicUsize::new(0),
    });
    let host = Host::with_execution_id("exec-1").with_subscriptions(registry.clone());

    let flow_doc = json!({
        "flow_id": "f",
        "nodes": [
            {"type": "start", "id": "s", "next": "lp"},
            {"type": "loop", "id": "lp", "collection": "$INPUT.items", "next": "o", "childFlow": {
                "nodes": [
                    {"type": "start", "id": "a", "next": "r"},
                    {"flow_id": "sub", "id": "r", "next": "b", "type": "reference"},
                    {"type": "end", "id": "b", "resultType": "success"},
                ],
            }},
            {"type": "end", "id": "o", "resultType": "success"},
        ],
    });
    struct P;
    impl plaita_engine::ir::FlowProvider for P {
        fn load_flow_json(&self, id: &str, _v: Option<&str>) -> Result<Value, String> {
            if id == "sub" {
                Ok(json!({
                    "flow_id": "sub",
                    "nodes": [
                        {"type": "start", "id": "x", "next": "ev"},
                        {"type": "event", "id": "ev", "event_type": "t", "next": "y"},
                        {"type": "end", "id": "y", "resultType": "success"},
                    ],
                }))
            } else {
                Err(format!("no flow {id}"))
            }
        }
    }
    let flow = ir::load_with_flow_provider(&serde_json::to_vec(&flow_doc).unwrap(), &P).unwrap();
    let err = flow.execute(&json!({"items": [1]}), &host).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("loop-iteration") && text.contains("crossed"),
        "named loop-boundary rejection expected, got: {text}"
    );
    assert_eq!(registry.registered.load(Ordering::SeqCst), 1);
    assert_eq!(
        registry.unregistered.load(Ordering::SeqCst),
        1,
        "no orphan subscription"
    );
}

#[test]
fn loop_boundary_rejection_is_not_swallowed_by_error_handler() {
    // 验证报告修正清单 #3：loop 节点自带 errorHandler continue 时，闸拒绝
    // 必须仍以终态上抛（SuspendRejected 拦截臂）。
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"lp"},
        {"type":"loop","id":"lp","collection":"$INPUT.items","next":"o","errorHandler":{"strategy":"continue"},"childFlow":{"nodes":[
            {"type":"start","id":"a","next":"ev"},
            {"type":"event","id":"ev","event_type":"t","next":"b"},
            {"type":"end","id":"b","resultType":"success"}
        ]}},
        {"type":"end","id":"o","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let err = flow.execute(&json!({"items": [1]}), &host()).unwrap_err();
    assert!(
        err.to_string().contains("loop-iteration"),
        "rejection must not be swallowed by errorHandler continue, got: {err}"
    );
}

#[test]
fn parallel_background_branch_is_statically_rejected() {
    // 后台分支（不在 joinBranches）：派发前的静态闸同步具名拒。
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"p"},
        {"type":"parallel","id":"p","branches":[
            {"name":"bg","flow":{"nodes":[
                {"type":"start","id":"bs","next":"be"},
                {"type":"event","id":"be","event_type":"t","next":"bend"},
                {"type":"end","id":"bend","resultType":"success"}
            ]}}
        ],"next":"o"},
        {"type":"end","id":"o","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let err = flow.execute(&json!({}), &host()).unwrap_err();
    assert!(
        err.to_string().contains("contains a suspending node"),
        "static rejection expected, got: {err}"
    );
}

#[test]
fn resume_during_cancel_is_refused() {
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"e1"},
        {"type":"event","id":"e1","event_type":"t","next":"end"},
        {"type":"end","id":"end","output":"$NODE.e1","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let host = host();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    host.cancel();
    let err = suspended.resume(ResumeKind::Event, json!({})).unwrap_err();
    assert!(
        matches!(err, FlowError::Cancelled { .. }),
        "cancel gate expected, got: {err}"
    );
}

#[test]
fn reference_flowid_producer_dialect_key_is_accepted() {
    // NS-M4 对拍集顺带发现：plaita 生产者方言写 `flowID`（ReferenceFlow
    // .LEGACY_KEYS），装载层原生折叠（不再需要 runner 侧归一）。
    struct P;
    impl plaita_engine::ir::FlowProvider for P {
        fn load_flow_json(&self, id: &str, _v: Option<&str>) -> Result<Value, String> {
            if id == "sub" {
                Ok(json!({
                    "flow_id": "sub",
                    "nodes": [
                        {"type": "start", "id": "a", "next": "b"},
                        {"type": "end", "id": "b", "output": "ok", "resultType": "success"},
                    ],
                }))
            } else {
                Err(format!("no flow {id}"))
            }
        }
    }
    // flowID（生产者方言）/ flow_version（camel）都走原生折叠。
    let doc = json!({
        "flow_id": "root",
        "nodes": [
            {"type": "start", "id": "s", "next": "r"},
            {"flowID": "sub", "flowVersion": "3", "id": "r", "input": {"n": 1}, "next": "o", "type": "reference"},
            {"type": "end", "id": "o", "output": "$NODE.r", "resultType": "success"},
        ],
    });
    let flow = ir::load_with_flow_provider(&serde_json::to_vec(&doc).unwrap(), &P)
        .expect("flowID producer dialect must load");
    assert_eq!(flow.find_node("r").unwrap().node_type, "reference");
}
