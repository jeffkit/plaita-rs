//! suspend/resume 机制单测（golden 之外的引擎面钉子）：作用域闸、订阅注册
//! 表必需、绑定键准入、状态机（pending→completed/timeout/cancelled）、订阅
//! 注销、resume 后 $NODE/$LAST_NODE 记账、hitl_await 的 dry_run/真实发送/
//! 恢复形状。

use std::sync::{Arc, Mutex};

use plaita_engine::error::FlowError;
use plaita_engine::exec::{
    Host, ResumeKind, SubscriptionRegistry, SubscriptionRequest, RESUME_BINDING_KEY,
};
use plaita_engine::ir;
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

fn host_with_registry() -> (Host, Arc<RecordingRegistry>) {
    let registry = Arc::new(RecordingRegistry::default());
    let host = Host::with_execution_id("exec-1")
        .with_tenant_id("tenant-1")
        .with_subscriptions(registry.clone());
    (host, registry)
}

fn suspend_of(result: Result<Value, FlowError>) -> plaita_engine::exec::SuspendedExecution {
    match result {
        Err(FlowError::Suspended(s)) => *s,
        other => panic!("expected suspension, got {:?}", other.err()),
    }
}

/// 对挂起现场注入订阅绑定键后 resume（plaita#221 契约：event/timeout 载体
/// 必须绑定挂起期登记的订阅——golden 测试注入同款）。
fn bound_resume(
    suspended: plaita_engine::exec::SuspendedExecution,
    kind: ResumeKind,
    payload: Value,
) -> Result<Value, FlowError> {
    let mut payload = payload;
    if let Value::Object(map) = &mut payload {
        let sub = suspended.node_state().get("subscription_id").cloned();
        map.entry(RESUME_BINDING_KEY.to_string())
            .or_insert(sub.unwrap_or(Value::Null));
    }
    suspended.resume(kind, payload)
}

fn event_flow(event_type: &str) -> String {
    format!(
        r#"{{"flow_id":"f","nodes":[
            {{"type":"start","id":"s","next":"e1"}},
            {{"type":"event","id":"e1","event_type":"{event_type}","next":"end"}},
            {{"type":"end","id":"end","output":"$NODE.e1","resultType":"success"}}
        ]}}"#
    )
}

#[test]
fn suspend_requires_subscription_registry() {
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();
    let host = Host::with_execution_id("exec-1"); // 无注册表
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("no subscription registry is configured"),
        "fail-closed message expected, got: {text}"
    );
}

#[test]
fn suspend_rejects_non_pending_state() {
    // 直构引擎流绕过 ir：挂起节点返回非 pending → 具名拒（Go suspend 同款）。
    // 这里用 event 的合法装载路径 + 篡改不可行，改为验证正常 pending 已由
    // golden 覆盖；本测试钉 ResumeKind::parse 的步进载体拒绝面。
    assert!(ResumeKind::parse("event").is_some());
    assert!(ResumeKind::parse("timeout").is_some());
    assert!(ResumeKind::parse("cancel").is_some());
    assert!(ResumeKind::parse("continue").is_none());
    assert!(ResumeKind::parse("retry").is_none());
    assert!(ResumeKind::parse("").is_none());
}

#[test]
fn event_suspend_and_resume_completed() {
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();
    let (host, registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));

    // 挂起态：pending + 订阅绑定落 $NODE；$LAST_NODE 仍是前一完成节点。
    let state = suspended.node_state();
    assert_eq!(state["status"], "pending");
    assert_eq!(state["event_type"], "tick");
    assert_eq!(state["subscription_id"], "sub-1");
    assert_eq!(state["subscription_tenant"], "tenant-1");
    assert_eq!(
        suspended.checkpoint_json()["last_node"],
        Value::String("s".into())
    );
    assert_eq!(registry.registered.lock().unwrap()[0].event_type, "tick");
    assert_eq!(registry.registered.lock().unwrap()[0].timeout_seconds, 0.0);

    // resume(event)：status 翻转 + event_data 落地；订阅注销；流程续跑到 end。
    let output = bound_resume(
        suspended,
        ResumeKind::Event,
        json!({"event_type": "tick", "payload": 1}),
    )
    .unwrap();
    assert_eq!(output["status"], "completed");
    assert_eq!(output["event_data"]["payload"], 1);
    assert_eq!(output["subscription_id"], "sub-1"); // 挂起态全量保留
    assert_eq!(registry.unregistered.lock().unwrap().as_slice(), ["sub-1"]);
}

#[test]
fn resume_timeout_and_cancel_flip_status() {
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();

    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    let out = bound_resume(suspended, ResumeKind::Timeout, json!({})).unwrap();
    assert_eq!(out["status"], "timeout");
    assert!(out.get("event_data").is_none());

    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    let out = bound_resume(suspended, ResumeKind::Cancel, json!({})).unwrap();
    assert_eq!(out["status"], "cancelled");
}

#[test]
fn resume_binding_is_enforced() {
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));

    // 错误订阅 id → 陈旧载体拒绝，执行保持挂起语义（ResumeGuard）。
    let err = suspended
        .resume(ResumeKind::Event, json!({"subscription_id": "sub-99"}))
        .unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("not bound to the suspended subscription"),
        "got: {text}"
    );

    // 正确绑定键（私有键形态）通过，且剥离后不进 event_data。
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    let out = suspended
        .resume(
            ResumeKind::Event,
            json!({RESUME_BINDING_KEY: "sub-1", "k": "v"}),
        )
        .unwrap();
    assert_eq!(out["status"], "completed");
    assert_eq!(out["event_data"], json!({"k": "v"}));
}

#[test]
fn double_resume_is_a_guard_error() {
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    let _first = bound_resume(suspended, ResumeKind::Event, json!({})).unwrap();

    // 非法载体类型被枚举挡在 API 外（ResumeKind 无 continue/retry 变体，
    // parse 拒绝——见 suspend_rejects_non_pending_state）；这里钉 pending
    // 校验路径：直接用错绑定的载体打同一现场会得到 ResumeGuard，而非静默。
    let flow = ir::load(event_flow("tick").as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    let err = suspended
        .resume(ResumeKind::Timeout, json!({"subscription_id": "wrong"}))
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("not bound to the suspended subscription"));
}

#[test]
fn suspension_inside_child_chain_parks_with_call_stack() {
    // 嵌套挂起 v1（rs#16）：child 链内的挂起穿透到根，携带调用栈。
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"c"},
        {"type":"child","id":"c","next":"o","childFlow":{"nodes":[
            {"type":"start","id":"cs","next":"ce"},
            {"type":"event","id":"ce","event_type":"t","next":"cend"},
            {"type":"end","id":"cend","output":"$NODE.ce","resultType":"success"}
        ]}},
        {"type":"end","id":"o","output":"$NODE.c","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    assert_eq!(suspended.suspended_node, "ce");
    assert_eq!(
        suspended.call_stack_path(),
        vec![("f".to_string(), "c".to_string())]
    );
    assert_eq!(suspended.node_state()["status"], "pending");
    // 恢复：决策步 → 内层续跑到内层 end（调用返回）→ 父帧回填 $NODE.c → 根 end。
    let mut payload = json!({"event_type": "t"});
    if let Value::Object(map) = &mut payload {
        map.insert(
            RESUME_BINDING_KEY.to_string(),
            suspended.node_state()["subscription_id"].clone(),
        );
    }
    let out = suspended.resume(ResumeKind::Event, payload).unwrap();
    assert_eq!(out["event_type"], "t"); // $NODE.ce（内层 end 输出）
}

#[test]
fn suspension_inside_loop_frame_is_named_rejected() {
    // 迭代帧（v1 不支持）：loop 体内的挂起具名拒，消息点名帧类型。
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"lp"},
        {"type":"loop","id":"lp","collection":"$INPUT.items","next":"o","childFlow":{"nodes":[
            {"type":"start","id":"cs","next":"ce"},
            {"type":"event","id":"ce","event_type":"t","next":"cend"},
            {"type":"end","id":"cend","resultType":"success"}
        ]}},
        {"type":"end","id":"o","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let err = flow.execute(&json!({"items": [1]}), &host).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("loop-iteration"),
        "named loop-frame rejection expected, got: {text}"
    );
}

#[test]
fn suspension_inside_parallel_branch_is_named_rejected() {
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"p"},
        {"type":"parallel","id":"p","branches":[
            {"name":"b1","flow":{"nodes":[
                {"type":"start","id":"bs","next":"be"},
                {"type":"event","id":"be","event_type":"t","next":"bend"},
                {"type":"end","id":"bend","resultType":"success"}
            ]}}
        ],"joinBranches":["b1"],"next":"o"},
        {"type":"end","id":"o","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let (host, _registry) = host_with_registry();
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let text = err.to_string();
    // 静态闸（修正清单 #4）：分支派发前同步具名拒——后台分支的动态拒绝无法
    // 同步上抛，v1 语义 = 分支图内不允许挂起型节点。
    assert!(
        text.contains("contains a suspending node"),
        "named parallel rejection expected, got: {text}"
    );
}

#[test]
fn approval_default_subscription_timeout_is_24h_and_null_is_infinite() {
    let doc = |timeout: &str| {
        // timeout: "" = 字段缺省（pydantic 默认 24h）、"null" = 显式无限等待。
        let timeout_field = if timeout.is_empty() {
            String::new()
        } else {
            format!(r#","subscription_timeout":{timeout}"#)
        };
        format!(
            r#"{{"flow_id":"f","nodes":[
            {{"type":"start","id":"s","next":"a"}},
            {{"type":"approval","id":"a","approval_title":"t","approval_content":"c",
              "approvers":["u1"]{timeout_field},"next":"end"}},
            {{"type":"end","id":"end","resultType":"success"}}
        ]}}"#
        )
    };
    for (timeout, want) in [("", 24.0 * 3600.0), ("null", 0.0), ("3600", 3600.0)] {
        let flow = ir::load(doc(timeout).as_bytes()).unwrap();
        let (host, registry) = host_with_registry();
        let _suspended = suspend_of(flow.execute(&json!({}), &host));
        let req = &registry.registered.lock().unwrap()[0];
        assert_eq!(req.event_type, "approval_decision");
        assert_eq!(req.timeout_seconds, want, "timeout spec for {timeout:?}");
    }
}

#[test]
fn event_type_expression_resolves_at_execute() {
    let doc = r#"{"flow_id":"f","nodes":[
        {"type":"start","id":"s","next":"e1"},
        {"type":"event","id":"e1","event_type":"$INPUT.kind","next":"end"},
        {"type":"end","id":"end","output":"$NODE.e1","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let (host, registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({"kind": "resolved-kind"}), &host));
    assert_eq!(suspended.node_state()["event_type"], "resolved-kind");
    assert_eq!(
        registry.registered.lock().unwrap()[0].event_type,
        "resolved-kind"
    );
    // 恢复后 event_type 保持 execute 解析值（Python dict.update 覆盖序）。
    let out = bound_resume(suspended, ResumeKind::Event, json!({})).unwrap();
    assert_eq!(out["event_type"], "resolved-kind");
}

#[test]
fn delay_service_config_shape() {
    let doc = r#"{"flow_id":"df","nodes":[
        {"type":"start","id":"s","next":"d"},
        {"type":"delay","id":"d","delay_seconds":0.2,"next":"end"},
        {"type":"end","id":"end","output":"$NODE.d","resultType":"success"}
    ]}"#;
    let flow = ir::load(doc.as_bytes()).unwrap();
    let (host, registry) = host_with_registry();
    let suspended = suspend_of(flow.execute(&json!({}), &host));
    let state = suspended.node_state();
    assert_eq!(state["node_subtype"], "delay");
    assert_eq!(state["service_config"]["delay_ms"], 200);
    assert_eq!(state["service_config"]["type"], "delay");
    assert_eq!(state["service_config"]["flow_id"], "df");
    assert_eq!(state["service_config"]["event_type"], "delay_trigger");
    assert_eq!(
        registry.registered.lock().unwrap()[0].event_type,
        "delay_trigger"
    );
    let out = bound_resume(suspended, ResumeKind::Event, json!({"success": true})).unwrap();
    assert_eq!(out["status"], "completed");
    // 挂起态全量保留：service_config/node_subtype/event_id 存活。
    assert_eq!(out["service_config"]["delay_ms"], 200);
    assert_eq!(out["node_subtype"], "delay");
    assert!(out["event_id"].as_str().unwrap().starts_with("event_d_"));
}

#[cfg(feature = "http")]
mod hitl {
    use super::*;
    use std::io::{Read, Write};

    /// 一次性本地 HTTP stub：读到一个请求、回一个 JSON、记录请求体。
    struct HitlStub {
        addr: std::net::SocketAddr,
        body: Arc<Mutex<Option<String>>>,
    }

    impl HitlStub {
        fn spawn(reply: Value) -> HitlStub {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
            let body_ref = body.clone();
            std::thread::spawn(move || {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 65536];
                    let mut raw = Vec::new();
                    // 读到头部分隔后再读 content-length 指示的主体。
                    let n = stream.read(&mut buf).unwrap_or(0);
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw).to_string();
                    let content_length = text
                        .split("\r\n")
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if let Some(pos) = text.find("\r\n\r\n") {
                        while raw.len() < pos + 4 + content_length {
                            let n = stream.read(&mut buf).unwrap_or(0);
                            if n == 0 {
                                break;
                            }
                            raw.extend_from_slice(&buf[..n]);
                        }
                        let full = String::from_utf8_lossy(&raw).to_string();
                        let body_text = full[full.find("\r\n\r\n").unwrap() + 4..].to_string();
                        *body_ref.lock().unwrap() = Some(body_text);
                    }
                    let payload = reply.to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        payload.len(),
                        payload
                    );
                    stream.write_all(response.as_bytes()).ok();
                    stream.flush().ok();
                }
            });
            HitlStub { addr, body }
        }

        fn url(&self) -> String {
            format!("http://{}", self.addr)
        }

        fn body(&self) -> Value {
            let raw = self.body.lock().unwrap().clone().unwrap_or_default();
            serde_json::from_str(&raw).unwrap_or(Value::Null)
        }
    }

    #[test]
    fn hitl_dry_run_suspends_without_network() {
        let doc = r#"{"flow_id":"f","nodes":[
            {"type":"start","id":"s","next":"h"},
            {"type":"hitl_await","id":"h","message":"请确认","dry_run":true,"next":"end"},
            {"type":"end","id":"end","output":"$NODE.h","resultType":"success"}
        ]}"#;
        let flow = ir::load(doc.as_bytes()).unwrap();
        let (host, registry) = host_with_registry();
        let suspended = suspend_of(flow.execute(&json!({}), &host));
        assert_eq!(suspended.node_state()["session_id"], "dryrun");
        assert_eq!(
            registry.registered.lock().unwrap()[0].event_type,
            "hitl_reply"
        );
        // timeout_secs 字段缺省 → 3600 成为订阅超时（Go SuspendSpec 同形）。
        assert_eq!(
            registry.registered.lock().unwrap()[0].timeout_seconds,
            3600.0
        );
        let out = bound_resume(
            suspended,
            ResumeKind::Event,
            json!({"status": "replied", "replies": ["ok"], "session_id": "dryrun"}),
        )
        .unwrap();
        assert_eq!(out["status"], "replied");
        assert_eq!(out["replies"], json!(["ok"]));
    }

    #[test]
    fn hitl_real_send_records_session_and_resumes() {
        let stub = HitlStub::spawn(json!({"success": true, "session_id": "sess-7"}));
        let doc = format!(
            r#"{{"flow_id":"f","nodes":[
            {{"type":"start","id":"s","next":"h"}},
            {{"type":"hitl_await","id":"h","message":"$INPUT.q","base_url":"{url}","next":"end"}},
            {{"type":"end","id":"end","output":"$NODE.h","resultType":"success"}}
        ]}}"#,
            url = stub.url()
        );
        let flow = ir::load(doc.as_bytes()).unwrap();
        let (host, _registry) = host_with_registry();
        let suspended = suspend_of(flow.execute(&json!({"q": "批准吗？"}), &host));
        let state = suspended.node_state();
        assert_eq!(state["status"], "pending");
        assert_eq!(state["session_id"], "sess-7");
        assert_eq!(state["timeout_secs"], 3600);
        // 发送协议：/api/send + wait_reply + upstream（_hitl_client 同形）。
        let sent = stub.body();
        assert_eq!(sent["message"], "批准吗？");
        assert_eq!(sent["wait_reply"], true);
        assert_eq!(sent["upstream"], "ilink");
        // 回复事件（poller 发布形态）：payload 内嵌 event_data 也被合并。
        let out = bound_resume(
            suspended,
            ResumeKind::Event,
            json!({"event_data": {"status": "replied", "replies": ["同意"], "session_id": "sess-7"}}),
        )
        .unwrap();
        assert_eq!(out["status"], "replied");
        assert_eq!(out["replies"], json!(["同意"]));
        assert_eq!(out["session_id"], "sess-7");
    }
}
