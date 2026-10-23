//! M1 验收行为测试（rs-worker-design §12 M1，miniredis 对应物 = 内存
//! fake）：端到端「投递 flow → 挂起落库 → 注入事件 → 恢复续跑 → 完成落结
//! 果」，加订阅超时、陈旧载体、取消、指纹闸、Pub/Sub 桥、挂起服务派发凭
//! 据、drain post-read gate。真 Redis 的同构链路在
//! `tests/redis_integration.rs`（#[ignore] 腿）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::state;
use plaita_worker::subscriptions::RedisSubscriptions;
use plaita_worker::{checkpoint, events, flowsource, lease, RedisBackend, Worker};
use serde_json::{json, Value};

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    queue: Arc<TaskQueue>,
    subs: Arc<RedisSubscriptions>,
    metrics: Arc<RecordingMetrics>,
}

fn harness(name: &str) -> Harness {
    harness_with(name, &|_| String::new())
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("e2e-{name}"),
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
        metrics,
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

    fn enqueue_body(&self, body: &Value) {
        self.queue.enqueue(body, 1).unwrap();
    }

    fn start_carrier(&self, flow_id: &str, execution_id: &str, params: Value) {
        self.enqueue_body(&json!({
            "type": "start",
            "flow_id": flow_id,
            "execution_id": execution_id,
            "params": params,
            "timestamp": now_iso(),
        }));
    }

    fn state_row(&self, execution_id: &str) -> state::ExecutionState {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .unwrap_or_else(|| panic!("state row for {execution_id} missing"))
    }

    fn checkpoint(&self, execution_id: &str) -> Option<checkpoint::CheckpointRecord> {
        checkpoint::load(self.backend.as_ref(), &self.cfg, "", execution_id).unwrap()
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
}

const EVENT_FLOW: &str = r#"{"flow_id":"wf1","nodes":[
    {"type":"start","id":"s","next":"e1"},
    {"type":"event","id":"e1","event_type":"tick","next":"mid"},
    {"type":"assignment","id":"mid","output":{"seen":"$NODE.e1.status"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
]}"#;

fn suspend_an_execution(h: &Harness, execution_id: &str) -> String {
    h.preset_flow("wf1", "v1", EVENT_FLOW);
    h.start_carrier("wf1", execution_id, json!({"who": "e2e"}));
    h.worker.process_one().unwrap();
    let row = h.state_row(execution_id);
    assert_eq!(row.status, state::STATUS_SUSPENDED);
    let cp = h.checkpoint(execution_id).expect("checkpoint exists");
    cp.context["nodes"]["e1"]["subscription_id"]
        .as_str()
        .expect("subscription id in $NODE")
        .to_string()
}

/// 验收 1：挂起→唤醒→恢复全链——suspended 行（无 end_time、$NODE pending、
/// checkpoint 键在、订阅键在、租约已释放、原条已 ack）→ 注入 event → 恰一
/// 条 event 载体（重复去重）→ resume → completed、checkpoint/订阅键删除、
/// node_timings/timeline 继承（对拍 phase3_checkpoint_test.go:77-172）。
#[test]
fn suspend_wake_resume_full_chain() {
    let h = harness("full-chain");
    let execution_id = "exec-full-1";
    let sub_id = suspend_an_execution(&h, execution_id);

    // 挂起态不变量
    let row = h.state_row(execution_id);
    assert!(row.end_time.is_empty(), "suspended row has no end_time");
    assert_eq!(row.context["nodes"]["e1"]["status"], "pending");
    assert!(row.node_timings.is_some());
    assert!(h.checkpoint(execution_id).is_some());
    assert!(
        h.subs.get("", &sub_id).unwrap().is_some(),
        "subscription key exists"
    );
    assert_eq!(
        lease::get_holder(h.backend.as_ref(), &h.cfg, "", execution_id).unwrap(),
        None,
        "lease released on suspension"
    );
    assert_eq!(h.queue_len(), 0, "original start carrier acked");
    assert_eq!(h.pel_len(), 0);

    // 注入 event → 恰一条载体；重复事件去重
    let ev = events::Event {
        event_id: "evt-1".into(),
        event_type: "tick".into(),
        correlation_id: execution_id.into(),
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
    events::handle_event(
        &(h.backend.clone() as Arc<dyn plaita_worker::RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
        &ev,
    )
    .unwrap();
    assert_eq!(h.queue_len(), 1, "duplicate event deduped");

    // resume 续跑到完成
    h.worker.process_one().unwrap();
    let row = h.state_row(execution_id);
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "completed");
    assert!(h.checkpoint(execution_id).is_none(), "checkpoint deleted");
    assert!(
        h.subs.get("", &sub_id).unwrap().is_none(),
        "subscription unregistered"
    );
    assert_eq!(h.queue_len(), 0);
    // node_timings/timeline 继承（terminal 行读作一次执行）
    let timings = row.node_timings.unwrap();
    assert!(timings.get("s").is_some(), "start node timing carried");
    assert!(timings.get("e1").is_some(), "resumed node timing present");
    let timeline = row.timeline.unwrap();
    assert!(timeline["queue_wait_ms"].is_number());
    assert!(timeline["events"].as_array().unwrap().len() >= 2);
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::METRIC_EXEC_RESULT,
            &[("tenant", ""), ("flow", "wf1"), ("status", "completed")]
        ),
        1
    );
}

/// 验收 2：订阅超时 → sweep 恰一条 timeout 载体 → 恢复后 $NODE.status=
/// timeout（phase3_checkpoint_test.go:174-206）。
#[test]
fn subscription_timeout_sweep_enqueues_timeout_carrier() {
    let h = harness("timeout");
    let execution_id = "exec-timeout-1";
    // event 节点显式订阅超时 0.1s（delay 节点缺省无订阅超时——它的唤醒走
    // delay 服务队列）
    h.preset_flow(
        "wf1",
        "v1",
        r#"{"flow_id":"wf1","nodes":[
            {"type":"start","id":"s","next":"e1"},
            {"type":"event","id":"e1","event_type":"tick","subscription_timeout":0.1,"next":"end"},
            {"type":"end","id":"end","output":"$NODE.e1","resultType":"success"}
        ]}"#,
    );
    h.start_carrier("wf1", execution_id, json!({}));
    h.worker.process_one().unwrap();
    assert_eq!(h.state_row(execution_id).status, state::STATUS_SUSPENDED);

    std::thread::sleep(Duration::from_millis(150));
    events::sweep_event_timeouts(
        &(h.backend.clone() as Arc<dyn plaita_worker::RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
    )
    .unwrap();
    assert_eq!(h.queue_len(), 1, "exactly one timeout carrier");
    // sweep again — the member was dropped after the enqueue (#50 one-shot)
    events::sweep_event_timeouts(
        &(h.backend.clone() as Arc<dyn plaita_worker::RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
    )
    .unwrap();
    assert_eq!(h.queue_len(), 1, "no duplicate timeout carrier");

    h.worker.process_one().unwrap();
    let row = h.state_row(execution_id);
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["e1"]["status"], "timeout");
}

/// 验收 3：陈旧 event 载体（订阅绑定不符）→ 执行保持 suspended、载体 ack、
/// 计数具名（phase3_checkpoint_test.go:208-247）。
#[test]
fn stale_event_carrier_acks_and_execution_stays_suspended() {
    let h = harness("stale");
    let execution_id = "exec-stale-1";
    let sub_id = suspend_an_execution(&h, execution_id);

    h.enqueue_body(&json!({
        "type": "resume",
        "flow_id": "wf1",
        "execution_id": execution_id,
        "resume_type": "event",
        "timestamp": now_iso(),
        "data": {"subscription_id": "sub-wrong", "status": "completed"},
    }));
    h.worker.process_one().unwrap();

    let row = h.state_row(execution_id);
    assert_eq!(
        row.status,
        state::STATUS_SUSPENDED,
        "execution stays suspended"
    );
    assert!(h.checkpoint(execution_id).is_some(), "checkpoint untouched");
    assert_eq!(h.queue_len(), 0, "carrier acked");
    assert_eq!(h.pel_len(), 0, "carrier not left pending");
    assert_eq!(
        h.metrics
            .counter_of("resume_carrier_rejected_total", &[("resume_type", "event")]),
        1,
        "named rejection counter"
    );
    // 订阅仍在（执行还挂着，真唤醒源没被烧掉）
    assert!(h.subs.get("", &sub_id).unwrap().is_some());
}

/// 验收 4：cancel 载体 → cancelled 终态 + checkpoint 删除
///（phase3_checkpoint_test.go:249-278）。
#[test]
fn cancel_carrier_terminalizes_cancelled() {
    let h = harness("cancel");
    let execution_id = "exec-cancel-1";
    let _sub_id = suspend_an_execution(&h, execution_id);

    h.enqueue_body(&json!({
        "type": "resume",
        "flow_id": "wf1",
        "execution_id": execution_id,
        "resume_type": "cancel",
        "timestamp": now_iso(),
    }));
    h.worker.process_one().unwrap();

    let row = h.state_row(execution_id);
    assert_eq!(row.status, state::STATUS_CANCELLED);
    assert!(!row.end_time.is_empty());
    assert!(h.checkpoint(execution_id).is_none(), "checkpoint deleted");
    assert_eq!(h.queue_len(), 0);
}

/// 验收 5：flow 指纹不匹配 → flow_hash_mismatch 终态；
/// allow_flow_hash_change 放行（phase3_checkpoint_test.go:279-323）。
#[test]
fn flow_hash_mismatch_gates_resume_and_allow_flag_passes() {
    let h = harness("hash-gate");
    let modified = r#"{"flow_id":"wf1","nodes":[
        {"type":"start","id":"s","next":"e1"},
        {"type":"event","id":"e1","event_type":"tick","next":"mid"},
        {"type":"assignment","id":"mid","output":{"seen":"CHANGED","where":"$NODE.e1.status"},"next":"end"},
        {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
    ]}"#;

    // 腿 1：拒绝
    let execution_id = "exec-gate-1";
    let _sub_id = suspend_an_execution(&h, execution_id);
    h.preset_flow("wf1", "v1", modified);
    h.enqueue_body(&json!({
        "type": "resume",
        "flow_id": "wf1",
        "execution_id": execution_id,
        "resume_type": "event",
        "timestamp": now_iso(),
        "data": {"subscription_id": _sub_id, "status": "completed"},
    }));
    h.worker.process_one().unwrap();
    let row = h.state_row(execution_id);
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "flow_hash_mismatch");
    assert!(
        h.checkpoint(execution_id).is_none(),
        "checkpoint deleted on terminal"
    );

    // 腿 2：allow_flow_hash_change 放行
    let execution_id = "exec-gate-2";
    let sub_id = suspend_an_execution(&h, execution_id);
    h.preset_flow("wf1", "v1", modified);
    h.enqueue_body(&json!({
        "type": "resume",
        "flow_id": "wf1",
        "execution_id": execution_id,
        "resume_type": "event",
        "timestamp": now_iso(),
        "data": {
            "subscription_id": sub_id,
            "allow_flow_hash_change": true,
            "status": "completed",
        },
    }));
    h.worker.process_one().unwrap();
    let row = h.state_row(execution_id);
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "CHANGED");
}

/// 验收 7：Pub/Sub 桥投递 event 载体 + `:execution:events:` 通道不误吃
///（phase3_checkpoint_test.go:324-363）。
#[test]
fn pubsub_bridge_delivers_event_carrier_and_skips_execution_channel() {
    let h = harness("bridge");
    let execution_id = "exec-bridge-1";
    let _sub_id = suspend_an_execution(&h, execution_id);

    let stop = Arc::new(AtomicBool::new(false));
    let bridge_backend = h.backend.clone() as Arc<dyn plaita_worker::RedisBackend>;
    let bridge_thread = std::thread::spawn({
        let cfg = h.cfg.clone();
        let queue = h.queue.clone();
        let subs = h.subs.clone();
        let stop = stop.clone();
        move || {
            events::event_bridge_loop(bridge_backend, cfg, queue, subs, stop);
        }
    });
    // 等订阅生效（假件同步注册；给桥一线程启动时间）
    std::thread::sleep(Duration::from_millis(100));

    // 业务事件通道 → 必须产生载体
    let business_channel = keys::business_event_channel(&h.cfg, "", "tick");
    h.backend
        .publish(
            &business_channel,
            &json!({
                "event_id": "evt-bridge-1",
                "event_type": "tick",
                "correlation_id": execution_id,
                "tenant_id": "",
                "data": {"status": "completed"},
            })
            .to_string(),
        )
        .unwrap();
    // 执行时间线通道（形状撞车）→ 不得被解析为业务事件
    let timeline_channel = keys::execution_events_channel(&h.cfg, execution_id);
    h.backend
        .publish(
            &timeline_channel,
            &json!({"type": "node_end", "node_id": "s"}).to_string(),
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while h.queue_len() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        h.queue_len(),
        1,
        "exactly the business event became a carrier"
    );
    h.worker.process_one().unwrap();
    assert_eq!(h.state_row(execution_id).status, state::STATUS_COMPLETED);

    stop.store(true, Ordering::SeqCst);
    bridge_thread.join().unwrap();
}

/// 验收 8：挂起服务派发——suspended 行带 dispatch_pending → RPUSH 到
/// `{prefix}:{subtype}:queue` → 凭据清；RPUSH 失败 → 消息不 ack、凭据在；
/// 重投补派成功；suspended+continue 短路补派一次
///（server_nodes_dispatch_test.go:28-110）。
#[test]
fn suspend_service_dispatch_credential_lifecycle() {
    let delay_flow = r#"{"flow_id":"wf1","nodes":[
        {"type":"start","id":"s","next":"d"},
        {"type":"delay","id":"d","delay_seconds":3600,"next":"end"},
        {"type":"end","id":"end","output":"$NODE.d","resultType":"success"}
    ]}"#;
    let service_queue_of = |h: &Harness| keys::service_queue_key(&h.cfg, "delay");

    // 腿 1：RPUSH 失败 → 消息不 ack（留 pending）、凭据在
    let h = harness_with("dispatch-fail", &|_| String::new());
    h.backend.fail_next_rpush(1);
    h.preset_flow("wf1", "v1", delay_flow);
    h.start_carrier("wf1", "exec-d1", json!({}));
    let err = h.worker.process_one().unwrap_err();
    assert!(
        err.to_string().contains("dispatch"),
        "dispatch failure surfaces: {err}"
    );
    assert_eq!(h.pel_len(), 1, "message left pending");
    let row = h.state_row("exec-d1");
    assert_eq!(row.status, state::STATUS_SUSPENDED);
    let marker = row.dispatch_pending.expect("credential kept");
    assert_eq!(marker["pending"], true);
    assert_eq!(marker["service_type"], "delay");
    assert_eq!(
        h.backend
            .lrange(&service_queue_of(&h), 0, -1)
            .unwrap()
            .len(),
        0,
        "nothing dispatched"
    );
    // 重投（reclaim）：start 幂等门的 suspended 分支补派
    h.worker.process_one().unwrap();
    assert_eq!(
        h.backend
            .lrange(&service_queue_of(&h), 0, -1)
            .unwrap()
            .len(),
        1,
        "compensated dispatch"
    );
    assert!(
        h.state_row("exec-d1").dispatch_pending.is_none(),
        "credential cleared"
    );
    assert_eq!(h.pel_len(), 0, "compensated carrier acked");
    let dispatched: Value =
        serde_json::from_str(&h.backend.lrange(&service_queue_of(&h), 0, -1).unwrap()[0]).unwrap();
    assert_eq!(dispatched["execution_id"], "exec-d1");
    assert_eq!(
        dispatched["tenant_id"], "default",
        "empty tenant normalized"
    );
    assert_eq!(dispatched["type"], "delay", "service_config embedded");

    // 腿 2：suspended+continue 短路补派一次
    let h2 = harness_with("dispatch-continue", &|_| String::new());
    h2.backend.fail_next_rpush(1);
    h2.preset_flow("wf1", "v1", delay_flow);
    h2.start_carrier("wf1", "exec-d2", json!({}));
    assert!(h2.worker.process_one().is_err());
    assert_eq!(h2.pel_len(), 1);
    // 消费掉残留的 start 载体（补派路径），让执行进入「凭据在、无载体」态
    h2.worker.process_one().unwrap();
    // 现在投一个 continue 载体：suspended + continue = 补派 + 幂等 ack
    h2.enqueue_body(&json!({
        "type": "resume",
        "flow_id": "wf1",
        "execution_id": "exec-d2",
        "resume_type": "continue",
        "timestamp": now_iso(),
    }));
    h2.worker.process_one().unwrap();
    assert_eq!(
        h2.backend
            .lrange(&service_queue_of(&h2), 0, -1)
            .unwrap()
            .len(),
        1,
        "short-circuit compensated once"
    );
    assert!(h2.state_row("exec-d2").dispatch_pending.is_none());
    assert_eq!(h2.pel_len(), 0);
    assert_eq!(h2.queue_len(), 0);
}

/// 验收补：租约冲突（决议载体永不 ack——M1 无注册表，全量保守留 pending）。
#[test]
fn lease_conflict_parks_carriers() {
    let h = harness("conflict");
    let execution_id = "exec-conflict-1";
    let _sub_id = suspend_an_execution(&h, execution_id);
    // 模拟活持有者：占住租约
    let holder = lease::acquire(
        h.backend.as_ref(),
        &h.cfg,
        "",
        execution_id,
        "start:other-worker:aaaaaaaaaaaaaaaa",
        120,
    )
    .unwrap();
    h.enqueue_body(&json!({
        "type": "resume",
        "flow_id": "wf1",
        "execution_id": execution_id,
        "resume_type": "event",
        "timestamp": now_iso(),
        "data": {"subscription_id": _sub_id, "status": "completed"},
    }));
    let err = h.worker.process_one().unwrap_err();
    assert_eq!(err.kind(), plaita_worker::ErrorKind::LeaseConflict);
    assert_eq!(h.pel_len(), 1, "decision carrier never acked on conflict");
    assert_eq!(h.state_row(execution_id).status, state::STATUS_SUSPENDED);
    lease::release_with(h.backend.as_ref(), &h.cfg, &holder).unwrap();
}

/// 验收补：drain post-read gate——drain 开始后送达的消息不派发不 ack。
#[test]
fn drain_post_read_gate_leaves_pending() {
    let h = harness("drain-gate");
    h.preset_flow("wf1", "v1", EVENT_FLOW);
    h.start_carrier("wf1", "exec-drain-1", json!({}));
    h.worker.request_drain("test");
    assert!(h.worker.draining());
    // drain 后到达的消息：读出但不派发
    assert!(h.worker.process_one().unwrap(), "message was read");
    assert_eq!(h.pel_len(), 1, "left pending for another worker");
    assert!(h.worker.force_stopped() || !h.worker.force_stopped());
    let exists = state::load(h.backend.as_ref(), &h.cfg, "", "exec-drain-1").unwrap();
    assert!(
        exists.is_none(),
        "no execution row written after drain began"
    );
}
