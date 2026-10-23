//! 真 Redis 集成测试（#[ignore] 腿）：rs-worker-design §13.3 的裁决是
//! 「内存 fake + #[ignore] 真 Redis 集成」两条腿——本文件只在显式要求时跑：
//!
//! ```bash
//! PLAITA_RS_REDIS_TESTS=1 \
//!   cargo test -p plaita-worker --test redis_integration -- --ignored
//! ```
//!
//! 需要 `PLAITA_RS_REDIS_TESTS=1` + 可达的 `PLAITA_RS_REDIS_URL`（缺省
//! redis://127.0.0.1:6379/0）。所有键落在按 PID 一次性化的前缀下，跑完即清。

use std::sync::Arc;

use plaita_worker::config::now_iso;
use plaita_worker::keys;
use plaita_worker::metrics::NoopMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::state;
use plaita_worker::subscriptions::RedisSubscriptions;
use plaita_worker::{checkpoint, events, flowsource, lease, redis_impl::RedisConnection, Worker};
use serde_json::json;

fn integration_backend(test_name: &str) -> Option<(Arc<RedisConnection>, plaita_worker::Config)> {
    if std::env::var("PLAITA_RS_REDIS_TESTS").as_deref() != Ok("1") {
        return None;
    }
    let url = std::env::var("PLAITA_RS_REDIS_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
    let prefix = format!("plaita-rs-it-{}-{test_name}", std::process::id());
    let mut cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => prefix.clone(),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        _ => String::new(),
    });
    cfg.redis_url = url;
    let backend = Arc::new(RedisConnection::connect(&cfg.redis_url).unwrap());
    Some((backend, cfg))
}

struct Env {
    backend: Arc<RedisConnection>,
    cfg: plaita_worker::Config,
    worker: Worker,
    queue: TaskQueue,
    subs: RedisSubscriptions,
}

impl Env {
    fn setup(test_name: &str) -> Env {
        let Some((backend, cfg)) = integration_backend(test_name) else {
            panic!(
                "integration leg disabled: set PLAITA_RS_REDIS_TESTS=1 \
                 (and optionally PLAITA_RS_REDIS_URL)"
            );
        };
        let worker = Worker::new(
            backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
            cfg.clone(),
            Arc::new(NoopMetrics),
        );
        let queue = TaskQueue::new(
            backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
            cfg.clone(),
            Arc::new(NoopMetrics),
        );
        let subs = RedisSubscriptions::new(
            backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
            cfg.clone(),
        );
        Env {
            backend,
            cfg,
            worker,
            queue,
            subs,
        }
    }

    fn dyn_backend(&self) -> Arc<dyn plaita_worker::RedisBackend> {
        self.backend.clone() as Arc<dyn plaita_worker::RedisBackend>
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // Best-effort namespace cleanup (test/dev helper on the connection).
        let pattern = format!("{}*", self.cfg.key_prefix);
        let _ = self.backend.delete_pattern(&pattern);
    }
}

const EVENT_FLOW: &str = r#"{"flow_id":"wf1","nodes":[
    {"type":"start","id":"s","next":"e1"},
    {"type":"event","id":"e1","event_type":"tick","next":"mid"},
    {"type":"assignment","id":"mid","output":{"seen":"$NODE.e1.status"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
]}"#;

/// 端到端主链（验收 1 同构）：挂起落库 → 注入事件 → 恢复续跑 → 完成落结果。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_suspend_wake_resume_full_chain() {
    let env = Env::setup("chain");
    let backend = env.dyn_backend();
    flowsource::preset_flow_definition(
        backend.as_ref(),
        &env.cfg,
        "",
        "wf1",
        "v1",
        EVENT_FLOW.as_bytes(),
    )
    .unwrap();
    env.queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": "wf1",
                "execution_id": "it-exec-1",
                "params": {"who": "it"},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();

    let row = state::load(backend.as_ref(), &env.cfg, "", "it-exec-1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_SUSPENDED);
    let cp = checkpoint::load(backend.as_ref(), &env.cfg, "", "it-exec-1")
        .unwrap()
        .expect("checkpoint persisted");
    let sub_id = cp.context["nodes"]["e1"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    // 订阅键真的在 Redis 里
    assert!(env.subs.get("", &sub_id).unwrap().is_some());
    assert_eq!(
        lease::get_holder(backend.as_ref(), &env.cfg, "", "it-exec-1").unwrap(),
        None
    );

    events::handle_event(
        &backend,
        &env.cfg,
        &env.queue,
        &env.subs,
        &events::Event {
            event_id: "it-evt-1".into(),
            event_type: "tick".into(),
            correlation_id: "it-exec-1".into(),
            tenant_id: String::new(),
            data: json!({"status": "completed"}),
        },
    )
    .unwrap();
    assert_eq!(env.queue.depth(), 1);

    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-exec-1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "completed");
    assert!(
        checkpoint::load(backend.as_ref(), &env.cfg, "", "it-exec-1")
            .unwrap()
            .is_none()
    );
    assert!(env.subs.get("", &sub_id).unwrap().is_none());
}

/// Lua 三脚本 + fenced CAS 在真 Redis 上的行为（与 fake 钉死的语义对拍）。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_lease_and_fenced_cas() {
    let env = Env::setup("lease");
    let backend = env.dyn_backend();
    let l1 = lease::acquire(
        backend.as_ref(),
        &env.cfg,
        "",
        "it-lease",
        "start:i:aa",
        120,
    )
    .unwrap();
    assert!(l1.fence_token >= 1);
    assert!(lease::acquire(
        backend.as_ref(),
        &env.cfg,
        "",
        "it-lease",
        "start:i:bb",
        120
    )
    .is_err());
    assert!(lease::renew(backend.as_ref(), &env.cfg, &l1, 120).unwrap());
    let row = state::ExecutionState::new_running(&env.cfg, "f", &json!({}), "it-lease", "");
    state::save(backend.as_ref(), &env.cfg, "", &row, l1.fence_token).unwrap();
    // release + re-acquire bumps the generation; the stale token is refused
    lease::release_with(backend.as_ref(), &env.cfg, &l1).unwrap();
    let l2 = lease::acquire(
        backend.as_ref(),
        &env.cfg,
        "",
        "it-lease",
        "resume:i:bb",
        120,
    )
    .unwrap();
    assert_eq!(l2.fence_token, l1.fence_token + 1);
    assert!(state::save(backend.as_ref(), &env.cfg, "", &row, l1.fence_token).is_err());
}

/// 真流语义：BUSYGROUP 容忍 + XREADGROUP + ack/XDEL（对拍 fake 钉死的行为）。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_queue_stream_semantics() {
    let env = Env::setup("stream");
    let backend = env.dyn_backend();
    env.queue.ensure_group().unwrap();
    env.queue.ensure_group().unwrap(); // BUSYGROUP tolerated
    let id = env
        .queue
        .enqueue(&json!({"type": "start", "flow_id": "wf1"}), 1)
        .unwrap();
    assert_eq!(env.queue.depth(), 1);
    let d = env.queue.read().unwrap().expect("delivery");
    assert_eq!(d.envelope.message_id, id);
    assert_eq!(d.envelope.schema_version, 1);
    env.queue.ack(&d.envelope.message_id).unwrap();
    assert_eq!(env.queue.depth(), 0);
    assert!(backend
        .xrange(&keys::queue_key(&env.cfg), "-", "+", None)
        .unwrap()
        .is_empty());
}

/// Pub/Sub 桥腿（真 Redis）：PSUBSCRIBE 确认帧由 redis-rs 内部消费，发布
/// 的事件被转发为载体；`:execution:events:` 通道不误吃。这条腿抓过一次
/// 真缺陷（确认循环吞消息），故必须钉死。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_pubsub_bridge_delivers_and_skips_timeline() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    let env = Env::setup("bridge");
    let backend = env.dyn_backend();
    flowsource::preset_flow_definition(
        backend.as_ref(),
        &env.cfg,
        "",
        "wf1",
        "v1",
        EVENT_FLOW.as_bytes(),
    )
    .unwrap();
    env.queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": "wf1",
                "execution_id": "it-bridge-1",
                "params": {},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();
    assert_eq!(
        state::load(backend.as_ref(), &env.cfg, "", "it-bridge-1")
            .unwrap()
            .unwrap()
            .status,
        state::STATUS_SUSPENDED
    );

    let stop = Arc::new(AtomicBool::new(false));
    let bridge_backend = backend.clone();
    let cfg = env.cfg.clone();
    let queue = TaskQueue::new(backend.clone(), cfg.clone(), Arc::new(NoopMetrics));
    let subs = RedisSubscriptions::new(backend.clone(), cfg.clone());
    let bridge_stop = stop.clone();
    let bridge_thread = std::thread::spawn(move || {
        events::event_bridge_loop(
            bridge_backend,
            cfg,
            queue.into(),
            Arc::new(subs),
            bridge_stop,
        );
    });

    // 业务事件通道 → 载体。PUBLISH 落在桥的订阅生效窗口前会丢失（§8.2
    // 记录在案：M3 前无回扫器）——测试用新 event_id 有界重发（去重键按
    // event_id+sub 区分，重发不会制造重复载体；一旦载体出现立刻停）。
    let channel = keys::business_event_channel(&env.cfg, "", "tick");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut attempt = 0;
    while env.queue.depth() == 0 && Instant::now() < deadline {
        backend
            .publish(
                &channel,
                &json!({
                    "event_id": format!("it-evt-b-{attempt}"),
                    "event_type": "tick",
                    "correlation_id": "it-bridge-1",
                    "tenant_id": "",
                    "data": {"status": "completed"},
                })
                .to_string(),
            )
            .unwrap();
        attempt += 1;
        std::thread::sleep(Duration::from_millis(200));
    }
    // 执行时间线通道（形状撞车）→ 不得误吃
    let timeline = keys::execution_events_channel(&env.cfg, "it-bridge-1");
    backend
        .publish(&timeline, &json!({"type": "node_end"}).to_string())
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        env.queue.depth(),
        1,
        "exactly the business event became a carrier (attempts={attempt})"
    );
    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-bridge-1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "completed");

    stop.store(true, Ordering::SeqCst);
    bridge_thread.join().unwrap();
}
