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

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
        Self::setup_with(test_name, &|_| {})
    }

    /// `tweak` 在 Worker::new 前改 Config（flow_source URL / 心跳间隔等）。
    fn setup_with(test_name: &str, tweak: &dyn Fn(&mut plaita_worker::Config)) -> Env {
        let Some((backend, mut cfg)) = integration_backend(test_name) else {
            panic!(
                "integration leg disabled: set PLAITA_RS_REDIS_TESTS=1 \
                 (and optionally PLAITA_RS_REDIS_URL)"
            );
        };
        tweak(&mut cfg);
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
    // rs#17 WK-M3：终态行 context = 根执行 FlowEnd 的九根精确快照（真
    // Redis 腿同形状核对；行为面详见 tests/terminal_context_e2e.rs）。
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
        assert!(
            row.context.get(root).is_some(),
            "terminal context root {root:?} present on real Redis"
        );
    }
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

// ---------------------------------------------------------------- 第十五轮接线腿
//
// B4 第十五轮（读穿透 × registry 入位）的真 Redis 形制，对齐上面 4 例：
// 内存 fake 已钉的负控格在此逐格实弹。console 臂用自宿主 TCP stub
// （r8 教训①：先读全请求再应答）。

/// 可变路由的假 console（同 readthrough_e2e.rs 形制；hash 拒格腿要运行期
/// 换定义）。
struct ItStub {
    url: String,
    hits: Arc<AtomicUsize>,
    routes: Arc<Mutex<HashMap<String, (u16, String)>>>,
}

impl ItStub {
    fn new(routes: Vec<(&'static str, u16, String)>) -> ItStub {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let route_map: Arc<Mutex<HashMap<String, (u16, String)>>> = Arc::new(Mutex::new(
            routes
                .into_iter()
                .map(|(p, s, b)| (p.to_string(), (s, b)))
                .collect(),
        ));
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_t = hits.clone();
        let routes_t = route_map.clone();
        std::thread::spawn(move || {
            for incoming in listener.incoming() {
                let Ok(stream) = incoming else { continue };
                let hits = hits_t.clone();
                let routes = routes_t.clone();
                std::thread::spawn(move || it_handle_conn(stream, hits, routes));
            }
        });
        ItStub {
            url,
            hits,
            routes: route_map,
        }
    }

    fn set_body(&self, path: &str, body: String) {
        self.routes
            .lock()
            .unwrap()
            .insert(path.to_string(), (200, body));
    }

    fn hit_count(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

fn it_handle_conn(
    mut stream: TcpStream,
    hits: Arc<AtomicUsize>,
    routes: Arc<Mutex<HashMap<String, (u16, String)>>>,
) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        let n = stream.read(&mut tmp).unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let request_line = text.split("\r\n").next().unwrap_or("");
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    hits.fetch_add(1, Ordering::SeqCst);
    let (status, reason, body) = match routes.lock().unwrap().get(&path) {
        Some((s, b)) => (*s, "OK", b.clone()),
        None => (404, "Not Found", "{}".to_string()),
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: \
         {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.flush();
}

const FID_RT: &str = "wf-it-rt";

fn it_version_detail(status: &str, definition: &str) -> String {
    json!({
        "flow_id": FID_RT,
        "version": "v1",
        "status": status,
        "definition": definition,
    })
    .to_string()
}

fn it_flow_detail() -> String {
    json!({
        "versions": [{
            "version": "v1",
            "status": "published",
            "published_at": "2026-10-23T00:00:00",
            "created_at": "2026-10-23T00:00:00",
        }],
    })
    .to_string()
}

const EVENT_FLOW_MODIFIED: &str = r#"{"flow_id":"wf1","nodes":[
    {"type":"start","id":"s","next":"e1"},
    {"type":"event","id":"e1","event_type":"tick","next":"mid"},
    {"type":"assignment","id":"mid","output":{"seen":"CHANGED","where":"$NODE.e1.status"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
]}"#;

/// 腿 5（读穿透全链）：本地零定义 → start 载体 → console latest 解析 +
/// published 装载（r11 hash 门同路径）→ 挂起 → 注入事件 → 恢复完成。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_readthrough_local_miss_pull_through_full_chain() {
    let stub = ItStub::new(vec![
        ("/api/flows/wf-it-rt", 200, it_flow_detail()),
        (
            "/api/flows/wf-it-rt/versions/v1",
            200,
            it_version_detail("published", EVENT_FLOW),
        ),
    ]);
    let env = Env::setup_with("rtchain", &|cfg| {
        cfg.flow_source.url = stub.url.clone();
    });
    let backend = env.dyn_backend();
    // 本地腿零定义：定义只存在于 console
    env.queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": FID_RT,
                "execution_id": "it-rt-1",
                "params": {"who": "rt"},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();

    let row = state::load(backend.as_ref(), &env.cfg, "", "it-rt-1")
        .unwrap()
        .unwrap();
    assert_eq!(
        row.status,
        state::STATUS_SUSPENDED,
        "definition came from console"
    );
    assert!(!row.flow_hash.is_empty());
    assert_eq!(row.flow_hash_algo, "flow-definition-json-sortkeys-v2-rust");
    let hits_after_start = stub.hit_count();
    assert!(
        hits_after_start >= 2,
        "latest + detail (got {hits_after_start})"
    );

    events::handle_event(
        &backend,
        &env.cfg,
        &env.queue,
        &env.subs,
        &events::Event {
            event_id: "it-rt-evt-1".into(),
            event_type: "tick".into(),
            correlation_id: "it-rt-1".into(),
            tenant_id: String::new(),
            data: json!({"status": "completed"}),
        },
    )
    .unwrap();
    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-rt-1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "completed");
}

/// 腿 6：console 404 → 具名确定性终态 + 负缓存——第二条载体零 console
/// 调用（真 Redis 上无 TTL 边界抖动窗口）。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_console_404_negative_caches_named_terminal() {
    let stub = ItStub::new(vec![]);
    let env = Env::setup_with("rt404", &|cfg| {
        cfg.flow_source.url = stub.url.clone();
    });
    let backend = env.dyn_backend();
    for exec in ["it-rt-n1", "it-rt-n2"] {
        env.queue
            .enqueue(
                &json!({
                    "type": "start",
                    "flow_id": FID_RT,
                    "execution_id": exec,
                    "params": {},
                    "timestamp": now_iso(),
                }),
                1,
            )
            .unwrap();
        env.worker.process_one().unwrap();
        let row = state::load(backend.as_ref(), &env.cfg, "", exec)
            .unwrap()
            .unwrap();
        assert_eq!(row.status, state::STATUS_ERROR);
        assert_eq!(row.error.unwrap()["error_type"], "flow_not_found");
    }
    assert_eq!(
        stub.hit_count(),
        1,
        "second carrier served from the negative cache (latest call only)"
    );
}

/// 腿 7：未配置 URL = 特性关闭——本地 miss 即具名确定性终态，零 HTTP 面
/// （连 stub 都不存在；消息不得谎称查过 console）。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_unconfigured_local_semantics() {
    let env = Env::setup("rtnone");
    let backend = env.dyn_backend();
    env.queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": FID_RT,
                "execution_id": "it-rt-u1",
                "params": {},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-rt-u1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "flow_not_found");
    let msg = err["message"].as_str().unwrap();
    assert!(msg.contains("flow definition unavailable"), "got: {msg}");
    assert!(!msg.contains("console"), "got: {msg}");
}

/// 腿 8：消费侧 hash 拒格（真 Redis × console 源）：挂起后 console 同版本
/// 换定义 → resume 指纹硬闸具名终态 flow_hash_mismatch；allow 流标放行臂
/// 落 COMPLETED 且吃到变更后输出。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_hash_mismatch_named_rejection_on_console_source() {
    let stub = ItStub::new(vec![
        ("/api/flows/wf-it-rt", 200, it_flow_detail()),
        (
            "/api/flows/wf-it-rt/versions/v1",
            200,
            it_version_detail("published", EVENT_FLOW),
        ),
    ]);
    let env = Env::setup_with("rthash", &|cfg| {
        cfg.flow_source.url = stub.url.clone();
    });
    let backend = env.dyn_backend();

    // 腿 1：拒绝
    env.queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": FID_RT,
                "execution_id": "it-rt-h1",
                "params": {},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-rt-h1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_SUSPENDED);
    let cp = checkpoint::load(backend.as_ref(), &env.cfg, "", "it-rt-h1")
        .unwrap()
        .expect("checkpoint exists");
    let sub_id = cp.context["nodes"]["e1"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();

    stub.set_body(
        "/api/flows/wf-it-rt/versions/v1",
        it_version_detail("published", EVENT_FLOW_MODIFIED),
    );
    env.queue
        .enqueue(
            &json!({
                "type": "resume",
                "flow_id": FID_RT,
                "execution_id": "it-rt-h1",
                "resume_type": "event",
                "timestamp": now_iso(),
                "data": {"subscription_id": sub_id, "status": "completed"},
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-rt-h1")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_ERROR);
    assert_eq!(row.error.unwrap()["error_type"], "flow_hash_mismatch");

    // 腿 2：allow_flow_hash_change 放行（新执行，吃到 CHANGED 输出）
    env.queue
        .enqueue(
            &json!({
                "type": "start",
                "flow_id": FID_RT,
                "execution_id": "it-rt-h2",
                "params": {},
                "timestamp": now_iso(),
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();
    let cp = checkpoint::load(backend.as_ref(), &env.cfg, "", "it-rt-h2")
        .unwrap()
        .expect("checkpoint exists");
    let sub_id2 = cp.context["nodes"]["e1"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    env.queue
        .enqueue(
            &json!({
                "type": "resume",
                "flow_id": FID_RT,
                "execution_id": "it-rt-h2",
                "resume_type": "event",
                "timestamp": now_iso(),
                "data": {
                    "subscription_id": sub_id2,
                    "allow_flow_hash_change": true,
                    "status": "completed",
                },
            }),
            1,
        )
        .unwrap();
    env.worker.process_one().unwrap();
    let row = state::load(backend.as_ref(), &env.cfg, "", "it-rt-h2")
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "CHANGED");
}

/// 腿 9：registry 全生命周期实弹（#82 写入格）——run() 起真心跳（1s 拍）
/// → 注册键在册：ServiceInfo 顶层 + metadata.engine + capability_manifest
/// 五键（dict 形态、ir_contract_version 与引擎同源、supported 白名单全
/// 等）→ drain → stopped 墓碑 + 键删除。
#[test]
#[ignore = "real-Redis leg: run with PLAITA_RS_REDIS_TESTS=1 cargo test -- --ignored"]
fn real_redis_registry_lifecycle_manifest_five_keys() {
    let env = Env::setup_with("registry", &|cfg| {
        cfg.heartbeat_interval_secs = 1;
    });
    let backend = env.dyn_backend();
    // Env.worker 不动（Drop 边界）；另起一个同配置的 Worker 跑完整生命周期。
    let run_worker = Arc::new(Worker::new(
        backend.clone(),
        env.cfg.clone(),
        Arc::new(NoopMetrics),
    ));
    let handle = {
        let w = run_worker.clone();
        std::thread::spawn(move || w.run())
    };

    // 注册键出现（starting → running 心跳在写）
    let key = keys::registry_worker_key(&env.cfg);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut raw = None;
    while std::time::Instant::now() < deadline {
        if let Some(v) = backend.get(&key).unwrap() {
            raw = Some(v);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let raw = raw.expect("registry entry must appear once run() is up");
    let info: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(info["service_type"], "flow_worker");
    assert_eq!(info["instance_id"], env.cfg.instance_id);
    assert_eq!(info["status"], "running");
    let meta = &info["metadata"];
    assert_eq!(meta["engine"], "plaita-rs");
    assert_eq!(meta["queue_name"], env.cfg.queue_name);
    let manifest = &meta["capability_manifest"];
    assert!(manifest.is_object(), "manifest must ride as a nested dict");
    for k in [
        "engine",
        "ir_contract_version",
        "supported_node_types",
        "unsupported_node_types",
        "features",
    ] {
        assert!(manifest.get(k).is_some(), "manifest key {k} missing");
    }
    assert_eq!(
        manifest["ir_contract_version"],
        plaita_engine::ir::CONTRACT_VERSION
    );
    // 清单与引擎白名单同源全等（accept-list 自动生成不变量）
    let engine_manifest = plaita_engine::ir::capability_manifest();
    assert_eq!(
        manifest["supported_node_types"], engine_manifest["supported_node_types"],
        "supported list mirrors the engine accept-list exactly"
    );
    assert_eq!(
        manifest["unsupported_node_types"],
        engine_manifest["unsupported_node_types"]
    );

    // drain → 停机序列：心跳线程 join 后墓碑+DEL，键消失
    run_worker.request_drain("test-complete");
    handle.join().unwrap().expect("run() clean exit");
    assert!(
        backend.get(&key).unwrap().is_none(),
        "shutdown tombstone+DEL removes the registry key"
    );
}
