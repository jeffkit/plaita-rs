//! 消费循环 × console 读穿透负控/正控（B4 第十五轮·全量入位接线）：worker
//! 取载体 → FlowSource 本地 miss → console 回源（r13 臂）→ hash 门 →
//! ir::load → 执行。形制 = engine `flowsource_console.rs` 的自宿主 TCP stub
//! （每测试自带，r8 教训①先读全请求）+ worker e2e 的内存 fake harness。
//!
//! 任务书负控格：
//!  1. 本地 miss→回源→执行成功      `console_readthrough_serves_start_to_completion`
//!  2. 404→具名负缓存               `console_404_negative_caches_named_terminal`
//!  3. 未配置=本地语义               `unconfigured_url_keeps_local_semantics`
//!  4. 瞬态不终态化（留 pending）     `console_transient_leaves_message_pending`
//!  5. 消费侧 hash 不匹配具名拒格     `console_changed_definition_gates_resume_by_name`
//!
//! registry 写入格（manifest 五键在册）在 `registry.rs` 单测 +
//! `redis_integration.rs` 真 Redis 腿。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use plaita_engine::flowsource::LocalFlowStore;
use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::metrics::NoopMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::state;
use plaita_worker::{checkpoint, flowsource, RedisBackend, Worker};
use serde_json::json;

// ---------------------------------------------------------------- 假 console

/// 可变路由的假 console：`routes` 按 path 给 (status, body)，运行期可换
/// （hash 拒格负控要在两次装载之间换定义）。命中计数含全部请求。
struct Stub {
    url: String,
    hits: Arc<AtomicUsize>,
    routes: Arc<Mutex<HashMap<String, (u16, String)>>>,
}

impl Stub {
    fn new(routes: Vec<(&'static str, u16, String)>) -> Stub {
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
                std::thread::spawn(move || handle_conn(stream, hits, routes));
            }
        });
        Stub {
            url,
            hits,
            routes: route_map,
        }
    }

    /// 换掉一个路由的应答体（同 status 200）。
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

fn handle_conn(
    mut stream: TcpStream,
    hits: Arc<AtomicUsize>,
    routes: Arc<Mutex<HashMap<String, (u16, String)>>>,
) {
    // 先读全请求头再应答（r8 教训①：未读全即关 → macOS 对未读收缓冲回
    // RST 掐客户端读状态行，抖动假红）。
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
        Some((s, b)) => (*s, reason_phrase(*s), b.clone()),
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

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        404 => "Not Found",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------- 夹具

const FID: &str = "wf-rt";

/// 跑到完成的直通流（无挂起面）。
const STRAIGHT_FLOW: &str = r#"{"flow_id":"wf-rt","nodes":[
    {"type":"start","id":"s","next":"a"},
    {"type":"assignment","id":"a","output":{"echo":"$INPUT.who"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.a","resultType":"success"}
]}"#;

/// 挂起流（hash 拒格负控用；与 e2e.rs EVENT_FLOW 同构）。
const EVENT_FLOW: &str = r#"{"flow_id":"wf-rt","nodes":[
    {"type":"start","id":"s","next":"e1"},
    {"type":"event","id":"e1","event_type":"tick","next":"mid"},
    {"type":"assignment","id":"mid","output":{"seen":"$NODE.e1.status"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
]}"#;

/// hash 拒格负控的「变更后」定义（同一 flow_id/version，输出可辨）。
const EVENT_FLOW_MODIFIED: &str = r#"{"flow_id":"wf-rt","nodes":[
    {"type":"start","id":"s","next":"e1"},
    {"type":"event","id":"e1","event_type":"tick","next":"mid"},
    {"type":"assignment","id":"mid","output":{"seen":"CHANGED","where":"$NODE.e1.status"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
]}"#;

fn version_detail_body(status: &str, definition: &str) -> String {
    json!({
        "flow_id": FID,
        "version": "v1",
        "status": status,
        "definition": definition,
    })
    .to_string()
}

fn flow_detail_body(definition_status: &str) -> String {
    json!({
        "versions": [{
            "version": "v1",
            "status": definition_status,
            "published_at": "2026-10-23T00:00:00",
            "created_at": "2026-10-23T00:00:00",
        }],
    })
    .to_string()
}

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    queue: Arc<TaskQueue>,
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("rt-{name}"),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        _ => extra(k),
    });
    let backend = Arc::new(FakeRedis::new());
    let worker = Arc::new(Worker::new(
        backend.clone() as Arc<dyn RedisBackend>,
        cfg.clone(),
        Arc::new(NoopMetrics),
    ));
    let queue = Arc::new(TaskQueue::new(
        backend.clone() as Arc<dyn RedisBackend>,
        cfg.clone(),
        Arc::new(NoopMetrics),
    ));
    Harness {
        backend,
        cfg,
        worker,
        queue,
    }
}

fn harness(name: &str) -> Harness {
    harness_with(name, &|_| String::new())
}

fn harness_console(name: &str, url: &str) -> Harness {
    let url = url.to_string();
    harness_with(name, &move |k| {
        if k == "PLAITA_CONSOLE_FLOW_SOURCE_URL" {
            url.clone()
        } else {
            String::new()
        }
    })
}

impl Harness {
    fn enqueue_start(&self, execution_id: &str) {
        self.queue
            .enqueue(
                &json!({
                    "type": "start",
                    "flow_id": FID,
                    "execution_id": execution_id,
                    "params": {"who": "rt"},
                    "timestamp": now_iso(),
                }),
                1,
            )
            .unwrap();
    }

    fn state_row(&self, execution_id: &str) -> state::ExecutionState {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .unwrap_or_else(|| panic!("state row for {execution_id} missing"))
    }

    fn pel_len(&self) -> usize {
        self.backend
            .xpending_ext(
                &plaita_worker::keys::queue_key(&self.cfg),
                &self.cfg.consumer_group,
                "-",
                "+",
                100,
            )
            .unwrap()
            .len()
    }
}

// ---------------------------------------------------------------- 负控/正控

/// 格 1（正控）：本地 miss → console latest 解析 + published 装载 → 执行
/// 成功；回写默认关（#303 清除语义只读）。
#[test]
fn console_readthrough_serves_start_to_completion() {
    let stub = Stub::new(vec![
        ("/api/flows/wf-rt", 200, flow_detail_body("published")),
        (
            "/api/flows/wf-rt/versions/v1",
            200,
            version_detail_body("published", STRAIGHT_FLOW),
        ),
    ]);
    let h = harness_console("serve", &stub.url);
    h.enqueue_start("rt-exec-1");
    h.worker.process_one().unwrap();

    let row = h.state_row("rt-exec-1");
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["a"]["echo"], "rt");
    // latest 解析 + 版本明细，恰好两次 console 调用
    assert_eq!(stub.hit_count(), 2, "latest + detail, nothing else");
    // 回写默认关：引擎存储无落地
    let store =
        flowsource::RedisFlowStore::new(h.backend.clone() as Arc<dyn RedisBackend>, h.cfg.clone());
    assert!(
        matches!(store.get_raw("", FID, "v1"), Ok(None)),
        "write-back must stay off by default (#303)"
    );
}

/// 格 2：console 404 → 具名确定性终态 flow_not_found + 负缓存——第二条
/// 载体零 console 调用（TTL 窗内）。
#[test]
fn console_404_negative_caches_named_terminal() {
    let stub = Stub::new(vec![]);
    let h = harness_console("neg404", &stub.url);

    h.enqueue_start("rt-exec-n1");
    h.worker.process_one().unwrap();
    let row = h.state_row("rt-exec-n1");
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "flow_not_found");

    h.enqueue_start("rt-exec-n2");
    h.worker.process_one().unwrap();
    let row2 = h.state_row("rt-exec-n2");
    assert_eq!(row2.status, state::STATUS_ERROR);
    assert_eq!(row2.error.unwrap()["error_type"], "flow_not_found");

    assert_eq!(
        stub.hit_count(),
        1,
        "second carrier must be served from the negative cache"
    );
}

/// 格 3：未配置 URL = 特性关闭，逐字节同 WK-M1 本地语义——本地 miss 即
/// 确定性 NotFound，零 HTTP 面（无 stub 可打）。
#[test]
fn unconfigured_url_keeps_local_semantics() {
    let h = harness("noconsole");
    h.enqueue_start("rt-exec-u1");
    h.worker.process_one().unwrap();
    let row = h.state_row("rt-exec-u1");
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "flow_not_found");
    let msg = err["message"].as_str().unwrap();
    assert!(
        msg.contains("flow definition unavailable"),
        "named deterministic miss expected, got: {msg}"
    );
    assert!(
        !msg.contains("console"),
        "feature-off miss must not pretend a console lookup happened: {msg}"
    );
}

/// 格 4：console 瞬态（503 烧完 attempts）→ 绝不伪造成 404、不终态化、
/// 不进负缓存——消息留 PEL 等重投。
#[test]
fn console_transient_leaves_message_pending() {
    let stub = Stub::new(vec![("/api/flows/wf-rt", 503, "{}".to_string())]);
    let h = harness_console("transient", &stub.url);
    h.enqueue_start("rt-exec-t1");

    let result = h.worker.process_one();
    assert!(result.is_err(), "transient must surface as Err: {result:?}");
    // 无终态行、无 ack（PEL 1 条）、attempts=2 恰两次调用
    assert!(
        state::load(h.backend.as_ref(), &h.cfg, "", "rt-exec-t1")
            .unwrap()
            .is_none(),
        "no terminal row"
    );
    assert_eq!(h.pel_len(), 1, "message stays pending");
    assert_eq!(stub.hit_count(), 2, "default attempts=2");

    // 负缓存未被污染：换 200 后同一条消息重投即成功
    stub.set_body("/api/flows/wf-rt", flow_detail_body("published"));
    stub.set_body(
        "/api/flows/wf-rt/versions/v1",
        version_detail_body("published", STRAIGHT_FLOW),
    );
    h.worker.process_one().unwrap();
    let row = h.state_row("rt-exec-t1");
    assert_eq!(row.status, state::STATUS_COMPLETED);
}

/// 格 5：消费侧 hash 拒格——resume 载体重载的定义（console 侧已变更，
/// 同 flow_id/version）与挂起时记录的指纹不一致 → flow_hash_mismatch 具
/// 名终态；checkpoint 清除。
#[test]
fn console_changed_definition_gates_resume_by_name() {
    let stub = Stub::new(vec![
        ("/api/flows/wf-rt", 200, flow_detail_body("published")),
        (
            "/api/flows/wf-rt/versions/v1",
            200,
            version_detail_body("published", EVENT_FLOW),
        ),
    ]);
    let h = harness_console("hashgate", &stub.url);

    // start（定义来自 console）→ 挂起
    h.enqueue_start("rt-exec-h1");
    h.worker.process_one().unwrap();
    let row = h.state_row("rt-exec-h1");
    assert_eq!(row.status, state::STATUS_SUSPENDED);
    let cp = checkpoint::load(h.backend.as_ref(), &h.cfg, "", "rt-exec-h1")
        .unwrap()
        .expect("checkpoint exists");
    let sub_id = cp.context["nodes"]["e1"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        !row.flow_hash.is_empty(),
        "start stamps the fingerprint from the console-sourced definition"
    );

    // console 侧同版本换定义（发布新内容）
    stub.set_body(
        "/api/flows/wf-rt/versions/v1",
        version_detail_body("published", EVENT_FLOW_MODIFIED),
    );

    h.queue
        .enqueue(
            &json!({
                "type": "resume",
                "flow_id": FID,
                "execution_id": "rt-exec-h1",
                "resume_type": "event",
                "timestamp": now_iso(),
                "data": {"subscription_id": sub_id, "status": "completed"},
            }),
            1,
        )
        .unwrap();
    h.worker.process_one().unwrap();

    let row = h.state_row("rt-exec-h1");
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "flow_hash_mismatch");
    assert_eq!(
        err["details"]["upgrade_suspected"],
        json!(false),
        "same algo label on both sides — a real content change, not an upgrade"
    );
    assert!(
        checkpoint::load(h.backend.as_ref(), &h.cfg, "", "rt-exec-h1")
            .unwrap()
            .is_none(),
        "checkpoint deleted on the terminal"
    );
}
