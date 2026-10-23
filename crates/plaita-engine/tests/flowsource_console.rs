//! #299 §2 console 读穿透负控/正控（B4 第十三轮·全量入位第三步）。
//!
//! 权威形制 = plaita-go `pkg/worker/console_flowsource_test.go`（B3 侧
//! TestConsolePullThrough* 家族的 Rust 对照形）。负控「红」在基线为编译级：
//! `flowsource` 模块与其 console 回源臂均为本轮新符号，基线无接受面
//! （r8 单②同款取证）。
//!
//! 任务书五格：
//!  1. miss→回源→hash 门→装载成功     `pull_through_on_miss_gates_hash_and_caches_locally`
//!  2. 404→具名负缓存                 `console_404_is_negative_cached`
//!  3. 不可达→瞬态不伪造 404          `unreachable_console_is_transient_not_404`
//!  4. 命中路径零 console 调用         `local_hit_makes_zero_console_calls`
//!  5. 未配置=特性关闭（与现状逐字节同）`unconfigured_console_is_feature_off`
//!
//! 另钉：瞬态重试后成功 / attempts 耗尽仍瞬态 / latest 解析（draft 跳过）/
//! draft 版本拒载 / hash 门 mismatch 执法 / unparsable 定义确定性拒 /
//! 租户头透传与缺省映射 / engine_payload 注入口径 / 回写默认关。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use plaita_engine::flowsource::{FlowSource, FlowSourceConfig, LocalFlowStore, MemoryFlowStore};
use plaita_engine::ir::{engine_flow_payload, flow_definition_hash, FLOW_HASH_ALGO};

const FID: &str = "hello_world";

// ---------------------------------------------------------------- 假 console

struct Stub {
    url: String,
    hits: Arc<AtomicUsize>,
    headers: Arc<Mutex<HashMap<String, String>>>,
}

/// 每连接一线程的假 console：`routes` 按 path 给 (status, body)；
/// `fail_first` 个请求先回 502（瞬态重试负控用）。
fn spawn_stub(routes: Vec<(&'static str, u16, String)>, fail_first: usize) -> Stub {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let headers = Arc::new(Mutex::new(HashMap::new()));
    let route_map: Arc<HashMap<String, (u16, String)>> = Arc::new(
        routes
            .into_iter()
            .map(|(p, s, b)| (p.to_string(), (s, b)))
            .collect(),
    );
    let fail_left = Arc::new(AtomicUsize::new(fail_first));
    let hits_t = hits.clone();
    let headers_t = headers.clone();
    std::thread::spawn(move || {
        for incoming in listener.incoming() {
            let Ok(stream) = incoming else { continue };
            let hits = hits_t.clone();
            let headers = headers_t.clone();
            let routes = route_map.clone();
            let fail_left = fail_left.clone();
            std::thread::spawn(move || {
                handle_conn(stream, hits, headers, routes, fail_left);
            });
        }
    });
    Stub { url, hits, headers }
}

fn handle_conn(
    mut stream: TcpStream,
    hits: Arc<AtomicUsize>,
    headers: Arc<Mutex<HashMap<String, String>>>,
    routes: Arc<HashMap<String, (u16, String)>>,
    fail_left: Arc<AtomicUsize>,
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
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    let mut got = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            got.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    headers.lock().unwrap().extend(got);

    hits.fetch_add(1, Ordering::SeqCst);
    let (status, reason, body) = if fail_left.load(Ordering::SeqCst) > 0 {
        fail_left.fetch_sub(1, Ordering::SeqCst);
        (502, "Bad Gateway", "{}".to_string())
    } else {
        match routes.get(&path) {
            Some((s, b)) => (*s, reason_phrase(*s), b.clone()),
            None => (404, "Not Found", "{}".to_string()),
        }
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
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------- 夹具

fn console_cfg(url: &str) -> FlowSourceConfig {
    FlowSourceConfig {
        url: url.to_string(),
        api_key: "test-admin-key".to_string(),
        timeout: Duration::from_secs(5),
        negative_ttl: Duration::from_secs(30),
        attempts: 2,
        write_back: false,
        default_tenant: "default".to_string(),
    }
}

fn hello_flow_json() -> String {
    r#"{"flow_id": "hello_world", "runtime": "python", "nodes": [
        {"type": "start", "id": "s", "next": "e"},
        {"type": "end", "id": "e", "resultType": "success", "output": "hi"}
    ]}"#
    .to_string()
}

fn version_detail_body(status: &str, definition: &str) -> String {
    serde_json::json!({
        "flow_id": FID,
        "version": "3",
        "status": status,
        "runtime": "python",
        "definition": definition,
    })
    .to_string()
}

/// console 发布口径的夹具定义：base（作者原文，无保留键）注入
/// flow_id/version 后按 #60 算法求摘要，作为 definitionHash 随定义发布。
/// 读穿透侧引擎载荷注入必须复现同一计算域，门才放行。
fn definition_with_hash(fid: &str, ver: &str, base: &str) -> String {
    let injected = engine_flow_payload(fid, ver, base.as_bytes()).unwrap();
    let h = flow_definition_hash(injected.as_bytes()).unwrap();
    let mut doc: serde_json::Value = serde_json::from_str(base).unwrap();
    doc.as_object_mut()
        .unwrap()
        .insert("definitionHash".into(), serde_json::json!(h));
    doc.to_string()
}

/// 指纹与文档不符的夹具（hash 门 mismatch 执法负控）：摘要按「base+junk」
/// 计，而门剥保留键后的计算域是 base——必不匹配。
fn definition_with_wrong_hash(base: &str) -> String {
    let mut hashed: serde_json::Value = serde_json::from_str(base).unwrap();
    hashed
        .as_object_mut()
        .unwrap()
        .insert("junk".into(), serde_json::json!("x"));
    let mut doc: serde_json::Value = serde_json::from_str(base).unwrap();
    doc.as_object_mut()
        .unwrap()
        .insert("definitionHash".into(), serde_json::json!("deadbeef"));
    doc.to_string()
}

fn detail_route(status: &str, definition: &str) -> (&'static str, u16, String) {
    (
        "/api/flows/hello_world/versions/3",
        200,
        version_detail_body(status, definition),
    )
}

// ---------------------------------------------------------------- 五格负控

/// 格1：miss→回源→hash 门→装载成功；回写后第二次调用零 console 调用。
#[test]
fn pull_through_on_miss_gates_hash_and_caches_locally() {
    let base = hello_flow_json();
    let served = definition_with_hash(FID, "3", &base);
    let stub = spawn_stub(vec![detail_route("published", &served)], 0);
    let store = Arc::new(MemoryFlowStore::new());
    let mut cfg = console_cfg(&stub.url);
    cfg.write_back = true;
    let fs = FlowSource::new(store.clone(), cfg).unwrap();

    let def = fs.get_flow("", FID, "3").unwrap();
    assert_eq!(def.version, "3");
    assert_eq!(def.flow.id, FID);
    assert_eq!(def.hash_algo, FLOW_HASH_ALGO);
    // 引擎载荷注入口径：flow_id/version 注入后的 canonical 形式。
    let raw_text = String::from_utf8(def.raw.clone()).unwrap();
    assert!(
        raw_text.contains(r#""version":"3""#),
        "payload must inject version: {raw_text}"
    );
    assert!(raw_text.contains(r#""flow_id":"hello_world""#));
    assert_eq!(flow_definition_hash(&def.raw).unwrap(), def.hash);

    // 回写落地：本地 store 已有该定义，第二次调用不再打 console。
    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
    let def2 = fs.get_flow("", FID, "3").unwrap();
    assert_eq!(def2.raw, def.raw);
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        1,
        "write-back must serve the second lookup locally, not from the console"
    );
    assert!(
        matches!(store.get_raw("", FID, "3"), Ok(Some(_))),
        "write-back must land the store"
    );
}

/// 格2：console 404 = 确定性 NotFound + 负缓存窗口吸收重复 miss；窗口过期
/// 才回源。
#[test]
fn console_404_is_negative_cached() {
    let stub = spawn_stub(vec![], 0); // 全 404
    let mut cfg = console_cfg(&stub.url);
    cfg.negative_ttl = Duration::from_millis(250);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), cfg).unwrap();

    let err = fs.get_flow("", "ghost", "9").unwrap_err();
    assert!(
        err.is_not_found(),
        "console 404 = deterministic not-found: {err:?}"
    );
    assert!(!err.is_transient());
    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);

    let err2 = fs.get_flow("", "ghost", "9").unwrap_err();
    assert!(err2.is_not_found());
    assert!(
        err2.to_string().contains("negative-cached"),
        "repeat miss must be absorbed by the negative cache: {err2}"
    );
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        1,
        "negative cache must absorb repeat misses"
    );

    std::thread::sleep(Duration::from_millis(300));
    let _ = fs.get_flow("", "ghost", "9").unwrap_err();
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        2,
        "expired window must read through again"
    );
}

/// 格3：console 不可达 = 瞬态，绝不伪造成不存在（那会把在途执行终态化）；
/// 瞬态不进负缓存。
#[test]
fn unreachable_console_is_transient_not_404() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let closed = format!("http://{}", listener.local_addr().unwrap());
    drop(listener); // 端口已关 = console 不可达

    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&closed)).unwrap();
    for i in 0..2 {
        let err = fs.get_flow("", "f", "1").unwrap_err();
        assert!(
            err.is_transient(),
            "unreachable console degrades to transient, not not-found (call {i}): {err:?}"
        );
        assert!(
            !err.is_not_found(),
            "不可达绝不能伪造成不存在——那会把在途执行终态化: {err:?}"
        );
    }
}

/// 瞬态 5xx 在 attempts 内重试后成功。
#[test]
fn transient_5xx_retried_then_succeeds() {
    let served = definition_with_hash(FID, "3", &hello_flow_json());
    let stub = spawn_stub(vec![detail_route("published", &served)], 1);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let def = fs.get_flow("", FID, "3").unwrap();
    assert_eq!(def.version, "3");
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        2,
        "one transient 5xx must be retried"
    );
}

/// attempts 耗尽仍是瞬态（不转确定性、不进负缓存）。
#[test]
fn attempts_exhausted_surfaces_transient() {
    let served = definition_with_hash(FID, "3", &hello_flow_json());
    let stub = spawn_stub(vec![detail_route("published", &served)], 3);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let err = fs.get_flow("", FID, "3").unwrap_err();
    assert!(
        err.is_transient(),
        "exhausted attempts stay transient: {err:?}"
    );
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        2,
        "attempts cap bounds the console calls"
    );
}

/// 格4：本地命中零 console 调用（既有装载路径零回归）。
#[test]
fn local_hit_makes_zero_console_calls() {
    let stub = spawn_stub(vec![], 0);
    let store = Arc::new(MemoryFlowStore::new());
    store.preset("", FID, "latest", hello_flow_json().as_bytes());
    let fs = FlowSource::new(store, console_cfg(&stub.url)).unwrap();

    let def = fs.get_flow("", FID, "").unwrap();
    assert_eq!(def.version, "latest");
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        0,
        "console must not be consulted on a local hit"
    );
}

/// 格5：未配置 = 特性关闭，语义与现状逐字节同——miss 即确定性 NotFound，
/// 无任何 console 面。
#[test]
fn unconfigured_console_is_feature_off() {
    let cfg = console_cfg("");
    assert!(cfg.url.is_empty());
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), cfg).unwrap();

    let err = fs.get_flow("", "ghost", "9").unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
    let text = err.to_string();
    assert!(
        text.contains("flow ghost"),
        "pre-#299 miss message shape: {text}"
    );
    assert!(text.contains("version 9"), "{text}");
}

// ---------------------------------------------------------------- 另钉语义

/// latest 哨兵在 console 侧解析为最新 published（draft 跳过）；任务租户
/// 透传为 X-Tenant-ID。
#[test]
fn latest_resolves_newest_published_with_tenant_header() {
    // 指纹域含注入身份：latest 解析出的具体版本是 2.0，夹具摘要须按 2.0
    // 的注入载荷计（门对身份错位会具名拒——上一版夹具按 "3" 计即被当场
    // 拒，反证门在回源路径执法）。
    let served = definition_with_hash(FID, "2.0", &hello_flow_json());
    let list_body = serde_json::json!({
        "flow_id": FID,
        "versions": [
            {"version": "1.0", "status": "published", "published_at": "2026-10-20T10:00:00"},
            {"version": "2.0", "status": "published", "published_at": "2026-10-21T10:00:00"},
            {"version": "3.0", "status": "draft"},
        ],
    })
    .to_string();
    let stub = spawn_stub(
        vec![
            ("/api/flows/hello_world", 200, list_body),
            (
                "/api/flows/hello_world/versions/2.0",
                200,
                version_detail_body("published", &served),
            ),
        ],
        0,
    );
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let def = fs.get_flow("acme", FID, "").unwrap();
    assert_eq!(def.version, "2.0", "newest published wins; draft skipped");
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        2,
        "one flow-detail + one version-detail call"
    );
    assert_eq!(
        stub.headers
            .lock()
            .unwrap()
            .get("x-tenant-id")
            .map(String::as_str),
        Some("acme"),
        "task tenant must pass through as X-Tenant-ID"
    );
}

/// 空/缺省租户映射为具名缺省租户（console 的 X-Tenant-ID 须指向真实租户行）。
#[test]
fn default_tenant_maps_explicitly() {
    let served = definition_with_hash(FID, "3", &hello_flow_json());
    let stub = spawn_stub(vec![detail_route("published", &served)], 0);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let _ = fs.get_flow("", FID, "3").unwrap();
    assert_eq!(
        stub.headers
            .lock()
            .unwrap()
            .get("x-tenant-id")
            .map(String::as_str),
        Some("default")
    );
    assert_eq!(
        stub.headers
            .lock()
            .unwrap()
            .get("x-admin-api-key")
            .map(String::as_str),
        Some("test-admin-key"),
        "admin key must ride X-Admin-API-Key"
    );
}

/// 未发布版本不可执行：确定性 NotFound + 具名 not published。
#[test]
fn draft_version_rejected() {
    let served = hello_flow_json();
    let stub = spawn_stub(vec![detail_route("draft", &served)], 0);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let err = fs.get_flow("", FID, "3").unwrap_err();
    assert!(err.is_not_found(), "未发布版本不可执行: {err:?}");
    assert!(err.to_string().contains("not published"), "{err}");
    let _ = fs.get_flow("", FID, "3").unwrap_err();
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        1,
        "deterministic rejection is negative-cached"
    );
}

/// 读穿透定义必须过 hash 门（r11 的门在同路径，天然执法）：mismatch 具名拒
/// + 负缓存。
#[test]
fn hash_gate_rejects_mismatched_pull_through() {
    let served = definition_with_wrong_hash(&hello_flow_json());
    let stub = spawn_stub(vec![detail_route("published", &served)], 0);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let err = fs.get_flow("", FID, "3").unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
    assert!(
        err.to_string().contains("flow_hash_mismatch"),
        "the r11 hash gate must fire on the pull-through path: {err}"
    );
    let _ = fs.get_flow("", FID, "3").unwrap_err();
    assert_eq!(
        stub.hits.load(Ordering::SeqCst),
        1,
        "gate rejection is negative-cached"
    );
}

/// published 但 unparsable 的定义 = 确定性拒 + 负缓存（重投不可能成功）。
#[test]
fn unparsable_definition_is_deterministic_and_cached() {
    let stub = spawn_stub(vec![detail_route("published", "not-json{{{")], 0);
    let fs = FlowSource::new(Arc::new(MemoryFlowStore::new()), console_cfg(&stub.url)).unwrap();

    let err = fs.get_flow("", FID, "3").unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
    assert!(err.to_string().contains("unparsable"), "{err}");
    let _ = fs.get_flow("", FID, "3").unwrap_err();
    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
}

/// 回写默认关：读穿透保持对本地存储只读（#303 删除后不复活语义）。
#[test]
fn write_back_disabled_by_default() {
    let served = definition_with_hash(FID, "3", &hello_flow_json());
    let stub = spawn_stub(vec![detail_route("published", &served)], 0);
    let store = Arc::new(MemoryFlowStore::new());
    let fs = FlowSource::new(store.clone(), console_cfg(&stub.url)).unwrap();

    assert!(fs.get_flow("", FID, "3").is_ok());
    assert!(
        matches!(store.get_raw("", FID, "3"), Ok(None)),
        "write-back must stay off unless explicitly opted in"
    );
}

/// 引擎载荷注入口径（Go engineFlowPayload 对照）：字面量保真、canonical
/// 稳定、与等价手工文档同指纹。
#[test]
fn engine_payload_injects_identity_and_preserves_literals() {
    let out = engine_flow_payload(
        "fid",
        "7",
        br#"{"nodes":[],"inputType":1,"big":1234567890123456789}"#,
    )
    .unwrap();
    assert!(out.contains(r#""flow_id":"fid""#), "{out}");
    assert!(out.contains(r#""version":"7""#), "{out}");
    assert!(
        out.contains("1234567890123456789"),
        "number literals must survive verbatim, not f64: {out}"
    );
    // 指纹与等价手工文档一致（canonical 形式对同一文档稳定，键序无关）。
    let equivalent =
        br#"{"version":"7","nodes":[],"inputType":1,"big":1234567890123456789,"flow_id":"fid"}"#;
    assert_eq!(
        flow_definition_hash(out.as_bytes()).unwrap(),
        flow_definition_hash(equivalent).unwrap()
    );
}

/// 本地路径装载拒（ir 拒载清单）= 确定性 not loadable（与 Go 同口径：该
/// 引擎构建下永久失败，不烧重试预算）。
#[test]
fn local_unloadable_definition_is_deterministic() {
    let store = Arc::new(MemoryFlowStore::new());
    store.preset(
        "",
        FID,
        "1",
        br#"{"flow_id": "x", "nodes": [{"type": "agentrun", "id": "a"}]}"#,
    );
    let mut cfg = console_cfg("");
    cfg.url = String::new();
    let fs = FlowSource::new(store, cfg).unwrap();

    let err = fs.get_flow("", FID, "1").unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
    assert!(err.to_string().contains("not loadable"), "{err}");
}
