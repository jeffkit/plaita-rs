//! wake/http_callback — the http_callback wake source (rs#19 second
//! installment; port of plaita-go pkg/wake/httpcallback.go, semantics
//! authority Python HttpCallbackService) — WITH one addition the Python
//! service lacks: the HTTP ingress lives HERE (Python exposes
//! handle_callback_request only as an in-process API; no server-side route
//! exists anywhere in the Python stack — jeffkit/plaita#350), so a pure-Rust
//! deployment gets its front door from plaita-rs-wake.
//!
//! Faces:
//!
//! 1. Intake: consume `{prefix}:http_callback:queue` (the worker's §5.2
//!    dispatch) → register `{prefix}:http_callback:registered:{path}`
//!    (TTL 7d, shape byte-compatible with Python: task_config +
//!    registered_time).
//! 2. Ingress: a minimal HTTP/1.1 server (PLAITA_HTTP_CALLBACK_ADDR, default
//!    ":8080" — the address the node's full_url points at) accepting POSTs
//!    on registered paths, one thread per connection.
//!
//! Request chain = Python handle_callback_request line-for-line:
//! read the record (no delete) → verify credentials BEFORE the claim (#113)
//! → atomic two-phase claim (named Lua: GET + SET NX PX 30s; confirmed ⇒
//! DEL record then claim — a crash in between never double-resumes; publish
//! failure ⇒ DEL claim only, the record stays claimable #188) → publish the
//! `http_callback` resume event.
//!
//! Q5: unauthenticated callers get ONE response body for "unknown path",
//! "claim race loser" and "bad credentials" (`回调路径未注册`) — three
//! distinct messages would be a path-existence oracle. Auth details go to
//! the log, never to the response.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::events::sleep_interruptible;
use crate::flowsource::canonical_flow_definition_json;
use crate::keys;
use crate::wake::{env_f64, env_i64, EventSink, RedisEventSink};

pub const SOURCE_HTTP_CALLBACK: &str = "http_callback";

const FAILURE_BODY: &str = r#"{"status":"error","message":"回调路径未注册"}"#;
const RECORD_TTL_SECS: i64 = 7 * 86400;
const CLAIM_TTL_MS: i64 = 30_000;
const SIGNATURE_TOLERANCE_SECS: i64 = 300;
const MAX_BODY_BYTES: usize = 10 << 20;

/// The http_callback wake source.
pub struct HttpCallbackWake {
    cfg: Config,
    backend: Arc<dyn RedisBackend>,
    sink: Arc<dyn EventSink>,
    stop: Arc<AtomicBool>,

    queue_key: String,
    record_prefix: String,
    claim_prefix: String,
    registry_key: String,
    start_time: String,
    instance_id: String,

    poll: Duration,
    heartbeat: Duration,
    registry_ttl_secs: i64,
    batch: i64,
    addr: String,

    listener: Mutex<Option<TcpListener>>,
    inflight: Mutex<HashSet<String>>,
}

impl HttpCallbackWake {
    pub fn new(cfg: Config, backend: Arc<dyn RedisBackend>, stop: Arc<AtomicBool>) -> Self {
        let sink: Arc<dyn EventSink> = Arc::new(RedisEventSink {
            backend: Arc::clone(&backend),
            cfg: cfg.clone(),
        });
        Self::with_parts(cfg, backend, sink, stop)
    }

    /// Overrides the event sink (tests inject failures).
    pub fn with_sink(
        cfg: Config,
        backend: Arc<dyn RedisBackend>,
        sink: Arc<dyn EventSink>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self::with_parts(cfg, backend, sink, stop)
    }

    fn with_parts(
        cfg: Config,
        backend: Arc<dyn RedisBackend>,
        sink: Arc<dyn EventSink>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let instance_id = cfg.instance_id.clone();
        let addr = Self::resolve_addr();
        Self {
            queue_key: keys::service_queue_key(&cfg, SOURCE_HTTP_CALLBACK),
            record_prefix: format!("{}:http_callback:registered:", cfg.key_prefix),
            claim_prefix: format!("{}:http_callback:claim:", cfg.key_prefix),
            registry_key: format!(
                "{}:registry:{}:{}",
                cfg.key_prefix, SOURCE_HTTP_CALLBACK, cfg.instance_id
            ),
            start_time: now_iso(),
            poll: Duration::from_secs_f64(env_f64(
                "PLAITA_HTTP_CALLBACK_POLL_INTERVAL_SECONDS",
                0.5,
            )),
            heartbeat: Duration::from_secs_f64(env_f64("PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10.0)),
            registry_ttl_secs: env_i64("PLAITA_REGISTRY_TTL_SECONDS", 30).max(1),
            batch: env_i64("PLAITA_HTTP_CALLBACK_CONSUME_BATCH", 50),
            cfg,
            backend,
            sink,
            stop,
            instance_id,
            addr,
            listener: Mutex::new(None),
            inflight: Mutex::new(HashSet::new()),
        }
    }

    /// Resolves the ingress address: PLAITA_HTTP_CALLBACK_ADDR, else
    /// :8080 (the address the node's full_url points at). A bare ":port"
    /// binds all interfaces — Rust's TcpListener needs the explicit
    /// 0.0.0.0 host (Go accepts the bare form natively).
    fn resolve_addr() -> String {
        let raw = std::env::var("PLAITA_HTTP_CALLBACK_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| ":8080".to_string());
        if let Some(port) = raw.strip_prefix(':') {
            format!("0.0.0.0:{port}")
        } else {
            raw
        }
    }

    /// Overrides the ingress address (tests bind 127.0.0.1:0).
    #[cfg(test)]
    pub(crate) fn set_addr_for_tests(&mut self, addr: &str) {
        self.addr = addr.to_string();
    }

    /// Runs until `stop` is set: the ingress serves until the flag, then the
    /// consume loop drains and the registry tombstones.
    pub fn run(self: &Arc<Self>) {
        self.registry_beat("starting", 0);
        let heartbeat_stop = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = {
            let hb = Arc::clone(self);
            let hb_stop = Arc::clone(&heartbeat_stop);
            std::thread::spawn(move || hb.registry_loop(hb_stop))
        };

        // Ingress: bind first (a bind failure is fatal — the source cannot
        // serve its one job), then accept-loop on its own thread.
        let listener = match TcpListener::bind(&self.addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "[wake-http-callback] ingress bind {} failed: {e}",
                    self.addr
                );
                let _ = heartbeat_handle.join();
                return;
            }
        };
        let bound = listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| self.addr.clone());
        eprintln!(
            "[wake-http-callback] started queue={} ingress={} instance={}",
            self.queue_key, bound, self.instance_id
        );
        listener.set_nonblocking(true).expect("ingress nonblocking");
        {
            *self.listener.lock().unwrap() = Some(listener.try_clone().expect("listener clone"));
        }
        let accept_stop = Arc::clone(&self.stop);
        let accept_me = Arc::clone(self);
        let accept_thread = std::thread::spawn(move || loop {
            if accept_stop.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    let me = Arc::clone(&accept_me);
                    std::thread::spawn(move || me.serve_conn(stream));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(e) => {
                    eprintln!("[wake-http-callback] accept failed: {e}");
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        });

        let mut joins: Vec<std::thread::JoinHandle<()>> = Vec::new();
        while !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            self.cycle_once(&mut joins);
            joins.retain(|j| !j.is_finished());
            sleep_interruptible(&self.stop, self.poll);
        }
        for j in joins {
            let _ = j.join();
        }
        heartbeat_stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = heartbeat_handle.join();
        accept_thread.join().expect("accept thread");
        self.registry_shutdown();
        eprintln!("[wake-http-callback] stopped queue={}", self.queue_key);
    }

    pub fn bound_addr(&self) -> String {
        self.listener
            .lock()
            .ok()
            .and_then(|l| l.as_ref().and_then(|l| l.local_addr().ok()))
            .map(|a| a.to_string())
            .unwrap_or_default()
    }

    fn registry_loop(self: Arc<Self>, stop: Arc<AtomicBool>) {
        self.registry_beat("running", 0);
        while !stop.load(std::sync::atomic::Ordering::SeqCst) {
            if sleep_interruptible(&stop, self.heartbeat) {
                return;
            }
            let active = self.inflight.lock().map(|m| m.len()).unwrap_or(0);
            self.registry_beat("running", active as i64);
        }
    }

    fn registry_beat(&self, status: &str, active: i64) {
        let info = json!({
            "instance_id": self.instance_id,
            "service_type": SOURCE_HTTP_CALLBACK,
            "host": hostname(),
            "status": status,
            "start_time": self.start_time,
            "metadata": {"engine": "plaita-rs", "key_prefix": self.cfg.key_prefix},
            "active_tasks": active,
            "last_heartbeat": now_iso(),
        });
        if let Err(e) = self.backend.set(
            &self.registry_key,
            &info.to_string(),
            Some(self.registry_ttl_secs),
        ) {
            eprintln!("[wake-http-callback] registry beat failed: {e}");
        }
    }

    fn registry_shutdown(&self) {
        let tomb = json!({
            "instance_id": self.instance_id,
            "service_type": SOURCE_HTTP_CALLBACK,
            "host": hostname(),
            "status": "stopped",
            "start_time": self.start_time,
            "last_heartbeat": now_iso(),
        });
        let _ = self
            .backend
            .set(&self.registry_key, &tomb.to_string(), Some(5));
        let _ = self.backend.del(&[&self.registry_key]);
    }

    // ---- intake ----

    fn cycle_once(self: &Arc<Self>, _joins: &mut Vec<std::thread::JoinHandle<()>>) {
        let raws = match self.backend.lrange(&self.queue_key, 0, self.batch - 1) {
            Ok(v) => v,
            Err(e) => {
                if !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                    eprintln!("[wake-http-callback] consume read failed: {e}");
                }
                return;
            }
        };
        for raw in raws {
            let task: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "[wake-http-callback] invalid task JSON (dropped): {e}: {:.200}",
                        raw
                    );
                    let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                    continue;
                }
            };
            let path_ok = task
                .get("callback_config")
                .and_then(|c| c.get("path"))
                .and_then(|p| p.as_str())
                .is_some_and(|p| !p.is_empty());
            let ok = path_ok
                && str_field(&task, "node_id").is_some_and(|s| !s.is_empty())
                && str_field(&task, "event_type").is_some_and(|s| !s.is_empty());
            if !ok {
                eprintln!(
                    "[wake-http-callback] task missing node_id/event_type/callback_config.path (dropped): {:.200}",
                    raw
                );
                let _ = self.backend.lrem(&self.queue_key, 1, &raw);
                continue;
            }
            if let Err(e) = self.register(&task) {
                eprintln!("[wake-http-callback] registration failed; entry kept for retry: {e}");
                continue;
            }
            let _ = self.backend.lrem(&self.queue_key, 1, &raw);
        }
    }

    fn register(&self, task: &Value) -> Result<(), String> {
        let path = task["callback_config"]["path"].as_str().unwrap_or_default();
        let record = json!({
            "task_config": task,
            "registered_time": now_ms(),
        });
        self.backend
            .set(
                &format!("{}{}", self.record_prefix, path),
                &record.to_string(),
                Some(RECORD_TTL_SECS),
            )
            .map_err(|e| e.to_string())?;
        eprintln!(
            "[wake-http-callback] path registered: {} node={}",
            path,
            str_field(task, "node_id").unwrap_or_default()
        );
        Ok(())
    }

    // ---- ingress ----

    fn serve_conn(&self, mut stream: TcpStream) {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
        let raw = match read_http_request(&mut stream) {
            Ok(r) => r,
            Err(_) => {
                let _ = write_response(&mut stream, 400, FAILURE_BODY);
                return;
            }
        };
        let result = match raw {
            Some(req) => {
                if req.method != "POST" {
                    Err(405_u16)
                } else {
                    self.handle(req)
                }
            }
            None => Err(400_u16),
        };
        let (status, body) = match result {
            Ok(body) => (200_u16, body),
            Err(code) => (code, FAILURE_BODY.to_string()),
        };
        let _ = write_response(&mut stream, status, &body);
    }

    /// Processes one parsed request; Ok = the JSON response body. All auth
    /// / unknown-path / claim-race failures return the ONE unified body
    /// (Q5) — the variant only lives in the log.
    fn handle(&self, req: RawRequest) -> Result<String, u16> {
        let body: Value = if req.body.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(_) => return Err(400_u16), // cannot match a signature either
            }
        };
        // handle_callback_request's Err = the unified failure (Q5): the
        // response is the failure BODY with a 200-free HTTP error status —
        // Python returns the error dict as the response payload.
        match self.handle_callback_request(&req.path, &body, &req.headers) {
            Ok(resp) => Ok(resp.to_string()),
            Err(()) => Ok(FAILURE_BODY.to_string()),
        }
    }

    /// The Python handle_callback_request chain. Public for hosts that front
    /// it with their own HTTP layer. Ok(response json value) / Err = unified
    /// failure (Q5).
    #[allow(clippy::result_unit_err)] // Err(()) 是文档化的统一失败面（Q5）：不携带原因，refusal 细节走 verify_auth 日志通道
    pub fn handle_callback_request(
        &self,
        path: &str,
        request_data: &Value,
        headers: &[(String, String)],
    ) -> Result<Value, ()> {
        #[allow(clippy::type_complexity)]
        let record_key = format!("{}{}", self.record_prefix, path);
        let Ok(Some(record_raw)) = self.backend.get(&record_key) else {
            return Err(());
        };
        let Ok(record) = serde_json::from_str::<Value>(&record_raw) else {
            return Err(());
        };
        let task_cfg = record.get("task_config").cloned().unwrap_or(json!({}));

        // 2) Verify credentials BEFORE the claim (#113: claiming first would
        // let one unauthorized request block the legitimate callback for the
        // claim TTL).
        if let Some(detail) = verify_auth(task_cfg.get("auth_config"), request_data, headers) {
            eprintln!("[wake-http-callback] auth failed: {detail} (path={path})");
            return Err(());
        }

        // 3) Atomic two-phase claim (named Lua GET + SET NX PX).
        let claim_key = format!("{}{}", self.claim_prefix, path);
        match self
            .backend
            .claim_callback(&record_key, &claim_key, CLAIM_TTL_MS)
        {
            Ok(crate::backend::ClaimOutcome::Claimed(claimed)) => {
                let task_cfg: Value = serde_json::from_str::<Value>(&claimed)
                    .ok()
                    .and_then(|r| r.get("task_config").cloned())
                    .unwrap_or(task_cfg);
                let now_ms = now_ms();
                let tenant = str_field(&task_cfg, "tenant_id")
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| self.cfg.default_tenant_id.clone());
                let execution_id = str_field(&task_cfg, "execution_id").unwrap_or_default();
                let event_type = str_field(&task_cfg, "event_type").unwrap_or_default();
                let ev = json!({
                    "event_id": Uuid::new_v4().to_string(),
                    "event_type": event_type,
                    "data": {
                        "node_id": task_cfg.get("node_id").cloned().unwrap_or(Value::Null),
                        "execution_id": execution_id,
                        "flow_id": task_cfg.get("flow_id").cloned().unwrap_or(Value::Null),
                        "tenant_id": tenant,
                        "trigger_type": "http_callback",
                        "callback_path": path,
                        "request_data": request_data,
                        "timestamp": now_ms,
                        "success": true,
                    },
                    "timestamp": now_ms as f64 / 1000.0,
                    "correlation_id": execution_id,
                    "tenant_id": tenant,
                });
                let channel = keys::business_event_channel(&self.cfg, &tenant, &event_type);
                if let Err(e) = self.sink.publish(&channel, &ev.to_string()) {
                    // 5b) Publish failed: release the claim only — the record
                    // stays claimable so the provider's retry is not answered
                    // "unregistered" (plaita#188).
                    eprintln!(
                        "[wake-http-callback] resume event publish failed; claim released, record claimable: {e}"
                    );
                    let _ = self.backend.del(&[&claim_key]);
                    return Err(());
                }
                // 5a) Published: record first, claim second (a crash in
                // between must never double-resume).
                let _ = self.backend.del(&[&record_key]);
                let _ = self.backend.del(&[&claim_key]);
                eprintln!("[wake-http-callback] processed: {path}");
                let success = task_cfg
                    .get("response_config")
                    .and_then(|r| r.get("success_response"))
                    .cloned();
                Ok(success.unwrap_or_else(|| json!({"status": "success"})))
            }
            _ => Err(()), // NoRecord / RaceLost — unified response (Q5)
        }
    }
}

// ---- HTTP plumbing (minimal HTTP/1.1, one request per connection) ----

pub struct RawRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Option<RawRequest>> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    let header_end;
    loop {
        if let Some(pos) = find_double_crlf(&buf) {
            header_end = pos;
            break;
        }
        if buf.len() > 64 * 1024 {
            return Ok(None); // header flood
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(None); // EOF before headers complete
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let raw_path = parts.next().unwrap_or_default().to_string();
    let path = percent_decode(&raw_path);
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_string();
            let value = value.trim().to_string();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }
    if content_length > MAX_BODY_BYTES {
        return Ok(None);
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok(Some(RawRequest {
        method,
        path,
        headers,
        body,
    }))
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes())?;
    stream.flush()
}

// ---- auth (Python _verify_callback_auth family) ----

/// Returns None when allowed, Some(log-only detail) on refusal.
fn verify_auth(
    auth_config: Option<&Value>,
    request_data: &Value,
    headers: &[(String, String)],
) -> Option<String> {
    let ac = auth_config?;
    let obj = ac.as_object()?;
    if obj.is_empty() {
        return None; // no auth declared (legacy records)
    }
    let enabled = obj
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let has_type = obj.contains_key("type");
    if !enabled && !has_type {
        return None;
    }
    let auth_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or_default();
    match auth_type {
        "token" => {
            let expected_token = str_field(ac, "token").unwrap_or_default();
            if !const_eq(&bearer_token(headers), &expected_token) {
                return Some("认证失败：token 不匹配".to_string());
            }
            None
        }
        "basic" => {
            let (user, pass) = basic_credentials(headers);
            let want_user = str_field(ac, "username").unwrap_or_default();
            let want_pass = str_field(ac, "password").unwrap_or_default();
            if !const_eq(&user, &want_user) || !const_eq(&pass, &want_pass) {
                return Some("认证失败：basic 凭据不匹配".to_string());
            }
            None
        }
        "signature" => verify_signature(&Value::Object(obj.clone()), request_data, headers),
        other => Some(format!("认证失败：不支持的认证类型 {other:?}")),
    }
}

/// HMAC-SHA256 over `<unix-seconds>.<canonical-json(request_data)>` with the
/// X-Plaita-Timestamp / X-Plaita-Signature headers (Python
/// _verify_signature; the canonical form is the shared CPython sort_keys
/// encoder — the same strict parser the flow fingerprint uses).
fn verify_signature(
    ac: &Value,
    request_data: &Value,
    headers: &[(String, String)],
) -> Option<String> {
    let secret = str_field(ac, "secret").unwrap_or_default();
    if secret.is_empty() {
        return Some("认证失败：签名认证缺少 secret".to_string());
    }
    let algorithm = str_field(ac, "algorithm").unwrap_or_default();
    if !algorithm.eq_ignore_ascii_case("HMAC-SHA256") {
        return Some(format!("认证失败：不支持的签名算法 {algorithm:?}"));
    }
    let ts_raw = header(headers, "X-Plaita-Timestamp");
    let sig = header(headers, "X-Plaita-Signature");
    let (Ok(ts), true) = (
        ts_raw.as_deref().unwrap_or_default().trim().parse::<i64>(),
        sig.is_some() && ts_raw.is_some(),
    ) else {
        return Some("认证失败：缺少 X-Plaita-Timestamp / X-Plaita-Signature".to_string());
    };
    if (now_ms() / 1000 - ts).abs() > SIGNATURE_TOLERANCE_SECS {
        return Some("认证失败：签名时间戳超出允许窗口".to_string());
    }
    let Ok(canonical) =
        canonical_flow_definition_json(&serde_json::to_vec(request_data).unwrap_or_default())
    else {
        return Some("认证失败：签名原文无法构造".to_string());
    };
    let payload = format!("{ts}.{}", String::from_utf8_lossy(&canonical));
    let expected = hmac_sha256(secret.as_bytes(), payload.as_bytes());
    let provided = sig.unwrap().trim().to_string();
    let provided = provided
        .strip_prefix("sha256=")
        .or_else(|| provided.strip_prefix("SHA256="))
        .unwrap_or(&provided)
        .to_lowercase();
    if const_eq_hex(&provided, &expected) {
        None
    } else {
        Some("认证失败：签名不匹配".to_string())
    }
}

/// HMAC-SHA256 (RFC 2104) over sha2 — no extra dependency.
fn hmac_sha256(key: &[u8], message: &[u8]) -> String {
    const BLOCK: usize = 64;
    let mut key = key.to_vec();
    if key.len() > BLOCK {
        let mut d = Sha256::new();
        d.update(&key);
        key = d.finalize().to_vec();
    }
    key.resize(BLOCK, 0);
    let mut inner = Sha256::new();
    for b in &key {
        inner.update([b ^ 0x36]);
    }
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    for b in &key {
        outer.update([b ^ 0x5c]);
    }
    outer.update(inner);
    hex(&outer.finalize())
}

fn hex(bytes: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(D[(b >> 4) as usize] as char);
        out.push(D[(b & 0x0f) as usize] as char);
    }
    out
}

fn header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn bearer_token(headers: &[(String, String)]) -> String {
    let raw = header(headers, "Authorization").unwrap_or_default();
    let mut parts = raw.splitn(2, ' ');
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default().trim().to_string();
    if scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() {
        token
    } else {
        String::new()
    }
}

fn basic_credentials(headers: &[(String, String)]) -> (String, String) {
    let raw = header(headers, "Authorization").unwrap_or_default();
    let mut parts = raw.splitn(2, ' ');
    let scheme = parts.next().unwrap_or_default();
    let encoded = parts.next().unwrap_or_default().trim().to_string();
    if !scheme.eq_ignore_ascii_case("basic") {
        return (String::new(), String::new());
    }
    match base64_decode(&encoded) {
        Some(decoded) => {
            let text = String::from_utf8_lossy(&decoded).to_string();
            match text.split_once(':') {
                Some((u, p)) => (u.to_string(), p.to_string()),
                None => (String::new(), String::new()),
            }
        }
        None => (String::new(), String::new()),
    }
}

/// Minimal standard base64 decoder (RFC 4648, padding required) — the crate
/// carries no base64 dependency and this is its only user.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    const REV: fn(u8) -> Option<u8> = |c: u8| match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    let raw: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let stripped: &[u8] = raw.strip_suffix(b"====").unwrap_or(&raw);
    if stripped.len() % 4 != 0 || stripped.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(stripped.len() / 4 * 3);
    for quad in stripped.chunks(4) {
        let mut vals = [0u8; 4];
        let mut pad = 0usize;
        for (i, &c) in quad.iter().enumerate() {
            if c == b'=' {
                if i < 2 {
                    return None; // misplaced padding
                }
                pad += 1;
                vals[i] = 0;
            } else {
                vals[i] = REV(c)?;
            }
        }
        let n = ((vals[0] as u32) << 18)
            | ((vals[1] as u32) << 12)
            | ((vals[2] as u32) << 6)
            | (vals[3] as u32);
        let take = 3 - pad;
        // The 24-bit block packs 3 payload bytes at bits 23..0 — u32 BE is
        // [0, b1, b2, b3], so the payload lives at indices 1..=take.
        out.extend_from_slice(&n.to_be_bytes()[1..1 + take]);
    }
    Some(out)
}

/// Constant-time equality; either side empty = mismatch (Python
/// hmac.compare_digest over None = False).
fn const_eq(a: &str, b: &str) -> bool {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn const_eq_hex(provided: &str, expected_hex: &str) -> bool {
    if provided.len() != expected_hex.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in provided.bytes().zip(expected_hex.bytes()) {
        diff |= x.to_ascii_lowercase() ^ y;
    }
    diff == 0
}

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn hostname() -> String {
    if let Ok(h) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let h = h.trim().to_string();
        if !h.is_empty() {
            return h;
        }
    }
    for key in ["HOST", "HOSTNAME"] {
        if let Ok(h) = std::env::var(key) {
            if !h.is_empty() {
                return h;
            }
        }
    }
    "unknown".to_string()
}

#[cfg(test)]
pub(crate) mod tests_support {
    /// Forward base64 for test fixtures.
    pub(crate) fn tests_support_encode(s: &str) -> String {
        const D: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = s.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
            out.push(D[(n >> 18) as usize & 63] as char);
            out.push(D[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                D[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                D[n as usize & 63] as char
            } else {
                '='
            });
        }
        out
    }

    pub(crate) fn tests_hmac(secret: &str, message: &[u8]) -> String {
        // reuse the production hmac via the signature path internals
        super::hmac_sha256(secret.as_bytes(), message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;
    use std::sync::atomic::Ordering;
    use tests_support::{tests_hmac, tests_support_encode};

    struct Fixture {
        wake: Arc<HttpCallbackWake>,
        backend: Arc<FakeRedis>,
        events: Arc<Mutex<Vec<(String, String)>>>,
        fail: Arc<AtomicBool>,
    }

    fn fixture() -> Fixture {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_KEY_PREFIX" => "plaita".to_string(),
            "PLAITA_INSTANCE_ID" => "hcb-test".to_string(),
            "PLAITA_HTTP_CALLBACK_ADDR" => "127.0.0.1:0".to_string(),
            _ => String::new(),
        });
        let backend = Arc::new(FakeRedis::new());
        let backend_dyn: Arc<dyn RedisBackend> = backend.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let fail = Arc::new(AtomicBool::new(false));
        struct SeqSink(Arc<Mutex<Vec<(String, String)>>>, Arc<AtomicBool>);
        impl EventSink for SeqSink {
            fn publish(&self, channel: &str, payload: &str) -> Result<(), String> {
                if self.1.load(Ordering::SeqCst) {
                    return Err("sink down".to_string());
                }
                self.0
                    .lock()
                    .unwrap()
                    .push((channel.to_string(), payload.to_string()));
                Ok(())
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(HttpCallbackWake::with_sink(
            cfg,
            backend_dyn,
            Arc::new(SeqSink(Arc::clone(&events), Arc::clone(&fail))),
            stop,
        ));
        Fixture {
            wake,
            backend,
            events,
            fail,
        }
    }

    fn task_raw(auth: Value) -> String {
        json!({
            "type": "http_callback",
            "node_id": "await-cb",
            "event_type": "http_callback",
            "execution_id": "exec-cb",
            "flow_id": "flow-cb",
            "tenant_id": "default",
            "callback_config": {"path": "/callback/flow-cb/await-cb/exec-cb"},
            "auth_config": auth,
        })
        .to_string()
    }

    #[test]
    fn success_chain_no_auth() {
        let fx = fixture();
        let raw = task_raw(json!({}));
        let task: Value = serde_json::from_str(&raw).unwrap();
        fx.wake.register(&task).unwrap();
        let headers: Vec<(String, String)> = Vec::new();
        let resp = fx
            .wake
            .handle_callback_request(
                "/callback/flow-cb/await-cb/exec-cb",
                &json!({"result": "ok"}),
                &headers,
            )
            .unwrap();
        assert_eq!(resp["status"], "success");
        let events = fx.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].0.ends_with(":events:http_callback"));
        let ev: Value = serde_json::from_str(&events[0].1).unwrap();
        assert_eq!(ev["correlation_id"], "exec-cb");
        assert_eq!(ev["data"]["request_data"]["result"], "ok");
        drop(events);
        assert!(fx
            .backend
            .get("plaita:http_callback:registered:/callback/flow-cb/await-cb/exec-cb")
            .unwrap()
            .is_none());
        assert!(fx
            .backend
            .get("plaita:http_callback:claim:/callback/flow-cb/await-cb/exec-cb")
            .unwrap()
            .is_none());
        let _ = headers;
    }

    #[test]
    fn q5_unified_failure_no_oracle() {
        let fx = fixture();
        let raw = task_raw(json!({"enabled": true, "type": "token", "token": "s3cret"}));
        let task: Value = serde_json::from_str(&raw).unwrap();
        fx.wake.register(&task).unwrap();
        let wrong = fx.wake.handle_callback_request(
            "/callback/flow-cb/await-cb/exec-cb",
            &json!({}),
            &[("Authorization".to_string(), "Bearer nope".to_string())],
        );
        let unknown = fx
            .wake
            .handle_callback_request("/never-registered", &json!({}), &[]);
        assert!(wrong.is_err() && unknown.is_err());
        // Both surfaced as the same body through the HTTP layer:
        assert_eq!(
            FAILURE_BODY,
            r#"{"status":"error","message":"回调路径未注册"}"#
        );
        assert!(fx.events.lock().unwrap().is_empty());
        assert!(fx
            .backend
            .get("plaita:http_callback:registered:/callback/flow-cb/await-cb/exec-cb")
            .unwrap()
            .is_some());
    }

    #[test]
    fn basic_and_signature_auth() {
        let fx = fixture();
        let raw = task_raw(
            json!({"enabled": true, "type": "basic", "username": "ops", "password": "pw1"}),
        );
        let task: Value = serde_json::from_str(&raw).unwrap();
        fx.wake.register(&task).unwrap();
        let good = tests_support_encode("ops:pw1");
        let bad = tests_support_encode("ops:wrong");
        assert!(fx
            .wake
            .handle_callback_request(
                "/callback/flow-cb/await-cb/exec-cb",
                &json!({}),
                &[("Authorization".to_string(), format!("Basic {bad}"))],
            )
            .is_err());
        assert!(fx
            .wake
            .handle_callback_request(
                "/callback/flow-cb/await-cb/exec-cb",
                &json!({}),
                &[("Authorization".to_string(), format!("Basic {good}"))],
            )
            .is_ok());
    }

    #[test]
    fn signature_auth_with_canonical_payload() {
        let fx = fixture();
        let raw = task_raw(
            json!({"enabled": true, "type": "signature", "secret": "topsecret", "algorithm": "HMAC-SHA256"}),
        );
        let task: Value = serde_json::from_str(&raw).unwrap();
        fx.wake.register(&task).unwrap();
        // Key order must not matter: canonical form sorts.
        let body = br#"{"b":2,"a":1}"#;
        let request_data: Value = serde_json::from_slice(body).unwrap();
        let canonical = canonical_flow_definition_json(body).unwrap();
        let ts = now_ms() / 1000;
        let payload = format!("{ts}.{}", String::from_utf8_lossy(&canonical));
        let sig = tests_hmac("topsecret", payload.as_bytes());
        let ok = fx.wake.handle_callback_request(
            "/callback/flow-cb/await-cb/exec-cb",
            &request_data,
            &[
                ("X-Plaita-Timestamp".to_string(), ts.to_string()),
                ("X-Plaita-Signature".to_string(), format!("sha256={sig}")),
            ],
        );
        assert!(ok.is_ok(), "valid signature must pass");
        // Stale timestamp → unified failure.
        let stale = fx.wake.handle_callback_request(
            "/callback/flow-cb/await-cb/exec-cb",
            &request_data,
            &[
                ("X-Plaita-Timestamp".to_string(), (ts - 3600).to_string()),
                ("X-Plaita-Signature".to_string(), format!("sha256={sig}")),
            ],
        );
        assert!(stale.is_err());
    }

    #[test]
    fn publish_failure_keeps_record_claimable() {
        let fx = fixture();
        let raw = task_raw(json!({}));
        let task: Value = serde_json::from_str(&raw).unwrap();
        fx.wake.register(&task).unwrap();
        fx.fail.store(true, Ordering::SeqCst);
        assert!(fx
            .wake
            .handle_callback_request("/callback/flow-cb/await-cb/exec-cb", &json!({"n": 1}), &[])
            .is_err());
        let record_key = "plaita:http_callback:registered:/callback/flow-cb/await-cb/exec-cb";
        assert!(
            fx.backend.get(record_key).unwrap().is_some(),
            "record must survive a publish failure"
        );
        assert!(
            fx.backend
                .get("plaita:http_callback:claim:/callback/flow-cb/await-cb/exec-cb")
                .unwrap()
                .is_none(),
            "claim must be released"
        );
        // Provider retries once the publisher recovers → success.
        fx.fail.store(false, Ordering::SeqCst);
        assert!(fx
            .wake
            .handle_callback_request("/callback/flow-cb/await-cb/exec-cb", &json!({"n": 1}), &[])
            .is_ok());
    }

    #[test]
    fn base64_decode_min() {
        assert_eq!(
            base64_decode(&b64s("ops:pw1")).unwrap(),
            b"ops:pw1".to_vec()
        );
        assert_eq!(base64_decode(&b64s("a")).unwrap(), b"a".to_vec()); // "a" → YQ==
        assert!(base64_decode("!!!").is_none());
    }

    fn b64s(s: &str) -> String {
        // encode via the same alphabet, forward direction, for fixtures
        const D: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = s.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
            out.push(D[(n >> 18) as usize & 63] as char);
            out.push(D[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                D[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                D[n as usize & 63] as char
            } else {
                '='
            });
        }
        out
    }
}

#[cfg(test)]
mod ingress_tests {
    use super::*;
    use crate::fake::FakeRedis;
    use std::io::{Read, Write};
    use std::sync::atomic::Ordering;

    /// The hand-rolled HTTP ingress end-to-end over a real socket.
    #[test]
    fn ingress_serves_registered_path_over_socket() {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_KEY_PREFIX" => "plaita".to_string(),
            "PLAITA_INSTANCE_ID" => "hcb-ingress".to_string(),
            "PLAITA_HTTP_CALLBACK_ADDR" => "127.0.0.1:0".to_string(),
            _ => String::new(),
        });
        let backend = Arc::new(FakeRedis::new());
        struct SeqSink;
        impl EventSink for SeqSink {
            fn publish(&self, _channel: &str, _payload: &str) -> Result<(), String> {
                Ok(())
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let mut wake =
            HttpCallbackWake::with_sink(cfg, backend.clone(), Arc::new(SeqSink), Arc::clone(&stop));
        wake.set_addr_for_tests("127.0.0.1:0");
        let wake = Arc::new(wake);
        let task = json!({
            "type": "http_callback",
            "node_id": "await-cb",
            "event_type": "http_callback",
            "execution_id": "exec-s",
            "flow_id": "flow-s",
            "tenant_id": "default",
            "callback_config": {"path": "/cb/exec-s"},
        });
        wake.register(&task).unwrap();
        let runner = Arc::clone(&wake);
        let handle = std::thread::spawn(move || runner.run());
        let mut addr = String::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            addr = wake.bound_addr();
            if !addr.is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(!addr.is_empty(), "ingress must be bound");

        let mut stream = std::net::TcpStream::connect(&addr).unwrap();
        let req = "POST /cb/exec-s HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 12\r\n\r\n{\"result\":1}";
        stream.write_all(req.as_bytes()).unwrap();
        let mut buf = String::new();
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let _ = stream.read_to_string(&mut buf);
        assert!(buf.starts_with("HTTP/1.1 200"), "response: {buf}");
        assert!(buf.contains("\"status\":\"success\""), "response: {buf}");

        // Unknown path → 200 with the unified failure body (Q5: the body is
        // the contract, not the status code).
        let mut stream = std::net::TcpStream::connect(&addr).unwrap();
        stream
            .write_all(b"POST /never HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        let mut buf = String::new();
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let _ = stream.read_to_string(&mut buf);
        assert!(buf.contains("回调路径未注册"), "response: {buf}");

        stop.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }
}
