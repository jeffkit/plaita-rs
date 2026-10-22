//! The `http` node — the pragmatic core of plaita-go's http.go: expression
//! URL/query/headers/body, a per-request timeout, JSON-lenient response
//! parsing and the plaita response contract
//! `{data, status, statusText, headers}` as the node result (the response
//! structure is also staged in the current context as `RESPONSE`).
//!
//! Error classification follows plaita-go: request-generation failures (bad
//! URL/parameters) are deterministic (code 1001, never retried); transport
//! failures (sent, no response) are transient (code 1002). Non-2xx statuses
//! are NOT errors — the response structure flows downstream and the flow
//! decides (plaita behaviour).
//!
//! Not ported (documented): delegate/addressing (internal infrastructure,
//! removed from plaita-go too), manual hop-by-hop redirect policy (ureq
//! follows redirects), SSRF operator switches (the loader rejects flows
//! that rely on them).

use std::io::Read;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use crate::context::SharedCtx;
use crate::error::NodeError;
use crate::model::Node;
use crate::nodes::StepOutcome;

const DEFAULT_CONTENT_TYPE: &str = "application/json";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_CTX_KEY: &str = "RESPONSE";

/// 响应体字节上限的运营旋钮（plaita#31 对齐，B4 r8）：Python
/// `node/http.py::HTTP_MAX_RESPONSE_BYTES_ENV` 同名同义。
pub(crate) const HTTP_MAX_RESPONSE_BYTES_ENV: &str = "PLAITA_HTTP_MAX_RESPONSE_BYTES";

/// 默认帽 10MiB（Python `node/http.py::DEFAULT_MAX_RESPONSE_BYTES`）。
pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

/// Parses [`HTTP_MAX_RESPONSE_BYTES_ENV`]: unset → the default (silently,
/// Python's `name not in env` early return). Anything present must be a
/// strict integer ≥ 1 (strip-then `[+-]?\d+`: ` 2048 `/`+4096` pass;
/// `` /`abc`/`1.5`/`1e3`/`0`/`-5` are invalid). `Err` carries the warning
/// text — the caller prints it and falls back to the default (Python
/// `optional_int_env` logs a WARNING with the same meaning, never silent).
pub(crate) fn parse_max_response_bytes(raw: Option<&str>) -> Result<usize, String> {
    fn invalid(raw: &str) -> String {
        format!(
            "{HTTP_MAX_RESPONSE_BYTES_ENV}={raw:?} is not a valid integer (>=1); falling back \
             to the default response cap of {DEFAULT_MAX_RESPONSE_BYTES} bytes (NOT silently \
             unlimited)"
        )
    }
    let Some(raw) = raw else {
        return Ok(DEFAULT_MAX_RESPONSE_BYTES);
    };
    // Python parse_int: strict `[+-]?\d+` on the stripped text; i128 parse
    // accepts exactly that shape (sign + ascii digits) and rejects
    // `1.5`/`1e3`/`0x10`/blank the same way.
    match raw.trim().parse::<i128>() {
        // Above u64 the cap is beyond any readable body; Rust cannot hold
        // Python's unbounded int — warning + default keeps this observable
        // and fail-closed (deliberate divergence on that extreme edge).
        Ok(v) if v >= 1 => usize::try_from(v).map_err(|_| invalid(raw)),
        _ => Err(invalid(raw)),
    }
}

/// Resolves the effective response cap for one http-node execution, warning
/// on stderr when the env value is invalid (the Rust dialect of Python's
/// `logger.warning` in `optional_int_env`).
pub(crate) fn http_max_response_bytes() -> usize {
    let raw = std::env::var(HTTP_MAX_RESPONSE_BYTES_ENV).ok();
    match parse_max_response_bytes(raw.as_deref()) {
        Ok(cap) => cap,
        Err(warning) => {
            eprintln!("[http] {warning}");
            DEFAULT_MAX_RESPONSE_BYTES
        }
    }
}

#[derive(Debug)]
enum BodyReadError {
    /// Over the configured cap — the named, knobby error (Python
    /// `ResponseTooLargeError`).
    TooLarge {
        cap: usize,
    },
    Io(std::io::Error),
    NotUtf8(std::str::Utf8Error),
}

/// Streams at most `cap + 1` bytes off the response body; `len > cap`
/// surfaces as [`BodyReadError::TooLarge`] (mirrors Python
/// `_read_capped_requests_body`: 64KiB-chunk counting would only slow this
/// down — `Read::take` bounds the memory the same way).
fn read_capped_body(mut reader: impl Read, cap: usize) -> Result<String, BodyReadError> {
    let mut buf = Vec::new();
    reader
        .by_ref()
        .take(cap.saturating_add(1) as u64)
        .read_to_end(&mut buf)
        .map_err(BodyReadError::Io)?;
    if buf.len() > cap {
        return Err(BodyReadError::TooLarge { cap });
    }
    String::from_utf8(buf).map_err(|e| BodyReadError::NotUtf8(e.utf8_error()))
}

#[allow(clippy::too_many_arguments)] // mirrors the flat node field shape
pub(crate) fn execute_http(
    node: &Node,
    method: &str,
    content_type: &str,
    url: &str,
    query: Option<&Value>,
    headers: Option<&Value>,
    body: Option<&Value>,
    rc: &SharedCtx,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    // -- request generation (deterministic errors) ------------------------
    let url_text = eval_string_field(node, "url", url, rc)?;
    let method = if method.is_empty() { "GET" } else { method };
    let content_type = if content_type.is_empty() {
        DEFAULT_CONTENT_TYPE
    } else {
        content_type
    };

    let mut req_url = url_text.clone();
    if let Some(q) = query {
        if !q.is_null() {
            let rendered = eval_value(node, "query", q, rc)?;
            let qs = render_query(&rendered)
                .ok_or_else(|| gen_err(node, "query must be an object or a string"))?;
            if !qs.is_empty() {
                let sep = if req_url.contains('?') { '&' } else { '?' };
                req_url = format!("{req_url}{sep}{qs}");
            }
        }
    }
    let parsed: url::Url = req_url
        .parse()
        .map_err(|e| gen_err(node, &format!("invalid url {req_url:?}: {e}")))?;

    // budget = the smaller of the node timeout and the node/flow deadline
    // (mirrors execute_code: the flow budget must bound the request, never
    // be widened by it)
    let budget = request_budget(node, deadline);
    if budget.is_zero() {
        return Err(NodeError::timeout(format!(
            "http node {:?} timed out before send (node/flow deadline already passed)",
            node.id
        )));
    }
    let mut req = ureq::request(method, parsed.as_str()).timeout(budget);
    req = req.set("Content-Type", content_type);
    if let Some(hs) = headers {
        if !hs.is_null() {
            let rendered = eval_value(node, "headers", hs, rc)?;
            let Value::Object(map) = rendered else {
                return Err(gen_err(node, "headers must be an object"));
            };
            for (k, v) in &map {
                let val = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                req = req.set(k, &val);
            }
        }
    }

    // -- send (transport errors are transient) ----------------------------
    let payload: Option<String> = if matches!(method, "GET" | "HEAD") {
        None
    } else {
        let rendered = match body {
            Some(b) if !b.is_null() => eval_value(node, "body", b, rc)?,
            _ => Value::Null,
        };
        match &rendered {
            Value::String(s) => Some(s.clone()),
            Value::Null => Some(String::new()),
            other => Some(
                serde_json::to_string(other)
                    .map_err(|e| gen_err(node, &format!("body encode: {e}")))?,
            ),
        }
    };

    let started = Instant::now();
    let send_result = match payload {
        None => req.call(),
        Some(p) => req.send_string(&p),
    };
    let response = match send_result {
        Ok(r) => r,
        Err(ureq::Error::Status(_code, resp)) => {
            // non-2xx is NOT an error: the response structure flows downstream
            resp
        }
        Err(e) => {
            // a transport failure past the budget is the deadline firing
            // (plaita-go surfaces ctx.Err() the same way), not a network
            // hiccup — classify as Timeout so timeoutHandler can catch it
            if started.elapsed() >= budget {
                return Err(NodeError::timeout(format!(
                    "http node: request timed out (node/flow deadline exceeded): {e}"
                )));
            }
            return Err(NodeError {
                message: format!("http node: {e}"),
                class: crate::error::ErrorClass::Transient,
                code: crate::error::NODE_DO_REQUEST_ERROR_CODE,
            });
        }
    };

    let status = response.status();
    let status_text = response.status_text().trim().to_string();
    let mut header_map = Map::new();
    for name in response.headers_names() {
        if let Some(v) = response.header(&name) {
            header_map.insert(name, Value::String(v.to_string()));
        }
    }
    let content_type_resp = response
        .header("content-type")
        .unwrap_or("")
        .to_ascii_lowercase();

    let body_bytes =
        read_capped_body(response.into_reader(), http_max_response_bytes()).map_err(|e| {
            let msg = match e {
                // Python ResponseTooLargeError 同文：点名帽值与提升路径
                BodyReadError::TooLarge { cap } => format!(
                    "HTTP response body exceeds maxResponseBytes={cap}; \
                     raise {HTTP_MAX_RESPONSE_BYTES_ENV} to allow larger bodies"
                ),
                // ureq 2.x `into_string` 对 IO/非 UTF-8 的同族旧方言
                BodyReadError::Io(e) => format!("read body: {e}"),
                BodyReadError::NotUtf8(e) => format!("read body: {e}"),
            };
            NodeError::transient_with_code(
                format!("http node: {msg}"),
                crate::error::NODE_EXEC_ERROR_CODE,
            )
        })?;
    let data: Value = if content_type_resp.contains("json") {
        serde_json::from_str::<Value>(&body_bytes).unwrap_or(Value::String(body_bytes.clone()))
    } else {
        Value::String(body_bytes)
    };

    let response_value = json!({
        "data": data,
        "status": status,
        "statusText": status_text,
        "headers": Value::Object(header_map),
    });
    rc.set_current(RESPONSE_CTX_KEY, response_value.clone());

    // Logical result: raw parsed body by default, evaluated output when the
    // author set one (plaita _build_response_result).
    let logical = match &node.output {
        None => response_value,
        Some(out) => eval_value(node, "output", out, rc)?,
    };
    crate::nodes::validate_output_pub(node, "response body", &logical)?;
    Ok(StepOutcome {
        result: logical,
        next: node.next.clone(),
    })
}

fn eval_value(node: &Node, what: &str, v: &Value, rc: &SharedCtx) -> Result<Value, NodeError> {
    plaita_expression::eval_value(v, rc.as_ref())
        .map_err(|e| gen_err(node, &format!("failed to eval {what}: {e}")))
}

fn eval_string_field(
    node: &Node,
    what: &str,
    v: &str,
    rc: &SharedCtx,
) -> Result<String, NodeError> {
    let rendered = eval_value(node, what, &Value::String(v.to_string()), rc)?;
    match rendered {
        Value::String(s) => Ok(s),
        other => Ok(plaita_expression::py_str(&other)),
    }
}

fn gen_err(node: &Node, msg: &str) -> NodeError {
    NodeError::deterministic(format!("http node {:?}: {msg}", node.id))
}

fn next_timeout(node: &Node) -> Duration {
    node.timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT)
}

/// The whole-request budget: the node timeout (30s when unset), tightened
/// by the node/flow deadline when one is in force. Never widened by the
/// deadline — a deadline can only shorten the request budget.
fn request_budget(node: &Node, deadline: Option<Instant>) -> Duration {
    let mut budget = next_timeout(node);
    if let Some(dl) = deadline {
        budget = budget.min(dl.saturating_duration_since(Instant::now()));
    }
    budget
}

/// Renders a query object as `k=v&k2=v2` (percent-encoding via url crate);
/// a plain string passes through.
fn render_query(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Object(map) => {
            let mut parts = Vec::new();
            for (k, val) in map {
                let value_text = match val {
                    Value::String(s) => s.clone(),
                    Value::Null => String::new(),
                    other => other.to_string(),
                };
                let mut serializer = form_urlencoded::Serializer::new(String::new());
                serializer.append_pair(k, &value_text);
                parts.push(serializer.finish());
            }
            Some(parts.join("&"))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeKind;

    /// Python `optional_int_env(name, 10MiB, minimum=1)` 的值域口径：
    /// 未设静默回默认；strip 后 `[+-]?\d+` 严格整数、<1 非法；非法=Err
    /// 警告文本（调用方 stderr 告警+回默认，绝不静默）。
    #[test]
    fn max_response_bytes_env_parsing() {
        assert_eq!(
            parse_max_response_bytes(None),
            Ok(DEFAULT_MAX_RESPONSE_BYTES)
        );
        assert_eq!(parse_max_response_bytes(Some("1024")), Ok(1024));
        assert_eq!(parse_max_response_bytes(Some(" 2048 ")), Ok(2048));
        assert_eq!(parse_max_response_bytes(Some("+4096")), Ok(4096));
        assert_eq!(
            parse_max_response_bytes(Some("10485760")),
            Ok(DEFAULT_MAX_RESPONSE_BYTES)
        );
        for bad in ["abc", "", "  ", "0", "-5", "1.5", "1e3", "0x10", "１０"] {
            let got = parse_max_response_bytes(Some(bad));
            assert!(got.is_err(), "{bad:?} must be rejected");
            assert!(
                got.unwrap_err().contains("NOT silently unlimited"),
                "{bad:?}"
            );
        }
        // 超出 u64 的帽值：Rust 存不下 Python 的无界 int——fail-closed 回默认
        let huge = "18446744073709551616"; // u64::MAX + 1
        assert!(parse_max_response_bytes(Some(huge)).is_err(), "{huge}");
        // i64 上限内的合法大值照常通过
        assert_eq!(
            parse_max_response_bytes(Some("9223372036854775807")),
            Ok(usize::try_from(9223372036854775807i64).unwrap())
        );
    }

    /// 边界=`total > max_bytes` 才拒（Python 同口径）：恰在帽上放行，
    /// 帽+1 具名拒；非 UTF-8 维持 ureq `into_string` 的旧拒面。
    #[test]
    fn capped_body_boundary_is_greater_than_cap() {
        let body = "x".repeat(2048);
        assert_eq!(
            read_capped_body(body.as_bytes(), 2048).unwrap(),
            body,
            "exactly at the cap must pass"
        );
        match read_capped_body(body.as_bytes(), 1024) {
            Err(BodyReadError::TooLarge { cap }) => assert_eq!(cap, 1024),
            other => panic!("expected TooLarge, got {other:?}"),
        }
        match read_capped_body(&[0xffu8, 0xfe][..], 16) {
            Err(BodyReadError::NotUtf8(_)) => {}
            other => panic!("expected NotUtf8, got {other:?}"),
        }
        assert_eq!(read_capped_body(&b"ok"[..], 16).unwrap(), "ok");
    }

    #[test]
    fn query_rendering() {
        assert_eq!(
            render_query(&json!({"a": "1", "b": "x y", "c": null})).unwrap(),
            "a=1&b=x+y&c="
        );
        assert_eq!(render_query(&json!("a=1")).unwrap(), "a=1");
        assert!(render_query(&json!(5)).is_none());
    }

    #[test]
    fn budget_is_node_timeout_tightened_by_deadline() {
        let dl_in_1s = Instant::now() + Duration::from_secs(1);
        // unset node timeout keeps the 30s default when no deadline is in force
        assert_eq!(
            request_budget(&node_with_timeout(None), None),
            Duration::from_secs(30)
        );
        // a deadline only ever tightens, never widens (wall-clock tolerant:
        // the remaining time shrinks while the assertion runs)
        let in_window = |b: Duration, lo: u64, hi: u64| {
            assert!(
                b >= Duration::from_millis(lo) && b <= Duration::from_millis(hi),
                "budget {b:?} outside [{lo},{hi}] ms"
            );
        };
        in_window(
            request_budget(&node_with_timeout(None), Some(dl_in_1s)),
            900,
            1000,
        );
        in_window(
            request_budget(
                &node_with_timeout(Some(Duration::from_secs(10))),
                Some(dl_in_1s),
            ),
            900,
            1000,
        );
        assert_eq!(
            request_budget(&node_with_timeout(Some(Duration::from_secs(10))), None),
            Duration::from_secs(10)
        );
        // an already-passed deadline yields the zero-budget gate
        let past = Instant::now() - Duration::from_secs(1);
        assert_eq!(
            request_budget(&node_with_timeout(None), Some(past)),
            Duration::ZERO
        );
    }

    fn node_with_timeout(t: Option<Duration>) -> Node {
        Node {
            id: "h".to_string(),
            node_type: "http".to_string(),
            name: String::new(),
            desc: String::new(),
            output: None,
            timeout: t,
            error_policy: Default::default(),
            timeout_policy: Default::default(),
            next: None,
            output_type: None,
            metadata: Default::default(),
            kind: NodeKind::Http {
                method: "GET".to_string(),
                content_type: String::new(),
                url: "http://127.0.0.1:9/x".to_string(),
                query: None,
                headers: None,
                body: None,
            },
        }
    }
}
