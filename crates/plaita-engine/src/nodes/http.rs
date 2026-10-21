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
//! that rely on them), response byte caps.

use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::context::SharedCtx;
use crate::error::NodeError;
use crate::model::Node;
use crate::nodes::StepOutcome;

const DEFAULT_CONTENT_TYPE: &str = "application/json";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_CTX_KEY: &str = "RESPONSE";

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

    let mut req = ureq::request(method, parsed.as_str()).timeout(next_timeout(node));
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

    let body_bytes = response.into_string().map_err(|e| {
        NodeError::transient_with_code(
            format!("http node: read body: {e}"),
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

    #[test]
    fn query_rendering() {
        assert_eq!(
            render_query(&json!({"a": "1", "b": "x y", "c": null})).unwrap(),
            "a=1&b=x+y&c="
        );
        assert_eq!(render_query(&json!("a=1")).unwrap(), "a=1");
        assert!(render_query(&json!(5)).is_none());
    }
}
