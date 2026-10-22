//! Small helpers over generic JSON objects shared by the ir passes.

use serde_json::{Map, Value};

pub(crate) fn get_str(m: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(Value::String(s)) = m.get(*k) {
            return Some(s.clone());
        }
    }
    None
}

pub(crate) fn first_any<'a>(m: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    for k in keys {
        if let Some(v) = m.get(*k) {
            return Some(v);
        }
    }
    None
}

/// Removes the first present key among `keys` and returns its value.
pub(crate) fn take(m: &mut Map<String, Value>, keys: &[&str]) -> Option<Value> {
    let mut out = None;
    for k in keys {
        if let Some(v) = m.remove(*k) {
            if out.is_none() {
                out = Some(v);
            }
        }
    }
    out
}

pub(crate) fn is_object(v: &Value) -> bool {
    v.is_object()
}

/// JSON type name for the typed-field gate's "got" half (mirrors the Go
/// unmarshal / pydantic wording: must be X, got string).
pub(crate) fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub(crate) fn obj<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m)
}

/// Stashes `v` (stringified) under `key` in a metadata map unless present.
pub(crate) fn stash_into(meta: &mut Map<String, Value>, key: &str, v: Value) {
    if meta.contains_key(key) {
        return;
    }
    let s = match v {
        Value::String(s) => s,
        other => other.to_string(),
    };
    meta.insert(key.to_string(), Value::String(s));
}

pub(crate) fn next_is_empty(n: &Map<String, Value>) -> bool {
    match n.get("next") {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        Some(_) => false,
    }
}

pub(crate) fn as_float(v: &Value) -> Result<f64, String> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| "expected a number".to_string()),
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("expected a number, got {s:?}")),
        other => Err(format!("expected a number, got {other}")),
    }
}

pub(crate) fn node_label(n: &Map<String, Value>) -> String {
    if let Some(id) = get_str(n, &["id"]) {
        if !id.is_empty() {
            return id;
        }
    }
    get_str(n, &["type"]).unwrap_or_default()
}
