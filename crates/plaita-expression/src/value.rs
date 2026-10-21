//! Python value semantics over the JSON value domain.
//!
//! plaita expressions evaluate over JSON data (`None`, `bool`, `int`, `float`,
//! `str`, `list`, `dict`). This module is the single home of the Python rules
//! the rest of the crate relies on: the numeric domain (`bool` is an `int`
//! subclass, `int` and `float` compare cross-type, `"1" == 1` is `False`),
//! truthiness, `==`, ordering and containment.

use serde_json::{Map, Value};

use crate::error::EvalError;

/// JSON object type used throughout (insertion-ordered thanks to
/// serde_json's `preserve_order` feature — Python dict order semantics).
pub type Object = Map<String, Value>;

/// Python `int` view of a value: integers (i64/u64 numbers) and bools
/// (`True == 1`). Floats, even integral ones, are NOT ints.
pub fn py_int(v: &Value) -> Option<i64> {
    match v {
        Value::Bool(b) => Some(i64::from(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Python `float` view of a value: only f64 numbers.
pub fn py_float(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) if n.is_f64() => n.as_f64(),
        _ => None,
    }
}

/// Python numeric view (int or float, bool included) as f64.
pub fn py_numeric(v: &Value) -> Option<f64> {
    if let Some(i) = py_int(v) {
        return Some(i as f64);
    }
    py_float(v)
}

/// Builds a float value, refusing the non-finite results JSON cannot carry
/// (Python raises `OverflowError` for e.g. `10.0 ** 400`).
pub fn float_value(f: f64) -> Result<Value, EvalError> {
    if !f.is_finite() {
        return Err(EvalError::overflow(format!(
            "float result {} is not representable",
            crate::pyrepr::py_float_repr(f)
        )));
    }
    Ok(Value::from(f))
}

/// Python `bool(v)` for the JSON value domain.
pub fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Python type name of a value (for TypeError messages).
pub fn py_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) => {
            if n.is_f64() {
                "float"
            } else {
                "int"
            }
        }
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// Python `==`: `None == None`, cross-type numeric equality (`1 == 1.0`,
/// `True == 1`), exact string equality, element-wise lists, key/value dicts.
/// Unrelated types are simply not equal (Python `==` never raises here).
pub fn equal(l: &Value, r: &Value) -> bool {
    if l.is_null() || r.is_null() {
        return l.is_null() && r.is_null();
    }
    if let (Some(a), Some(b)) = (py_numeric(l), py_numeric(r)) {
        return a == b;
    }
    match (l, r) {
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| equal(x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a
                    .iter()
                    .all(|(k, v)| b.get(k).map(|w| equal(v, w)).unwrap_or(false))
        }
        _ => false,
    }
}

/// Python ordering (`<`, `>`, ...): numbers against numbers (bool included),
/// strings against strings; anything else is a `TypeError`.
pub fn compare(op: &str, l: &Value, r: &Value) -> Result<std::cmp::Ordering, EvalError> {
    if let (Some(a), Some(b)) = (py_numeric(l), py_numeric(r)) {
        return Ok(a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Value::String(a), Value::String(b)) = (l, r) {
        return Ok(a.cmp(b));
    }
    Err(EvalError::type_error(format!(
        "{}: unsupported operand types: '{}' and '{}'",
        op,
        py_type_name(l),
        py_type_name(r)
    )))
}

/// Python `needle in container`: substring on str (needle must be str),
/// `==` membership on list, key test on dict; other containers are a
/// `TypeError`.
pub fn contains(op: &str, container: &Value, needle: &Value) -> Result<bool, EvalError> {
    match container {
        Value::Null => Err(EvalError::type_error(format!(
            "{op}: argument of type 'NoneType' is not a container"
        ))),
        Value::String(s) => match needle {
            Value::String(n) => Ok(s.contains(n.as_str())),
            other => Err(EvalError::type_error(format!(
                "{op}: 'in <string>' requires string as left operand, got {}",
                py_type_name(other)
            ))),
        },
        Value::Array(items) => Ok(items.iter().any(|v| equal(v, needle))),
        Value::Object(o) => match needle {
            Value::String(k) => Ok(o.contains_key(k)),
            _ => Ok(false),
        },
        other => Err(EvalError::type_error(format!(
            "{op}: unsupported container type {} (string, list or dict expected)",
            py_type_name(other)
        ))),
    }
}

/// Coerces an index-like argument the way Python requires integers
/// ("object cannot be interpreted as an integer"). Integral floats are
/// accepted because JSON transports may lose the int/float distinction.
pub fn py_index_arg(fn_name: &str, v: &Value) -> Result<i64, EvalError> {
    if let Some(i) = py_int(v) {
        return Ok(i);
    }
    if let Some(f) = py_float(v) {
        if f.fract() == 0.0 && f.is_finite() {
            return Ok(f as i64);
        }
        return Err(EvalError::type_error(format!(
            "$F.{fn_name}: 'float' object cannot be interpreted as an integer"
        )));
    }
    Err(EvalError::type_error(format!(
        "$F.{fn_name}: '{}' object cannot be interpreted as an integer",
        py_type_name(v)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numeric_domain() {
        assert_eq!(py_int(&json!(3)), Some(3));
        assert_eq!(py_int(&json!(true)), Some(1));
        assert_eq!(py_int(&json!(3.0)), None);
        assert_eq!(py_float(&json!(3.0)), Some(3.0));
        assert_eq!(py_numeric(&json!("3")), None);
    }

    #[test]
    fn truthiness() {
        for falsy in [json!(null), json!(false), json!(0), json!(0.0), json!(""), json!([]), json!({})] {
            assert!(!is_truthy(&falsy), "{falsy}");
        }
        for truthy in [json!(true), json!(1), json!(-0.5), json!("x"), json!([0]), json!({"a": null})] {
            assert!(is_truthy(&truthy), "{truthy}");
        }
    }

    #[test]
    fn python_equality() {
        assert!(equal(&json!(1), &json!(1.0)));
        assert!(equal(&json!(true), &json!(1)));
        assert!(!equal(&json!("1"), &json!(1)));
        assert!(equal(&json!([1, "a"]), &json!([1.0, "a"])));
        assert!(equal(&json!({"a": [1]}), &json!({"a": [1.0]})));
        assert!(!equal(&json!({"a": 1}), &json!({"a": 1, "b": 2})));
        assert!(equal(&json!(null), &json!(null)));
        assert!(!equal(&json!(null), &json!(0)));
    }

    #[test]
    fn containment() {
        assert!(contains("in", &json!("hello"), &json!("ell")).unwrap());
        assert!(contains("in", &json!([1, 2]), &json!(2.0)).unwrap());
        assert!(contains("in", &json!({"k": 1}), &json!("k")).unwrap());
        assert!(!contains("in", &json!({"k": 1}), &json!(1)).unwrap());
        assert!(contains("in", &json!(5), &json!(1)).is_err());
        assert!(contains("in", &json!("abc"), &json!(1)).is_err());
    }
}
