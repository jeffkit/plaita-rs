//! Python `str()` / `repr()` rendering for the JSON value domain.
//!
//! Template interpolation is `str(value)` and `$F.concat` is
//! `"".join(str(arg) ...)`, so every textual rendering of a structured value
//! goes through here to stay byte-compatible with CPython: `None`, `True`,
//! float repr (`3.0`, `1e+16`), single-quoted string repr, and dict/list repr
//! in insertion order (the Go engine had to sort keys; serde_json's
//! `preserve_order` lets this port keep Python's order).

use serde_json::Value;

use crate::value::{py_float, py_int};

/// Python `str(v)`.
pub fn py_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        _ => py_repr(v),
    }
}

/// Python `repr(v)`.
pub fn py_repr(v: &Value) -> String {
    match v {
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Number(_) => {
            if let Some(i) = py_int(v) {
                i.to_string()
            } else if let Some(f) = py_float(v) {
                py_float_repr(f)
            } else {
                // u64 beyond i64: render the raw JSON digits.
                v.to_string()
            }
        }
        Value::String(s) => py_str_quote(s),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(py_repr).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", py_str_quote(k), py_repr(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

/// Python string repr quoting: prefer single quotes, switch to double quotes
/// when the text contains `'` but no `"`, escape backslashes, the chosen
/// quote and control characters.
pub fn py_str_quote(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') { '"' } else { '\'' };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || ((c as u32) >= 0x7f && (c as u32) < 0xa0) => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// Python `repr(float)`: shortest round-trip digits; positional notation with
/// a guaranteed `.0` tail when the decimal exponent is in (-4, 16],
/// scientific (`1e-05`, `1e+16`) otherwise; `inf`/`-inf`/`nan` spellings.
pub fn py_float_repr(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf".to_string() } else { "-inf".to_string() };
    }
    let neg = if f.is_sign_negative() { "-" } else { "" };
    let a = f.abs();
    if a == 0.0 {
        return format!("{neg}0.0");
    }
    // Rust's LowerExp uses the shortest round-trip digit string, the same
    // digit selection as CPython's repr.
    let sci = format!("{a:e}"); // "d[.ddd]e[-]x"
    let (mant, exp) = sci.split_once('e').expect("LowerExp always has an exponent");
    let exp: i32 = exp.parse().expect("exponent is an integer");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let decpt = exp + 1; // value = 0.<digits> * 10^decpt

    if decpt <= -4 || decpt > 16 {
        let mut out = String::new();
        out.push_str(neg);
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let e = decpt - 1;
        out.push('e');
        out.push(if e < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", e.abs()));
        return out;
    }
    if decpt <= 0 {
        return format!("{neg}0.{}{}", "0".repeat((-decpt) as usize), digits);
    }
    let decpt = decpt as usize;
    if decpt >= digits.len() {
        return format!("{neg}{}{}.0", digits, "0".repeat(decpt - digits.len()));
    }
    format!("{neg}{}.{}", &digits[..decpt], &digits[decpt..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn float_repr_matches_python() {
        let cases = [
            (3.0, "3.0"),
            (0.1, "0.1"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1e16, "1e+16"),
            (1234567890123456.0, "1234567890123456.0"),
            (-2.5, "-2.5"),
            (100.0, "100.0"),
            (1.5e300, "1.5e+300"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (123456.789, "123456.789"),
        ];
        for (f, want) in cases {
            assert_eq!(py_float_repr(f), want, "{f}");
        }
        assert_eq!(py_float_repr(f64::NAN), "nan");
    }

    #[test]
    fn str_and_repr() {
        assert_eq!(py_str(&json!(null)), "None");
        assert_eq!(py_str(&json!(true)), "True");
        assert_eq!(py_str(&json!(3)), "3");
        assert_eq!(py_str(&json!(3.0)), "3.0");
        assert_eq!(py_str(&json!("x")), "x");
        assert_eq!(py_repr(&json!("x")), "'x'");
        assert_eq!(py_repr(&json!("it's")), "\"it's\"");
        assert_eq!(py_repr(&json!("a'b\"c")), "'a\\'b\"c'");
        assert_eq!(py_str(&json!([1, "a", null])), "[1, 'a', None]");
        assert_eq!(py_str(&json!({"b": 2, "a": [1.0]})), "{'b': 2, 'a': [1.0]}");
        assert_eq!(py_repr(&json!("tab\tnl\n")), "'tab\\tnl\\n'");
    }
}
