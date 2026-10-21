//! printf-style `%` string formatting — Python `%` operator semantics
//! (subset: the conversion types and flags plaita flows actually use).
//!
//! `%s`/`%r` render via PyStr/PyRepr, `%d/%i/%u` truncate to int, `%f` keeps
//! the default 6-decimal precision, `%x/%X/%o` are integer radix, `%e/%E/%g`
//! follow C float formats, `%%` is a literal percent. Positional arguments
//! come as a list (tuple) or a single value; `%(key)s` reads a mapping.

use serde_json::Value;

use crate::error::EvalError;
use crate::pyrepr::{py_repr, py_str};
use crate::value::{py_int, py_numeric, py_type_name};

struct Spec {
    left_align: bool,
    plus_sign: bool,
    space_sign: bool,
    zero_pad: bool,
    alternate: bool,
    width: Option<usize>,
    precision: Option<usize>,
    conv: char,
}

pub fn py_format(fmt: &str, args: &Value) -> Result<String, EvalError> {
    // Resolve the positional/mapping argument shape once.
    let tuple: Option<Vec<Value>> = match args {
        Value::Array(a) => Some(a.clone()),
        _ => None,
    };
    let mapping = match args {
        Value::Object(o) => Some(o.clone()),
        _ => None,
    };
    let positional: Vec<Value> = tuple.unwrap_or_else(|| vec![args.clone()]);
    let mut pos_index = 0usize;

    let chars: Vec<char> = fmt.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c != '%' {
            out.push(c);
            i += 1;
            continue;
        }
        i += 1;
        if i >= chars.len() {
            return Err(EvalError::value_error(
                "incomplete format at end of string",
            ));
        }
        // mapping key
        let mut key: Option<String> = None;
        if chars[i] == '(' {
            let mut depth = 1;
            let start = i + 1;
            i += 1;
            while i < chars.len() && depth > 0 {
                if chars[i] == '(' {
                    depth += 1;
                } else if chars[i] == ')' {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                i += 1;
            }
            if depth != 0 || i >= chars.len() {
                return Err(EvalError::value_error("incomplete format key"));
            }
            key = Some(chars[start..i].iter().collect());
            i += 1;
        }
        // flags
        let mut spec = Spec {
            left_align: false,
            plus_sign: false,
            space_sign: false,
            zero_pad: false,
            alternate: false,
            width: None,
            precision: None,
            conv: '%',
        };
        while i < chars.len() {
            match chars[i] {
                '-' => spec.left_align = true,
                '+' => spec.plus_sign = true,
                ' ' => spec.space_sign = true,
                '0' => spec.zero_pad = true,
                '#' => spec.alternate = true,
                _ => break,
            }
            i += 1;
        }
        // width
        let mut width = String::new();
        while i < chars.len() && chars[i].is_ascii_digit() {
            width.push(chars[i]);
            i += 1;
        }
        spec.width = width.parse().ok();
        // precision
        if i < chars.len() && chars[i] == '.' {
            i += 1;
            let mut prec = String::new();
            while i < chars.len() && chars[i].is_ascii_digit() {
                prec.push(chars[i]);
                i += 1;
            }
            spec.precision = prec.parse().ok();
        }
        // length modifiers (h/l/L) are accepted and ignored, like CPython
        while i < chars.len() && matches!(chars[i], 'h' | 'l' | 'L') {
            i += 1;
        }
        if i >= chars.len() {
            return Err(EvalError::value_error("incomplete format"));
        }
        spec.conv = chars[i];
        i += 1;

        let arg: Value = match (&key, &mapping) {
            (Some(k), Some(m)) => m
                .get(k)
                .cloned()
                .ok_or_else(|| EvalError::key_error(format!("KeyError: {k}")))?,
            (Some(k), None) => {
                return Err(EvalError::type_error(format!(
                    "format requires a mapping (got key {k:?})"
                )))
            }
            (None, _) => {
                if spec.conv == '%' {
                    Value::Null
                } else {
                    let v = positional.get(pos_index).cloned().ok_or_else(|| {
                        EvalError::type_error("not enough arguments for format string")
                    })?;
                    pos_index += 1;
                    v
                }
            }
        };

        let rendered = render_spec(&spec, &arg)?;
        if spec.conv == '%' {
            // '%%' contributes no argument; the render path emits '%'
        }
        out.push_str(&rendered);
    }
    if keyless_conversions_used(fmt) && pos_index < positional.len() {
        return Err(EvalError::type_error("not all arguments converted during string formatting"));
    }
    Ok(out)
}

fn keyless_conversions_used(fmt: &str) -> bool {
    // any %-conversion that is not a mapping key and not the literal %%
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '%' {
            match chars.get(i + 1) {
                Some('%') => i += 1,
                Some('(') => {}
                Some(_) => return true,
                None => return true,
            }
        }
        i += 1;
    }
    false
}

fn sign_prefix(spec: &Spec, negative: bool) -> String {
    if negative {
        "-".to_string()
    } else if spec.plus_sign {
        "+".to_string()
    } else if spec.space_sign {
        " ".to_string()
    } else {
        String::new()
    }
}

fn pad(spec: &Spec, sign: &str, digits: &str) -> String {
    let total = spec.width.unwrap_or(0);
    let body_len = sign.chars().count() + digits.chars().count();
    let fill = total.saturating_sub(body_len);
    if fill == 0 {
        return format!("{sign}{digits}");
    }
    if spec.left_align {
        return format!("{sign}{digits}{}", " ".repeat(fill));
    }
    if spec.zero_pad {
        return format!("{sign}{}{digits}", "0".repeat(fill));
    }
    format!("{}{sign}{digits}", " ".repeat(fill))
}

fn pad_str(spec: &Spec, s: &str) -> String {
    // precision on %s truncates BEFORE padding (Python semantics)
    let s = match spec.precision {
        Some(p) => s.chars().take(p).collect::<String>(),
        None => s.to_string(),
    };
    let total = spec.width.unwrap_or(0);
    let fill = total.saturating_sub(s.chars().count());
    if fill == 0 {
        return s;
    }
    if spec.left_align {
        format!("{s}{}", " ".repeat(fill))
    } else {
        format!("{}{s}", " ".repeat(fill))
    }
}

fn render_spec(spec: &Spec, arg: &Value) -> Result<String, EvalError> {
    match spec.conv {
        '%' => Ok("%".to_string()),
        's' => Ok(pad_str(spec, &py_str(arg))),
        'r' | 'a' => Ok(pad_str(spec, &py_repr(arg))),
        'c' => match arg {
            Value::String(s) if s.chars().count() == 1 => Ok(pad_str(spec, s)),
            v => {
                let code = py_int(v).ok_or_else(|| {
                    EvalError::type_error(format!("%c requires int or char, got {}", py_type_name(v)))
                })?;
                let ch = char::from_u32(code as u32)
                    .ok_or_else(|| EvalError::value_error("%c arg not in range(0x110000)"))?;
                Ok(pad_str(spec, &ch.to_string()))
            }
        },
        'd' | 'i' | 'u' => {
            let i = match arg {
                Value::Number(n) if n.is_f64() => {
                    let f = n.as_f64().unwrap();
                    if !f.is_finite() {
                        return Err(EvalError::type_error("integer argument expected, got float"));
                    }
                    f as i64
                }
                v => py_int(v).ok_or_else(|| {
                    EvalError::type_error(format!(
                        "%d format: a number is required, got {}",
                        py_type_name(v)
                    ))
                })?,
            };
            Ok(pad(spec, &sign_prefix(spec, i < 0), &i.abs().to_string()))
        }
        'f' | 'F' => {
            let f = py_numeric(arg).ok_or_else(|| {
                EvalError::type_error(format!(
                    "must be real number, not {}",
                    py_type_name(arg)
                ))
            })?;
            let prec = spec.precision.unwrap_or(6) as usize;
            let s = format!("{:.*}", prec, f.abs());
            Ok(pad(spec, &sign_prefix(spec, f.is_sign_negative()), &s))
        }
        'e' | 'E' => {
            let f = py_numeric(arg).ok_or_else(|| {
                EvalError::type_error(format!("must be real number, not {}", py_type_name(arg)))
            })?;
            let prec = spec.precision.unwrap_or(6) as usize;
            let s = format!("{:.*e}", prec, f.abs());
            let s = pythonize_exp(&s, spec.conv == 'E');
            Ok(pad(spec, &sign_prefix(spec, f.is_sign_negative()), &s))
        }
        'g' | 'G' => {
            let f = py_numeric(arg).ok_or_else(|| {
                EvalError::type_error(format!("must be real number, not {}", py_type_name(arg)))
            })?;
            let prec = spec.precision.unwrap_or(6).max(1) as usize;
            let s = format!("{:.*e}", prec - 1, f.abs());
            let (_mant, exp) = s.split_once('e').unwrap_or((s.as_str(), "0"));
            let exp_v: i32 = exp.parse().unwrap_or(0);
            let s = if exp_v < -4 || exp_v >= prec as i32 {
                let t = pythonize_exp(&s, spec.conv == 'G');
                let t = strip_frac_zeros(t, true);
                t
            } else {
                strip_frac_zeros(format!("{:.*}", prec.saturating_sub(1 + exp_v.max(0) as usize).max(0), f.abs()), false)
            };
            Ok(pad(spec, &sign_prefix(spec, f.is_sign_negative()), &s))
        }
        'x' | 'X' | 'o' | 'b' => {
            let i = py_int(arg).ok_or_else(|| {
                EvalError::type_error(format!("%{}/o requires an integer, got {}", spec.conv, py_type_name(arg)))
            })?;
            let digits = match spec.conv {
                'x' => format!("{:x}", i.unsigned_abs()),
                'X' => format!("{:X}", i.unsigned_abs()),
                'o' => format!("{:o}", i.unsigned_abs()),
                _ => format!("{:b}", i.unsigned_abs()),
            };
            let digits = if spec.alternate {
                match spec.conv {
                    'x' => format!("0x{digits}"),
                    'X' => format!("0X{digits}"),
                    'o' => format!("0o{digits}"),
                    _ => format!("0b{digits}"),
                }
            } else {
                digits
            };
            Ok(pad(spec, &sign_prefix(spec, i < 0), &digits))
        }
        other => Err(EvalError::value_error(format!(
            "unsupported format character {other:?}"
        ))),
    }
}

fn pythonize_exp(s: &str, upper: bool) -> String {
    // Rust "1.5e3" → Python "1.500000e+03"
    let (mant, exp) = match s.split_once(['e', 'E']) {
        Some(x) => x,
        None => return s.to_string(),
    };
    let ev: i32 = exp.parse().unwrap_or(0);
    let m = if upper { mant.to_uppercase() } else { mant.to_string() };
    format!("{m}e{}{:02}", if ev < 0 { '-' } else { '+' }, ev.abs())
}

fn strip_frac_zeros(s: String, sci: bool) -> String {
    if sci {
        // "1.500000e+03" → "1.5e+03"
        if let Some((mant, _exp)) = s.split_once(['e', 'E']) {
            let mant = mant.trim_end_matches('0').trim_end_matches('.').to_string();
            let e = &s[mant.len() + if mant.is_empty() { 1 } else { 0 }..];
            let _ = e;
            let idx = s.find(['e', 'E']).unwrap();
            return format!("{}{}", mant, &s[idx..]);
        }
        s
    } else if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::py_float;
    use serde_json::json;

    fn fmt(f: &str, a: Value) -> String {
        py_format(f, &a).unwrap()
    }

    #[test]
    fn basic() {
        assert_eq!(fmt("%s", json!("x")), "x");
        assert_eq!(fmt("%s", json!(3.0)), "3.0");
        assert_eq!(fmt("%r", json!("x")), "'x'");
        assert_eq!(fmt("%d", json!(42)), "42");
        assert_eq!(fmt("%d", json!(3.9)), "3");
        assert_eq!(fmt("%d", json!(-3.9)), "-3");
        assert_eq!(fmt("%5d", json!(42)), "   42");
        assert_eq!(fmt("%-5d|", json!(42)), "42   |");
        assert_eq!(fmt("%05d", json!(-42)), "-0042");
        assert_eq!(fmt("%+d", json!(42)), "+42");
        assert_eq!(fmt("%x", json!(255)), "ff");
        assert_eq!(fmt("%#x", json!(255)), "0xff");
        assert_eq!(fmt("%o", json!(8)), "10");
        assert_eq!(fmt("%.3f", json!(3.14159)), "3.142");
        assert_eq!(fmt("%f", json!(3)), "3.000000");
        assert_eq!(fmt("%.2e", json!(12345.6789)), "1.23e+04");
        assert_eq!(fmt("%s=%d", json!(["x", 5])), "x=5");
        assert_eq!(fmt("%(name)s is %(age)d", json!({"name": "k", "age": 3})), "k is 3");
        assert_eq!(fmt("100%%", json!(null)), "100%");
        assert_eq!(fmt("%.2s", json!("hello")), "he");
        assert_eq!(fmt("%10s|", json!("ab")), "        ab|");
    }

    #[test]
    fn errors() {
        assert!(py_format("%d", &json!("x")).is_err());
        assert!(py_format("%s %s", &json!(["a"])).is_err());
        assert!(py_format("%(k)s", &json!(["a"])).is_err());
        assert!(py_format("abc%", &json!(null)).is_err());
    }

    #[test]
    fn float_type_name() {
        assert_eq!(py_float(&json!(1.5)), Some(1.5));
    }
}
