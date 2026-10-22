//! The `$F` function registry — Python operator/builtin semantics ported
//! from plaita `plaita/core/expression.py` via plaita-go's function library.
//!
//! Rules that hold crate-wide:
//! - numeric domain follows Python (`bool` is an `int` subclass, int×int
//!   stays int, any float operand makes the result float, `/` is true
//!   division, `%` follows the divisor's sign);
//! - pure list/dict functions return NEW containers and never alias their
//!   input (plaita#69 copy discipline: mutating a fresh result must not
//!   write through to the context);
//! - mutators (`set`/`delete`/`clear`/`pop`/...) operate on copies and
//!   return `None`;
//! - missing/None arguments become the parameter's zero value (mirrors Go's
//!   reflect zero-value bridge for plaita passing `None` into functions).

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::Value;

use crate::error::EvalError;
use crate::pyfmt::py_format;
use crate::pyrepr::{py_repr, py_str};
use crate::strftime::py_strftime;
use crate::value::{
    contains as py_contains, equal, float_value, py_float, py_index_arg, py_int, py_numeric,
    py_type_name, Object,
};

pub type Func = fn(&[Value]) -> Result<Value, EvalError>;

// ---------------------------------------------------------------------------
// argument helpers
// ---------------------------------------------------------------------------

fn type_err(fn_name: &str, expected: &str, v: &Value) -> EvalError {
    EvalError::type_error(format!(
        "$F.{fn_name}: argument must be {expected}, got '{}'",
        py_type_name(v)
    ))
}

fn arity(_fn_name: &str, args: &[Value], n: usize) -> Result<(), EvalError> {
    if args.len() != n {
        return Err(EvalError::type_error(format!(
            "function expects {n} arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

fn arity_min(_fn_name: &str, args: &[Value], min: usize) -> Result<(), EvalError> {
    if args.len() < min {
        return Err(EvalError::type_error(format!(
            "function expects at least {min} arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

/// A str argument; `None` becomes `""` (zero value).
fn str_arg(fn_name: &str, args: &[Value], i: usize) -> Result<String, EvalError> {
    match &args[i] {
        Value::Null => Ok(String::new()),
        Value::String(s) => Ok(s.clone()),
        v => Err(type_err(fn_name, "str", v)),
    }
}

/// A list argument; `None` becomes an empty list (zero value).
fn list_arg(fn_name: &str, args: &[Value], i: usize) -> Result<Vec<Value>, EvalError> {
    match &args[i] {
        Value::Null => Ok(Vec::new()),
        Value::Array(a) => Ok(a.clone()),
        v => Err(type_err(fn_name, "list", v)),
    }
}

/// A dict argument; `None` becomes `None` (nil map) — callers decide whether
/// that is an error (`$F.set`) or a miss (`$F.get`).
fn map_arg(fn_name: &str, args: &[Value], i: usize) -> Result<Option<Object>, EvalError> {
    match &args[i] {
        Value::Null => Ok(None),
        Value::Object(o) => Ok(Some(o.clone())),
        v => Err(type_err(fn_name, "dict", v)),
    }
}

// ---------------------------------------------------------------------------
// math (Python operator semantics)
// ---------------------------------------------------------------------------

fn int_add_overflow(a: i64, b: i64) -> Option<i64> {
    a.checked_add(b)
}

fn int_mul_overflow(a: i64, b: i64) -> Option<i64> {
    a.checked_mul(b)
}

fn py_operator_type_err(op: &str, symbol: &str, a: &Value, b: &Value) -> EvalError {
    EvalError::type_error(format!(
        "$F.{op}: unsupported operand type(s) for {symbol}: '{}' and '{}'",
        py_type_name(a),
        py_type_name(b)
    ))
}

/// `a + b`: numbers add, strings concatenate, lists concatenate (new list).
fn fn_add(args: &[Value]) -> Result<Value, EvalError> {
    arity("add", args, 2)?;
    let (a, b) = (&args[0], &args[1]);
    if let (Some(ai), Some(bi)) = (py_int(a), py_int(b)) {
        return int_add_overflow(ai, bi)
            .map(Value::from)
            .ok_or_else(|| EvalError::overflow("$F.add: integer addition result too large"));
    }
    if let (Some(af), Some(bf)) = (py_numeric(a), py_numeric(b)) {
        return float_value(af + bf);
    }
    if let (Value::String(as_), Value::String(bs)) = (a, b) {
        return Ok(Value::String(format!("{as_}{bs}")));
    }
    if let (Value::Array(al), Value::Array(bl)) = (a, b) {
        let mut out = al.clone();
        out.extend(bl.iter().cloned());
        return Ok(Value::Array(out));
    }
    Err(py_operator_type_err("add", "+", a, b))
}

/// `a - b` (numbers only).
fn fn_sub(args: &[Value]) -> Result<Value, EvalError> {
    arity("sub", args, 2)?;
    let (a, b) = (&args[0], &args[1]);
    if let (Some(ai), Some(bi)) = (py_int(a), py_int(b)) {
        // checked_sub 不产生中间负数：先前的 `-bi` 在 bi == i64::MIN 处自身
        // 溢出（release 回绕 → sub(0, MIN) 静默返回负值 / debug panic）。
        // 真值在域内的 sub(MIN, MIN)=0 直接放行（Python: 0），域外的
        // sub(0, MIN)=2^63 落 add/mul/pow 同款 OverflowError。
        return ai
            .checked_sub(bi)
            .map(Value::from)
            .ok_or_else(|| EvalError::overflow("$F.sub: integer subtraction result too large"));
    }
    if let (Some(af), Some(bf)) = (py_numeric(a), py_numeric(b)) {
        return float_value(af - bf);
    }
    Err(py_operator_type_err("sub", "-", a, b))
}

/// 结果规模预算（Python `_EXPR_*` 同口径）：allowed =
/// min(max(input_size × GROWTH_FACTOR, 软下限 100_000), 硬上限 1 亿)。
const EXPR_MAX_RESULT_SIZE: usize = 100_000;
const EXPR_GROWTH_FACTOR: usize = 4;
const EXPR_MAX_RESULT_HARD: usize = 100_000_000;

fn allowed_result_size(input_size: usize) -> usize {
    input_size
        .saturating_mul(EXPR_GROWTH_FACTOR)
        .clamp(EXPR_MAX_RESULT_SIZE, EXPR_MAX_RESULT_HARD)
}

/// Python str/list 重复的**分配之前**规模闸（`_size_refusal` 同款文案）：
/// projected = seq_len × n 超过 allowed 即 ValueError，绝不先分配再死。
/// 先前的闸卡在「重复次数 ≤1e8」上，结果规模（|seq|×n）从未被检查——
/// 100 元素数组 ×1e8 是次数闸放行的合法值，却要求 ~760GB。
/// 口径注释：seq_len 用 Rust 字节长度（`str::len`），Python 计字符——
/// 字节 ≥ 字符，只会更严不放宽；多字节串的 limit 数字与 Python 不同、
/// 判定只会更早拒绝（plaita-go 用 rune 计数闸、字节分配，是另一种不一致，
/// 不照抄）。空序列投影为 0，任何倍数都合法（Python 同：`'' * 1e9 == ''`）。
fn repeat_len(fn_name: &str, seq_len: usize, n: i64, unit: &str) -> Result<usize, EvalError> {
    if n <= 0 {
        return Ok(0);
    }
    let count = usize::try_from(n).unwrap_or(usize::MAX);
    let projected = seq_len.saturating_mul(count);
    let allowed = allowed_result_size(seq_len);
    if projected > allowed {
        return Err(EvalError::value_error(format!(
            "$F.{fn_name}({seq_len} * {n}) result too large: {projected} {unit} \
             (limit {allowed}). Refusing to build an unbounded result."
        )));
    }
    Ok(count)
}

/// `a * b`: numbers multiply; str/list repeat by an int (new sequence).
fn fn_mul(args: &[Value]) -> Result<Value, EvalError> {
    arity("mul", args, 2)?;
    let (a, b) = (&args[0], &args[1]);
    if let (Some(ai), Some(bi)) = (py_int(a), py_int(b)) {
        return int_mul_overflow(ai, bi)
            .map(Value::from)
            .ok_or_else(|| EvalError::overflow("$F.mul: integer multiplication result too large"));
    }
    if let (Some(af), Some(bf)) = (py_numeric(a), py_numeric(b)) {
        return float_value(af * bf);
    }
    if let Value::String(s) = a {
        if let Some(bi) = py_int(b) {
            let count = repeat_len("mul", s.len(), bi, "chars")?;
            return Ok(Value::String(s.repeat(count)));
        }
    }
    if let Value::String(s) = b {
        if let Some(ai) = py_int(a) {
            let count = repeat_len("mul", s.len(), ai, "chars")?;
            return Ok(Value::String(s.repeat(count)));
        }
    }
    if let Value::Array(al) = a {
        if let Some(bi) = py_int(b) {
            let count = repeat_len("mul", al.len(), bi, "elements")?;
            let mut out = Vec::with_capacity(al.len().saturating_mul(count));
            for _ in 0..count {
                out.extend(al.iter().cloned());
            }
            return Ok(Value::Array(out));
        }
    }
    if let Value::Array(bl) = b {
        if let Some(ai) = py_int(a) {
            let count = repeat_len("mul", bl.len(), ai, "elements")?;
            let mut out = Vec::with_capacity(bl.len().saturating_mul(count));
            for _ in 0..count {
                out.extend(bl.iter().cloned());
            }
            return Ok(Value::Array(out));
        }
    }
    Err(py_operator_type_err("mul", "*", a, b))
}

/// `a / b`: true division, always float; division by zero errors.
fn fn_div(args: &[Value]) -> Result<Value, EvalError> {
    arity("div", args, 2)?;
    let (Some(af), Some(bf)) = (py_numeric(&args[0]), py_numeric(&args[1])) else {
        return Err(py_operator_type_err("div", "/", &args[0], &args[1]));
    };
    if bf == 0.0 {
        return Err(EvalError::zero_division("$F.div: division by zero"));
    }
    float_value(af / bf)
}

/// `a % b`: printf-style string formatting on str, Python-sign modulo else.
fn fn_mod(args: &[Value]) -> Result<Value, EvalError> {
    arity("mod", args, 2)?;
    if let Value::String(s) = &args[0] {
        return py_format(s, &args[1]).map(Value::String);
    }
    if let (Some(ai), Some(bi)) = (py_int(&args[0]), py_int(&args[1])) {
        if bi == 0 {
            return Err(EvalError::zero_division("$F.mod: division by zero"));
        }
        // Rust guarantees a panic for i64::MIN % -1 in every profile; Python
        // and plaita-go both yield 0 here (Go's two's-complement wrap
        // coincides with CPython's -2**63 % -1 == 0), and 0 is inside the
        // i64 domain — no f64 fallback involved.
        let mut r = ai.checked_rem(bi).unwrap_or(0);
        if r != 0 && (r < 0) != (bi < 0) {
            r += bi;
        }
        return Ok(Value::from(r));
    }
    let (Some(af), Some(bf)) = (py_numeric(&args[0]), py_numeric(&args[1])) else {
        return Err(py_operator_type_err("mod", "%", &args[0], &args[1]));
    };
    if bf == 0.0 {
        return Err(EvalError::zero_division("$F.mod: division by zero"));
    }
    let mut r = af % bf;
    if r != 0.0 && (r < 0.0) != (bf < 0.0) {
        r += bf;
    }
    float_value(r)
}

fn int_pow(base: i64, exp: i64) -> Result<Value, EvalError> {
    let mut base = base;
    let mut exp = exp;
    let mut r: i64 = 1;
    while exp > 0 {
        if exp & 1 == 1 {
            r = int_mul_overflow(r, base)
                .ok_or_else(|| EvalError::overflow("$F.pow: integer result too large"))?;
        }
        exp >>= 1;
        if exp == 0 {
            break;
        }
        base = int_mul_overflow(base, base)
            .ok_or_else(|| EvalError::overflow("$F.pow: integer result too large"))?;
    }
    Ok(Value::from(r))
}

/// `a ** b`: int ** non-negative int stays int; int ** negative int is a
/// float; `0 ** negative` errors; a negative base with a fractional exponent
/// would be complex in Python — refused.
fn fn_pow(args: &[Value]) -> Result<Value, EvalError> {
    arity("pow", args, 2)?;
    let (a, b) = (&args[0], &args[1]);
    if let (Some(ai), Some(bi)) = (py_int(a), py_int(b)) {
        if bi < 0 {
            if ai == 0 {
                return Err(EvalError::zero_division(
                    "$F.pow: 0.0 cannot be raised to a negative power",
                ));
            }
            return float_value((ai as f64).powf(bi as f64));
        }
        return int_pow(ai, bi);
    }
    let (Some(fa), Some(fb)) = (py_numeric(a), py_numeric(b)) else {
        return Err(py_operator_type_err("pow", "**", a, b));
    };
    if fa == 0.0 && fb < 0.0 {
        return Err(EvalError::zero_division(
            "$F.pow: 0.0 cannot be raised to a negative power",
        ));
    }
    let r = fa.powf(fb);
    if r.is_nan() && !fa.is_nan() && !fb.is_nan() {
        return Err(EvalError::value_error(
            "$F.pow: complex result not supported (negative base with fractional exponent)",
        ));
    }
    float_value(r)
}

fn fn_abs(args: &[Value]) -> Result<Value, EvalError> {
    arity("abs", args, 1)?;
    if let Some(i) = py_int(&args[0]) {
        // abs(i64::MIN) = 2^63 出 i64 域：裸 abs() 是 std 写明的双 profile
        // 分歧（debug panic / release 回绕回负值）。落 add/mul/pow 同款
        // OverflowError（AGENTS 文档化残余「>i64 报 OverflowError」出口；
        // Python 权威给 2**63，plaita-go 静默回负——负值不落任何允许出口，
        // Go 侧同根问题按跨仓同根拆单提报）。
        return i
            .checked_abs()
            .map(Value::from)
            .ok_or_else(|| EvalError::overflow("$F.abs: integer absolute value too large"));
    }
    if let Some(f) = py_float(&args[0]) {
        return Ok(Value::from(f.abs()));
    }
    Err(EvalError::type_error(format!(
        "$F.abs: bad operand type for abs(): '{}'",
        py_type_name(&args[0])
    )))
}

fn py_to_int(fn_name: &str, v: &Value) -> Result<i64, EvalError> {
    if let Some(i) = py_int(v) {
        return Ok(i);
    }
    if let Some(f) = py_float(v) {
        if !f.is_finite() || f > i64::MAX as f64 || f < i64::MIN as f64 {
            return Err(EvalError::overflow(format!(
                "$F.{fn_name}: cannot convert float to integer"
            )));
        }
        return Ok(f as i64);
    }
    Err(EvalError::type_error(format!(
        "$F.{fn_name}: bad operand type: '{}'",
        py_type_name(v)
    )))
}

fn fn_ceil(args: &[Value]) -> Result<Value, EvalError> {
    arity("ceil", args, 1)?;
    if let Some(i) = py_int(&args[0]) {
        return Ok(Value::from(i));
    }
    let f = py_float(&args[0]).ok_or_else(|| type_err("ceil", "number", &args[0]))?;
    Ok(Value::from(f.ceil() as i64))
}

fn fn_floor(args: &[Value]) -> Result<Value, EvalError> {
    arity("floor", args, 1)?;
    if let Some(i) = py_int(&args[0]) {
        return Ok(Value::from(i));
    }
    let f = py_float(&args[0]).ok_or_else(|| type_err("floor", "number", &args[0]))?;
    Ok(Value::from(f.floor() as i64))
}

fn fn_trunc(args: &[Value]) -> Result<Value, EvalError> {
    arity("trunc", args, 1)?;
    Ok(Value::from(py_to_int("trunc", &args[0])?))
}

fn fn_sqrt(args: &[Value]) -> Result<Value, EvalError> {
    arity("sqrt", args, 1)?;
    let f = py_numeric(&args[0]).ok_or_else(|| type_err("sqrt", "number", &args[0]))?;
    if f < 0.0 {
        return Err(EvalError::value_error("$F.sqrt: math domain error"));
    }
    float_value(f.sqrt())
}

/// Python `round`: banker's rounding on the exact binary value; int result
/// when ndigits is omitted, float stays float with ndigits given.
fn fn_round(args: &[Value]) -> Result<Value, EvalError> {
    arity_min("round", args, 1)?;
    if let Some(i) = py_int(&args[0]) {
        return Ok(Value::from(i));
    }
    let f = py_float(&args[0]).ok_or_else(|| {
        EvalError::type_error(format!(
            "$F.round: type {} doesn't define __round__ method",
            py_type_name(&args[0])
        ))
    })?;
    if args.len() == 1 {
        return py_round_to_int(f).map(Value::from);
    }
    let n = py_index_arg("round", &args[1])?;
    if !f.is_finite() {
        // round(inf/nan, n) 原样返回（Python 实测）——必须先于值域判断
        return float_value(f);
    }
    if n == 0 {
        return float_value(py_round_digits(f, 0));
    }
    // CPython 口径（本地实跑核对）：任何 f64 的十进制展开至多 1074 位
    // （2^-1074 亚常规尾数），n ≥ 1075 时舍入位置在展开之外 → 恒等
    // （round(1e-20, 4294967297) == 1e-20）；n ∈ [18, 1074] 仍是真实按位
    // 舍入（round(1e-20, 18) == 0.0、round(1e-20, 20) == 1e-20）——旧实现
    // `n as i32` 截断在这里给出 1.2/错值，py_round_digits 的 min(17) 钳位
    // 则把 [18,1074] 整段错成 0.0 面。负向：|n| ≥ 309 时 10^|n| 已超 f64
    // 上限（1.8e308），任何有限值都舍到 ±0.0（round(1.5, -2147483648) ==
    // 0.0 / round(-1.5, -2147483648) == -0.0），旧实现的 powi(-n) 在这里
    // 溢出 panic。收窄到 [-308, 1074] 后 i32 强转安全。
    if n > 1074 {
        return float_value(f);
    }
    if n < -308 {
        return float_value(0.0f64.copysign(f));
    }
    float_value(py_round_digits(f, n as i32))
}

fn py_round_to_int(f: f64) -> Result<i64, EvalError> {
    if !f.is_finite() {
        return Err(EvalError::overflow(format!(
            "$F.round: cannot convert float {} to integer",
            crate::pyrepr::py_float_repr(f)
        )));
    }
    let s = format!("{f:.0}");
    s.parse::<i64>()
        .map_err(|_| EvalError::overflow("$F.round: rounded integer too large"))
}

fn py_round_digits(f: f64, n: i32) -> f64 {
    if !f.is_finite() {
        return f;
    }
    if n < 0 {
        let scale = 10f64.powi(-n);
        let shifted = format!("{:.0}", f / scale);
        let v: f64 = shifted.parse().unwrap_or(f);
        return v * scale;
    }
    // 调用方（fn_round）已把 n 收窄到 [0, 1074]：任何 f64 的十进制展开
    // 至多 1074 位，这个宽度内的 format+parse 就是 Python 的按位舍入。
    // 旧 min(17) 钳位会把 round(1e-20, 20) 错成 0.0（Python: 1e-20）。
    let s = format!("{:.*}", n as usize, f);
    s.parse().unwrap_or(f)
}

// ---------------------------------------------------------------------------
// string
// ---------------------------------------------------------------------------

fn fn_lower(args: &[Value]) -> Result<Value, EvalError> {
    arity("lower", args, 1)?;
    Ok(Value::String(str_arg("lower", args, 0)?.to_lowercase()))
}

fn fn_upper(args: &[Value]) -> Result<Value, EvalError> {
    arity("upper", args, 1)?;
    Ok(Value::String(str_arg("upper", args, 0)?.to_uppercase()))
}

/// Unicode titlecase of a word-initial character (Python str.capitalize /
/// str.title use the titlecase mapping, not uppercase — 'ß' → "Ss", the
/// digraph families map to their title forms). std has no titlecase API, so
/// the special-casing set is a table; everything else uppercases.
fn py_titlecase_first(c: char) -> String {
    match c {
        '\u{00DF}' => "Ss".to_string(),                                 // ß
        '\u{01C4}' | '\u{01C5}' | '\u{01C6}' => '\u{01C5}'.to_string(), // Ǆ ǅ ǆ
        '\u{01C7}' | '\u{01C8}' | '\u{01C9}' => '\u{01C8}'.to_string(), // Ǉ ǈ ǉ
        '\u{01CA}' | '\u{01CB}' | '\u{01CC}' => '\u{01CB}'.to_string(), // Ǌ ǋ ǌ
        '\u{01F1}' | '\u{01F2}' | '\u{01F3}' => '\u{01F2}'.to_string(), // Ǳ ǲ ǳ
        other => other.to_uppercase().collect(),
    }
}

/// Python str.capitalize: first char TITLECASED, the REST lowercased.
fn fn_capitalize(args: &[Value]) -> Result<Value, EvalError> {
    arity("capitalize", args, 1)?;
    let s = str_arg("capitalize", args, 0)?;
    let mut chars = s.chars();
    let out = match chars.next() {
        None => s,
        Some(first) => {
            let mut out = py_titlecase_first(first);
            out.extend(chars.flat_map(|c| c.to_lowercase()));
            out
        }
    };
    Ok(Value::String(out))
}

/// Python str.title: a word starts after every non-alphabetic character.
fn fn_title(args: &[Value]) -> Result<Value, EvalError> {
    arity("title", args, 1)?;
    let s = str_arg("title", args, 0)?;
    let mut out = String::with_capacity(s.len());
    let mut prev_is_letter = false;
    for c in s.chars() {
        if !prev_is_letter && c.is_alphabetic() {
            out.push_str(&py_titlecase_first(c));
        } else {
            out.extend(c.to_lowercase());
        }
        prev_is_letter = c.is_alphabetic();
    }
    Ok(Value::String(out))
}

fn fn_strip(args: &[Value]) -> Result<Value, EvalError> {
    arity("strip", args, 1)?;
    Ok(Value::String(str_arg("strip", args, 0)?.trim().to_string()))
}

fn fn_lstrip(args: &[Value]) -> Result<Value, EvalError> {
    arity("lstrip", args, 1)?;
    Ok(Value::String(
        str_arg("lstrip", args, 0)?.trim_start().to_string(),
    ))
}

fn fn_rstrip(args: &[Value]) -> Result<Value, EvalError> {
    arity("rstrip", args, 1)?;
    Ok(Value::String(
        str_arg("rstrip", args, 0)?.trim_end().to_string(),
    ))
}

fn fn_replace(args: &[Value]) -> Result<Value, EvalError> {
    arity("replace", args, 3)?;
    let s = str_arg("replace", args, 0)?;
    let old = str_arg("replace", args, 1)?;
    let new = str_arg("replace", args, 2)?;
    Ok(Value::String(s.replace(&old, &new)))
}

/// Python str.split: an empty separator is a ValueError.
fn fn_split(args: &[Value]) -> Result<Value, EvalError> {
    arity("split", args, 2)?;
    let s = str_arg("split", args, 0)?;
    let sep = str_arg("split", args, 1)?;
    if sep.is_empty() {
        return Err(EvalError::value_error("$F.split: empty separator"));
    }
    Ok(Value::Array(
        s.split(sep.as_str())
            .map(|p| Value::String(p.to_string()))
            .collect(),
    ))
}

/// Python `b.join(a)`: every element must be a str.
fn fn_join(args: &[Value]) -> Result<Value, EvalError> {
    arity("join", args, 2)?;
    let items = list_arg("join", args, 0)?;
    let sep = str_arg("join", args, 1)?;
    let mut parts = Vec::with_capacity(items.len());
    for (i, v) in items.iter().enumerate() {
        match v {
            Value::String(s) => parts.push(s.clone()),
            other => {
                return Err(EvalError::type_error(format!(
                    "$F.join: sequence item {i}: expected str instance, {} found",
                    py_type_name(other)
                )))
            }
        }
    }
    Ok(Value::String(parts.join(&sep)))
}

fn fn_startswith(args: &[Value]) -> Result<Value, EvalError> {
    arity("startswith", args, 2)?;
    let s = str_arg("startswith", args, 0)?;
    let p = str_arg("startswith", args, 1)?;
    Ok(Value::Bool(s.starts_with(&p)))
}

fn fn_endswith(args: &[Value]) -> Result<Value, EvalError> {
    arity("endswith", args, 2)?;
    let s = str_arg("endswith", args, 0)?;
    let p = str_arg("endswith", args, 1)?;
    Ok(Value::Bool(s.ends_with(&p)))
}

/// `"".join(str(arg) for arg in args)` — Python str() rendering per arg.
fn fn_concat(args: &[Value]) -> Result<Value, EvalError> {
    Ok(Value::String(
        args.iter().map(py_str).collect::<Vec<_>>().join(""),
    ))
}

fn fn_is_digit(args: &[Value]) -> Result<Value, EvalError> {
    arity("isDigit", args, 1)?;
    let s = str_arg("isDigit", args, 0)?;
    Ok(Value::Bool(
        !s.is_empty() && s.chars().all(|c| c.is_numeric()),
    ))
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

/// Python `len`: str by characters, list, dict.
fn fn_len(args: &[Value]) -> Result<Value, EvalError> {
    arity("len", args, 1)?;
    let n = match &args[0] {
        Value::String(s) => s.chars().count(),
        Value::Array(a) => a.len(),
        Value::Object(o) => o.len(),
        other => {
            return Err(EvalError::type_error(format!(
                "$F.len: object of type '{}' has no len()",
                py_type_name(other)
            )))
        }
    };
    Ok(Value::from(n))
}

/// Python `a.index(b)`: substring search on str (CHARACTER index), `==`
/// membership on list; a miss raises.
fn fn_index(args: &[Value]) -> Result<Value, EvalError> {
    arity("index", args, 2)?;
    match &args[0] {
        Value::String(s) => {
            let needle = str_arg("index", args, 1)?;
            match s.find(&needle) {
                Some(byte_idx) => Ok(Value::from(s[..byte_idx].chars().count())),
                None => Err(EvalError::value_error("$F.index: substring not found")),
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                if equal(v, &args[1]) {
                    return Ok(Value::from(i));
                }
            }
            Err(EvalError::value_error(format!(
                "$F.index: {} is not in list",
                py_repr(&args[1])
            )))
        }
        other => Err(EvalError::type_error(format!(
            "$F.index: '{}' object has no attribute 'index'",
            py_type_name(other)
        ))),
    }
}

fn slice_clamp(i: i64, l: usize) -> usize {
    let l = l as i64;
    let i = if i < 0 { i + l } else { i };
    i.clamp(0, l) as usize
}

/// Python `a[b:c]`: clamped, negative indices, str by characters; new value.
fn fn_slice(args: &[Value]) -> Result<Value, EvalError> {
    arity("slice", args, 3)?;
    let lo = py_index_arg("slice", &args[1])?;
    let hi = py_index_arg("slice", &args[2])?;
    match &args[0] {
        Value::Array(items) => {
            let a = slice_clamp(lo, items.len());
            let b = slice_clamp(hi, items.len());
            Ok(Value::Array(if b <= a {
                Vec::new()
            } else {
                items[a..b].to_vec()
            }))
        }
        Value::String(s) => {
            let chars: Vec<char> = s.chars().collect();
            let a = slice_clamp(lo, chars.len());
            let b = slice_clamp(hi, chars.len());
            let out: String = if b <= a {
                String::new()
            } else {
                chars[a..b].iter().collect()
            };
            Ok(Value::String(out))
        }
        other => Err(EvalError::type_error(format!(
            "$F.slice: '{}' object is not subscriptable",
            py_type_name(other)
        ))),
    }
}

/// Python `a + [b]`: NEW list.
fn fn_append(args: &[Value]) -> Result<Value, EvalError> {
    arity("append", args, 2)?;
    let mut out = list_arg("append", args, 0)?;
    out.push(args[1].clone());
    Ok(Value::Array(out))
}

fn fn_extend(args: &[Value]) -> Result<Value, EvalError> {
    fn_add(args)
}

/// Python `a[:b] + [c] + a[b:]`: NEW list, clamped.
fn fn_insert(args: &[Value]) -> Result<Value, EvalError> {
    arity("insert", args, 3)?;
    let s = list_arg("insert", args, 0)?;
    let at = slice_clamp(py_index_arg("insert", &args[1])?, s.len());
    let mut out = Vec::with_capacity(s.len() + 1);
    out.extend_from_slice(&s[..at]);
    out.push(args[2].clone());
    out.extend_from_slice(&s[at..]);
    Ok(Value::Array(out))
}

/// Python `list.remove`: removes the FIRST `==` match, new list; a miss is
/// a ValueError.
fn fn_remove(args: &[Value]) -> Result<Value, EvalError> {
    arity("remove", args, 2)?;
    let s = list_arg("remove", args, 0)?;
    for (i, v) in s.iter().enumerate() {
        if equal(v, &args[1]) {
            let mut out = Vec::with_capacity(s.len() - 1);
            out.extend_from_slice(&s[..i]);
            out.extend_from_slice(&s[i + 1..]);
            return Ok(Value::Array(out));
        }
    }
    Err(EvalError::value_error(format!(
        "$F.remove: list.remove({}): {} not in list",
        py_repr(&args[1]),
        py_repr(&args[1])
    )))
}

/// Python `a[::-1]`: NEW list.
fn fn_reverse(args: &[Value]) -> Result<Value, EvalError> {
    arity("reverse", args, 1)?;
    let mut out = list_arg("reverse", args, 0)?;
    out.reverse();
    Ok(Value::Array(out))
}

/// Python `a[b]`: list/string/dict indexing; misses raise (IndexError /
/// KeyError)。整数下标才走 py_index_arg——dict 的字符串键分支必须在它之前，
/// 否则不可达（Python `{'k':5}['k']` == 5；Go 同型缺陷见 plaita-go#42）。
fn fn_get_list_item(args: &[Value]) -> Result<Value, EvalError> {
    arity("getListItem", args, 2)?;
    if let Value::Object(map) = &args[0] {
        let Some(Value::String(k)) = args.get(1) else {
            return Err(EvalError::key_error(format!(
                "$F.getListItem: KeyError: {}",
                py_repr(&args[1])
            )));
        };
        return map
            .get(k)
            .cloned()
            .ok_or_else(|| EvalError::key_error(format!("$F.getListItem: KeyError: {k:?}")));
    }
    let idx = py_index_arg("getListItem", &args[1])?;
    match &args[0] {
        Value::Array(items) => {
            let n = items.len() as i64;
            let j = if idx < 0 { idx + n } else { idx };
            if j < 0 || j >= n {
                return Err(EvalError::index_error(
                    "$F.getListItem: IndexError: list index out of range",
                ));
            }
            Ok(items[j as usize].clone())
        }
        Value::String(s) => {
            let chars: Vec<char> = s.chars().collect();
            let n = chars.len() as i64;
            let j = if idx < 0 { idx + n } else { idx };
            if j < 0 || j >= n {
                return Err(EvalError::index_error(
                    "$F.getListItem: IndexError: string index out of range",
                ));
            }
            Ok(Value::String(chars[j as usize].to_string()))
        }
        other => Err(EvalError::type_error(format!(
            "$F.getListItem: '{}' object is not subscriptable",
            py_type_name(other)
        ))),
    }
}

/// Python `a.__setitem__(b, c)` on a COPY; returns None.
fn fn_set_list_item(args: &[Value]) -> Result<Value, EvalError> {
    arity("setListItem", args, 3)?;
    let s = list_arg("setListItem", args, 0)?;
    let idx = py_index_arg("setListItem", &args[1])?;
    let n = s.len() as i64;
    let j = if idx < 0 { idx + n } else { idx };
    if j < 0 || j >= n {
        return Err(EvalError::index_error(
            "$F.setListItem: IndexError: list assignment index out of range",
        ));
    }
    let mut out = s;
    out[j as usize] = args[2].clone();
    Ok(Value::Null)
}

/// Python `x.get(k) if isinstance(x, dict) else x`——dict 缺键是 None，
/// 不是元素本身。
fn sort_key_of(v: &Value, sort_key: &str) -> Value {
    if !sort_key.is_empty() {
        if let Value::Object(o) = v {
            return o.get(sort_key).cloned().unwrap_or(Value::Null);
        }
    }
    v.clone()
}

/// Python `sorted(a, key=lambda x: x.get(k) if dict else x, reverse=desc)`:
/// new list, stable; a failed comparison is a TypeError（先前吞错静默返回
/// 未排序列表——给错值比报错糟）。
fn fn_sort(args: &[Value]) -> Result<Value, EvalError> {
    arity_min("sort", args, 1)?;
    let sort_key = match args.get(1) {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let desc = matches!(args.get(2), Some(Value::Bool(true)));
    let out = list_arg("sort", args, 0)?;
    // Schwartzian 变换：每个元素只取一次 key（Python sorted(key=...) 的形状）。
    // 旧实现是插入排序且每次比较重新取两次键（各 clone 一次）——O(n²) 带分配，
    // 64k 倒序整数 release 实测 54.9s；Python sorted()/Go SliceStable 都是
    // O(n log n)，n 来自运行期数据，慢不是可接受的有意行为。
    let mut keyed: Vec<(Value, Value)> = out
        .into_iter()
        .map(|v| {
            let k = sort_key_of(&v, &sort_key);
            (k, v)
        })
        .collect();
    // 可比性预检：O(n) 相邻比较。compare 只认 numeric 与 str 两族，原序里
    // 只要存在跨类元素就必然有一对相邻跨类键——错误类型与文案与现在同源，
    // 且预检后比较器闭包不可能再失败（sort_by 的比较器不能向上抛错，也不能
    // 用「错误槽 + 一律 Equal」让全序契约破洞）。
    for pair in keyed.windows(2) {
        crate::value::compare("$F.sort", &pair[0].0, &pair[1].0)?;
    }
    // slice::sort_by 稳定（等价 Timsort/SliceStable），相等键原相对顺序保持；
    // desc 用 Ordering::reverse 表达。
    keyed.sort_by(|(ka, _), (kb, _)| {
        let o = crate::value::compare("$F.sort", ka, kb).unwrap_or(std::cmp::Ordering::Equal);
        if desc {
            o.reverse()
        } else {
            o
        }
    });
    Ok(Value::Array(keyed.into_iter().map(|(_, v)| v).collect()))
}

// ---------------------------------------------------------------------------
// dict
// ---------------------------------------------------------------------------

fn fn_keys(args: &[Value]) -> Result<Value, EvalError> {
    arity("keys", args, 1)?;
    let m = map_arg("keys", args, 0)?.unwrap_or_default();
    Ok(Value::Array(
        m.keys().map(|k| Value::String(k.clone())).collect(),
    ))
}

fn fn_values(args: &[Value]) -> Result<Value, EvalError> {
    arity("values", args, 1)?;
    let m = map_arg("values", args, 0)?.unwrap_or_default();
    Ok(Value::Array(m.values().cloned().collect()))
}

fn fn_items(args: &[Value]) -> Result<Value, EvalError> {
    arity("items", args, 1)?;
    let m = map_arg("items", args, 0)?.unwrap_or_default();
    Ok(Value::Array(
        m.into_iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k), v]))
            .collect(),
    ))
}

/// Python dict.get(key, default).
fn fn_get(args: &[Value]) -> Result<Value, EvalError> {
    arity_min("get", args, 2)?;
    let m = map_arg("get", args, 0)?.unwrap_or_default();
    let key = str_arg("get", args, 1)?;
    Ok(match m.get(&key) {
        Some(v) => v.clone(),
        None => args.get(2).cloned().unwrap_or(Value::Null),
    })
}

/// Python `a.update({b: c})` on a copy; returns None.
fn fn_set(args: &[Value]) -> Result<Value, EvalError> {
    arity("set", args, 3)?;
    let Some(m) = map_arg("set", args, 0)? else {
        return Err(EvalError::type_error(
            "$F.set: 'NoneType' object has no attribute 'update'",
        ));
    };
    let key = str_arg("set", args, 1)?;
    let mut out = m;
    out.insert(key, args[2].clone());
    Ok(Value::Null)
}

/// Python `a.pop(b)` (dict form) on a copy; returns None; missing key raises.
fn fn_delete(args: &[Value]) -> Result<Value, EvalError> {
    arity("delete", args, 2)?;
    let Some(m) = map_arg("delete", args, 0)? else {
        return Err(EvalError::key_error(format!(
            "$F.delete: KeyError: {}",
            py_repr(&args[1])
        )));
    };
    let key = str_arg("delete", args, 1)?;
    if !m.contains_key(&key) {
        return Err(EvalError::key_error(format!(
            "$F.delete: KeyError: {key:?}"
        )));
    }
    let mut out = m;
    out.shift_remove(&key);
    Ok(Value::Null)
}

/// Python `a.clear()` on a copy; returns None.
fn fn_clear(args: &[Value]) -> Result<Value, EvalError> {
    arity("clear", args, 1)?;
    map_arg("clear", args, 0)?.ok_or_else(|| {
        EvalError::type_error("$F.clear: 'NoneType' object has no attribute 'clear'")
    })?;
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// pop (list + dict)
// ---------------------------------------------------------------------------

/// Python `a.pop(b)`: list.pop(index) or dict.pop(key); returns the removed
/// element; operates on a copy (plaita#69 + issue #9 copy discipline).
fn fn_pop(args: &[Value]) -> Result<Value, EvalError> {
    arity("pop", args, 2)?;
    match &args[0] {
        Value::Null => Err(EvalError::type_error(
            "$F.pop: 'NoneType' object has no attribute 'pop'",
        )),
        Value::Object(m) => {
            let key = match str_arg("pop", args, 1) {
                Ok(k) => k,
                Err(_) => {
                    return Err(EvalError::key_error(format!(
                        "$F.pop: KeyError: {}",
                        py_repr(&args[1])
                    )))
                }
            };
            let v = m
                .get(&key)
                .cloned()
                .ok_or_else(|| EvalError::key_error(format!("$F.pop: KeyError: {key:?}")))?;
            Ok(v)
        }
        Value::String(_) => Err(EvalError::type_error(
            "$F.pop: 'str' object has no attribute 'pop'",
        )),
        Value::Array(items) => {
            let idx = py_index_arg("pop", &args[1])?;
            let n = items.len() as i64;
            let j = if idx < 0 { idx + n } else { idx };
            if j < 0 || j >= n {
                return Err(EvalError::index_error(
                    "$F.pop: IndexError: pop index out of range",
                ));
            }
            Ok(items[j as usize].clone())
        }
        other => Err(EvalError::type_error(format!(
            "$F.pop: '{}' object has no attribute 'pop'",
            py_type_name(other)
        ))),
    }
}

// ---------------------------------------------------------------------------
// json / logic / compare
// ---------------------------------------------------------------------------

/// Python json.loads: integers decode as ints, floats as floats; trailing
/// garbage raises.
fn fn_json_loads(args: &[Value]) -> Result<Value, EvalError> {
    arity("json_loads", args, 1)?;
    let s = str_arg("json_loads", args, 0)?;
    serde_json::from_str::<Value>(&s).map_err(|e| EvalError::other(format!("$F.json_loads: {e}")))
}

/// Python json.dumps with defaults: `, ` / `: ` separators, ensure_ascii.
fn fn_json_dumps(args: &[Value]) -> Result<Value, EvalError> {
    arity("json_dumps", args, 1)?;
    let mut out = String::new();
    py_write_json(&mut out, &args[0])?;
    Ok(Value::String(out))
}

fn py_write_json(out: &mut String, v: &Value) -> Result<(), EvalError> {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) if n.is_f64() => {
            out.push_str(&crate::pyrepr::py_float_repr(n.as_f64().unwrap()))
        }
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => out.push_str(&py_json_quote(s)),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                py_write_json(out, item)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (k, val)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&py_json_quote(k));
                out.push_str(": ");
                py_write_json(out, val)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn py_json_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if (c as u32) < 0x7f => out.push(c),
            c => {
                // ensure_ascii: every non-ASCII rune escaped, astral as pairs
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

/// Python `a and b` with value semantics.
fn fn_and(args: &[Value]) -> Result<Value, EvalError> {
    arity("and", args, 2)?;
    if !crate::value::is_truthy(&args[0]) {
        return Ok(args[0].clone());
    }
    Ok(args[1].clone())
}

/// Python `or` over variadic args: first truthy VALUE, `False` when all falsy.
fn fn_or(args: &[Value]) -> Result<Value, EvalError> {
    arity_min("or", args, 1)?;
    for v in args {
        if crate::value::is_truthy(v) {
            return Ok(v.clone());
        }
    }
    Ok(Value::Bool(false))
}

fn fn_not(args: &[Value]) -> Result<Value, EvalError> {
    arity("not", args, 1)?;
    Ok(Value::Bool(!crate::value::is_truthy(&args[0])))
}

fn fn_eq(args: &[Value]) -> Result<Value, EvalError> {
    arity("eq", args, 2)?;
    Ok(Value::Bool(equal(&args[0], &args[1])))
}

fn fn_ne(args: &[Value]) -> Result<Value, EvalError> {
    arity("ne", args, 2)?;
    Ok(Value::Bool(!equal(&args[0], &args[1])))
}

fn fn_gt(args: &[Value]) -> Result<Value, EvalError> {
    arity("gt", args, 2)?;
    Ok(Value::Bool(
        crate::value::compare("gt", &args[0], &args[1])? == std::cmp::Ordering::Greater,
    ))
}

fn fn_gte(args: &[Value]) -> Result<Value, EvalError> {
    arity("gte", args, 2)?;
    Ok(Value::Bool(
        crate::value::compare("gte", &args[0], &args[1])? != std::cmp::Ordering::Less,
    ))
}

fn fn_lt(args: &[Value]) -> Result<Value, EvalError> {
    arity("lt", args, 2)?;
    Ok(Value::Bool(
        crate::value::compare("lt", &args[0], &args[1])? == std::cmp::Ordering::Less,
    ))
}

fn fn_lte(args: &[Value]) -> Result<Value, EvalError> {
    arity("lte", args, 2)?;
    Ok(Value::Bool(
        crate::value::compare("lte", &args[0], &args[1])? != std::cmp::Ordering::Greater,
    ))
}

/// Python `a if c else b` — EAGER (both branches evaluated, plain call
/// semantics), condition is Python truthiness.
fn fn_ifelse(args: &[Value]) -> Result<Value, EvalError> {
    arity("ifelse", args, 3)?;
    if crate::value::is_truthy(&args[0]) {
        Ok(args[1].clone())
    } else {
        Ok(args[2].clone())
    }
}

/// Python `b not in a`.
fn fn_not_contains(args: &[Value]) -> Result<Value, EvalError> {
    arity("notContains", args, 2)?;
    let contained = py_contains("notContains", &args[0], &args[1])?;
    Ok(Value::Bool(!contained))
}

// Go extensions kept for compatibility with Go-authored flows (documented
// divergence from plaita's default registry, same as plaita-go).
fn fn_between(args: &[Value]) -> Result<Value, EvalError> {
    arity("between", args, 2)?;
    let v = py_numeric(&args[0]).ok_or_else(|| type_err("between", "number", &args[0]))?;
    let bounds = list_arg("between", args, 1)?;
    if bounds.len() != 2 {
        return Err(EvalError::value_error(format!(
            "value in between/notBetween condition should have two bound, but got len: {}",
            bounds.len()
        )));
    }
    let low = py_numeric(&bounds[0]).ok_or_else(|| type_err("between", "number", &bounds[0]))?;
    let high = py_numeric(&bounds[1]).ok_or_else(|| type_err("between", "number", &bounds[1]))?;
    Ok(Value::Bool(v >= low && v <= high))
}

fn fn_not_between(args: &[Value]) -> Result<Value, EvalError> {
    arity("notBetween", args, 2)?;
    let v = fn_between(args)?;
    Ok(Value::Bool(!v.as_bool().unwrap_or(false)))
}

fn fn_in(args: &[Value]) -> Result<Value, EvalError> {
    arity("in", args, 2)?;
    Ok(Value::Bool(py_contains("in", &args[1], &args[0])?))
}

fn fn_not_in(args: &[Value]) -> Result<Value, EvalError> {
    arity("notIn", args, 2)?;
    Ok(Value::Bool(!py_contains("notIn", &args[1], &args[0])?))
}

fn fn_contains_ext(args: &[Value]) -> Result<Value, EvalError> {
    arity("contains", args, 2)?;
    Ok(Value::Bool(py_contains("contains", &args[0], &args[1])?))
}

fn fn_like(args: &[Value]) -> Result<Value, EvalError> {
    arity("like", args, 2)?;
    let l = str_arg("like", args, 0)?;
    let r = str_arg("like", args, 1)?;
    Ok(Value::Bool(l.contains(&r)))
}

// ---------------------------------------------------------------------------
// date
// ---------------------------------------------------------------------------

fn fn_now(args: &[Value]) -> Result<Value, EvalError> {
    let layout = match args.first() {
        Some(Value::String(s)) => s.clone(),
        None | Some(Value::Null) => "%Y-%m-%d %H:%M:%S".to_string(),
        Some(other) => return Err(type_err("now", "str", other)),
    };
    let now = chrono::Local::now();
    Ok(Value::String(py_strftime(&now, &layout)))
}

fn fn_today(args: &[Value]) -> Result<Value, EvalError> {
    let layout = match args.first() {
        Some(Value::String(s)) => s.clone(),
        None | Some(Value::Null) => "%Y-%m-%d".to_string(),
        Some(other) => return Err(type_err("today", "str", other)),
    };
    let now = chrono::Local::now();
    let today = now.with_time(chrono::NaiveTime::MIN).single().unwrap_or(
        now.date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(now.timezone())
            .single()
            .unwrap_or(now),
    );
    Ok(Value::String(py_strftime(&today, &layout)))
}

// ---------------------------------------------------------------------------
// registry
// ---------------------------------------------------------------------------

macro_rules! functions {
    ($($name:literal => $f:expr),+ $(,)?) => {
        &[$(($name, $f as Func)),+]
    };
}

static REGISTRY: &[(&str, Func)] = functions![
    // math
    "add" => fn_add,
    "sub" => fn_sub,
    "mul" => fn_mul,
    "div" => fn_div,
    "mod" => fn_mod,
    "pow" => fn_pow,
    "abs" => fn_abs,
    "ceil" => fn_ceil,
    "floor" => fn_floor,
    "round" => fn_round,
    "trunc" => fn_trunc,
    "sqrt" => fn_sqrt,
    // string
    "lower" => fn_lower,
    "upper" => fn_upper,
    "capitalize" => fn_capitalize,
    "title" => fn_title,
    "strip" => fn_strip,
    "lstrip" => fn_lstrip,
    "rstrip" => fn_rstrip,
    "replace" => fn_replace,
    "split" => fn_split,
    "join" => fn_join,
    "startswith" => fn_startswith,
    "endswith" => fn_endswith,
    "concat" => fn_concat,
    "isDigit" => fn_is_digit,
    // list
    "len" => fn_len,
    "length" => fn_len,
    "index" => fn_index,
    "slice" => fn_slice,
    "append" => fn_append,
    "addListItem" => fn_append,
    "extend" => fn_extend,
    "insert" => fn_insert,
    "insertListItem" => fn_insert,
    "remove" => fn_remove,
    "reverse" => fn_reverse,
    "sort" => fn_sort,
    "getListItem" => fn_get_list_item,
    "setListItem" => fn_set_list_item,
    "delListItem" => fn_pop,
    // dict
    "keys" => fn_keys,
    "getDictKeys" => fn_keys,
    "values" => fn_values,
    "getDictValues" => fn_values,
    "items" => fn_items,
    "get" => fn_get,
    "getDictValue" => fn_get,
    "set" => fn_set,
    "setDictValue" => fn_set,
    "delete" => fn_delete,
    "delDictValue" => fn_delete,
    "clear" => fn_clear,
    "clearDict" => fn_clear,
    // date
    "today" => fn_today,
    "now" => fn_now,
    // json
    "json_loads" => fn_json_loads,
    "json_dumps" => fn_json_dumps,
    // logic
    "and" => fn_and,
    "or" => fn_or,
    "not" => fn_not,
    // comparison / flow / container
    "eq" => fn_eq,
    "ne" => fn_ne,
    "gt" => fn_gt,
    "gte" => fn_gte,
    "lt" => fn_lt,
    "lte" => fn_lte,
    "ifelse" => fn_ifelse,
    "notContains" => fn_not_contains,
    "pop" => fn_pop,
    // Go extensions (documented divergence, kept for compatibility)
    "between" => fn_between,
    "notBetween" => fn_not_between,
    "in" => fn_in,
    "notIn" => fn_not_in,
    "contains" => fn_contains_ext,
    "like" => fn_like,
];

fn registry_map() -> &'static HashMap<&'static str, Func> {
    static MAP: OnceLock<HashMap<&'static str, Func>> = OnceLock::new();
    MAP.get_or_init(|| REGISTRY.iter().copied().collect())
}

/// Looks up a `$F` function by name.
pub fn lookup(name: &str) -> Option<Func> {
    registry_map().get(name).copied()
}

/// All registered function names.
pub fn names() -> Vec<&'static str> {
    REGISTRY.iter().map(|(n, _)| *n).collect()
}

/// Number of registered functions (test introspection).
pub fn count() -> usize {
    REGISTRY.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::{eval_str, MapRoots};
    use serde_json::json;

    fn roots(v: Value) -> MapRoots<'static> {
        let mut r = MapRoots::new();
        r.set("INPUT", v);
        r
    }

    /// Evaluates with the expression context {INPUT: input}.
    fn call(expr: &str, input: Value) -> Value {
        eval_str(expr, &roots(input)).unwrap()
    }

    #[test]
    fn mod_i64_min_minus_one_is_zero() {
        // Rust defines i64::MIN % -1 as a panic in every profile; Python and
        // plaita-go both yield 0 (Go's two's-complement wrap coincides with
        // CPython's -2**63 % -1 == 0) and 0 is inside the i64 domain.
        let e = json!({});
        assert_eq!(
            call("$F.mod(-9223372036854775808, -1)", e.clone()),
            json!(0)
        );
        assert_eq!(
            call(
                "$F.mod($INPUT.a, $INPUT.b)",
                json!({"a": -9223372036854775808i64, "b": -1})
            ),
            json!(0)
        );
        // sign-follows-divisor semantics unchanged
        assert_eq!(call("$F.mod(7, 3)", e.clone()), json!(1));
        assert_eq!(call("$F.mod(-7, 3)", e.clone()), json!(2));
        assert_eq!(call("$F.mod(7, -3)", e.clone()), json!(-2));
        assert_eq!(call("$F.mod('x=%d', 5)", e.clone()), json!("x=5"));
    }

    #[test]
    fn math_python_semantics() {
        let e = json!({});
        assert_eq!(call("$F.add(1, 2)", e.clone()), json!(3));
        assert_eq!(call("$F.add(1, 2.5)", e.clone()), json!(3.5));
        assert_eq!(call("$F.add('a', 'b')", e.clone()), json!("ab"));
        assert_eq!(
            call("$F.add($INPUT.a, $INPUT.b)", json!({"a": [1], "b": [2, 3]})),
            json!([1, 2, 3])
        );
        assert_eq!(call("$F.sub(5, 2)", e.clone()), json!(3));
        assert_eq!(call("$F.mul(3, 4)", e.clone()), json!(12));
        assert_eq!(call("$F.mul('ab', 3)", e.clone()), json!("ababab"));
        assert_eq!(call("$F.mul('ab', -1)", e.clone()), json!(""));
        assert_eq!(call("$F.div(4, 2)", e.clone()), json!(2.0));
        assert_eq!(call("$F.div(1, 3)", e.clone()), json!(0.3333333333333333));
        assert_eq!(call("$F.mod(-7, 3)", e.clone()), json!(2));
        assert_eq!(
            call("$F.mod('%s=%d', $INPUT.t)", json!({"t": ["x", 5]})),
            json!("x=5")
        );
        assert_eq!(call("$F.pow(2, 10)", e.clone()), json!(1024));
        assert_eq!(call("$F.pow(2, -2)", e.clone()), json!(0.25));
        assert_eq!(call("$F.abs(-3.5)", e.clone()), json!(3.5));
        assert_eq!(call("$F.ceil(2.3)", e.clone()), json!(3));
        assert_eq!(call("$F.floor(2.7)", e.clone()), json!(2));
        assert_eq!(call("$F.trunc(-2.7)", e.clone()), json!(-2));
        assert_eq!(call("$F.sqrt(9)", e.clone()), json!(3.0));
        assert_eq!(call("$F.round(2.5)", e.clone()), json!(2));
        assert_eq!(call("$F.round(3.5)", e.clone()), json!(4));
        assert_eq!(call("$F.round(2.567, 2)", e.clone()), json!(2.57));
        assert_eq!(call("$F.round(3, 1)", e.clone()), json!(3));
        assert!(eval_str("$F.div(1, 0)", &roots(e.clone())).is_err());
        assert!(eval_str("$F.sqrt(-1)", &roots(e.clone())).is_err());
        assert!(eval_str("$F.mod(1, 0)", &roots(e.clone())).is_err());
        assert!(eval_str("$F.add(1, 'a')", &roots(e)).is_err());
    }

    #[test]
    fn strings() {
        let e = json!({});
        assert_eq!(call("$F.upper('straße')", e.clone()), json!("STRASSE"));
        assert_eq!(call("$F.lower('ABC')", e.clone()), json!("abc"));
        assert_eq!(
            call("$F.capitalize('hello WORLD')", e.clone()),
            json!("Hello world")
        );
        assert_eq!(
            call("$F.title(\"they're bill's\")", e.clone()),
            json!("They'Re Bill'S")
        );
        assert_eq!(call("$F.strip('  x  ')", e.clone()), json!("x"));
        assert_eq!(call("$F.replace('aaa', 'a', 'b')", e.clone()), json!("bbb"));
        assert_eq!(
            call("$F.split('a,b,c', ',')", e.clone()),
            json!(["a", "b", "c"])
        );
        assert!(eval_str("$F.split('abc', '')", &roots(e.clone())).is_err());
        assert_eq!(
            call("$F.join($INPUT.items, '-')", json!({"items": ["a", "b"]}),),
            json!("a-b")
        );
        assert!(eval_str(
            "$F.join($INPUT.items, '-')",
            &roots(json!({"items": [1, "b"]}))
        )
        .is_err());
        assert_eq!(call("$F.startswith('abc', 'ab')", e.clone()), json!(true));
        assert_eq!(call("$F.endswith('abc', 'bc')", e.clone()), json!(true));
        assert_eq!(
            call("$F.concat(1, 'a', None, $INPUT.x)", json!({"x": [1]})),
            json!("1aNone[1]")
        );
        assert_eq!(call("$F.isDigit('123')", e.clone()), json!(true));
        assert_eq!(call("$F.isDigit('1a')", e.clone()), json!(false));
    }

    #[test]
    fn lists_and_dicts() {
        assert_eq!(call("$F.len('héllo')", json!({})), json!(5));
        assert_eq!(call("$F.len($INPUT.a)", json!({"a": [1, 2]})), json!(2));
        assert_eq!(call("$F.len($INPUT.a)", json!({"a": {"x": 1}})), json!(1));
        assert_eq!(
            call("$F.index($INPUT.a, 1)", json!({"a": [3, 1, 2]})),
            json!(1)
        );
        assert!(eval_str("$F.index($INPUT.a, 9)", &roots(json!({"a": [1]}))).is_err());
        assert_eq!(
            call("$F.slice($INPUT.a, 1, 3)", json!({"a": [1, 2, 3, 4]})),
            json!([2, 3])
        );
        assert_eq!(call("$F.slice('hello', -3, -1)", json!({})), json!("ll"));
        assert_eq!(
            call("$F.append($INPUT.a, 2)", json!({"a": [1]})),
            json!([1, 2])
        );
        assert_eq!(
            call("$F.insert($INPUT.a, 1, 'x')", json!({"a": [1, 2, 3]})),
            json!([1, "x", 2, 3])
        );
        assert_eq!(
            call("$F.remove($INPUT.a, 1)", json!({"a": [1, 2, 1]})),
            json!([2, 1])
        );
        assert_eq!(
            call("$F.reverse($INPUT.a)", json!({"a": [1, 2]})),
            json!([2, 1])
        );
        assert_eq!(call("$F.getListItem('héllo', 1)", json!({})), json!("é"));
        assert_eq!(
            call("$F.getListItem($INPUT.a, -1)", json!({"a": [9]})),
            json!(9)
        );
        assert!(eval_str("$F.getListItem($INPUT.a, 5)", &roots(json!({"a": [9]}))).is_err());
        assert_eq!(
            call("$F.sort($INPUT.a)", json!({"a": [3, 1, 2]})),
            json!([1, 2, 3])
        );
        assert_eq!(
            call("$F.sort($INPUT.a, None, true)", json!({"a": [3, 1, 2]})),
            json!([3, 2, 1])
        );
        assert_eq!(
            call("$F.sort($INPUT.a, 'k')", json!({"a": [{"k": 3}, {"k": 1}]})),
            json!([{"k": 1}, {"k": 3}])
        );
        // insertion order preserved (Python dict order; the Go engine sorted
        // keys instead and documents that as a divergence)
        assert_eq!(
            call("$F.keys($INPUT.a)", json!({"a": {"b": 1, "a": 2, "c": 3}})),
            json!(["b", "a", "c"])
        );
        assert_eq!(
            call("$F.values($INPUT.a)", json!({"a": {"b": 1, "a": 2}})),
            json!([1, 2])
        );
        assert_eq!(
            call("$F.items($INPUT.a)", json!({"a": {"b": 1}})),
            json!([["b", 1]])
        );
        assert_eq!(
            call("$F.get($INPUT.a, 'k')", json!({"a": {"k": 5}})),
            json!(5)
        );
        assert_eq!(
            call("$F.get($INPUT.a, 'no', 7)", json!({"a": {"k": 5}})),
            json!(7)
        );
        assert_eq!(
            call("$F.get($INPUT.a, 'no')", json!({"a": {"k": 5}})),
            json!(null)
        );
        // pop is a mutator: live roots are refused (plaita#69); fresh
        // containers are the legal targets
        assert_eq!(
            call(
                "$F.pop($F.slice($INPUT.a, 0, 3), -1)",
                json!({"a": [1, 2, 3]})
            ),
            json!(3)
        );
        assert_eq!(
            call("$F.pop($F.json_loads('{\"k\": 5}'), 'k')", json!({})),
            json!(5)
        );
    }

    #[test]
    fn json_and_logic() {
        let e = json!({});
        assert_eq!(
            call("$F.json_loads('{\"a\": [1, 2.5]}')", e.clone()),
            json!({"a": [1, 2.5]})
        );
        assert!(eval_str("$F.json_loads('[1, 2,]')", &roots(e.clone())).is_err());
        assert_eq!(
            call(
                "$F.json_dumps($INPUT.a)",
                json!({"a": {"b": 2, "c": [1.0]}})
            ),
            json!("{\"b\": 2, \"c\": [1.0]}")
        );
        assert_eq!(
            call("$F.json_dumps('é中')", e.clone()),
            json!("\"\\u00e9\\u4e2d\"")
        );
        assert_eq!(call("$F.and(1, 2)", e.clone()), json!(2));
        assert_eq!(call("$F.and(0, 2)", e.clone()), json!(0));
        assert_eq!(call("$F.or('', 0, '3')", e.clone()), json!("3"));
        assert_eq!(call("$F.or(0, '')", e.clone()), json!(false));
        assert_eq!(call("$F.not('x')", e.clone()), json!(false));
        assert_eq!(call("$F.not('')", e), json!(true));
    }

    #[test]
    fn compare_fns() {
        let e = json!({});
        assert_eq!(call("$F.eq(1, 1.0)", e.clone()), json!(true));
        assert_eq!(call("$F.eq('a', 'b')", e.clone()), json!(false));
        assert_eq!(call("$F.ne(1, 2)", e.clone()), json!(true));
        assert_eq!(call("$F.gt(3, 2)", e.clone()), json!(true));
        assert_eq!(call("$F.gte(2, 2.0)", e.clone()), json!(true));
        assert_eq!(call("$F.lt('a', 'b')", e.clone()), json!(true));
        assert_eq!(call("$F.lte(2, 1)", e.clone()), json!(false));
        assert_eq!(call("$F.ifelse(1, 'yes', 'no')", e.clone()), json!("yes"));
        assert_eq!(call("$F.ifelse('', 'yes', 'no')", e.clone()), json!("no"));
        assert_eq!(call("$F.contains('hello', 'ell')", e.clone()), json!(true));
        assert_eq!(
            call("$F.notContains($INPUT.a, 3)", json!({"a": [1, 2]})),
            json!(true)
        );
        assert_eq!(
            call("$F.in(1, $INPUT.a)", json!({"a": [1, 2]})),
            json!(true)
        );
        assert_eq!(
            call("$F.notIn(3, $INPUT.a)", json!({"a": [1, 2]})),
            json!(true)
        );
        assert_eq!(
            call("$F.between(5, $INPUT.a)", json!({"a": [1, 10]})),
            json!(true)
        );
        assert_eq!(call("$F.like('hello', 'ell')", e), json!(true));
    }

    #[test]
    fn dates_render() {
        let v = call("$F.now('%Y-%m-%dT%H:%M:%S')", json!({}));
        let s = v.as_str().unwrap();
        assert_eq!(s.len(), 19);
        assert_eq!(&s[4..5], "-");
        let v = call("$F.today('%Y/%m/%d')", json!({}));
        assert!(v.as_str().unwrap().contains('/'));
    }

    #[test]
    fn audit_fix_regressions() {
        // getListItem 对 dict 的字符串键可达（Python a['k'] 语义；Go 同型缺陷 plaita-go#42）
        assert_eq!(
            call("$F.getListItem($INPUT.a, 'k')", json!({"a": {"k": 5}})),
            json!(5)
        );
        assert!(eval_str(
            "$F.getListItem($INPUT.a, 'nope')",
            &roots(json!({"a": {"k": 5}}))
        )
        .is_err());
        // sort：比较失败是 TypeError（先前静默返回未排序列表）
        assert!(eval_str("$F.sort($INPUT.a)", &roots(json!({"a": [3, "x", 1]}))).is_err());
        // sort dict 缺键回退 None → None 与值混排同样 TypeError（Python x.get(k)）
        assert!(eval_str(
            "$F.sort($INPUT.a, 'k')",
            &roots(json!({"a": [{"k": 2}, {}]}))
        )
        .is_err());
        // mul 超 1 亿：具名错误（先前静默截断到 1 亿）
        assert!(eval_str("$F.mul('a', 150000000)", &roots(json!({}))).is_err());
    }

    #[test]
    fn none_zero_value_bridge() {
        // plaita passes None into functions; scalars take their zero value,
        // mappings take the nil-map zero value — except len(None), which is a
        // TypeError exactly like Python.
        assert_eq!(call("$F.lower($INPUT.missing)", json!({})), json!(""));
        assert_eq!(call("$F.keys($INPUT.missing)", json!({})), json!([]));
        assert!(eval_str("$F.len($INPUT.missing)", &roots(json!({}))).is_err());
    }

    // -- #5：$F.mul 结果规模闸（Python _size_refusal 同款，分配之前具名拒绝）--

    fn mul_oversize_input() -> Value {
        json!({"s": "a".repeat(100), "a": (0..100).collect::<Vec<i64>>()})
    }

    #[test]
    fn mul_refuses_projected_oversize() {
        // 次数闸（≤1e8）放行 100 元素 ×1e8 = 1e10 元素 ≈760GB——规模闸必须拦：
        // str 与 list、正序与逆序、字面量与运行期数据同一道闸
        for expr in [
            "$F.mul('ab', 100000000)",
            "$F.mul(100000000, 'ab')",
            "$F.mul($INPUT.s, 200000)",
            "$F.mul(200000, $INPUT.s)",
            "$F.mul($INPUT.a, 100000)",
            "$F.mul(100000, $INPUT.a)",
        ] {
            let err = eval_str(expr, &roots(mul_oversize_input()))
                .err()
                .unwrap_or_else(|| panic!("{expr}: expected refusal, got Ok"));
            let msg = err.to_string();
            assert!(msg.starts_with("ValueError: "), "{expr}: {msg}");
            assert!(msg.contains("result too large"), "{expr}: {msg}");
            assert!(
                msg.contains("Refusing to build an unbounded result."),
                "{expr}: {msg}"
            );
        }
        // 文案与 Python 逐字同款：seq_len * n、投影数、limit、单位
        let msg = eval_str("$F.mul('ab', 100000000)", &roots(json!({})))
            .err()
            .unwrap()
            .to_string();
        assert_eq!(
            msg,
            "ValueError: $F.mul(2 * 100000000) result too large: 200000000 chars \
             (limit 100000). Refusing to build an unbounded result."
        );
        let msg = eval_str("$F.mul($INPUT.a, 100000)", &roots(mul_oversize_input()))
            .err()
            .unwrap()
            .to_string();
        assert!(
            msg.contains("$F.mul(100 * 100000) result too large: 10000000 elements (limit 100000)"),
            "{msg}"
        );
    }

    #[test]
    fn mul_scale_gate_matches_python_budget() {
        // 闸不能过紧：4× 放大系数必须放行（Python 实测口径，pre-fix 已绿的
        // 防错断言——漏了 GROWTH_FACTOR 会把这几条误杀）
        assert_eq!(
            call("$F.mul($INPUT.s, 3)", json!({"s": "a".repeat(50000)}))
                .as_str()
                .unwrap()
                .len(),
            150000
        );
        assert_eq!(
            call("$F.mul($INPUT.s, 2)", json!({"s": "a".repeat(1000000)}))
                .as_str()
                .unwrap()
                .len(),
            2000000
        );
        // 边界：投影 == limit 放行（Python 比较是严格大于）
        assert_eq!(
            call("$F.mul($INPUT.s, 4)", json!({"s": "a".repeat(25000)}))
                .as_str()
                .unwrap()
                .len(),
            100000
        );
        // 越界一档：limit 跟着输入规模走（4×50000=200000）
        let msg = eval_str(
            "$F.mul($INPUT.s, 5)",
            &roots(json!({"s": "a".repeat(50000)})),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            msg.contains("$F.mul(50000 * 5) result too large: 250000 chars (limit 200000)"),
            "{msg}"
        );
        // 空序列 × 巨数：Python 是合法的空结果（projected 0），次数闸口径误杀过它
        assert_eq!(call("$F.mul('', 200000000)", json!({})), json!(""));
        assert_eq!(
            call("$F.mul($INPUT.e, 200000000)", json!({"e": []})),
            json!([])
        );
    }

    #[test]
    fn mul_semantics_unchanged_under_scale_gate() {
        // 语义不回归：短串/数组/负零倍数/整数溢出/浮点全原样
        let e = json!({});
        assert_eq!(call("$F.mul('ab', 3)", e.clone()), json!("ababab"));
        assert_eq!(call("$F.mul('ab', -1)", e.clone()), json!(""));
        assert_eq!(call("$F.mul('ab', 0)", e.clone()), json!(""));
        // 文法没有列表字面量——数组两侧方向都走 $INPUT
        assert_eq!(
            call("$F.mul($INPUT.l, 2)", json!({"l": [1]})),
            json!([1, 1])
        );
        assert_eq!(
            call("$F.mul(2, $INPUT.l)", json!({"l": ["x"]})),
            json!(["x", "x"])
        );
        assert_eq!(call("$F.mul(3, 4)", e.clone()), json!(12));
        assert_eq!(call("$F.mul(3, 4.5)", e.clone()), json!(13.5));
        assert!(eval_str("$F.mul(9223372036854775807, 2)", &roots(e)).is_err());
    }

    // -- #7：i64::MIN 取负族（sub/abs）与 round 的 ndigits 值域 --------------

    fn overflow_of(expr: &str, input: Value) -> String {
        let msg = eval_str(expr, &roots(input))
            .err()
            .unwrap_or_else(|| panic!("{expr}: expected OverflowError, got Ok"))
            .to_string();
        assert!(msg.starts_with("OverflowError: "), "{expr}: {msg}");
        msg
    }

    #[test]
    fn int_negation_family_is_checked() {
        // abs(i64::MIN) = 2^63 出 i64 域：与同文件 add/mul/pow 同款 OverflowError
        //（AGENTS 文档化残余「>i64 报 OverflowError」；Python 给 2**63，plaita-go
        // 静默回负——负值不落任何允许出口）。字面量与运行期数据两条路径。
        let min = json!({"a": -9223372036854775808i64});
        for expr in ["$F.abs(-9223372036854775808)", "$F.abs($INPUT.a)"] {
            overflow_of(expr, min.clone());
        }
        // sub(0, MIN) 真值 2^63 同款；pre-fix 的 -bi 在 MIN 上自身溢出
        //（release 回绕出静默负值 / debug panic）
        for expr in ["$F.sub(0, -9223372036854775808)", "$F.sub(0, $INPUT.a)"] {
            overflow_of(expr, min.clone());
        }
        // sub(MIN, MIN) 真值 0 在域内：checked_sub 直接判定（Python: 0，
        // 不是 OverflowError——不用「先取负再 checked_add」的写法）
        assert_eq!(call("$F.sub($INPUT.a, $INPUT.a)", min.clone()), json!(0));
        // 正控：已 checked 的路径不回归
        overflow_of("$F.sub(-9223372036854775808, 1)", min.clone());
        overflow_of("$F.add(-9223372036854775808, -1)", min.clone());
        overflow_of("$F.mul(-9223372036854775808, -1)", min.clone());
        overflow_of("$F.pow(2, 64)", json!({}));
        // 常规语义
        let e = json!({});
        assert_eq!(call("$F.sub(5, 2)", e.clone()), json!(3));
        assert_eq!(call("$F.sub(5, 2.5)", e.clone()), json!(2.5));
        assert_eq!(call("$F.abs(-5)", e.clone()), json!(5));
        assert_eq!(call("$F.abs(-5.5)", e.clone()), json!(5.5));
    }

    #[test]
    fn round_ndigits_domain_is_python_exact() {
        let e = json!({});
        // i32 截断钉子（不涉及任何溢出）：4294967297 as i32 == 1 → 旧实现 1.2
        assert_eq!(
            call("$F.round(1.2345, 4294967297)", e.clone()),
            json!(1.2345)
        );
        // 2^31 两个方向（pre：debug panic / release OverflowError nan）
        assert_eq!(call("$F.round(1.5, 2147483648)", e.clone()), json!(1.5));
        assert_eq!(call("$F.round(1.5, -2147483648)", e.clone()), json!(0.0));
        // -0.0 符号保留：JSON 数值比较区分不了 ±0，走位型断言
        let neg0 = call("$F.round(-1.5, -2147483648)", e.clone());
        assert_eq!(neg0.as_f64().unwrap().to_bits(), (-0.0f64).to_bits());
        assert_eq!(call("$F.round(1.5, -400)", e.clone()), json!(0.0));
        // 正向大 n 不是恒等：按位舍入的真实区间语义（1e-20 的有效位在
        // 第 20 位小数）——旧 min(17) 钳位在这里给出 0.0 错值
        assert_eq!(call("$F.round(1e-20, 18)", e.clone()), json!(0.0));
        assert_eq!(call("$F.round(1e-20, 20)", e.clone()), json!(1e-20));
        assert_eq!(call("$F.round(1e-20, 4294967297)", e.clone()), json!(1e-20));
        // 正控不回归
        assert_eq!(call("$F.round(1.5, 0)", e.clone()), json!(2.0));
        assert_eq!(call("$F.round(1234.5, -3)", e.clone()), json!(1000.0));
        assert_eq!(call("$F.round(1.5, 20)", e.clone()), json!(1.5));
        assert_eq!(call("$F.round(2.5)", e.clone()), json!(2));
        assert_eq!(call("$F.round(1.2345, 2)", e.clone()), json!(1.23));
        assert_eq!(call("$F.round(3, 1)", e.clone()), json!(3));
    }

    // -- #6：$F.sort 算法改制的守恒锚与性能分母 ------------------------------

    #[test]
    fn sort_behavior_conserved() {
        // 稳定性：相等键保持原相对顺序（Python Timsort / Go SliceStable 同约）
        let st = json!({"a": [{"k": 1, "i": 0}, {"k": 1, "i": 1}, {"k": 0, "i": 2}]});
        assert_eq!(
            call("$F.sort($INPUT.a, 'k')", st.clone()),
            json!([{"k": 0, "i": 2}, {"k": 1, "i": 0}, {"k": 1, "i": 1}])
        );
        assert_eq!(
            call("$F.sort($INPUT.a, 'k', true)", st),
            json!([{"k": 1, "i": 0}, {"k": 1, "i": 1}, {"k": 0, "i": 2}])
        );
        // 字符串序
        assert_eq!(
            call("$F.sort($INPUT.a)", json!({"a": ["b", "a", "c"]})),
            json!(["a", "b", "c"])
        );
        // None 键参与比较仍是 TypeError（缺键回退 None 的既有语义）
        assert!(eval_str(
            "$F.sort($INPUT.a, 'k')",
            &roots(json!({"a": [{"k": Value::Null}, {"k": 1}]}))
        )
        .is_err());
    }

    #[test]
    fn sort_large_input_within_budget() {
        // #6 性能分母（防退化）：64k 倒序整数，旧插入排序 release 实测
        // 17.56s（debug 分钟级），O(n log n) 在 debug 下也远低于 5s。
        // 结果守恒断言同测：升序且恰为 0..64000（多重集不变）。
        let input: Vec<i64> = (0..64_000).rev().collect();
        let t0 = std::time::Instant::now();
        let out = eval_str("$F.sort($INPUT)", &roots(json!(input))).unwrap();
        let dt = t0.elapsed();
        let arr = out.as_array().unwrap();
        assert_eq!(arr.len(), 64_000);
        assert_eq!(arr[0].as_i64().unwrap(), 0);
        assert_eq!(arr[63_999].as_i64().unwrap(), 63_999);
        assert!(arr
            .windows(2)
            .all(|w| w[0].as_i64().unwrap() <= w[1].as_i64().unwrap()));
        assert!(
            dt < std::time::Duration::from_secs(5),
            "sort 64k took {dt:?}"
        );
    }
}
