//! Flow IR source + definition fingerprint (§4.6, port of flowsource.go +
//! canonicaljson.go): the IR lives in the engine-storage shape
//!
//! ```text
//! {ns}:flow:{flow_id}:{version}   = definition JSON (SET, no TTL)
//! {ns}:flow_list                  = SET of flow_id
//! {ns}:flow_versions:{flow_id}    = SET of version
//! ```
//!
//! With no explicit version the reader prefers the literal `latest` member,
//! then the numerically-largest version (storage/redis.py:764-782).
//!
//! The fingerprint must be BYTE-EQUAL to Python's
//! `compute_flow_definition_hash`: sha256 over
//! `json.dumps(json.loads(raw), sort_keys=True, ensure_ascii=False,
//! separators=(",",":"))`. serde_json cannot serve (its float presentation
//! and big-int handling differ from CPython), so the canonicalizer below is
//! a faithful port of Go's canonicaljson.go: a strict one-document JSON
//! scan that preserves number literals and re-renders them through Python's
//! float repr rules. Acceptance = the `testdata/flow_hash_golden.json`
//! fixture (exported from the live Python engine).

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::config::Config;
use crate::error::{ErrorKind, WorkerError};
use crate::keys;

/// The rs-stamped algorithm marker. Same rationale as Go's `-v2-go` (#60):
/// a shared label on a separately-maintained implementation is exactly the
/// "same name, different algorithm" blind spot. The resume gate compares
/// HASHES only — the label difference surfaces in upgrade_suspected
/// diagnostics, never blocks a hash-equal resume.
pub const FLOW_HASH_ALGO: &str = "flow-definition-json-sortkeys-v2-rs";

/// The empty-version sentinel (engine storage tracks it as a literal member
/// of the versions set).
pub const LATEST_VERSION: &str = "latest";

/// A resolved flow: raw deployment JSON + fingerprint + the loaded engine
/// flow.
#[derive(Debug)]
pub struct FlowDefinition {
    pub flow_id: String,
    pub version: String,
    pub raw: Vec<u8>,
    pub hash: String,
    pub hash_algo: String,
    pub workflow: plaita_engine::Flow,
}

/// Canonical flow-definition JSON: the byte-exact rs counterpart of Python
/// `canonical_flow_definition_json`. Strict RFC-8259: one document exactly
/// (trailing data is an error), NaN/Infinity literals rejected, lone
/// surrogate escapes rejected by name (A129-01 — Python's hash chain
/// refuses them at utf-8 encode, so accepting them here would fold both
/// sides to U+FFFD and hash a definition the Python pool can never hash).
pub fn canonical_flow_definition_json(raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut p = Parser {
        b: raw,
        i: 0,
        depth: 0,
    };
    p.skip_ws();
    let v = p.parse_value()?;
    p.skip_ws();
    if p.i != p.b.len() {
        return Err("trailing data after flow definition JSON".to_string());
    }
    Ok(emit(&v))
}

/// The flow fingerprint (§4.6): sha256 over the canonical form.
pub fn flow_definition_hash(raw: &[u8]) -> Result<String, String> {
    let canonical = canonical_flow_definition_json(raw)?;
    let sum = Sha256::digest(&canonical);
    let mut hex = String::with_capacity(64);
    for b in sum {
        hex.push_str(&format!("{:02x}", b));
    }
    Ok(hex)
}

// -- canonical value model ----------------------------------------------------

/// Values with EXACT number literals preserved (Python int/float split:
/// a literal without `.`/`e` is an int and keeps its digits verbatim).
#[derive(Debug, Clone)]
enum CVal {
    Null,
    Bool(bool),
    Str(String),
    Int(String),   // exact decimal digits (normalized: -0 → 0)
    Float(String), // rendered through python_float_repr
    Arr(Vec<CVal>),
    // sort_keys=True: Python compares str keys by code point; UTF-8 byte
    // order equals it for valid Unicode, so BTreeMap<String, _> sorts right.
    // Duplicate keys: Python json.loads keeps the LAST value.
    Obj(BTreeMap<String, CVal>),
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
    depth: u32,
}

const MAX_DEPTH: u32 = 512; // defensive; flow definitions nest shallowly

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(format!(
                "flow definition json: expected {:?} at byte {}",
                c as char, self.i
            ))
        }
    }

    fn parse_value(&mut self) -> Result<CVal, String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("flow definition json: nesting too deep".to_string());
        }
        let out = match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(CVal::Str),
            Some(b't') => self.parse_lit("true", CVal::Bool(true)),
            Some(b'f') => self.parse_lit("false", CVal::Bool(false)),
            Some(b'n') => self.parse_lit("null", CVal::Null),
            // RFC-8259-INVALID literals: Python's parse_constant accepts
            // NaN/Infinity, its dumps re-emits them — but the Go decoder
            // refuses them (recorded residual in the golden fixture), and
            // so does this decoder (fail-closed for the mixed fleet).
            Some(b'N') => {
                Err("flow definition json: NaN literal is not valid RFC-8259 JSON".into())
            }
            Some(b'I') => {
                Err("flow definition json: Infinity literal is not valid RFC-8259 JSON".into())
            }
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            Some(c) => Err(format!(
                "flow definition json: unexpected byte {:?} at {}",
                c as char, self.i
            )),
            None => Err("flow definition json: unexpected end of input".to_string()),
        };
        self.depth -= 1;
        out
    }

    fn parse_lit(&mut self, lit: &str, v: CVal) -> Result<CVal, String> {
        if self.b[self.i..].starts_with(lit.as_bytes()) {
            self.i += lit.len();
            Ok(v)
        } else {
            Err(format!(
                "flow definition json: invalid literal at {}",
                self.i
            ))
        }
    }

    fn parse_object(&mut self) -> Result<CVal, String> {
        self.expect(b'{')?;
        let mut map = BTreeMap::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(CVal::Obj(map));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let value = self.parse_value()?;
            map.insert(key, value); // dup keys: last wins (Python json.loads)
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b'}') => {
                    self.i += 1;
                    return Ok(CVal::Obj(map));
                }
                _ => return Err(format!("flow definition json: bad object at {}", self.i)),
            }
        }
    }

    fn parse_array(&mut self) -> Result<CVal, String> {
        self.expect(b'[')?;
        let mut arr = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(CVal::Arr(arr));
        }
        loop {
            self.skip_ws();
            arr.push(self.parse_value()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b']') => {
                    self.i += 1;
                    return Ok(CVal::Arr(arr));
                }
                _ => return Err(format!("flow definition json: bad array at {}", self.i)),
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let Some(c) = self.peek() else {
                return Err("flow definition json: unterminated string".to_string());
            };
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let Some(esc) = self.peek() else {
                        return Err("flow definition json: bad escape".to_string());
                    };
                    self.i += 1;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let cp = self.parse_hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                // high surrogate: must pair
                                if self.b[self.i..].starts_with(b"\\u") {
                                    self.i += 2;
                                    let lo = self.parse_hex4()?;
                                    if (0xDC00..0xE000).contains(&lo) {
                                        let combined =
                                            0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                        out.push(char::from_u32(combined).ok_or_else(|| {
                                            "flow definition json: bad surrogate pair".to_string()
                                        })?);
                                    } else {
                                        return Err(
                                            "flow definition json: lone leading surrogate in \
                                             \\u escape (A129-01)"
                                                .to_string(),
                                        );
                                    }
                                } else {
                                    return Err(
                                        "flow definition json: lone leading surrogate in \\u \
                                         escape (A129-01)"
                                            .to_string(),
                                    );
                                }
                            } else if (0xDC00..0xE000).contains(&cp) {
                                return Err(
                                    "flow definition json: lone trailing surrogate in \\u \
                                     escape (A129-01)"
                                        .to_string(),
                                );
                            } else {
                                out.push(char::from_u32(cp).ok_or_else(|| {
                                    "flow definition json: bad code point".to_string()
                                })?);
                            }
                        }
                        _ => return Err("flow definition json: bad escape char".to_string()),
                    }
                }
                c if c < 0x20 => {
                    return Err("flow definition json: raw control character in string".to_string())
                }
                _ => {
                    // collect the full UTF-8 sequence
                    let start = self.i - 1;
                    let width = utf8_width(c).ok_or_else(|| {
                        "flow definition json: invalid utf-8 in string".to_string()
                    })?;
                    let end = start + width;
                    if end > self.b.len() {
                        return Err("flow definition json: truncated utf-8".to_string());
                    }
                    let s = std::str::from_utf8(&self.b[start..end])
                        .map_err(|_| "flow definition json: invalid utf-8 in string".to_string())?;
                    out.push_str(s);
                    self.i = end;
                }
            }
        }
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        if self.i + 4 > self.b.len() {
            return Err("flow definition json: truncated \\u escape".to_string());
        }
        let s = std::str::from_utf8(&self.b[self.i..self.i + 4])
            .map_err(|_| "flow definition json: bad \\u escape".to_string())?;
        let cp = u32::from_str_radix(s, 16)
            .map_err(|_| "flow definition json: bad \\u escape".to_string())?;
        self.i += 4;
        Ok(cp)
    }

    fn parse_number(&mut self) -> Result<CVal, String> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        let int_start = self.i;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.i += 1;
        }
        if self.i == int_start {
            return Err(format!("flow definition json: bad number at {start}"));
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.i += 1;
            let frac_start = self.i;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.i += 1;
            }
            if self.i == frac_start {
                return Err(format!("flow definition json: bad number at {start}"));
            }
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            is_float = true;
            self.i += 1;
            if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                self.i += 1;
            }
            let exp_start = self.i;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.i += 1;
            }
            if self.i == exp_start {
                return Err(format!("flow definition json: bad number at {start}"));
            }
        }
        let lit = std::str::from_utf8(&self.b[start..self.i])
            .map_err(|_| "flow definition json: bad number".to_string())?;
        if !is_float {
            // Python int: exact decimal digits; json.loads("-0") → 0
            if lit == "-0" {
                return Ok(CVal::Int("0".to_string()));
            }
            return Ok(CVal::Int(lit.to_string()));
        }
        // Python float: IEEE double rendered by float.__repr__. Rust's f64
        // parse saturates ("1e400" → inf, "1e-400" → 0.0), matching Python
        // json.loads + dumps's allow_nan spelling.
        let f: f64 = lit
            .parse()
            .map_err(|_| "flow definition json: bad float".to_string())?;
        Ok(CVal::Float(python_float_repr(f)))
    }
}

fn utf8_width(b: u8) -> Option<usize> {
    match b {
        0x00..=0x7F => Some(1),
        0xC2..=0xDF => Some(2),
        0xE0..=0xEF => Some(3),
        0xF0..=0xF4 => Some(4),
        _ => None,
    }
}

/// Renders the canonical text: CPython encoder rules with
/// ensure_ascii=False — only the ESCAPE_DCT entries are escaped; `<`, `>`,
/// `&`, `/`, DEL, U+2028/2029 and all non-ASCII are raw UTF-8.
fn emit(v: &CVal) -> Vec<u8> {
    let mut out = Vec::new();
    emit_into(&mut out, v);
    out
}

fn emit_into(out: &mut Vec<u8>, v: &CVal) {
    match v {
        CVal::Null => out.extend_from_slice(b"null"),
        CVal::Bool(true) => out.extend_from_slice(b"true"),
        CVal::Bool(false) => out.extend_from_slice(b"false"),
        CVal::Str(s) => emit_python_string(out, s),
        CVal::Int(lit) => out.extend_from_slice(lit.as_bytes()),
        CVal::Float(repr) => out.extend_from_slice(repr.as_bytes()),
        CVal::Arr(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                emit_into(out, item);
            }
            out.push(b']');
        }
        CVal::Obj(map) => {
            out.push(b'{');
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                emit_python_string(out, k);
                out.push(b':');
                emit_into(out, v);
            }
            out.push(b'}');
        }
    }
}

fn emit_python_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{0008}' => out.extend_from_slice(b"\\b"),
            '\u{000C}' => out.extend_from_slice(b"\\f"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
            }
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
}

/// Renders f the way CPython float.__repr__ does (shortest round-trip
/// digits, fixed notation while the decimal point sits in (-4, 16],
/// exponential form with ≥2 exponent digits outside it, ".0" on integral
/// values, json.dumps's allow_nan spelling out of range). The shortest
/// round-trip digit string is unique per double, so Rust/Go/CPython always
/// agree on the digits — only the presentation differs and is rebuilt here.
fn python_float_repr(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let mut sign = "";
    let mut f = f;
    if f.is_sign_negative() {
        sign = "-";
        f = -f;
    }
    if f == 0.0 {
        return format!("{sign}0.0");
    }
    // Rust's LowerExp produces the shortest round-trip digits: "d.ddde<exp>"
    let s = format!("{f:e}");
    let (mantissa, exp) = s.split_once('e').expect("LowerExp always has e");
    let exp: i32 = exp.parse().expect("LowerExp exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let decpt = exp + 1; // value = 0.<digits> × 10^decpt

    let mut out = String::from(sign);
    if decpt <= -4 || decpt > 16 {
        // exponential form, ≥2 exponent digits (Python: "1e-05", "1e+100")
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        let e = decpt - 1;
        if e < 0 {
            out.push('-');
        } else {
            out.push('+');
        }
        let es = e.abs().to_string();
        if es.len() < 2 {
            out.push('0');
        }
        out.push_str(&es);
    } else if decpt <= 0 {
        out.push_str("0.");
        out.push_str(&"0".repeat((-decpt) as usize));
        out.push_str(&digits);
    } else if decpt as usize >= digits.len() {
        out.push_str(&digits);
        out.push_str(&"0".repeat(decpt as usize - digits.len()));
        out.push_str(".0");
    } else {
        out.push_str(&digits[..decpt as usize]);
        out.push('.');
        out.push_str(&digits[decpt as usize..]);
    }
    out
}

// -- storage shape -------------------------------------------------------------

/// Reads a flow definition from the engine-storage shape. Deterministic
/// missing/unloadable → `ErrFlowNotFound` family; transient read failure →
/// `ErrFlowLoad` (both named on WorkerError).
pub fn get_flow_definition(
    backend: &dyn crate::backend::RedisBackend,
    cfg: &Config,
    tenant: &str,
    flow_id: &str,
    version: &str,
) -> Result<FlowDefinition, WorkerError> {
    let resolved = if version.is_empty() {
        resolve_version(backend, cfg, tenant, flow_id)?
    } else {
        version.to_string()
    };
    let raw = backend
        .get(&keys::flow_key(cfg, tenant, flow_id, &resolved))
        .map_err(|e| WorkerError::new(ErrorKind::FlowLoad, format!("flow definition read: {e}")))?;
    let Some(raw) = raw else {
        return Err(WorkerError::new(
            ErrorKind::FlowNotFound,
            format!(
                "flow definition not found in engine storage: flow {flow_id}:{resolved} \
                 (tenant {tenant:?})"
            ),
        ));
    };
    let hash = flow_definition_hash(raw.as_bytes())
        .map_err(|e| WorkerError::new(ErrorKind::FlowLoad, format!("fingerprint: {e}")))?;
    // The IR loader is fail-closed: an unloadable flow is a permanent,
    // deterministic failure for this engine build (AGENTS.md 拒载清单).
    let workflow = plaita_engine::ir::load(raw.as_bytes()).map_err(|e| {
        WorkerError::new(
            ErrorKind::FlowNotFound,
            format!("flow {flow_id}:{resolved} not loadable: {e}"),
        )
    })?;
    Ok(FlowDefinition {
        flow_id: flow_id.to_string(),
        version: resolved,
        raw: raw.into_bytes(),
        hash,
        hash_algo: FLOW_HASH_ALGO.to_string(),
        workflow,
    })
}

/// The empty-version rule: prefer the literal `latest` member, else the
/// numerically-largest member of the versions set (§4.6).
fn resolve_version(
    backend: &dyn crate::backend::RedisBackend,
    cfg: &Config,
    tenant: &str,
    flow_id: &str,
) -> Result<String, WorkerError> {
    let mut versions = backend
        .smembers(&keys::flow_versions_key(cfg, tenant, flow_id))
        .map_err(|e| WorkerError::new(ErrorKind::FlowLoad, format!("versions set: {e}")))?;
    if versions.is_empty() {
        return Ok(LATEST_VERSION.to_string());
    }
    if versions.iter().any(|v| v == LATEST_VERSION) {
        return Ok(LATEST_VERSION.to_string());
    }
    versions.sort_by(|a, b| {
        numeric_version_key(a)
            .partial_cmp(&numeric_version_key(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(versions.pop().unwrap_or_else(|| LATEST_VERSION.to_string()))
}

fn numeric_version_key(v: &str) -> f64 {
    v.trim_start_matches('v')
        .parse::<f64>()
        .unwrap_or(f64::NEG_INFINITY)
}

/// Writes a definition into the storage shape — used by tests and shadow
/// fixtures (console engine_sync is the production writer).
pub fn preset_flow_definition(
    backend: &dyn crate::backend::RedisBackend,
    cfg: &Config,
    tenant: &str,
    flow_id: &str,
    version: &str,
    raw: &[u8],
) -> Result<(), WorkerError> {
    use crate::backend::TxOp;
    backend
        .tx(&[
            TxOp::Set {
                key: keys::flow_key(cfg, tenant, flow_id, version),
                value: String::from_utf8(raw.to_vec())
                    .map_err(|e| WorkerError::transient(e.to_string()))?,
                ttl_secs: None,
            },
            TxOp::Sadd {
                key: keys::flow_list_key(cfg, tenant),
                member: flow_id.to_string(),
            },
            TxOp::Sadd {
                key: keys::flow_versions_key(cfg, tenant, flow_id),
                member: version.to_string(),
            },
        ])
        .map_err(|e| WorkerError::transient(format!("preset flow: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalization_matches_python_presentation_rules() {
        // integer literal passthrough (Python int)
        let c = canonical_flow_definition_json(br#"{"a": 1, "b": -0}"#).unwrap();
        assert_eq!(String::from_utf8(c).unwrap(), r#"{"a":1,"b":0}"#);
        // float presentation: 1e2 → 100.0, 1.00 → 1.0, 1e-3 → 0.001
        let c = canonical_flow_definition_json(br#"{"retry":1e2,"r2":1.00,"r3":1e-3}"#).unwrap();
        assert_eq!(
            String::from_utf8(c).unwrap(),
            r#"{"r2":1.0,"r3":0.001,"retry":100.0}"#
        );
        // exponential boundaries (Python repr): decpt<=16 fixed (1e15),
        // decpt=17 exponential (1e16), ≥2 exponent digits
        let c =
            canonical_flow_definition_json(br#"{"a":1e15,"b":1e16,"c":1e21,"d":1e-4,"e":1e-5}"#)
                .unwrap();
        assert_eq!(
            String::from_utf8(c).unwrap(),
            r#"{"a":1000000000000000.0,"b":1e+16,"c":1e+21,"d":0.0001,"e":1e-05}"#
        );
        // sort_keys + ensure_ascii=False + no HTML escaping
        let c =
            canonical_flow_definition_json(r#"{"z":"<&>","中文":1,"a":"line\nbreak"}"#.as_bytes())
                .unwrap();
        assert_eq!(
            String::from_utf8(c).unwrap(),
            "{\"a\":\"line\\nbreak\",\"z\":\"<&>\",\"中文\":1}"
        );
        // strict decode: trailing data / NaN / lone surrogate rejected
        assert!(canonical_flow_definition_json(br#"{} {}"#).is_err());
        assert!(canonical_flow_definition_json(b"NaN").is_err());
        assert!(canonical_flow_definition_json(b"Infinity").is_err());
        assert!(canonical_flow_definition_json(br#""\ud800""#).is_err());
        assert!(canonical_flow_definition_json(br#""\udc00""#).is_err());
        assert!(canonical_flow_definition_json(br#""\ud83d\ude00""#).is_ok()); // paired OK
                                                                               // out-of-range floats follow Python's allow_nan spelling
        let c = canonical_flow_definition_json(br#"{"a":1e400,"b":1e-400}"#).unwrap();
        assert_eq!(String::from_utf8(c).unwrap(), r#"{"a":Infinity,"b":0.0}"#);
    }

    #[test]
    fn fingerprint_is_deterministic_and_order_insensitive() {
        let h1 = flow_definition_hash(br#"{"a":1,"b":{"x":true,"y":null}}"#).unwrap();
        let h2 = flow_definition_hash(br#"{"b":{ "y":null, "x":true },"a":1}"#).unwrap();
        assert_eq!(h1, h2, "sorted keys → same hash");
        assert_eq!(h1.len(), 64);
        assert_ne!(flow_definition_hash(br#"{"a":2}"#).unwrap(), h1);
    }

    #[test]
    fn resolve_version_prefers_latest_then_numeric_max() {
        use crate::backend::{RedisBackend, TxOp};
        use crate::fake::FakeRedis;
        let cfg = crate::config::Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                "fs-test".to_string()
            } else {
                String::new()
            }
        });
        let backend = std::sync::Arc::new(FakeRedis::new());
        backend
            .tx(&[
                TxOp::Set {
                    key: keys::flow_key(&cfg, "", "f", "1"),
                    value: "{}".into(),
                    ttl_secs: None,
                },
                TxOp::Set {
                    key: keys::flow_key(&cfg, "", "f", "10"),
                    value: "{}".into(),
                    ttl_secs: None,
                },
                TxOp::Set {
                    key: keys::flow_key(&cfg, "", "f", "2"),
                    value: "{}".into(),
                    ttl_secs: None,
                },
                TxOp::Sadd {
                    key: keys::flow_versions_key(&cfg, "", "f"),
                    member: "1".into(),
                },
                TxOp::Sadd {
                    key: keys::flow_versions_key(&cfg, "", "f"),
                    member: "10".into(),
                },
                TxOp::Sadd {
                    key: keys::flow_versions_key(&cfg, "", "f"),
                    member: "2".into(),
                },
            ])
            .unwrap();
        // numeric max (10 beats 2 lexicographically)
        let err = get_flow_definition(backend.as_ref(), &cfg, "", "f", "").unwrap_err();
        // "{}" is not a loadable flow — the version RESOLUTION succeeded and
        // the failure is the deterministic not-loadable family naming 10
        assert!(
            err.to_string().contains(":10"),
            "resolved to numeric max, got: {err}"
        );
        // explicit version wins
        let err = get_flow_definition(backend.as_ref(), &cfg, "", "f", "2").unwrap_err();
        assert!(
            err.to_string().contains(":2"),
            "explicit version kept, got: {err}"
        );
        // no versions set → literal latest
        let err = get_flow_definition(backend.as_ref(), &cfg, "", "g", "").unwrap_err();
        assert!(
            err.to_string().contains(":latest"),
            "fallback to latest, got: {err}"
        );
    }
}
