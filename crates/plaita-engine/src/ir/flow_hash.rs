//! 流程定义指纹门（plaita#60 语义，B4 r11 全量入位第一步）。
//!
//! 权威算法 = Python `plaita/server/flow_worker.py`
//! `canonical_flow_definition_json` + `compute_flow_definition_hash`：
//! `json.dumps(doc, sort_keys=True, ensure_ascii=False, separators=(",",":"))`
//! 过 **json.loads 解析后的存储原始定义**（model_validate 之前的「作者写下
//! 的语义」）→ sha256 hexdigest。Go 侧字节级复刻在 plaita-go
//! `pkg/worker/canonicaljson.go`（#60 的参照实现，本文件与其同构；
//! 字节等价性由 62 案 golden fixture 对拍 Python 钉死，
//! `testdata/flow_hash_golden.json` 逐字复用，禁止手改）。
//!
//! 为什么不用 serde_json 序列化：serde 会把数值解析成 f64 再按 Rust 规则
//! 重渲染（`1e2`→`1e2` vs Python `100.0`、>u64 大整数丢精度成 f64）——
//! 每一项都翻指纹。这里的扫描器**保留数值字面量原文**，渲染时才按
//! Python 规则重放（整数逐字透传；浮点走 float.__repr__ 口径：最短
//! round-trip 数字、小数点位置 ∈ (-4,16] 用定点否则指数（指数 ≥2 位）、
//! 整数值带 `.0` 尾、`1e400`→`Infinity`、`1e-400`→`0.0`、`-0`→`0`）。
//!
//! 解析域与 Go 同口径：恰好一个 JSON 文档（尾随数据=错）、RFC-8259 非法
//! 字面量（`NaN`/`Infinity` 裸词）拒绝、孤立代理逃逸具名拒绝（A1 r132 卡
//! G-1：Python 的 json.loads 收得进 str 但指纹链在 `.encode("utf-8")`
//! 处拒——解码层若把它们折叠成 U+FFFD，就会给「Python 永远算不出指纹
//! 的定义」算出指纹。serde_json 本就拒绝孤立代理，ir::load 的主路径
//! 先一步拦下；本扫描器自带同款拒绝，保证独立调用面同样 fail-closed）。
//!
//! 内嵌门契约（引擎装载路径）：部署定义可携带顶层保留键
//! `definitionHash`（+可选 `hashAlgo`）。装载前校验：无 `definitionHash`
//! = 跳过（向后兼容旧定义，Python classify 的 `no_guard` 同语义）；
//! `hashAlgo` 在场但非本引擎算法 = 具名拒（拒绝跨算法比摘要——比对 v1
//! 老口径只会得出假 mismatch）；不匹配 = `flow_hash_mismatch` 具名拒
//! （镜像 Go worker resume 硬门：mismatch→具名终态可判，不静默）。
//! 指纹计算域 = 定义**剥去保留键后**的文档——保留键是运载元数据，不是
//! 「作者写下的语义」的一部分；校验通过后两键从文档剥离，不进
//! normalize/引擎管道。resume 侧的 `allow_flow_hash_change` 逃生口属
//! worker 层（Python/Go），引擎装载门不给 override。

use serde_json::Value as Json;
use sha2::{Digest, Sha256};

/// 本引擎的指纹算法标签（Go 同款命名法：`flow-definition-json-sortkeys-v2-go`
/// / Python 为不带引擎尾的 `flow-definition-json-sortkeys-v2`）。摘要相等
/// 是跨引擎接受判据（62 案 golden 钉字节等价）；标签只参与「未知算法拒」。
pub const FLOW_HASH_ALGO: &str = "flow-definition-json-sortkeys-v2-rust";

/// 内嵌指纹的顶层保留键（校验通过后从文档剥离）。
pub const HASH_FIELD: &str = "definitionHash";

/// 算法标签保留键（可选；在场时必须等于 [`FLOW_HASH_ALGO`]）。
pub const HASH_ALGO_FIELD: &str = "hashAlgo";

/// 解析深度帽——镜像 serde_json 的递归限（装载主路径先用 serde 解析，
/// 更深的文档到不了本模块；对独立调用面兜底栈溢出）。
const MAX_DEPTH: usize = 128;

/// 扫描产物：JSON 值树，数值保留**字面量原文**（Python int/float 的
/// round-trip 规则在渲染时重放）。对象保插入序；重复键的值取最后一个
/// （json.loads parity），渲染时先去重再排序。
#[derive(Debug, Clone)]
enum CVal {
    Null,
    Bool(bool),
    Str(String),
    Num(String),
    Arr(Vec<CVal>),
    Obj(Vec<(String, CVal)>),
}

// ---------------------------------------------------------------- 扫描器

struct Scanner<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Scanner<'a> {
    fn error(&self, why: &str) -> String {
        format!(
            "ir: flow hash: invalid flow definition JSON at offset {}: {why}",
            self.i
        )
    }

    fn skip_ws(&mut self) {
        while matches!(self.b.get(self.i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    fn lit(&mut self, word: &[u8], v: CVal) -> Result<CVal, String> {
        if self.b[self.i..].starts_with(word) {
            self.i += word.len();
            Ok(v)
        } else {
            Err(self.error("invalid literal"))
        }
    }

    fn value(&mut self, depth: usize) -> Result<CVal, String> {
        if depth > MAX_DEPTH {
            return Err(self.error("nesting deeper than the engine parse domain"));
        }
        self.skip_ws();
        match self.b.get(self.i) {
            None => Err(self.error("unexpected end of input")),
            Some(b'n') => self.lit(b"null", CVal::Null),
            Some(b't') => self.lit(b"true", CVal::Bool(true)),
            Some(b'f') => self.lit(b"false", CVal::Bool(false)),
            Some(b'"') => Ok(CVal::Str(self.string()?)),
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(c) if *c == b'-' || c.is_ascii_digit() => Ok(CVal::Num(self.number()?)),
            Some(_) => Err(self.error("unexpected character")),
        }
    }

    fn array(&mut self, depth: usize) -> Result<CVal, String> {
        self.i += 1; // '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.b.get(self.i) == Some(&b']') {
            self.i += 1;
            return Ok(CVal::Arr(items));
        }
        loop {
            items.push(self.value(depth + 1)?);
            self.skip_ws();
            match self.b.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    return Ok(CVal::Arr(items));
                }
                _ => return Err(self.error("expected ',' or ']'")),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<CVal, String> {
        self.i += 1; // '{'
        let mut kvs: Vec<(String, CVal)> = Vec::new();
        self.skip_ws();
        if self.b.get(self.i) == Some(&b'}') {
            self.i += 1;
            return Ok(CVal::Obj(kvs));
        }
        loop {
            self.skip_ws();
            if self.b.get(self.i) != Some(&b'"') {
                return Err(self.error("expected a string key"));
            }
            let key = self.string()?;
            self.skip_ws();
            if self.b.get(self.i) != Some(&b':') {
                return Err(self.error("expected ':'"));
            }
            self.i += 1;
            let val = self.value(depth + 1)?;
            // json.loads parity: duplicate keys — the LAST value wins.
            match kvs.iter_mut().find(|(k, _)| *k == key) {
                Some(slot) => slot.1 = val,
                None => kvs.push((key, val)),
            }
            self.skip_ws();
            match self.b.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    return Ok(CVal::Obj(kvs));
                }
                _ => return Err(self.error("expected ',' or '}'")),
            }
        }
    }

    /// String body: escapes per JSON; lone surrogate escapes are named
    /// rejects (the Python hash chain refuses them at encode time), paired
    /// surrogates combine. Raw control chars <0x20 are invalid JSON.
    fn string(&mut self) -> Result<String, String> {
        self.i += 1; // opening '"'
        let mut out = String::new();
        loop {
            let start = self.i;
            while let Some(&c) = self.b.get(self.i) {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.i += 1;
            }
            // 多字节 UTF-8 序列不含 ASCII 字节，段切点必然落在边界上
            out.push_str(
                std::str::from_utf8(&self.b[start..self.i])
                    .map_err(|_| self.error("invalid UTF-8 in string"))?,
            );
            match self.b.get(self.i) {
                None => return Err(self.error("unterminated string")),
                Some(b'"') => {
                    self.i += 1;
                    return Ok(out);
                }
                Some(c) if *c < 0x20 => return Err(self.error("raw control character in string")),
                Some(b'\\') => {
                    self.i += 1;
                    let esc = self
                        .b
                        .get(self.i)
                        .copied()
                        .ok_or_else(|| self.error("unterminated escape"))?;
                    self.i += 1;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => out.push(self.unicode_escape()?),
                        _ => return Err(self.error("invalid escape")),
                    }
                }
                Some(_) => unreachable!("segment loop stopped on a non-raw byte"),
            }
        }
    }

    fn hex4(&mut self) -> Result<u16, String> {
        if self.i + 4 > self.b.len() {
            return Err(self.error("truncated \\u escape"));
        }
        let s = std::str::from_utf8(&self.b[self.i..self.i + 4])
            .map_err(|_| self.error("invalid \\u escape"))?;
        let v = u16::from_str_radix(s, 16).map_err(|_| self.error("invalid \\u escape"))?;
        self.i += 4;
        Ok(v)
    }

    fn unicode_escape(&mut self) -> Result<char, String> {
        let at = self.i - 2; // '\\u' 起始（诊断偏移）
        let hi = self.hex4()?;
        let lone = |hi: u16| {
            format!(
                "ir: flow hash: lone surrogate escape \\u{hi:04x} at offset {at}: not encodable \
                 to UTF-8 — outside the domain Python's flow-definition hash accepts"
            )
        };
        match hi {
            0xDC00..=0xDFFF => Err(lone(hi)),
            0xD800..=0xDBFF => {
                if self.b.get(self.i) == Some(&b'\\') && self.b.get(self.i + 1) == Some(&b'u') {
                    self.i += 2;
                    let lo = self.hex4()?;
                    if (0xDC00..=0xDFFF).contains(&lo) {
                        let cp = 0x10000 + ((hi as u32 - 0xD800) << 10) + (lo as u32 - 0xDC00);
                        char::from_u32(cp).ok_or_else(|| self.error("invalid surrogate pair"))
                    } else {
                        Err(lone(hi))
                    }
                } else {
                    Err(lone(hi))
                }
            }
            _ => Ok(char::from_u32(hi as u32).expect("BMP scalar")),
        }
    }

    /// JSON number grammar with literal preservation（语法校验；渲染时才做
    /// Python 数值重放）。
    fn number(&mut self) -> Result<String, String> {
        let start = self.i;
        if self.b.get(self.i) == Some(&b'-') {
            self.i += 1;
        }
        match self.b.get(self.i) {
            Some(b'0') => self.i += 1,
            Some(c) if c.is_ascii_digit() => {
                while matches!(self.b.get(self.i), Some(c) if c.is_ascii_digit()) {
                    self.i += 1;
                }
            }
            _ => return Err(self.error("invalid number")),
        }
        if self.b.get(self.i) == Some(&b'.') {
            self.i += 1;
            if !matches!(self.b.get(self.i), Some(c) if c.is_ascii_digit()) {
                return Err(self.error("invalid number fraction"));
            }
            while matches!(self.b.get(self.i), Some(c) if c.is_ascii_digit()) {
                self.i += 1;
            }
        }
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if !matches!(self.b.get(self.i), Some(c) if c.is_ascii_digit()) {
                return Err(self.error("invalid number exponent"));
            }
            while matches!(self.b.get(self.i), Some(c) if c.is_ascii_digit()) {
                self.i += 1;
            }
        }
        Ok(String::from_utf8_lossy(&self.b[start..self.i]).into_owned())
    }
}

/// Strict single-document scan（尾随数据=错；`NaN`/`Infinity` 裸词不在文法内
/// 自然被拒——与 Go 参照实现的 RFC-8259 解码域一致）。
fn scan(raw: &[u8]) -> Result<CVal, String> {
    let mut sc = Scanner { b: raw, i: 0 };
    let v = sc.value(0)?;
    sc.skip_ws();
    if sc.i != sc.b.len() {
        return Err(sc.error("trailing data after the flow definition document"));
    }
    Ok(v)
}

// ---------------------------------------------------------------- 渲染器

/// CPython `py_encode_basestring`（ensure_ascii=False）：只转义 ESCAPE_DCT
/// 条目——`"`、`\`、\b \f \n \r \t、其余 <0x20 控制符为小写 `\u00xx`；
/// `/`、DEL、U+2028/U+2029、全部非 ASCII 一律原样 UTF-8。
fn python_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let n = c as u32;
                out.push_str("\\u00");
                out.push(HEX[(n >> 4) as usize] as char);
                out.push(HEX[(n & 0xf) as usize] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// 数值字面量的 Python 重放：整数字面量（无 `.eE`）逐字透传（`-0`→`0`，
/// 大整数精度无损——Python int 任意精度）；浮点字面量按 float.__repr__。
fn python_number(lit: &str) -> String {
    if !lit.contains(['.', 'e', 'E']) {
        if lit == "-0" {
            return "0".to_string();
        }
        return lit.to_string();
    }
    // 扫描器已保证语法合法；溢出走 f64 无穷（Python float("1e999")=inf 同域）
    let f: f64 = lit.parse().unwrap_or(f64::NAN);
    python_float_repr(f)
}

/// CPython float.__repr__（format_float_short 'r' 模式）+ json.dumps 的
/// inf/nan 拼写。最短 round-trip 数字串对每个 double 唯一，Rust LowerExp
/// 与 CPython dtoa 给出相同数字串，只是呈现不同——按 Python 规则重建。
fn python_float_repr(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        }
        .to_string();
    }
    let sign = if f.is_sign_negative() { "-" } else { "" };
    let f = f.abs();
    if f == 0.0 {
        return format!("{sign}0.0");
    }
    let e = format!("{f:e}"); // d[.ddd]e±d（最短 round-trip）
    let (mant, exp) = e.split_once('e').expect("LowerExp shape");
    let exp: i32 = exp.parse().expect("LowerExp exponent");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let decpt = exp + 1;
    let mut out = String::from(sign);
    if decpt <= -4 || decpt > 16 {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        let mut e = decpt - 1;
        if e < 0 {
            out.push('-');
            e = -e;
        } else {
            out.push('+');
        }
        let es = e.to_string();
        if es.len() < 2 {
            out.push('0');
        }
        out.push_str(&es);
    } else if decpt <= 0 {
        out.push_str("0.");
        for _ in 0..(-decpt) {
            out.push('0');
        }
        out.push_str(&digits);
    } else if decpt >= digits.len() as i32 {
        out.push_str(&digits);
        for _ in 0..(decpt - digits.len() as i32) {
            out.push('0');
        }
        out.push_str(".0");
    } else {
        out.push_str(&digits[..decpt as usize]);
        out.push('.');
        out.push_str(&digits[decpt as usize..]);
    }
    out
}

/// `sort_keys=True`：Python 按码点比 str 键——合法 UTF-8 下 UTF-8 字节序
/// 与码点序一致（Rust String Ord = 字节序，Go 参照实现同论证）。重复键已
/// 在扫描时按 last-wins 去重。
fn canonical(v: &CVal, out: &mut String) {
    match v {
        CVal::Null => out.push_str("null"),
        CVal::Bool(true) => out.push_str("true"),
        CVal::Bool(false) => out.push_str("false"),
        CVal::Str(s) => python_string(s, out),
        CVal::Num(lit) => out.push_str(&python_number(lit)),
        CVal::Arr(items) => {
            out.push('[');
            for (i, x) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(x, out);
            }
            out.push(']');
        }
        CVal::Obj(kvs) => {
            let mut keys: Vec<(&str, &CVal)> = kvs.iter().map(|(k, v)| (k.as_str(), v)).collect();
            keys.sort_unstable_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, val)) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                python_string(k, out);
                out.push(':');
                canonical(val, out);
            }
            out.push('}');
        }
    }
}

fn canonical_text(doc: &CVal) -> String {
    let mut out = String::new();
    canonical(doc, &mut out);
    out
}

fn sha256_hex(data: &[u8]) -> String {
    let d = Sha256::digest(data);
    let mut s = String::with_capacity(64);
    for b in d {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------- 公开面

/// 规范化文本（Python `canonical_flow_definition_json` 的字节级对应）。
/// 定义**原样**入参（含保留键——剥离语义在门路径）。
pub fn canonical_definition_json(raw: &[u8]) -> Result<String, String> {
    Ok(canonical_text(&scan(raw)?))
}

/// Python `compute_flow_definition_hash` 的字节级对应：规范化文本 →
/// sha256 hexdigest（小写）。
pub fn flow_definition_hash(raw: &[u8]) -> Result<String, String> {
    Ok(sha256_hex(canonical_definition_json(raw)?.as_bytes()))
}

/// console 读穿透的引擎载荷（#299 §2 `engineFlowPayload` 的 Rust 对应）：
/// console 存储的定义原文（`flow_versions.definition`，不含 flow_id/version
/// ——那是列不是文档字段）注入身份两键后，按 Python canonical 口径渲染。
/// 走本文件的扫描器（数值字面量原文保真），不用 serde——serde 的 f64 重渲染
/// 会翻指纹（模块头注记）；产物字节即 canonical 形式，注入后喂
/// [`crate::ir::load`] 时内嵌指纹门（计算域 = 剥保留键后的文档）按 console
/// 发布时登记的 `definitionHash` 执法——outbox 的 definition_hash 正是按
/// 注入后载荷计的。
///
/// 与 Go 参照实现同口径：根非对象 = 错（json.Decode into map 拒标量/数组）；
/// 文档内已有 flow_id/version 时注入值胜（Go map 赋值覆盖语义）。
pub fn engine_flow_payload(
    flow_id: &str,
    version: &str,
    definition: &[u8],
) -> Result<String, String> {
    let doc = scan(definition)?;
    let mut kvs = match doc {
        CVal::Obj(kvs) => kvs,
        _ => return Err("ir: flow hash: flow definition must be a JSON object".to_string()),
    };
    kvs.retain(|(k, _)| k != "flow_id" && k != "version");
    kvs.push(("flow_id".to_string(), CVal::Str(flow_id.to_string())));
    kvs.push(("version".to_string(), CVal::Str(version.to_string())));
    Ok(canonical_text(&CVal::Obj(kvs)))
}

/// 剥去顶层保留键后的指纹（内嵌门的计算域）。
fn hash_excluding_reserved(raw: &[u8]) -> Result<String, String> {
    let doc = scan(raw)?;
    let stripped = match doc {
        CVal::Obj(kvs) => CVal::Obj(
            kvs.into_iter()
                .filter(|(k, _)| k != HASH_FIELD && k != HASH_ALGO_FIELD)
                .collect(),
        ),
        other => other,
    };
    Ok(sha256_hex(canonical_text(&stripped).as_bytes()))
}

/// 内嵌指纹门（`ir::load` 装载前调用）：定义带 [`HASH_FIELD`] 时校验，
/// 不匹配具名拒；无该键 = 跳过（向后兼容旧定义）。校验通过后保留键从
/// 文档剥离，不进 normalize/引擎管道。`raw` 是未经 serde 的原始字节
/// （数值字面量保真域），`doc` 是已解析的装载文档（被本函数原位清理）。
pub fn verify_definition_hash(raw: &[u8], doc: &mut Json) -> Result<(), String> {
    let Some(obj) = doc.as_object_mut() else {
        return Ok(()); // 非对象文档：无门可查，交装载管线的既有错误面
    };
    let Some(declared) = obj.get(HASH_FIELD) else {
        return Ok(()); // 无 hash 字段 = 跳过（Python classify 的 no_guard）
    };
    let declared = declared.as_str().ok_or_else(|| {
        format!("ir: flow hash: `{HASH_FIELD}` must be a string (the sha256 hexdigest)")
    })?;
    if let Some(algo) = obj.get(HASH_ALGO_FIELD) {
        let algo = algo
            .as_str()
            .ok_or_else(|| format!("ir: flow hash: `{HASH_ALGO_FIELD}` must be a string"))?;
        if algo != FLOW_HASH_ALGO {
            return Err(format!(
                "ir: flow hash: unsupported `{HASH_ALGO_FIELD}` {algo:?} — this engine \
                 fingerprints with {FLOW_HASH_ALGO}; refusing a cross-algorithm digest \
                 comparison (a v1/other-algo digest would read as a false flow_hash_mismatch)"
            ));
        }
    }
    let current = hash_excluding_reserved(raw)?;
    if current != declared {
        return Err(format!(
            "ir: flow hash mismatch (flow_hash_mismatch): the definition does not match its \
             `{HASH_FIELD}` (stored={declared} current={current}); refusing to load — \
             re-publish the flow to refresh the fingerprint"
        ));
    }
    obj.remove(HASH_FIELD);
    obj.remove(HASH_ALGO_FIELD);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 编码器口径快照：数值重放 / ESCAPE_DCT / sort_keys / 重复键 last-wins。
    /// 与 Python 的全量对拍在 tests/flow_hash.rs 的 62 案 golden。
    #[test]
    fn canonical_shapes() {
        assert_eq!(python_number("-0"), "0");
        assert_eq!(
            python_number("123456789012345678901234567890"),
            "123456789012345678901234567890"
        );
        assert_eq!(python_number("1e2"), "100.0");
        assert_eq!(python_number("1.0"), "1.0");
        assert_eq!(python_number("1.50"), "1.5");
        assert_eq!(python_number("1e-3"), "0.001");
        assert_eq!(python_number("1e-5"), "1e-05");
        assert_eq!(python_number("1e16"), "1e+16");
        assert_eq!(python_number("1e15"), "1000000000000000.0");
        assert_eq!(python_number("1e400"), "Infinity");
        assert_eq!(python_number("-1e400"), "-Infinity");
        assert_eq!(python_number("1e-400"), "0.0");
        assert_eq!(python_number("-0.0"), "-0.0");
        assert_eq!(python_number("0.1"), "0.1");
        // 字面量先落 double 再取最短 round-trip 数字——……67e16 落到 ……68e16
        assert_eq!(
            python_number("1.2345678901234567e16"),
            "1.2345678901234568e+16"
        );

        let mut s = String::new();
        python_string("a\"b\\c\nd\u{8}e\u{c}f\tg\u{1}h/i\u{7f}jk", &mut s);
        // DEL(0x7f) 不转义（ESCAPE_DCT 只覆盖 <0x20 + " \），Python 同口径
        assert_eq!(s, "\"a\\\"b\\\\c\\nd\\be\\ff\\tg\\u0001h/i\u{7f}jk\"");

        let doc = scan(br#"{"b":1,"a":2,"a":3}"#).unwrap();
        assert_eq!(canonical_text(&doc), r#"{"a":3,"b":1}"#);
        assert_eq!(
            canonical_text(&scan(br#" [1 , 2.50 , "x\ny" ] "#).unwrap()),
            r#"[1,2.5,"x\ny"]"#
        );
    }

    /// 解析域负控：尾随数据 / 裸 NaN / 孤立代理 —— 全部具名拒。
    #[test]
    fn scan_domain_rejects() {
        assert!(scan(b"{} {}").unwrap_err().contains("trailing data"));
        assert!(scan(b"NaN").is_err());
        assert!(scan(br#""\ud800""#).unwrap_err().contains("lone surrogate"));
        assert!(scan(br#""\udc00""#).unwrap_err().contains("lone surrogate"));
        assert!(scan(br#""\ud83d""#).unwrap_err().contains("lone surrogate"));
        // 配对代理合法合成
        assert_eq!(
            canonical_text(&scan(b"\"\xf0\x9f\x98\x80\"").unwrap()),
            "\"\u{1f600}\""
        );
    }

    /// 门路径剥离语义：保留键不参与指纹——同一文档换 hashAlgo 标签不影响
    /// 计算出的摘要。
    #[test]
    fn reserved_keys_are_outside_the_fingerprint_domain() {
        let base = br#"{"flow_id":"a","nodes":[]}"#;
        let with_hash = br#"{"definitionHash":"deadbeef","hashAlgo":"x","flow_id":"a","nodes":[]}"#;
        assert_eq!(
            hash_excluding_reserved(base).unwrap(),
            hash_excluding_reserved(with_hash).unwrap()
        );
    }
}
