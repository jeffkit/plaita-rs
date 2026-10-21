//! Hand-written recursive-descent parser for the plaita expression grammar.
//!
//! Semantic authority: `plaita/core/expression_parser.py` (`_build_grammar`).
//! The grammar, with pyparsing's default whitespace skipping between tokens:
//!
//! ```text
//! constant      = boolean | string | number
//! boolean       = True | False | true | false | null | None     (keywords)
//! string        = "..." | '...'      (backslash escapes, doubled-quote escape)
//! number        = [+-]? ( \d+ (\.\d*)? | \.\d+ ) ([eE][+-]?\d+)?   (int when no . / e)
//! identifier    = [A-Za-z][A-Za-z0-9_]*
//! name_token    = [A-Za-z0-9_\-$]+
//! root          = (not "$F.") "$" name_token?
//! segment       = "[" [-]\d+ "]" | "." \d+ | "." name_token
//! variable      = root segment*
//! function_call = "$F." identifier "(" [ expr ("," expr)* [","] ] ")"
//! expr          = constant | function_call | variable
//! prefix_expr   = function_call | variable                       (parse_all)
//! interpolation = "{%" expr "%}"                                 (scanned)
//! ```
//!
//! There are no infix operators, list/dict literals or f-strings: those are
//! parse errors, exactly as in plaita.

use serde_json::Value;

use crate::error::ParseError;

/// One step of a variable path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// `[n]` / `.n` — direct indexing (list position, negative from the end).
    Index(i64),
    /// `.name` — mapping key / attribute read (missing → `None`).
    Field(String),
}

/// Parsed expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Literal constant (`1`, `2.5`, `"s"`, `true`, `None`).
    Const(Value),
    /// `$root.seg...` — `root` is spelled WITHOUT the `$` (`"INPUT"`, `"NODE"`,
    /// a custom context key, or `""` for a bare `$`).
    Var { root: String, path: Vec<Segment> },
    /// `$F.name(args...)`.
    Call { name: String, args: Vec<Expr> },
}

/// A compiled template string: literal runs and `{% %}` interpolations.
#[derive(Debug, Clone, PartialEq)]
pub enum TemplatePart {
    Literal(String),
    Interp(Expr),
}

/// Parses a whole `$`-prefixed expression (`function_call | variable`),
/// requiring the entire input to be consumed.
pub fn parse(src: &str) -> Result<Expr, ParseError> {
    let mut p = Parser::new(src);
    p.skip_ws();
    let expr = p.parse_prefix_expr()?;
    p.skip_ws();
    if !p.at_end() {
        return Err(p.error("expected end of text"));
    }
    Ok(expr)
}

/// Scans a template string for `{% expr %}` interpolations. Returns `None`
/// when no fragment parses as an interpolation (the caller then keeps the
/// string unchanged — plaita's "unmatched template" behaviour); otherwise the
/// literal gaps and the compiled fragments in order.
pub fn parse_template(src: &str) -> Option<Vec<TemplatePart>> {
    let chars: Vec<char> = src.chars().collect();
    let mut parts: Vec<TemplatePart> = Vec::new();
    let mut literal_start = 0usize;
    let mut i = 0usize;
    let mut matched = false;
    while i + 1 < chars.len() {
        if chars[i] == '{' && chars[i + 1] == '%' {
            if let Some((expr, end)) = try_interpolation(&chars, i) {
                if literal_start < i {
                    parts.push(TemplatePart::Literal(chars[literal_start..i].iter().collect()));
                }
                parts.push(TemplatePart::Interp(expr));
                matched = true;
                literal_start = end;
                i = end;
                continue;
            }
        }
        i += 1;
    }
    if !matched {
        return None;
    }
    if literal_start < chars.len() {
        parts.push(TemplatePart::Literal(chars[literal_start..].iter().collect()));
    }
    Some(parts)
}

/// Attempts to parse `{%` ws expr ws `%}` at `start`; returns the expression
/// and the index just past `%}`.
fn try_interpolation(chars: &[char], start: usize) -> Option<(Expr, usize)> {
    let mut p = Parser {
        chars: chars.to_vec(),
        pos: start + 2,
    };
    p.skip_ws();
    let expr = p.parse_expr().ok()?;
    p.skip_ws();
    if p.peek() == Some('%') && p.peek_at(1) == Some('}') {
        return Some((expr, p.pos + 2));
    }
    None
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
}

const WS: &[char] = &[' ', '\t', '\n', '\r'];

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic()
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_name_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '$'
}

/// pyparsing `Keyword` boundary: the keyword must not be followed by an
/// identifier character (alphanums, `_`, `$`).
fn is_keyword_tail_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$'
}

impl Parser {
    fn new(src: &str) -> Parser {
        Parser {
            chars: src.chars().collect(),
            pos: 0,
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.chars.len()
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn starts_with(&self, s: &str) -> bool {
        let mut i = self.pos;
        for c in s.chars() {
            if self.chars.get(i) != Some(&c) {
                return false;
            }
            i += 1;
        }
        true
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if WS.contains(&c) {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn error(&self, message: &str) -> ParseError {
        ParseError {
            source_text: self.chars.iter().collect(),
            column: self.pos + 1,
            message: message.to_string(),
        }
    }

    /// `function_call | variable`
    fn parse_prefix_expr(&mut self) -> Result<Expr, ParseError> {
        if self.starts_with("$F.") {
            return self.parse_function_call();
        }
        if self.peek() == Some('$') {
            return self.parse_variable();
        }
        Err(self.error("expected a $-prefixed variable or $F.function(...) call"))
    }

    /// `constant | function_call | variable`
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        if let Some(c) = self.try_parse_constant()? {
            return Ok(c);
        }
        if self.starts_with("$F.") {
            return self.parse_function_call();
        }
        if self.peek() == Some('$') {
            return self.parse_variable();
        }
        Err(self.error(
            "expected a constant, a $-prefixed variable or a $F.function(...) call \
             (list/dict literals, operators and bare calls are not part of the grammar)",
        ))
    }

    fn try_parse_constant(&mut self) -> Result<Option<Expr>, ParseError> {
        // keywords
        for (kw, val) in [
            ("True", Value::Bool(true)),
            ("False", Value::Bool(false)),
            ("true", Value::Bool(true)),
            ("false", Value::Bool(false)),
            ("null", Value::Null),
            ("None", Value::Null),
        ] {
            if self.starts_with(kw) {
                let after = self.peek_at(kw.len());
                if !after.map(is_keyword_tail_char).unwrap_or(false) {
                    self.pos += kw.len();
                    return Ok(Some(Expr::Const(val)));
                }
            }
        }
        match self.peek() {
            Some('"') | Some('\'') => return self.parse_string().map(|s| Some(Expr::Const(Value::String(s)))),
            Some(c) if c.is_ascii_digit() || c == '.' || c == '+' || c == '-' => {
                if let Some(v) = self.try_parse_number() {
                    return Ok(Some(Expr::Const(v)));
                }
            }
            _ => {}
        }
        Ok(None)
    }

    /// pyparsing_common.number: int when the text has no `.`/exponent.
    fn try_parse_number(&mut self) -> Option<Value> {
        let start = self.pos;
        let mut i = self.pos;
        let sign = matches!(self.chars.get(i), Some('+') | Some('-'));
        if sign {
            i += 1;
        }
        let int_start = i;
        while matches!(self.chars.get(i), Some(c) if c.is_ascii_digit()) {
            i += 1;
        }
        let int_digits = i - int_start;
        let mut is_float = false;
        if self.chars.get(i) == Some(&'.') {
            let frac_start = i + 1;
            let mut j = frac_start;
            while matches!(self.chars.get(j), Some(c) if c.is_ascii_digit()) {
                j += 1;
            }
            let frac_digits = j - frac_start;
            if int_digits == 0 && frac_digits == 0 {
                return None;
            }
            is_float = true;
            i = j;
        } else if int_digits == 0 {
            return None;
        }
        // exponent
        if matches!(self.chars.get(i), Some('e') | Some('E')) {
            let mut j = i + 1;
            if matches!(self.chars.get(j), Some('+') | Some('-')) {
                j += 1;
            }
            let exp_start = j;
            while matches!(self.chars.get(j), Some(c) if c.is_ascii_digit()) {
                j += 1;
            }
            if j > exp_start {
                is_float = true;
                i = j;
            }
        }
        let text: String = self.chars[start..i].iter().collect();
        let value = if is_float {
            let f: f64 = text.parse().ok()?;
            Value::from(f)
        } else {
            match text.parse::<i64>() {
                Ok(n) => Value::from(n),
                // Beyond i64: Python ints are unbounded; fall back to float
                // like Go's json.Number→float path would.
                Err(_) => Value::from(text.parse::<f64>().ok()?),
            }
        };
        self.pos = i;
        Some(value)
    }

    /// pyparsing QuotedString with esc_char `\` and a doubled-quote escape.
    fn parse_string(&mut self) -> Result<String, ParseError> {
        let quote = self.peek().expect("caller checked the opening quote");
        self.pos += 1;
        let mut out = String::new();
        loop {
            let c = match self.peek() {
                None => return Err(self.error("unterminated string literal")),
                Some(c) => c,
            };
            if c == '\n' || c == '\r' {
                return Err(self.error("string literal must not span lines"));
            }
            if c == '\\' {
                match self.peek_at(1) {
                    None => return Err(self.error("unterminated string literal")),
                    Some(e) => {
                        out.push(match e {
                            'n' => '\n',
                            't' => '\t',
                            'r' => '\r',
                            'f' => '\u{0c}',
                            other => other,
                        });
                        self.pos += 2;
                        continue;
                    }
                }
            }
            if c == quote {
                // doubled quote escape ("" inside "..." / '' inside '...')
                if self.peek_at(1) == Some(quote) {
                    out.push(quote);
                    self.pos += 2;
                    continue;
                }
                self.pos += 1;
                return Ok(out);
            }
            out.push(c);
            self.pos += 1;
        }
    }

    fn parse_function_call(&mut self) -> Result<Expr, ParseError> {
        // Combine("$F." + identifier + "(") — no whitespace between parts.
        self.pos += 3;
        let name_start = self.pos;
        match self.peek() {
            Some(c) if is_ident_start(c) => self.pos += 1,
            _ => return Err(self.error("expected a function name after $F.")),
        }
        while matches!(self.peek(), Some(c) if is_ident_char(c)) {
            self.pos += 1;
        }
        let name: String = self.chars[name_start..self.pos].iter().collect();
        if self.peek() != Some('(') {
            return Err(self.error("expected '(' right after the function name"));
        }
        self.pos += 1;
        let mut args = Vec::new();
        self.skip_ws();
        if self.peek() == Some(')') {
            self.pos += 1;
            return Ok(Expr::Call { name, args });
        }
        loop {
            self.skip_ws();
            args.push(self.parse_expr()?);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                    self.skip_ws();
                    // trailing comma before ')'
                    if self.peek() == Some(')') {
                        self.pos += 1;
                        return Ok(Expr::Call { name, args });
                    }
                }
                Some(')') => {
                    self.pos += 1;
                    return Ok(Expr::Call { name, args });
                }
                _ => return Err(self.error("expected ',' or ')' in argument list")),
            }
        }
    }

    fn parse_variable(&mut self) -> Result<Expr, ParseError> {
        if self.starts_with("$F.") {
            return Err(self.error("$F. is the function namespace, not a context root"));
        }
        self.pos += 1; // '$'
        let root_start = self.pos;
        while matches!(self.peek(), Some(c) if is_name_token_char(c)) {
            self.pos += 1;
        }
        let root: String = self.chars[root_start..self.pos].iter().collect();
        let mut path = Vec::new();
        loop {
            let save = self.pos;
            self.skip_ws();
            match self.peek() {
                Some('[') => {
                    self.pos += 1;
                    self.skip_ws();
                    let num_start = self.pos;
                    if self.peek() == Some('-') {
                        self.pos += 1;
                    }
                    let digits_start = self.pos;
                    while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                        self.pos += 1;
                    }
                    if self.pos == digits_start {
                        return Err(self.error("expected an integer index inside [ ]"));
                    }
                    let text: String = self.chars[num_start..self.pos].iter().collect();
                    let idx: i64 = text
                        .parse()
                        .map_err(|_| self.error("index out of range for an integer"))?;
                    self.skip_ws();
                    if self.peek() != Some(']') {
                        return Err(self.error("expected ']' after index"));
                    }
                    self.pos += 1;
                    path.push(Segment::Index(idx));
                }
                Some('.') => {
                    self.pos += 1;
                    self.skip_ws();
                    match self.peek() {
                        Some(c) if c.is_ascii_digit() => {
                            // dot_int wins over dot_field (MatchFirst order)
                            let start = self.pos;
                            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                                self.pos += 1;
                            }
                            let text: String = self.chars[start..self.pos].iter().collect();
                            let idx: i64 = text
                                .parse()
                                .map_err(|_| self.error("index out of range for an integer"))?;
                            path.push(Segment::Index(idx));
                        }
                        Some(c) if is_name_token_char(c) => {
                            let start = self.pos;
                            while matches!(self.peek(), Some(c) if is_name_token_char(c)) {
                                self.pos += 1;
                            }
                            path.push(Segment::Field(self.chars[start..self.pos].iter().collect()));
                        }
                        _ => return Err(self.error("expected a field name or index after '.'")),
                    }
                }
                _ => {
                    self.pos = save;
                    break;
                }
            }
        }
        Ok(Expr::Var { root, path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn var(root: &str, path: Vec<Segment>) -> Expr {
        Expr::Var {
            root: root.to_string(),
            path,
        }
    }
    fn f(s: &str) -> Segment {
        Segment::Field(s.to_string())
    }
    fn i(n: i64) -> Segment {
        Segment::Index(n)
    }

    #[test]
    fn variables() {
        assert_eq!(parse("$INPUT").unwrap(), var("INPUT", vec![]));
        assert_eq!(parse("$INPUT.a.b").unwrap(), var("INPUT", vec![f("a"), f("b")]));
        assert_eq!(parse("$INPUT[0].name").unwrap(), var("INPUT", vec![i(0), f("name")]));
        assert_eq!(parse("$INPUT[-1]").unwrap(), var("INPUT", vec![i(-1)]));
        assert_eq!(parse("$INPUT.users.0").unwrap(), var("INPUT", vec![f("users"), i(0)]));
        assert_eq!(
            parse("$NODE.node-b_dED0Mj.aaaaa").unwrap(),
            var("NODE", vec![f("node-b_dED0Mj"), f("aaaaa")])
        );
        assert_eq!(parse("$PARENT.$NODE.x").unwrap(), var("PARENT", vec![f("$NODE"), f("x")]));
        assert_eq!(parse("$").unwrap(), var("", vec![]));
        assert_eq!(parse("$.a").unwrap(), var("", vec![f("a")]));
        assert_eq!(parse(" $INPUT ").unwrap(), var("INPUT", vec![]));
    }

    #[test]
    fn calls() {
        assert_eq!(
            parse("$F.add(1, 2)").unwrap(),
            Expr::Call {
                name: "add".into(),
                args: vec![Expr::Const(json!(1)), Expr::Const(json!(2))]
            }
        );
        assert_eq!(
            parse("$F.add( $F.mul(2, 3) , $INPUT.x ,)").unwrap(),
            Expr::Call {
                name: "add".into(),
                args: vec![
                    Expr::Call {
                        name: "mul".into(),
                        args: vec![Expr::Const(json!(2)), Expr::Const(json!(3))]
                    },
                    var("INPUT", vec![f("x")])
                ]
            }
        );
        assert_eq!(parse("$F.now()").unwrap(), Expr::Call { name: "now".into(), args: vec![] });
        assert_eq!(
            parse(r#"$F.concat('it''s', "a\"b", 'x\'y', 'tab\t')"#).unwrap(),
            Expr::Call {
                name: "concat".into(),
                args: vec![
                    Expr::Const(json!("it's")),
                    Expr::Const(json!("a\"b")),
                    Expr::Const(json!("x'y")),
                    Expr::Const(json!("tab\t")),
                ]
            }
        );
        assert_eq!(
            parse("$F.f(True, false, None, null, -1, 2.5, 1e3, .5)").unwrap(),
            Expr::Call {
                name: "f".into(),
                args: vec![
                    Expr::Const(json!(true)),
                    Expr::Const(json!(false)),
                    Expr::Const(json!(null)),
                    Expr::Const(json!(null)),
                    Expr::Const(json!(-1)),
                    Expr::Const(json!(2.5)),
                    Expr::Const(json!(1000.0)),
                    Expr::Const(json!(0.5)),
                ]
            }
        );
    }

    #[test]
    fn rejects() {
        assert!(parse("hello").is_err());
        assert!(parse("$F.add(1, 2) + 3").is_err());
        assert!(parse("$F.add(1 2)").is_err());
        assert!(parse("$F.add (1, 2)").is_err());
        assert!(parse("$F.concat").is_err());
        assert!(parse("$F.f([1, 2])").is_err());
        assert!(parse("$INPUT.0abc").is_err());
        // '$' is a legal name-token char (Python name_token), so a root key
        // can itself carry a '$' — this is "$INPUT the key", not a nested root
        assert_eq!(parse("$$INPUT").unwrap(), var("$INPUT", vec![]));
    }

    #[test]
    fn templates() {
        let parts = parse_template("my name is {%$INPUT[0].name%}").unwrap();
        assert_eq!(
            parts,
            vec![
                TemplatePart::Literal("my name is ".into()),
                TemplatePart::Interp(var("INPUT", vec![i(0), f("name")])),
            ]
        );
        let parts = parse_template("v: {% $INPUT %} / {% true %} / {% 'q' %}").unwrap();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[3], TemplatePart::Interp(Expr::Const(json!(true))));
        assert_eq!(parts[5], TemplatePart::Interp(Expr::Const(json!("q"))));
        assert_eq!(parse_template("x {%@INPUT[0].name%} y"), None);
        assert_eq!(parse_template("hi {%world%}"), None);
        assert_eq!(parse_template("a {% b"), None);
        let parts = parse_template("a {% $INPUT %} b {%").unwrap();
        assert_eq!(
            parts,
            vec![
                TemplatePart::Literal("a ".into()),
                TemplatePart::Interp(var("INPUT", vec![])),
                TemplatePart::Literal(" b {%".into()),
            ]
        );
        let parts = parse_template("img: ![]({%$NODE.step1.data.0.url%})").unwrap();
        assert_eq!(parts.len(), 3);
    }
}
