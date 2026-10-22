//! Expression evaluation with plaita semantics.
//!
//! Entry points mirror `plaita/core/expression_parser.py`:
//!
//! - [`eval_value`] — the facade over any JSON value: strings starting with
//!   `$` are whole expressions (native result type kept), strings containing
//!   `{% ... %}` are templates (`str()`-rendered interpolation), containers
//!   recurse element-wise, scalars pass through.
//! - [`eval_str`] / [`eval_expr`] — one expression against a [`Roots`] scope.
//!
//! Path semantics (missing keys): a missing context root is a `KeyError`
//! (flow-logic error); a missing field on an existing object yields `None`
//! and further *field* access through `None` keeps yielding `None`; index
//! misses (list out of range, key miss on a dict, indexing `None`) are
//! errors. Unknown `$F` functions fail loudly with a `NameError` +
//! did-you-mean hint. Mutator calls (`$F.pop` & co.) refuse live context
//! roots as their target (plaita#69 write protection).
//!
//! Runtime values are DATA. A string stored in a node result / input that
//! happens to look like an expression (`"$F.add(2,3)"`, `"{% ... %}"`) is
//! returned verbatim by path reads — never re-parsed. Expressions live in the
//! flow CONFIG only, where the engine evaluates them once at node execution;
//! a code/LLM node that wants an evaluated value evaluates it itself.
//! (Contract set 2026-10-21 after the plaita #104/#130 history review: the
//! Python engine's "nested expression string" re-evaluation is a legacy
//! artifact of its single evaluate() funnel — plaita#104 itself classifies it
//! as a hazard — with no scenario that cannot be expressed otherwise.)
//! `$NODE.raw.<node>` stays accepted as a compatibility modifier (verbatim is
//! already the only semantics; the "node literally named raw" collision rule
//! is kept, as in plaita-go).

use serde_json::Value;

use crate::error::{ErrorKind, EvalError};
use crate::functions;
use crate::parser::{parse, parse_template, Expr, Segment, TemplatePart};
use crate::pyrepr::py_str;
use crate::value::is_truthy;

/// The variable roots an expression may address (`$INPUT`, `$NODE`, ...).
///
/// Implemented by the engine's runtime context (which takes a read lock per
/// root access, so background parallel branches can evaluate `$PARENT`
/// against a live parent context without data races) and by [`MapRoots`]
/// for tests and tools.
pub trait Roots {
    /// Calls `f` with the value stored under root `name` (spelled without
    /// the `$`); returns `false` when the root is absent.
    fn with_root(&self, name: &str, f: &mut dyn FnMut(&Value)) -> bool;

    /// The parent execution scope (`$PARENT`), if any.
    fn parent(&self) -> Option<&dyn Roots> {
        None
    }

    /// All root names, for `$PARENT` materialisation and error hints.
    fn root_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// A simple [`Roots`] over an owned map, with an optional parent chain.
#[derive(Default, Clone)]
pub struct MapRoots<'a> {
    roots: serde_json::Map<String, Value>,
    parent: Option<&'a MapRoots<'a>>,
}

impl<'a> MapRoots<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, name: impl Into<String>, value: Value) {
        self.roots.insert(name.into(), value);
    }

    pub fn with_parent(parent: &'a MapRoots<'a>) -> Self {
        MapRoots {
            roots: Default::default(),
            parent: Some(parent),
        }
    }
}

impl Roots for MapRoots<'_> {
    fn with_root(&self, name: &str, f: &mut dyn FnMut(&Value)) -> bool {
        match self.roots.get(name) {
            Some(v) => {
                f(v);
                true
            }
            None => false,
        }
    }

    fn parent(&self) -> Option<&dyn Roots> {
        self.parent.map(|p| p as &dyn Roots)
    }

    fn root_names(&self) -> Vec<String> {
        self.roots.keys().cloned().collect()
    }
}

/// Roots a bare `$name` (not one of the built-ins) resolves against when the
/// engine exposes extra per-node variables (plaita's context entries beyond
/// the system roots, e.g. `LOOP-ITEM`). The engine's implementation folds
/// these into its root table; the default is an error for unknown roots.
pub const BUILTIN_ROOTS: &[&str] = &[
    "INPUT",
    "NODE",
    "PARENT",
    "ERROR",
    "ENV",
    "GLOBAL",
    "BRANCH",
    "FLOW_ID",
    "FLOW",
    "LAST_NODE",
    "EXECUTION_ID",
    "NULL",
    "EMPTYSTRING",
];

/// Evaluates any JSON value through the facade (containers recurse,
/// `$`-strings evaluate, `{% %}` templates interpolate, scalars pass).
pub fn eval_value(value: &Value, roots: &dyn Roots) -> Result<Value, EvalError> {
    match value {
        Value::String(s) => eval_str(s, roots),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(eval_value(item, roots).map_err(|e| {
                    EvalError::other(format!("failed to eval expression {item}: {e}"))
                })?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(
                    k.clone(),
                    eval_value(v, roots).map_err(|e| {
                        EvalError::other(format!("failed to eval expression {v}: {e}"))
                    })?,
                );
            }
            Ok(Value::Object(out))
        }
        other => Ok(other.clone()),
    }
}

/// Evaluates one string: `$...` whole expression, `{% %}` template, else
/// plain text.
pub fn eval_str(s: &str, roots: &dyn Roots) -> Result<Value, EvalError> {
    if s.starts_with('$') {
        let expr = parse(s)?;
        return eval_expr(&expr, roots);
    }
    if s.contains("{%") {
        return eval_template(s, roots);
    }
    Ok(Value::String(s.to_string()))
}

/// Evaluates a parsed expression.
pub fn eval_expr(expr: &Expr, roots: &dyn Roots) -> Result<Value, EvalError> {
    match expr {
        Expr::Const(v) => Ok(v.clone()),
        Expr::Var { root, path } => eval_var(root, path, roots),
        Expr::Call { name, args } => eval_call(name, args, roots),
    }
}

fn scalar_root(name: &str) -> Option<Value> {
    match name {
        "NULL" => Some(Value::Null),
        "EMPTYSTRING" => Some(Value::String(String::new())),
        _ => None,
    }
}

/// `$FLOW` is the documented alias of `$FLOW_ID` (plaita normalises it).
fn canonical_root(root: &str) -> String {
    if root == "FLOW" {
        "FLOW_ID".to_string()
    } else {
        root.to_string()
    }
}

fn eval_var(root: &str, path: &[Segment], roots: &dyn Roots) -> Result<Value, EvalError> {
    let root = canonical_root(root);

    if root == "PARENT" {
        return match roots.parent() {
            None if path.is_empty() => Ok(Value::Null),
            None => Err(EvalError::key_error(
                "'$PARENT' not found: this execution has no parent flow",
            )),
            Some(parent) => match path.split_first() {
                // bare $PARENT: the parent's roots as a mapping
                None => {
                    let mut out = serde_json::Map::new();
                    for name in parent.root_names() {
                        let mut v = Value::Null;
                        parent.with_root(&name, &mut |x| v = x.clone());
                        out.insert(name, v);
                    }
                    Ok(Value::Object(out))
                }
                Some((Segment::Field(first), rest)) => {
                    let inner_root = canonical_root(first.trim_start_matches('$'));
                    // plaita-go#80 r78 plaitaexpr 同款语义：`$PARENT.<name>` 命名
                    // 父作用域的一个根；父上不存在的根求值为 null（快照对象的
                    // 缺省字段读，reference 隔离 golden `$PARENT.n` → null 钉），
                    // 不再是 KeyError。标量根（$NULL/$EMPTYSTRING）恒在。
                    let scalar = scalar_root(&inner_root).is_some();
                    let mut found = scalar;
                    if !found {
                        parent.with_root(&inner_root, &mut |_| found = true);
                    }
                    if !found {
                        return Ok(Value::Null);
                    }
                    eval_var(&inner_root, rest, parent)
                }
                Some((Segment::Index(_), _)) => Err(EvalError::type_error(
                    "$PARENT must be followed by a root name, e.g. $PARENT.$INPUT.x",
                )),
            },
        };
    }

    if let Some(v) = scalar_root(&root) {
        // 标量根是运行时注入的普通值（Python 的 {"$NULL": None}），后继段按
        // 常规语义求值：$NULL[0] → TypeError、$EMPTYSTRING[0] → IndexError
        return eval_var_path(&root, &v, path);
    }

    let mut result: Option<Result<Value, EvalError>> = None;
    let found = roots.with_root(&root, &mut |v| {
        result = Some(eval_var_path(&root, v, path));
    });
    if !found {
        return Err(EvalError::key_error(format!(
            "'${root}' not found in expression context. Available roots (with $ prefix): \
             INPUT, NODE, GLOBAL, PARENT, ENV, FLOW_ID, FLOW(alias of FLOW_ID)"
        )));
    }
    result.expect("with_root reported found, so the callback ran")
}

/// `$NODE.raw.<node>[.path...]` — explicit verbatim accessor (plaita#130).
/// Since this engine never re-evaluates nested expression strings, stripping
/// the modifier only needs the collision rule: a node literally named `raw`
/// keeps being resolved as that node.
fn strip_raw_modifier(root: &str, path: &[Segment], nodes: &Value) -> Vec<Segment> {
    if root != "NODE" || path.len() < 2 {
        return path.to_vec();
    }
    let Segment::Field(first) = &path[0] else {
        return path.to_vec();
    };
    if first != "raw" {
        return path.to_vec();
    }
    let Segment::Field(second) = &path[1] else {
        return path.to_vec();
    };
    if second.is_empty() || second.chars().all(|c| c.is_ascii_digit()) {
        return path.to_vec();
    }
    if nodes
        .as_object()
        .map(|o| o.contains_key("raw"))
        .unwrap_or(false)
    {
        return path.to_vec();
    }
    path[1..].to_vec()
}

/// Traverses `path` under an existing root value with plaita's missing-key
/// semantics (see the module docs).
fn eval_var_path(root: &str, root_value: &Value, path: &[Segment]) -> Result<Value, EvalError> {
    let path = strip_raw_modifier(root, path, root_value);
    let mut cur = root_value.clone();
    for seg in &path {
        match cur {
            Value::Null => match seg {
                // field access through None keeps yielding None — except the
                // `_`-prefixed attribute gate (plaita#55: every non-mapping
                // base refuses private/dunder attribute reads)
                Segment::Field(name) => reject_private_attr(name)?,
                Segment::Index(_) => {
                    return Err(EvalError::type_error(
                        "cannot index None (plaita: 'NoneType' object is not subscriptable)",
                    ))
                }
            },
            Value::Object(map) => match seg {
                Segment::Field(name) => {
                    if name.starts_with("__") {
                        return Err(EvalError::value_error(format!(
                            "attribute access through '__'-prefixed path segment {name:?} is not allowed"
                        )));
                    }
                    cur = map.get(name).cloned().unwrap_or(Value::Null);
                }
                Segment::Index(i) => {
                    // JSON objects only carry string keys; a numeric index
                    // addresses the string form (Go engine behaviour; plaita
                    // would KeyError — documented divergence).
                    let key = i.to_string();
                    match map.get(&key) {
                        Some(v) => cur = v.clone(),
                        None => {
                            return Err(EvalError::key_error(format!("KeyError: {key:?}")));
                        }
                    }
                }
            },
            Value::Array(items) => match seg {
                Segment::Index(i) => {
                    let n = items.len() as i64;
                    let j = if *i < 0 { *i + n } else { *i };
                    if j < 0 || j >= n {
                        return Err(EvalError::index_error(
                            "IndexError: list index out of range",
                        ));
                    }
                    cur = items[j as usize].clone();
                }
                Segment::Field(_) => {
                    // a list is neither a mapping nor has attributes → None
                    cur = Value::Null;
                }
            },
            scalar => match seg {
                Segment::Field(_) => cur = Value::Null,
                Segment::Index(_) => {
                    return Err(EvalError::type_error(format!(
                        "'{}' object is not subscriptable",
                        crate::value::py_type_name(&scalar)
                    )));
                }
            },
        }
    }
    Ok(cur)
}

/// plaita#55 attribute-traversal gate on non-mapping bases: any `_`-prefixed
/// attribute read (`$NODE.n.__class__`, `$INPUT.list.__len__`, `$INPUT.s._x`)
/// is a ValueError instead of traversing into Python internals. Mapping
/// bases only gate `__` (single-underscore keys stay readable).
fn reject_private_attr(name: &str) -> Result<(), EvalError> {
    if name.starts_with('_') {
        return Err(EvalError::value_error(format!(
            "attribute access through '_'-prefixed segment {name:?} is not allowed on a non-mapping value"
        )));
    }
    Ok(())
}

/// $F functions Python's expression.py registers with has_side_effects=True.
const MUTATOR_FUNCS: &[&str] = &[
    "set",
    "setDictValue",
    "delete",
    "delDictValue",
    "clear",
    "clearDict",
    "pop",
    "delListItem",
    "setListItem",
];

/// Pure $F calls whose result is a NEW list by construction — the one legal
/// target shape for a mutator.
const FRESH_LIST_PRODUCERS: &[&str] = &[
    "append",
    "addListItem",
    "extend",
    "insert",
    "insertListItem",
    "remove",
    "slice",
    "reverse",
    "sort",
    "split",
    "keys",
    "values",
    "items",
    "getDictKeys",
    "getDictValues",
    "json_loads",
];

fn is_call(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Call { name, .. } => Some(name),
        _ => None,
    }
}

fn references_live_root(expr: &Expr) -> bool {
    match expr {
        Expr::Const(_) => false,
        Expr::Var { root, .. } => {
            matches!(
                canonical_root(root).as_str(),
                "INPUT" | "NODE" | "GLOBAL" | "ENV" | "ERROR" | "PARENT"
            )
        }
        Expr::Call { args, .. } => args.iter().any(references_live_root),
    }
}

/// plaita#69 write protection: a mutator whose TARGET argument (position 0)
/// references a live context root is a named `ValueError` before anything
/// evaluates. Fresh list producers are allowed outright; every other target
/// must reference no live root.
fn gate_mutator_live_root(name: &str, args: &[Expr]) -> Result<(), EvalError> {
    if !MUTATOR_FUNCS.contains(&name) || args.is_empty() {
        return Ok(());
    }
    let target = &args[0];
    if let Some(callee) = is_call(target) {
        if FRESH_LIST_PRODUCERS.contains(&callee) {
            return Ok(());
        }
    }
    if !references_live_root(target) {
        return Ok(());
    }
    Err(EvalError::value_error(format!(
        "$F.{name}: cannot mutate live context path (plaita#69 write protection: mutator targets \
         must be fresh containers, not $INPUT/$NODE/$GLOBAL/$PARENT/$ENV paths)"
    )))
}

fn eval_call(name: &str, args: &[Expr], roots: &dyn Roots) -> Result<Value, EvalError> {
    gate_mutator_live_root(name, args)?;
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_expr(arg, roots)?);
    }
    match functions::lookup(name) {
        Some(f) => f(&values),
        None => {
            let mut names: Vec<&str> = functions::names();
            names.sort_unstable();
            let hint = did_you_mean(name, &names);
            Err(EvalError::name_error(format!(
                "Unknown expression function '{name}' (default registry).{hint} Available: {names:?}"
            )))
        }
    }
}

/// Loose port of difflib.get_close_matches (similarity ≥ 0.6, top 3).
pub fn did_you_mean(bad: &str, candidates: &[&str]) -> String {
    let mut hits: Vec<(f64, &str)> = candidates
        .iter()
        .map(|c| (similarity(bad, c), *c))
        .filter(|(s, _)| *s >= 0.6)
        .collect();
    hits.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.cmp(b.1))
    });
    hits.truncate(3);
    if hits.is_empty() {
        return String::new();
    }
    let names: Vec<String> = hits.into_iter().map(|(_, n)| format!("'{n}'")).collect();
    format!(" Did you mean [{}]?", names.join(", "))
}

fn similarity(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    let longest = a.chars().count().max(b.chars().count());
    if longest == 0 {
        return 1.0;
    }
    1.0 - levenshtein(a, b) as f64 / longest as f64
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (cur[j - 1] + 1).min(prev[j] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// `{% ... %}` template interpolation: matched fragments are spliced in as
/// their `str()` rendering; fragments that do not parse stay literal, and a
/// string whose fragments all fail keeps its original text.
fn eval_template(s: &str, roots: &dyn Roots) -> Result<Value, EvalError> {
    let Some(parts) = parse_template(s) else {
        return Ok(Value::String(s.to_string()));
    };
    let mut out = String::new();
    for part in parts {
        match part {
            TemplatePart::Literal(text) => out.push_str(&text),
            TemplatePart::Interp(expr) => {
                let v = eval_expr(&expr, roots)?;
                out.push_str(&py_str(&v));
            }
        }
    }
    Ok(Value::String(out))
}

/// Python truthiness exposed for nodes (loop conditions, `if` nodes).
pub fn truthy(v: &Value) -> bool {
    is_truthy(v)
}

/// Re-exports used by engine-side condition evaluation.
pub use crate::value::{compare, contains, equal};

/// Kinds of errors worth surfacing verbatim at a call site that wants to
/// match on the exception class.
pub fn error_kind(err: &EvalError) -> ErrorKind {
    err.kind
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn roots(v: Value) -> MapRoots<'static> {
        let mut r = MapRoots::new();
        r.set("INPUT", v);
        r
    }

    #[test]
    fn facade_passthrough() {
        let r = roots(json!({}));
        assert_eq!(eval_value(&json!(42), &r).unwrap(), json!(42));
        assert_eq!(eval_value(&json!(null), &r).unwrap(), json!(null));
        assert_eq!(eval_value(&json!(true), &r).unwrap(), json!(true));
        assert_eq!(eval_value(&json!("hello"), &r).unwrap(), json!("hello"));
        assert_eq!(
            eval_value(&json!("{user.name}"), &r).unwrap(),
            json!("{user.name}")
        );
    }

    #[test]
    fn facade_recursion() {
        let mut r = MapRoots::new();
        r.set("INPUT", json!({"name": "Alice", "a": {"b": 1}}));
        assert_eq!(
            eval_value(&json!({"greeting": "$INPUT.name"}), &r).unwrap(),
            json!({"greeting": "Alice"})
        );
        assert_eq!(
            eval_value(&json!({"outer": {"inner": "$INPUT.a.b"}}), &r).unwrap(),
            json!({"outer": {"inner": 1}})
        );
        let mut r2 = MapRoots::new();
        r2.set("INPUT", json!(42));
        assert_eq!(
            eval_value(&json!(["$INPUT", "static"]), &r2).unwrap(),
            json!([42, "static"])
        );
    }

    #[test]
    fn variable_paths() {
        let r = roots(
            json!({"names": ["hello"], "users": [{"name": "hello"}], "a": {"b": {"c": "deep"}}}),
        );
        assert_eq!(
            eval_str("$INPUT", &r).unwrap(),
            json!({"names": ["hello"], "users": [{"name": "hello"}], "a": {"b": {"c": "deep"}}})
        );
        assert_eq!(eval_str("$INPUT.names[0]", &r).unwrap(), json!("hello"));
        assert_eq!(
            eval_str("$INPUT.users.0", &r).unwrap(),
            json!({"name": "hello"})
        );
        assert_eq!(eval_str("$INPUT.users.0.name", &r).unwrap(), json!("hello"));
        assert_eq!(eval_str("$INPUT.a.b.c", &r).unwrap(), json!("deep"));
        assert_eq!(eval_str("$INPUT.missing", &r).unwrap(), json!(null));
        assert_eq!(eval_str("$INPUT.missing.deeper", &r).unwrap(), json!(null));
        assert_eq!(eval_str("$NULL", &r).unwrap(), json!(null));
        assert_eq!(eval_str("$EMPTYSTRING", &r).unwrap(), json!(""));
    }

    #[test]
    fn missing_key_semantics() {
        let r = roots(json!({}));
        assert!(eval_str("$MISSINGROOT.x", &r).is_err());
        let err = eval_str("$INPUT.names[0]", &r).unwrap_err();
        // Python: a field miss yields None, and indexing None is a TypeError
        assert_eq!(err.kind, ErrorKind::TypeError);
        let list = roots(json!([[1, 2], [3]]));
        assert_eq!(eval_str("$INPUT[0][1]", &list).unwrap(), json!(2));
        assert_eq!(eval_str("$INPUT[-1][0]", &list).unwrap(), json!(3));
    }

    #[test]
    fn scalar_roots_and_errors() {
        let r = roots(json!(3));
        assert_eq!(eval_str("$F.add(3, $INPUT)", &r).unwrap(), json!(6));
        assert_eq!(
            eval_str("$F.does_not_exist(1, 2)", &r).unwrap_err().kind,
            ErrorKind::NameError
        );
        let err = eval_str("$F.uppr('x')", &r).unwrap_err();
        assert!(err.message.contains("Did you mean"), "{err}");
    }

    #[test]
    fn interpolation() {
        let r = roots(json!([{"name": "hello"}]));
        assert_eq!(
            eval_str("my name is {%$INPUT[0].name%}", &r).unwrap(),
            json!("my name is hello")
        );
        let r = roots(json!(3));
        assert_eq!(eval_str("v: {% $INPUT %}", &r).unwrap(), json!("v: 3"));
        assert_eq!(
            eval_str("let see: {%$F.add(3, $INPUT)%}", &r).unwrap(),
            json!("let see: 6")
        );
        assert_eq!(
            eval_str("x {%@INPUT[0].name%} y", &r).unwrap(),
            json!("x {%@INPUT[0].name%} y")
        );
        assert_eq!(eval_str("hi {%world%}", &r).unwrap(), json!("hi {%world%}"));
        assert_eq!(eval_str("a {% b", &r).unwrap(), json!("a {% b"));
        let r = roots(json!("x"));
        assert_eq!(
            eval_str("a {% $INPUT %} b {%", &r).unwrap(),
            json!("a x b {%")
        );
        // Python str() rendering rules inside templates
        let r = roots(json!({"a": null, "b": [1, 2.5], "c": "s"}));
        assert_eq!(
            eval_str("{%$INPUT%}", &r).unwrap(),
            json!("{'a': None, 'b': [1, 2.5], 'c': 's'}")
        );
        assert_eq!(
            eval_str("{% true %} {% None %} {% 1.5 %}", &r).unwrap(),
            json!("True None 1.5")
        );
    }

    #[test]
    fn runtime_strings_are_data_not_re_evaluated() {
        // 契约（2026-10-21）：运行期值是数据——节点结果里长得像表达式的字符串
        // 由路径读原样返回，绝不再解析；表达式只活在配置字段里（情况一的多层
        // 取值 $NODE.a.b.c 不受影响）
        let mut r = MapRoots::new();
        r.set(
            "NODE",
            json!({
                "n1": {"out": "$F.add(2,3)"},
                "n2": {"out": "$INPUT.x"},
                "n3": {"out": "{% $F.concat('a','b') %}"},
                "n4": {"deep": {"er": {"c": 5}}},
                "n5": {"arr": ["$F.add(1,1)"]},
                "n6": {"out": "$F.nope(1)"},
            }),
        );
        assert_eq!(eval_str("$NODE.n1.out", &r).unwrap(), json!("$F.add(2,3)"));
        assert_eq!(eval_str("$NODE.n2.out", &r).unwrap(), json!("$INPUT.x"));
        assert_eq!(
            eval_str("$NODE.n3.out", &r).unwrap(),
            json!("{% $F.concat('a','b') %}")
        );
        // 情况一：多层递进取值，一次求值
        assert_eq!(eval_str("$NODE.n4.deep.er.c", &r).unwrap(), json!(5));
        assert_eq!(
            eval_str("$NODE.n5.arr.0", &r).unwrap(),
            json!("$F.add(1,1)")
        );
        // 数据文本永不触发解析——哪怕是个会报 NameError 的"表达式"
        assert_eq!(eval_str("$NODE.n6.out", &r).unwrap(), json!("$F.nope(1)"));
        // 读法不影响语义：路径读与函数读结果一致（Python 侧两者不一致是
        // 实现残留的指纹，本引擎没有这条分裂）
        assert_eq!(
            eval_str("$F.get($NODE.n1, 'out')", &r).unwrap(),
            json!("$F.add(2,3)")
        );
    }

    #[test]
    fn raw_modifier_compat_and_collision() {
        let mut r = MapRoots::new();
        r.set("NODE", json!({"n": {"out": "$F.add(2,3)"}}));
        // raw 修饰符被接受并剥除；语义与裸路径读一致（都是原样）
        assert_eq!(
            eval_str("$NODE.raw.n.out", &r).unwrap(),
            json!("$F.add(2,3)")
        );
        assert_eq!(eval_str("$NODE.n.out", &r).unwrap(), json!("$F.add(2,3)"));
        // 碰撞规则：真有节点叫 raw → raw 按普通节点解析
        let mut r2 = MapRoots::new();
        r2.set("NODE", json!({"raw": {"n": {"out": 42}}}));
        assert_eq!(eval_str("$NODE.raw.n.out", &r2).unwrap(), json!(42));
    }

    #[test]
    fn mutator_gate() {
        let mut r = MapRoots::new();
        r.set("INPUT", json!({"m": {"a": 1}, "list": [1, 2]}));
        // mutating a live root is refused before evaluation
        let err = eval_str("$F.set($INPUT.m, 'b', 2)", &r).unwrap_err();
        assert_eq!(err.kind, ErrorKind::ValueError);
        // fresh producer targets are fine
        assert_eq!(
            eval_str("$F.pop($F.append($INPUT.list, 4), 0)", &r).unwrap(),
            json!(1)
        );
        // fresh-producer targets are fine for mutators
        assert_eq!(
            eval_str("$F.set($F.json_loads('{}'), 'a', 1)", &r).unwrap(),
            json!(null)
        );
    }

    #[test]
    fn parent_chain() {
        let mut outer = MapRoots::new();
        outer.set("INPUT", json!({"x": "outer"}));
        let mut inner = MapRoots::with_parent(&outer);
        inner.set("INPUT", json!({"x": "inner"}));
        assert_eq!(eval_str("$INPUT.x", &inner).unwrap(), json!("inner"));
        assert_eq!(
            eval_str("$PARENT.$INPUT.x", &inner).unwrap(),
            json!("outer")
        );
        assert_eq!(eval_str("$PARENT.INPUT.x", &inner).unwrap(), json!("outer"));
        assert!(eval_str("$PARENT", &inner).is_ok());
        assert!(eval_str("$PARENT.$INPUT.x", &MapRoots::new()).is_err());
        // plaita-go#80 r78 同款：父上不存在的根按快照对象的缺省字段读为
        // null（reference 隔离 golden `$PARENT.n` → null 钉），不再是 KeyError。
        assert_eq!(eval_str("$PARENT.n", &inner).unwrap(), json!(null));
        assert_eq!(eval_str("$PARENT.x", &inner).unwrap(), json!(null));
        assert_eq!(eval_str("$PARENT.$NODE.any", &inner).unwrap(), json!(null));
    }
}
