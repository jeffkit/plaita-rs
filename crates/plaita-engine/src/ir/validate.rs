//! Structural + reference validation of a normalized flow document (port of
//! plaita-go `pkg/ir/validate.go`, which ports plaita's ir_validate
//! DEFAULT_RULES):
//!
//! - R1: exactly one start, everything reachable from it;
//! - R3: `next` / switch-branch targets exist;
//! - R2: embedded flows reach an end node;
//! - R4: parallel `joinBranches ⊆ branches[].name`;
//! - R5: `$NODE.<id>` references resolve within the current (sub)flow;
//! - R6: `$F.<fn>(...)` calls are registered functions;
//! - R7: a static output literal matches the declared outputType;
//! - plus `warn_unknown_keys`: node config keys the engine has no field for
//!   are reported (log + node metadata) instead of being silently dropped.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use plaita_expression::function_names;

use crate::budget::{MAX_NODE_TIMEOUT, MAX_NODE_TIMEOUT_TEXT, MAX_RETRY_TIMES};
use crate::duration;
use crate::ir::scope::Scope;
use crate::ir::support::{first_any, get_str, node_label, type_name};
use crate::property::Property;

pub fn validate_doc(doc: &Value, scope: Scope) -> Result<(), String> {
    let Some(doc_obj) = doc.as_object() else {
        return Err("ir: flow document must be an object".to_string());
    };
    let flow_id = get_str(doc_obj, &["id"]).unwrap_or_default();
    // 流级 errorHandler 不是本引擎契约面（B4 r7）：plaita 的 Flow 模型无该
    // 字段，整流重投 ×(1+n) 是刻意不实现的 Go-ism——该键不再有 cap 校验，
    // 走 stash_unknown_flow_fields 的警告+metadata 通道。节点级 cap 不变
    // （Go checkRetryTimesCap 同款）。flow 级 timeout 有意不 cap。
    let Some(nodes) = doc_obj.get("nodes").and_then(|n| n.as_array()) else {
        return Err(format!("ir: flow {flow_id:?}: nodes missing"));
    };
    let mut node_objs: Vec<&Map<String, Value>> = Vec::with_capacity(nodes.len());
    for rn in nodes {
        let Some(n) = rn.as_object() else {
            return Err(format!("ir: flow {flow_id:?}: node entry is not an object"));
        };
        node_objs.push(n);
    }

    let mut ids: HashMap<String, bool> = HashMap::new();
    let mut starts = 0usize;
    for n in &node_objs {
        let Some(id) = get_str(n, &["id"]).filter(|s| !s.is_empty()) else {
            return Err(format!(
                "ir: node {:?}: missing id",
                get_str(n, &["type"]).unwrap_or_default()
            ));
        };
        if ids.contains_key(&id) {
            return Err(format!("ir: flow {flow_id:?}: duplicate node id {id:?}"));
        }
        ids.insert(id, true);
        if get_str(n, &["type"]).as_deref() == Some("start") {
            starts += 1;
        }
    }
    if starts != 1 {
        return Err(format!(
            "ir: flow {flow_id:?}: expected exactly one start node, got {starts}"
        ));
    }

    // collect edges: linear next + switch branch next + if false branch
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();
    let mut ends: HashSet<String> = HashSet::new();
    for n in &node_objs {
        let id = get_str(n, &["id"]).unwrap_or_default();
        // 类型错误的 next 不产生边会让 R1/R3 全绿假象（#9）——present 但非
        // 字符串即具名拒；null/空串仍是合法「无后继」
        match n.get("next") {
            None | Some(Value::Null) => {}
            Some(Value::String(next)) if !next.is_empty() => {
                edges.entry(id.clone()).or_default().push(next.clone());
            }
            Some(Value::String(_)) => {}
            Some(other) => {
                return Err(format!(
                    "ir: flow {flow_id:?}: node {id:?}: field \"next\" must be a string, got {}",
                    type_name(other)
                ));
            }
        }
        match get_str(n, &["type"]).as_deref() {
            Some("switch") => {
                if let Some(branches) = n.get("branches").and_then(|b| b.as_array()) {
                    for b in branches {
                        if let Some(next) = b
                            .as_object()
                            .and_then(|o| get_str(o, &["next"]))
                            .filter(|s| !s.is_empty())
                        {
                            edges.entry(id.clone()).or_default().push(next);
                        }
                    }
                }
            }
            // if 的假分支边（translateIf 已折叠 camel 别名 elseNext）。
            // Python ir_validate 把 else_next/elseNext 计入可达性边集——漏掉
            // 它会把仅从假分支可达的节点误判为不可达。
            Some("if") => {
                if let Some(e) = get_str(n, &["else_next", "elseNext"]).filter(|s| !s.is_empty()) {
                    edges.entry(id.clone()).or_default().push(e);
                }
            }
            Some("end") => {
                ends.insert(id.clone());
            }
            _ => {}
        }
    }
    for (id, outs) in &edges {
        for out in outs {
            if !ids.contains_key(out.as_str()) {
                return Err(format!(
                    "ir: flow {flow_id:?}: node {id:?} references missing node {out:?}"
                ));
            }
        }
    }

    let start_id = node_objs
        .iter()
        .find(|n| get_str(n, &["type"]).as_deref() == Some("start"))
        .and_then(|n| get_str(n, &["id"]))
        .unwrap_or_default();
    let reachable = bfs(&start_id, &edges);
    for n in &node_objs {
        let id = get_str(n, &["id"]).unwrap_or_default();
        if !reachable.contains(&id) {
            return Err(format!(
                "ir: flow {flow_id:?}: node {id:?} is not reachable from start"
            ));
        }
    }

    if scope == Scope::Child {
        let any_end_reachable = ends.iter().any(|id| reachable.contains(id));
        if !any_end_reachable {
            return Err(format!(
                "ir: embedded flow {flow_id:?} never reaches an end node"
            ));
        }
    }

    // parallel joinBranches ⊆ branches[].name（#9：present 非数组/元素非字符串
    // 具名拒，不再 and_then(as_array) 静默跳过；null 同缺省）
    for n in &node_objs {
        if get_str(n, &["type"]).as_deref() != Some("parallel") {
            continue;
        }
        let names: HashSet<String> = n
            .get("branches")
            .and_then(|b| b.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|b| b.as_object().and_then(|o| get_str(o, &["name"])))
                    .collect()
            })
            .unwrap_or_default();
        let nid = get_str(n, &["id"]).unwrap_or_default();
        match first_any(n, &["joinBranches", "join_branches"]) {
            None | Some(Value::Null) => {}
            Some(Value::Array(joins)) => {
                for (i, j) in joins.iter().enumerate() {
                    let Some(s) = j.as_str() else {
                        return Err(format!(
                            "ir: flow {flow_id:?}: parallel {nid:?}: joinBranches[{i}] must be a \
                             branch name string, got {}",
                            type_name(j)
                        ));
                    };
                    if !names.contains(s) {
                        return Err(format!(
                            "ir: flow {flow_id:?}: parallel {nid:?} joins unknown branch {s:?}"
                        ));
                    }
                }
            }
            Some(other) => {
                return Err(format!(
                    "ir: flow {flow_id:?}: parallel {nid:?}: joinBranches must be an array of \
                     branch names, got {}",
                    type_name(other)
                ));
            }
        }
    }

    // per-node rules (first error wins); they apply to every node incl. end —
    // end.output is the $NODE ref hot spot
    for n in &node_objs {
        check_budget_caps(&flow_id, n)?;
        check_node_refs(&flow_id, n, &ids)?;
        check_expression_functions(&flow_id, n)?;
        check_output_type_literal(&flow_id, n)?;
    }
    Ok(())
}

// --- budget caps（plaita#267/#35，与 plaita-go checkBudgetCaps 同名同语义）---

/// 装载期预算 cap：`errorHandler.retryTimes`（flow 级与节点级共用 cap 10）与
/// 节点 `timeout`（≤24h）——无界重试/超时预算让合法 flow 长期独占共享
/// worker，Go/Python 都在装载/构造期具名拒绝，本引擎先前是唯一放行的一家。
/// 时机在 normalize 之后：timeout 已是规范 ISO 8601 拼写（与 Go 一致）。
/// flow 级 timeout 有意不 cap（运营预算决策，Go validate.go 同注）。
fn check_budget_caps(flow_id: &str, n: &Map<String, Value>) -> Result<(), String> {
    if let Some(h) = n.get("errorHandler").and_then(|h| h.as_object()) {
        check_retry_times_cap(flow_id, h, &format!("node {:?}", node_label(n)))?;
    }
    if let Some(s) = n
        .get("timeout")
        .and_then(|t| t.as_str())
        .filter(|s| !s.is_empty())
    {
        if let Ok(d) = duration::parse_iso8601(s) {
            if d > MAX_NODE_TIMEOUT {
                return Err(format!(
                    "ir: flow {flow_id:?}: node {:?}: timeout {s} exceeds the \
                     {MAX_NODE_TIMEOUT_TEXT} cap (plaita#267/#35); a longer budget needs an \
                     explicit flow-level timeout, which still sits under the worker execution \
                     deadline",
                    node_label(n)
                ));
            }
        }
    }
    Ok(())
}

/// `workflow.MaxRetryTimes` applied to one NODE handler object — Go
/// checkRetryTimesCap 的同款文案（B4 r7 起 owner 恒为节点：flow 级
/// errorHandler 已随整流重投一并撤出契约面）。
fn check_retry_times_cap(
    flow_id: &str,
    handler: &Map<String, Value>,
    owner: &str,
) -> Result<(), String> {
    let Some(v) = handler.get("retryTimes").and_then(|v| v.as_f64()) else {
        return Ok(());
    };
    // 非数值/负数留给 de_u64_lenient 的类型层；这里只管值域上界
    if v <= MAX_RETRY_TIMES as f64 {
        return Ok(());
    }
    Err(format!(
        "ir: flow {flow_id:?}: {owner}: errorHandler.retryTimes {v} exceeds the cap \
         {MAX_RETRY_TIMES} (plaita#267/#35): retryTimes multiplies node attempts and would let \
         a legal flow monopolize a shared worker"
    ))
}

/// warn_unknown_keys 的落盘半边：未知键报告（stderr）+ 写进节点 metadata 的
/// `unknownFields`，使装载产物上可观测（Go validate.go:515-533 双通道，审计 F6）。
/// 在 validate_doc 之后以可变访问执行。
pub(crate) fn stash_unknown_node_fields(doc: &mut Value) {
    let flow_id = doc
        .as_object()
        .and_then(|o| o.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let Some(nodes) = doc.get_mut("nodes").and_then(|x| x.as_array_mut()) else {
        return;
    };
    for node in nodes.iter_mut() {
        let Some(obj) = node.as_object_mut() else {
            continue;
        };
        let node_type = get_str(obj, &["type"]).unwrap_or_default();
        let known = node_known_keys(&node_type);
        let mut unknown: Vec<String> = obj
            .keys()
            .filter(|k| !known.contains(&k.as_str()))
            .cloned()
            .collect();
        if unknown.is_empty() {
            continue;
        }
        unknown.sort();
        eprintln!(
            "[ir] flow {flow_id:?} node {:?} ({node_type}): unknown config key(s) {unknown:?} will be ignored by the engine (possible typo?)",
            node_label(obj)
        );
        let meta = obj
            .entry("metadata")
            .or_insert_with(|| Value::Object(Default::default()));
        if let Some(m) = meta.as_object_mut() {
            m.insert("unknownFields".into(), Value::String(unknown.join(",")));
        }
    }
}

// --- R5 / R6: expression reference scanning (ports _collect_refs) ---

/// `$NODE.<id>`: the id segment takes identifier characters only — dynamic
/// ids (`$NODE.$INPUT.k`) intentionally do not match (static analysis
/// boundary). Returns the matched span.
fn find_node_refs(s: &str) -> Vec<(usize, usize, String)> {
    let mut out = Vec::new();
    let b = s.as_bytes();
    let mut i = 0usize;
    while i + 6 <= b.len() {
        // 字节比较（非 str 切片）：多字节字符中间不得 panic
        if b[i..i + 6] == *b"$NODE." {
            let start = i + 6;
            let mut j = start;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_' || b[j] == b'-') {
                j += 1;
            }
            if j > start {
                // negative lookbehind: identifier char, '.' or '$' precedes
                let preceded = i > 0
                    && (b[i - 1] == b'_'
                        || b[i - 1] == b'.'
                        || b[i - 1] == b'$'
                        || b[i - 1].is_ascii_alphanumeric());
                if !preceded {
                    out.push((start, j, s[start..j].to_string()));
                }
            }
            i = j.max(i + 6);
        } else {
            i += 1;
        }
    }
    out
}

/// `$F.<fn>(` with optional whitespace before the paren.
fn find_func_refs(s: &str) -> Vec<String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 3 <= b.len() {
        if b[i..i + 3] == *b"$F." {
            let start = i + 3;
            let mut j = start;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            if j > start {
                let preceded = i > 0
                    && (b[i - 1] == b'_'
                        || b[i - 1] == b'.'
                        || b[i - 1] == b'$'
                        || b[i - 1].is_ascii_alphanumeric());
                if !preceded {
                    let mut k = j;
                    // Go regex \s*：换行也算（审计 F3）
                    while k < b.len() && b[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    if k < b.len() && b[k] == b'(' {
                        out.push(s[start..j].to_string());
                    }
                }
            }
            i = j.max(i + 3);
        } else {
            i += 1;
        }
    }
    out
}

/// Keys whose whole subtree carries no expression references
/// (ir_validate._UNSCANNED_KEYS). `code` is node source, not plaita
/// expressions; childFlow/child_flow/flow are nested graphs validated
/// against their own id scope; `error` is node-level only — an end node's
/// failure payload is literal data ("$NODE.x" inside it is text; an `error`
/// key nested anywhere deeper is scanned again).
const UNSCANNED_KEYS: &[&str] = &[
    "code",
    "desc",
    "description",
    "name",
    "title",
    "label",
    "node_name",
    "id",
    "type",
    "next",
    "else_next",
    "elseNext",
    "default",
    "flow_id",
    "flowId",
    "source_line",
    "language",
    "error",
    "childFlow",
    "child_flow",
    "flow",
];

/// unscannedKeys − {error} (ir_validate #144 split).
fn unscanned_nested(key: &str) -> bool {
    key != "error" && UNSCANNED_KEYS.contains(&key)
}

fn collect_refs(n: &Map<String, Value>, func_refs: bool) -> Vec<String> {
    let mut refs = Vec::new();
    for (k, v) in n {
        if UNSCANNED_KEYS.contains(&k.as_str()) {
            continue;
        }
        scan_ref_strings(v, func_refs, &mut refs);
    }
    refs
}

fn scan_ref_strings(v: &Value, func_refs: bool, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if func_refs {
                out.extend(find_func_refs(s));
            } else {
                out.extend(find_node_refs(s).into_iter().map(|(_, _, name)| name));
            }
        }
        Value::Array(items) => {
            for item in items {
                scan_ref_strings(item, func_refs, out);
            }
        }
        Value::Object(o) => {
            for (k, item) in o {
                if unscanned_nested(k) {
                    continue;
                }
                scan_ref_strings(item, func_refs, out);
            }
        }
        _ => {}
    }
}

fn unknown_refs(refs: Vec<String>, known: &dyn Fn(&str) -> bool) -> Vec<String> {
    let mut set: std::collections::BTreeSet<String> = Default::default();
    for r in refs {
        if !known(&r) {
            set.insert(r);
        }
    }
    set.into_iter().collect()
}

/// R5: every `$NODE.<id>` referenced from the node must exist in the current
/// (sub)flow — unknown ids evaluate to nil at runtime and the flow keeps
/// running on empty values (silent corruption in plaita, late nil here).
fn check_node_refs(
    flow_id: &str,
    n: &Map<String, Value>,
    ids: &HashMap<String, bool>,
) -> Result<(), String> {
    let refs = collect_refs(n, false);
    if refs.is_empty() {
        return Ok(());
    }
    let unknown = unknown_refs(refs, &|r| ids.contains_key(r));
    if unknown.is_empty() {
        return Ok(());
    }
    let candidates: Vec<&str> = ids.keys().map(String::as_str).collect();
    let hint = did_you_mean(&unknown, &candidates);
    Err(format!(
        "ir: flow {flow_id:?}: node {:?}: $NODE references unknown node id(s) {unknown:?}{hint} — \
         they would evaluate to nil at runtime; $NODE resolves within the current flow scope only",
        node_label(n)
    ))
}

/// R6: every `$F.<fn>(...)` must be registered in the default expression
/// registry — unknown functions are rejected at evaluation time (fail-closed,
/// plaita's default-registry NameError).
fn check_expression_functions(flow_id: &str, n: &Map<String, Value>) -> Result<(), String> {
    let refs = collect_refs(n, true);
    if refs.is_empty() {
        return Ok(());
    }
    let known: HashSet<&'static str> = function_names().into_iter().collect();
    let unknown = unknown_refs(refs, &|r| known.contains(r));
    if unknown.is_empty() {
        return Ok(());
    }
    let hint = did_you_mean(&unknown, &function_names());
    Err(format!(
        "ir: flow {flow_id:?}: node {:?}: $F calls unregistered function(s) {unknown:?}{hint} — \
         check the spelling; unknown $F functions are rejected at evaluation time",
        node_label(n)
    ))
}

/// difflib.get_close_matches (similarity ≥ 0.6, top 3) hint for one name.
fn did_you_mean(bad: &[String], candidates: &[&str]) -> String {
    for name in bad {
        let mut hits: Vec<(f64, &str)> = candidates
            .iter()
            .map(|c| (similarity(name, c), *c))
            .filter(|(s, _)| *s >= 0.6)
            .collect();
        hits.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(b.1))
        });
        hits.truncate(3);
        if hits.is_empty() {
            continue;
        }
        let names: Vec<String> = hits.into_iter().map(|(_, n)| format!("{n:?}")).collect();
        return format!(" (did you mean [{}] for ${name}?)", names.join(", "));
    }
    String::new()
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

/// R7: when output is a static literal and the node declares an outputType,
/// run Property.validate_value now — the literal would otherwise fail (or
/// pass unchecked) only at execution time.
fn check_output_type_literal(flow_id: &str, n: &Map<String, Value>) -> Result<(), String> {
    let Some(declared) = first_any(n, &["outputType", "output_type"]).and_then(|d| d.as_object())
    else {
        return Ok(());
    };
    if declared.is_empty() {
        return Ok(());
    }
    let Some(out) = n.get("output") else {
        return Ok(());
    };
    if out.is_null() || !is_static_value(out) {
        return Ok(());
    }
    let Ok(prop) = serde_json::from_value::<Property>(Value::Object(declared.clone())) else {
        return Ok(());
    };
    prop.validate_value(out).map_err(|e| {
        format!(
            "ir: flow {flow_id:?}: node {:?}: output literal {out} does not match declared outputType \
             (dataType {:?}): {e}",
            node_label(n),
            prop.data_type.map(|d| d.as_str().to_string()).unwrap_or_default()
        )
    })
}

/// Reports whether the IR value is final at compile time: no expression ($)
/// or template ({%) markers, recursing into containers.
fn is_static_value(v: &Value) -> bool {
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) => true,
        Value::String(s) => !s.contains('$') && !s.contains("{%"),
        Value::Array(items) => items.iter().all(is_static_value),
        Value::Object(o) => o.values().all(is_static_value),
    }
}

// --- unknown node key hygiene (ports warn_unknown_keys) ---

/// 流级未知键卫生（B4 r7）：plaita 的 Flow 模型没有流级 `errorHandler`——
/// 整流重投 ×(1+n) 是刻意不实现的 Go-ism（Python flow.py 无该字段，顶层
/// errorHandler=未知键 warning+忽略，流恒单次执行）。引擎对该键（两拼写）
/// 点名告警 + 落 metadata.unknownFields——把沉默变可见；字段本身被忽略，
/// 不再参与任何装载校验（流级 retryTimes cap 一并撤除）。
pub(crate) fn stash_unknown_flow_fields(doc: &mut Value) {
    let Some(obj) = doc.as_object_mut() else {
        return;
    };
    let found: Vec<String> = ["errorHandler", "error_handler"]
        .iter()
        .filter(|k| obj.contains_key(**k))
        .map(|k| k.to_string())
        .collect();
    if found.is_empty() {
        return;
    }
    let flow_id = get_str(obj, &["id"]).unwrap_or_default();
    eprintln!(
        "[ir] flow {flow_id:?}: flow-level errorHandler ({}) is not part of this engine's \
         contract and will be IGNORED (plaita has no flow-level error handling; the Go \
         whole-flow retryTimes re-run is deliberately not implemented — the flow runs exactly \
         once). Move it onto the nodes that need it.",
        found.join(", ")
    );
    let meta = obj
        .entry("metadata")
        .or_insert_with(|| Value::Object(Default::default()));
    if let Some(m) = meta.as_object_mut() {
        m.insert("unknownFields".into(), Value::String(found.join(",")));
    }
}

/// The JSON keys each node type accepts (mirrors the Go node structs; the
/// loader itself adds `metadata` during normalization).
fn node_known_keys(node_type: &str) -> Vec<&'static str> {
    const BASE: &[&str] = &[
        "id",
        "type",
        "name",
        "desc",
        "async",
        "output",
        "external",
        "timeout",
        "errorHandler",
        "timeoutHandler",
        "next",
        "outputType",
        "metadata",
    ];
    let kind: &[&str] = match node_type {
        "end" => &["resultType", "error"],
        "assignment" => &["upstreamOutput"],
        "switch" => &["branches"],
        "if" => &["condition", "else_next", "elseNext"],
        "loop" => &[
            "childFlow",
            "child_flow",
            "input",
            "collection",
            "condition",
            "itemType",
            "item_type",
            "typeDefs",
        ],
        "map" => &[
            "childFlow",
            "child_flow",
            "input",
            "collection",
            "concurrent",
            "maxConcurrent",
            "max_concurrent",
            "itemType",
            "item_type",
            "typeDefs",
        ],
        "filter" | "find" => &[
            "childFlow",
            "child_flow",
            "input",
            "collection",
            "itemType",
            "item_type",
            "typeDefs",
        ],
        "reduce" => &[
            "childFlow",
            "child_flow",
            "input",
            "collection",
            "initial",
            "itemType",
            "item_type",
            "typeDefs",
        ],
        "while" => &[
            "childFlow",
            "child_flow",
            "input",
            "condition",
            "max_iterations",
            "maxIterations",
        ],
        "mock" => &["value"],
        "event" => &[
            "event_type",
            "eventFilter",
            "event_filter",
            "subscriptionTimeout",
            "subscription_timeout",
        ],
        "hitl_await" => &[
            "message",
            "images",
            "base_url",
            "baseUrl",
            "timeout_secs",
            "timeoutSecs",
            "event_type",
            "eventType",
            "eventFilter",
            "event_filter",
            "dry_run",
            "dryRun",
        ],
        "delay" => &[
            "delay_seconds",
            "delaySeconds",
            "delay_unit",
            "delayUnit",
            "event_type",
            "eventType",
            "eventFilter",
            "event_filter",
            "subscriptionTimeout",
            "subscription_timeout",
        ],
        "approval" => &[
            "approval_title",
            "approvalTitle",
            "approval_content",
            "approvalContent",
            "approval_type",
            "approvalType",
            "approvers",
            "approval_strategy",
            "approvalStrategy",
            "auto_escalation",
            "autoEscalation",
            "escalation_timeout_hours",
            "escalationTimeoutHours",
            "escalation_approvers",
            "escalationApprovers",
            "form_fields",
            "formFields",
            "allow_comments",
            "allowComments",
            "require_comments",
            "requireComments",
            "notification_config",
            "notificationConfig",
            "event_type",
            "eventType",
            "eventFilter",
            "event_filter",
            "subscriptionTimeout",
            "subscription_timeout",
        ],
        "redis_queue" => &[
            "redis_host",
            "redisHost",
            "redis_port",
            "redisPort",
            "redis_db",
            "redisDb",
            "redis_password",
            "redisPassword",
            "queue_name",
            "queueName",
            "queue_type",
            "queueType",
            "timeout_seconds",
            "timeoutSeconds",
            "batch_size",
            "batchSize",
            "message_format",
            "messageFormat",
            "event_type",
            "eventType",
            "eventFilter",
            "event_filter",
            "subscriptionTimeout",
            "subscription_timeout",
        ],
        "kafka_queue" => &[
            "bootstrap_servers",
            "bootstrapServers",
            "security_protocol",
            "securityProtocol",
            "sasl_mechanism",
            "saslMechanism",
            "sasl_username",
            "saslUsername",
            "sasl_password",
            "saslPassword",
            "topic",
            "partition",
            "group_id",
            "groupId",
            "auto_offset_reset",
            "autoOffsetReset",
            "enable_auto_commit",
            "enableAutoCommit",
            "max_poll_records",
            "maxPollRecords",
            "message_format",
            "messageFormat",
            "timeout_ms",
            "timeoutMs",
            "event_type",
            "eventType",
            "eventFilter",
            "event_filter",
            "subscriptionTimeout",
            "subscription_timeout",
        ],
        "http_callback" => &[
            "callback_path",
            "callbackPath",
            "callback_method",
            "callbackMethod",
            "callback_timeout_minutes",
            "callbackTimeoutMinutes",
            "require_auth",
            "requireAuth",
            "auth_type",
            "authType",
            "auth_token",
            "authToken",
            "validate_headers",
            "validateHeaders",
            "validate_params",
            "validateParams",
            "success_response",
            "successResponse",
            "error_response",
            "errorResponse",
            "event_type",
            "eventType",
            "eventFilter",
            "event_filter",
            "subscriptionTimeout",
            "subscription_timeout",
        ],
        "reference" => &[
            "childFlow",
            "child_flow",
            "input",
            "flowId",
            "flow_id",
            "flowVersion",
            "flow_version",
        ],
        "child" => &["childFlow", "child_flow", "input"],
        "parallel" => &[
            "branches",
            "isConditional",
            "mode",
            "joinBranches",
            "join_branches",
        ],
        "code" => &[
            "language",
            "code",
            "input",
            "sandboxBackend",
            "sandbox_backend",
        ],
        "http" => &[
            "method",
            "contentType",
            "content_type",
            "url",
            "query",
            "headers",
            "body",
            "addressing",
            "delegate",
            "requestTimeout",
            "request_timeout",
            "maxRedirects",
            "max_redirects",
            "allowedHosts",
            "allowed_hosts",
            "deniedHosts",
            "denied_hosts",
            "blockPrivateNetworks",
            "block_private_networks",
        ],
        _ => &[],
    };
    BASE.iter().chain(kind.iter()).copied().collect()
}

fn bfs(start: &str, edges: &HashMap<String, Vec<String>>) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(start.to_string());
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(start.to_string());
    while let Some(cur) = queue.pop_front() {
        if let Some(outs) = edges.get(&cur) {
            for next in outs {
                if seen.insert(next.clone()) {
                    queue.push_back(next.clone());
                }
            }
        }
    }
    seen
}
