//! Normalization: rewrites a deployment-form flow document in place into the
//! engine's native JSON shape (port of plaita-go `pkg/ir/normalize.go`).
//!
//! - flow id: `flow_id` wins over legacy `id`; anonymous roots get a
//!   placeholder; embedded flows get `<node>.childFlow` /
//!   `<node>.branch.<name>`;
//! - alias keys fold (camel wins except `flow_id`): `global_context` →
//!   `context.global`, handler `continue_with` → `continue-with`, property
//!   `type/children/default/required` → engine spellings, the plaita-nodes
//!   camel↔snake field alias table;
//! - timeouts become ISO 8601 strings (milliseconds accepted); an explicit 0
//!   stays the strictest budget (plaita#138), negatives are rejected
//!   (plaita#139);
//! - lossy-but-informative fields (`source_line`, `maxRedirects`, unknown
//!   parallel modes) are stashed into metadata;
//! - `exposeEnv` is validated at load (no wildcards/blank entries);
//! - embedded flows (childFlow / branches[].flow) normalize + validate
//!   recursively (mirrors plaita's check_childflow_reachable_end).

use serde_json::{json, Map, Value};

use std::time::Instant;

use crate::duration;
use crate::ir::support::{
    as_float, get_str, is_object, next_is_empty, obj, stash_into, take, type_name,
};
use crate::ir::{scope::Scope, validate};

/// Node type accept-list (strict, fail-closed): types this engine implements.
/// 集合并五类 + case/mock/reference 的转正对齐 plaita-go#80 phase 1+2
/// （权威语义 = plaita/node/loop.py + decide.py SwitchLegacy + mock.py +
/// child.py ReferenceFlow；golden 对拍 tests/node_parity_golden.rs）。
pub const SUPPORTED_NODE_TYPES: &[&str] = &[
    "start",
    "end",
    "assignment",
    "switch",
    "if",
    "parallel",
    "loop",
    "child",
    "http",
    "code",
    "map",
    "filter",
    "find",
    "reduce",
    "while",
    "case",
    "mock",
    "reference",
    // 挂起族七类（suspend/resume 架构，B4 r12）：等待/恢复契约的引擎侧
    // （pending 载荷 + 决策载体 + 订阅注册表宿主面）已落地。
    "event",
    "hitl_await",
    "delay",
    "approval",
    "redis_queue",
    "kafka_queue",
    "http_callback",
    // The remainder of plaita-go's accept list is NOT implemented by
    // plaita-rs yet; each gets a targeted hint in KNOWN_UNSUPPORTED.
];

/// plaita-go's accept-list entries plaita-rs has not ported, with hints.
/// 留白族：外部宿主（agentrun + plaita-nodes——需要 agentproc/凭据/服务商
/// 基础设施）。挂起族七类已随 suspend/resume 架构转正（B4 r12）；其部署侧
/// 依赖（服务层/订阅存储）在 capability manifest 的 node_dependencies 键
/// 逐节点标注。
pub const KNOWN_UNSUPPORTED: &[(&str, &str)] = &[
    (
        "calculate",
        "unRecognized node type: calculate — plaita does not register this type \
         (the module is a leftover; the flow must not load here either)",
    ),
    (
        "ignore",
        "unRecognized node type: ignore — plaita does not register this type \
         (it is a Node base-class placeholder, not a deployable node)",
    ),
    ("sleep", "sleep node is not part of the plaita dialect"),
    ("trpc", "internal RPC node was removed from this engine"),
    ("mdb", "internal database node was removed from this engine"),
    (
        "readmdb",
        "internal database node was removed from this engine",
    ),
    (
        "writemdb",
        "internal database node was removed from this engine",
    ),
    ("redis", "redis node is not ported by plaita-rs yet"),
    (
        "agentrun",
        "agentrun node is not ported by plaita-rs yet (no agentproc host)",
    ),
    ("gate", "gate node is not ported by plaita-rs yet"),
    (
        "rate_limit",
        "rate_limit node is not ported by plaita-rs yet",
    ),
    ("report", "report node is not ported by plaita-rs yet"),
    ("notify", "notify node is not ported by plaita-rs yet"),
    (
        "feishu_webhook",
        "feishu_webhook node is not ported by plaita-rs yet",
    ),
    (
        "dingtalk_webhook",
        "dingtalk_webhook node is not ported by plaita-rs yet",
    ),
    (
        "slack_webhook",
        "slack_webhook node is not ported by plaita-rs yet",
    ),
    (
        "wecom_webhook",
        "wecom_webhook node is not ported by plaita-rs yet",
    ),
    (
        "generic_webhook",
        "generic_webhook node is not ported by plaita-rs yet",
    ),
    (
        "email_send",
        "email_send node is not ported by plaita-rs yet",
    ),
    (
        "api_request",
        "api_request node is not ported by plaita-rs yet",
    ),
    (
        "parse_json",
        "parse_json node is not ported by plaita-rs yet",
    ),
    ("writefile", "writefile node is not ported by plaita-rs yet"),
    ("capture", "capture node is not ported by plaita-rs yet"),
    (
        "git_publish",
        "git_publish node is not ported by plaita-rs yet",
    ),
    (
        "github_comment",
        "github_comment node is not ported by plaita-rs yet",
    ),
    ("llm", "llm node is not ported by plaita-rs yet"),
    ("decision", "decision node is not ported by plaita-rs yet"),
    ("hitl", "hitl node is not ported by plaita-rs yet"),
    (
        "sql_query",
        "sql_query node is not ported by plaita-rs yet (driver selection + credential \
         store are prerequisites; see plaita-go#84 for the same evaluation)",
    ),
];

/// camelCase spellings of plaita-nodes snake fields fold into the canonical
/// snake keys (the mirror of plaita-go `nodeFieldAliases`).
const NODE_FIELD_ALIASES: &[(&str, &str)] = &[
    ("timeoutSecs", "timeout_secs"),
    ("maxRetries", "max_retries"),
    ("dryRun", "dry_run"),
    ("storeDir", "store_dir"),
    ("baseDir", "base_dir"),
    ("gateName", "gate_name"),
    ("bodyIsHtml", "body_is_html"),
    ("timeoutMs", "timeout_ms"),
    ("raiseOnError", "raise_on_error"),
    ("allowRedirects", "allow_redirects"),
    ("joinFields", "join_fields"),
    ("workspaceRoot", "workspace_root"),
    ("worktreeDir", "worktree_dir"),
    ("branchName", "branch_name"),
    ("commitMessage", "commit_message"),
    ("planFile", "plan_file"),
    ("issueNumber", "issue_number"),
    ("mergeMode", "merge_mode"),
    ("mainClone", "main_clone"),
    ("baseBranch", "base_branch"),
    ("apiBase", "api_base"),
    ("apiKey", "api_key"),
    ("maxTokens", "max_tokens"),
    ("jsonMode", "json_mode"),
    ("extraBody", "extra_body"),
    ("minConfidence", "min_confidence"),
    ("onLowConfidence", "on_low_confidence"),
    ("defaultChoice", "default_choice"),
    ("baseUrl", "base_url"),
    ("pollInterval", "poll_interval"),
    ("waitReply", "wait_reply"),
];

/// Caps reference-chain resolution (a cycle must fail the load with a named
/// error, not recurse forever) — plaita-go#80 phase 2 同款上限。
const MAX_REFERENCE_RESOLVE_DEPTH: u32 = 32;

/// Carries the optional [`crate::ir::FlowProvider`] through normalization;
/// `depth` counts open reference resolutions along the current chain
/// (diamonds don't accumulate — only the active chain counts, as in Go).
pub(crate) struct FlowResolver<'a> {
    pub provider: &'a dyn crate::ir::FlowProvider,
}

pub fn normalize_doc(doc: &mut Value, scope: Scope) -> Result<(), String> {
    normalize_doc_with(doc, scope, None, 0)
}

pub fn normalize_doc_with(
    doc: &mut Value,
    scope: Scope,
    resolver: Option<&FlowResolver>,
    depth: u32,
) -> Result<(), String> {
    let Some(doc_obj) = doc.as_object_mut() else {
        return Err("ir: flow document must be an object".to_string());
    };

    // flow id: plaita emits flow_id; console exports may carry id. flow_id wins.
    let mut flow_id = get_str(doc_obj, &["flowId", "flow_id"]).unwrap_or_default();
    doc_obj.remove("flowId");
    doc_obj.remove("flow_id");
    if !flow_id.is_empty() {
        doc_obj.insert("id".into(), Value::String(flow_id.clone()));
    } else {
        flow_id = get_str(doc_obj, &["id"]).unwrap_or_default();
        if flow_id.is_empty() {
            doc_obj.insert("id".into(), Value::String("plaita-flow".into()));
        }
    }
    let flow_id = get_str(doc_obj, &["id"]).unwrap_or_default();

    // global context: plaita global_context → engine context.global。与 Go 对齐：
    // 仅当 context 键不存在时才建 context.global（显式 context 声明胜出，
    // 审计 F4）；两个拼写键都删净（审计 F8）。
    {
        let first = take(doc_obj, &["globalContext", "global_context"]);
        doc_obj.remove("globalContext");
        doc_obj.remove("global_context");
        if let Some(v) = first {
            if !doc_obj.contains_key("context") {
                doc_obj.insert("context".into(), obj([("global", v)]));
            }
        }
    }

    // exposeEnv: fold to the contract spelling and validate at load
    // (fail-closed; plaita _safe_environment rules pulled forward).
    fold_key(doc_obj, "expose_env", "exposeEnv");
    if let Some(v) = doc_obj.get("exposeEnv") {
        validate_expose_env(v).map_err(|e| format!("ir: flow {flow_id:?}: {e}"))?;
    }
    let meta = as_meta(doc_obj);
    doc_obj.insert("metadata".into(), Value::Object(meta.clone()));

    if let Some(v) = doc_obj.remove("timeout") {
        match normalize_timeout(&v)? {
            None => {}
            Some(s) => {
                doc_obj.insert("timeout".into(), Value::String(s));
            }
        }
    }

    for k in ["inputType", "input_type", "outputType", "output_type"] {
        if let Some(v) = doc_obj.get_mut(k) {
            let err = |m: String| format!("ir: flow {flow_id:?}: {k}: {m}");
            normalize_property(v).map_err(err)?;
            if k == "input_type" {
                let v = doc_obj.remove("input_type").unwrap();
                doc_obj.insert("inputType".into(), v);
            } else if k == "output_type" {
                let v = doc_obj.remove("output_type").unwrap();
                doc_obj.insert("outputType".into(), v);
            }
        }
    }

    let Some(nodes) = doc_obj.get_mut("nodes").and_then(|n| n.as_array_mut()) else {
        return Err(format!(
            "ir: flow {flow_id:?}: nodes must be a non-empty array"
        ));
    };
    if nodes.is_empty() {
        return Err(format!(
            "ir: flow {flow_id:?}: nodes must be a non-empty array"
        ));
    }
    for raw in nodes.iter_mut() {
        let Some(n) = raw.as_object_mut() else {
            return Err(format!(
                "ir: flow {flow_id:?}: nodes[] entry is not an object"
            ));
        };
        normalize_node(n, resolver, depth).map_err(|e| {
            format!(
                "ir: flow {flow_id:?}: node {:?}: {e}",
                n.get("id").and_then(|v| v.as_str()).unwrap_or("?")
            )
        })?;
        normalize_embedded(&flow_id, raw, resolver, depth)?;
    }

    validate::validate_doc(doc, scope).map_err(|e| e.to_string())?;
    validate::stash_unknown_node_fields(doc);
    validate::stash_unknown_flow_fields(doc);
    Ok(())
}

/// Recursively normalizes + validates the flow documents embedded in
/// loop/child/parallel nodes. The engine requires a non-blank workflow id,
/// which plaita's embedded flows never carry — one is synthesized.
fn normalize_embedded(
    flow_id: &str,
    node: &mut Value,
    resolver: Option<&FlowResolver>,
    depth: u32,
) -> Result<(), String> {
    let node_obj = match node.as_object_mut() {
        Some(o) => o,
        None => return Ok(()),
    };
    let node_id = get_str(node_obj, &["id"]).unwrap_or_default();
    let node_type = get_str(node_obj, &["type"]).unwrap_or_default();
    let embed = |sub: Option<&mut Value>, name: &str| -> Result<(), String> {
        let Some(sub) = sub else { return Ok(()) };
        if !is_object(sub) {
            return Ok(());
        }
        let sub_obj = sub.as_object_mut().unwrap();
        if get_str(sub_obj, &["id"]).unwrap_or_default().is_empty() {
            sub_obj.insert("id".into(), Value::String(format!("{node_id}.{name}")));
        }
        normalize_doc_with(sub, Scope::Child, resolver, depth)
            .map_err(|e| format!("ir: flow {flow_id:?}: node {node_id:?} {name}: {e}"))
    };

    match node_type.as_str() {
        "loop" | "child" | "map" | "filter" | "find" | "reduce" | "while" | "reference" => {
            if node_obj.contains_key("childFlow") {
                embed(node_obj.get_mut("childFlow"), "childFlow")?;
            }
        }
        "parallel" => {
            let Some(branches) = node_obj.get_mut("branches").and_then(|b| b.as_array_mut()) else {
                return Ok(());
            };
            for (i, rb) in branches.iter_mut().enumerate() {
                let Some(b) = rb.as_object_mut() else {
                    return Err(format!(
                        "ir: flow {flow_id:?}: node {node_id:?} branches[{i}] is not an object"
                    ));
                };
                let mut name = get_str(b, &["name"]).unwrap_or_default();
                if name.is_empty() {
                    name = format!("branch.{i}");
                }
                if b.contains_key("flow") {
                    embed(b.get_mut("flow"), &format!("branch.{name}"))?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn normalize_node(
    n: &mut Map<String, Value>,
    resolver: Option<&FlowResolver>,
    depth: u32,
) -> Result<(), String> {
    let node_type = get_str(n, &["type"]).unwrap_or_default().to_string();
    if node_type.is_empty() {
        return Err("missing type".to_string());
    }
    if !SUPPORTED_NODE_TYPES.contains(&node_type.as_str()) {
        if let Some((_, hint)) = KNOWN_UNSUPPORTED.iter().find(|(t, _)| *t == node_type) {
            return Err(format!("node type {node_type:?} is not supported: {hint}"));
        }
        return Err(format!(
            "node type {node_type:?} is not supported by this engine (supported: [{}])",
            SUPPORTED_NODE_TYPES.join(" ")
        ));
    }

    let mut meta = as_meta(n);

    // handler keys: snake spellings fold into camel (camel wins)
    fold_key(n, "timeout_handler", "timeoutHandler");
    fold_key(n, "error_handler", "errorHandler");
    normalize_handler(n, "timeoutHandler")?;
    normalize_handler(n, "errorHandler")?;

    // source_line: compile-time provenance, kept observable via metadata
    if let Some(v) = take(n, &["sourceLine", "source_line"]) {
        stash_into(&mut meta, "sourceLine", v);
    }
    n.insert("metadata".into(), Value::Object(meta.clone()));

    if let Some(v) = n.remove("timeout") {
        match normalize_timeout(&v)? {
            None => {}
            Some(s) => {
                n.insert("timeout".into(), Value::String(s));
            }
        }
    }
    if let Some(v) = n.get_mut("outputType") {
        if v.is_object() {
            normalize_property(v).map_err(|e| format!("outputType: {e}"))?;
        }
    }
    fold_key(n, "output_type", "outputType");

    for (from, to) in NODE_FIELD_ALIASES {
        fold_key(n, from, to);
    }

    match node_type.as_str() {
        "switch" => translate_switch(n),
        "if" => translate_if(n),
        "parallel" => translate_parallel(n),
        "loop" => translate_loop(n),
        "map" | "filter" | "find" | "reduce" => translate_collection(n, &node_type),
        "while" => translate_while(n),
        "child" => translate_child(n),
        "case" => translate_case(n),
        "mock" => Ok(()),
        "reference" => translate_reference(n, resolver, depth),
        "event" => translate_event(n),
        "hitl_await" => translate_hitl_await(n),
        "delay" => translate_delay(n),
        "approval" => translate_approval(n),
        "redis_queue" => translate_redis_queue(n),
        "kafka_queue" => translate_kafka_queue(n),
        "http_callback" => translate_http_callback(n),
        "http" => translate_http(n),
        "code" => translate_code(n),
        "end" => translate_end(n),
        _ => Ok(()),
    }
}

/// Canonicalizes an error/timeout handler: strategy spelling, plaita
/// error_code/error_message folded into the engine's code/message fields,
/// retry spelling folded.
fn normalize_handler(n: &mut Map<String, Value>, key: &str) -> Result<(), String> {
    let Some(raw) = n.get_mut(key) else {
        return Ok(());
    };
    let Some(h) = raw.as_object_mut() else {
        return Err(format!("{key} must be an object"));
    };
    fold_key(h, "default_value", "defaultValue");
    fold_key(h, "retry_times", "retryTimes");
    if let Some(Value::String(s)) = h.get("strategy") {
        if s == "continue_with" {
            h.insert("strategy".into(), Value::String("continue-with".into()));
        }
    }
    fold_key(h, "error_code", "code");
    fold_key(h, "error_message", "message");
    Ok(())
}

fn translate_switch(n: &mut Map<String, Value>) -> Result<(), String> {
    let Some(branches) = n.get_mut("branches").and_then(|b| b.as_array_mut()) else {
        return Ok(());
    };
    for (i, rb) in branches.iter_mut().enumerate() {
        let Some(b) = rb.as_object_mut() else {
            return Err(format!("branches[{i}] is not an object"));
        };
        if next_is_empty(b) {
            let name = get_str(b, &["name"]).unwrap_or_default();
            if name.is_empty() {
                return Err(format!("branches[{i}] has neither next nor name"));
            }
            b.insert("next".into(), Value::String(name));
        }
        if let Some(c) = b.get_mut("condition") {
            if c.is_object() {
                normalize_condition(c).map_err(|e| format!("branches[{i}] condition: {e}"))?;
            }
        }
    }
    Ok(())
}

/// The `if` node (decide.py Bool): `next` / `else_next` must be non-empty and
/// the condition shape is fail-closed — a partial condition would always take
/// the else branch (plaita decide.py:168-175 admits the defect shape; the
/// loader refuses it instead of silently succeeding).
fn translate_if(n: &mut Map<String, Value>) -> Result<(), String> {
    fold_key(n, "elseNext", "else_next");
    // 类型错误的 else_next 不再被 get_str 折成 "" 误报「缺假分支目标」(#9)
    if let Some(v) = n.get("else_next") {
        if !v.is_null() && !v.is_string() {
            return Err(format!(
                "if node field \"else_next\" must be a string, got {}",
                type_name(v)
            ));
        }
    }
    if next_is_empty(n) {
        return Err("if node requires a true-branch target (next)".to_string());
    }
    if get_str(n, &["else_next"]).unwrap_or_default().is_empty() {
        return Err("if node requires a false-branch target (else_next/elseNext)".to_string());
    }
    if let Some(v) = n.get("condition").cloned() {
        if !v.is_null() {
            let Some(c) = v.as_object() else {
                return Err("if condition must be an object ({field,operator,value} or {relation,conditions}), got non-object".to_string());
            };
            let has_field = c.contains_key("field");
            let has_operator = c.contains_key("operator");
            let has_value = c.contains_key("value");
            let conditions_len = c
                .get("conditions")
                .and_then(|x| x.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            let has_relation = c.contains_key("relation");
            if has_field && has_operator && has_value {
                // single condition (value may be null: eq/null is legal)
            } else if has_relation && conditions_len > 0 {
                let mut cv = v.clone();
                normalize_condition(&mut cv).map_err(|e| format!("if condition: {e}"))?;
                n.insert("condition".into(), cv);
            } else {
                return Err(
                    "if condition is malformed: needs {field,operator,value} or {relation,conditions[>0]}; \
                     a partial condition would always take the else branch"
                        .to_string(),
                );
            }
        }
    }
    Ok(())
}

fn normalize_condition(c: &mut Value) -> Result<(), String> {
    let Some(conds) = c.get_mut("conditions").and_then(|x| x.as_array_mut()) else {
        return Ok(());
    };
    for rc in conds.iter_mut() {
        let Some(rco) = rc.as_object_mut() else {
            return Err("condition group member is not an object".to_string());
        };
        if rco.contains_key("conditions") {
            normalize_condition(rc)?;
        }
    }
    Ok(())
}

fn translate_parallel(n: &mut Map<String, Value>) -> Result<(), String> {
    fold_key(n, "join_branches", "joinBranches");
    // plaita thread/process/coroutine all map onto OS threads here; known
    // values pass, unknown ones are stashed and normalized to "thread".
    if let Some(Value::String(m)) = n.get("mode").cloned() {
        if !matches!(
            m.as_str(),
            "thread" | "process" | "coroutine" | "artificial"
        ) {
            let mut meta = as_meta(n);
            stash_into(&mut meta, "parallelModeIgnored", Value::String(m.clone()));
            n.insert("metadata".into(), Value::Object(meta));
            n.insert("mode".into(), Value::String("thread".into()));
        }
    }
    let Some(branches) = n.get_mut("branches").and_then(|b| b.as_array_mut()) else {
        return Ok(());
    };
    for (i, rb) in branches.iter_mut().enumerate() {
        let Some(b) = rb.as_object_mut() else {
            return Err(format!("branches[{i}] is not an object"));
        };
        if let Some(inp) = b.get("input") {
            if !inp.is_null() && !inp.is_object() {
                return Err(format!(
                    "branches[{i}] input must be an object (plaita dict-only child input)"
                ));
            }
        }
        if let Some(c) = b.get_mut("condition") {
            if c.is_object() {
                normalize_condition(c).map_err(|e| format!("branches[{i}] condition: {e}"))?;
            }
        }
    }
    Ok(())
}

fn translate_loop(n: &mut Map<String, Value>) -> Result<(), String> {
    if let Some(v) = n.get_mut("itemType") {
        if v.is_object() {
            normalize_property(v).map_err(|e| format!("itemType: {e}"))?;
        }
    }
    fold_key(n, "item_type", "itemType");
    fold_key(n, "typeDefs", "itemType");
    Ok(())
}

fn translate_child(n: &mut Map<String, Value>) -> Result<(), String> {
    if n.contains_key("childFlow") {
        return Ok(());
    }
    for k in ["flowId", "flow_id", "flowVersion", "flow_version"] {
        if n.contains_key(k) {
            return Err(format!(
                "reference-style child flow ({k}) is not supported; inline the flow as childFlow"
            ));
        }
    }
    Err("child node requires an inline childFlow document".to_string())
}

/// loop.py BaseCollectionNode 家族（map/filter/find/reduce）共享的装载面：
/// itemType 归一化 + camel 别名折叠（loop 同款），map 另有 concurrent
/// （历史别名 async，Python `_normalize_concurrent`：concurrent 真值保留，
/// 否则取 bool(async)）与 maxConcurrent。类型错误具名拒（#9 卫生）。
fn translate_collection(n: &mut Map<String, Value>, node_type: &str) -> Result<(), String> {
    if let Some(v) = n.get_mut("itemType") {
        if v.is_object() {
            normalize_property(v).map_err(|e| format!("itemType: {e}"))?;
        }
    }
    fold_key(n, "item_type", "itemType");
    // typeDefs 是 BaseCollectionNode 的遗留别名（Python _setup_item_type）。
    fold_key(n, "typeDefs", "itemType");

    let want_bool = |m: &Map<String, Value>, k: &str| -> Result<Option<bool>, String> {
        match m.get(k) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(other) => Err(format!(
                "field {k:?} must be a boolean, got {}",
                type_name(other)
            )),
        }
    };
    if node_type == "map" {
        // Python `_normalize_concurrent`: concurrent 真值优先，否则 async 兜底。
        let concurrent = want_bool(n, "concurrent")?.unwrap_or(false);
        let legacy_async = want_bool(n, "async")?.unwrap_or(false);
        n.remove("async");
        if concurrent || legacy_async {
            n.insert("concurrent".into(), Value::Bool(true));
        } else {
            n.remove("concurrent");
        }
        match n.get("maxConcurrent") {
            None | Some(Value::Null) => {}
            Some(Value::Number(num)) if num.is_i64() || num.is_u64() => {
                // 负数装载期拒绝（Python 到线程池构造期才 ValueError，前置到
                // Load；Go translateCollection 同款）。0 = 不设限（`or None`）。
                if let Some(v) = num.as_i64() {
                    if v < 0 {
                        return Err(format!(
                            "maxConcurrent must be >= 0 (0 = unbounded), got {v}"
                        ));
                    }
                }
            }
            Some(other) => {
                return Err(format!(
                    "field \"maxConcurrent\" must be an integer, got {}",
                    type_name(other)
                ))
            }
        }
        fold_key(n, "max_concurrent", "maxConcurrent");
    }
    Ok(())
}

fn translate_while(n: &mut Map<String, Value>) -> Result<(), String> {
    // 部署 IR 双拼写都收：编译器 _compile_while 产 snake 的
    // child_flow/max_iterations，console 正典产 camel（plaita#89 dual-read）。
    fold_key(n, "child_flow", "childFlow");
    fold_key(n, "maxIterations", "max_iterations");
    match n.get("max_iterations") {
        None | Some(Value::Null) => {}
        Some(Value::Number(num)) if num.is_i64() || num.is_u64() => {}
        Some(other) => {
            return Err(format!(
                "field \"max_iterations\" must be an integer, got {}",
                type_name(other)
            ))
        }
    }
    // condition 形状 fail-closed 同 if：畸形 condition 在 Python 里解析失败后
    // 永不命中、零轮静默 None（decide.py 自认的缺陷形态），这里加载期拒绝；
    // 缺省/null 合法（Python condition=None：只执行一轮）。
    if let Some(v) = n.get("condition").cloned() {
        if !v.is_null() {
            let Some(c) = v.as_object() else {
                return Err(
                    "while condition must be an object ({field,operator,value} or \
                            {relation,conditions}), got non-object"
                        .to_string(),
                );
            };
            let has_field = c.contains_key("field");
            let has_operator = c.contains_key("operator");
            let has_value = c.contains_key("value");
            let conditions_len = c
                .get("conditions")
                .and_then(|x| x.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            let has_relation = c.contains_key("relation");
            if has_field && has_operator && has_value {
                // 单条件（value 允许 null：eq/null 比较是合法形状）。
            } else if has_relation && conditions_len > 0 {
                let mut cv = v.clone();
                normalize_condition(&mut cv).map_err(|e| format!("while condition: {e}"))?;
                n.insert("condition".into(), cv);
            } else {
                return Err(
                    "while condition is malformed: needs {field,operator,value} or \
                     {relation,conditions[>0]}; a malformed condition would silently run \
                     zero rounds"
                        .to_string(),
                );
            }
        }
    }
    Ok(())
}

/// 转正 plaita 的 case 节点（decide.py SwitchLegacy，等值匹配方言）。Python
/// 在模型校验期把它脱糖成普通 Switch：每条 case 变成
/// Branch(name=case.id, condition={field:target, operator:eq, value:case.value},
/// next=case.id)，外加恒置 isDefault 的兜底分支；本函数在 IR 归一化层做同款
/// 脱糖（节点 type 改写为 switch + 构造 branches，metadata 记 caseDialect 留
/// 痕），命中/输出语义与 switch 完全一致。拍板依据（plaita-go#80 r78 Python
/// 探针 + node_parity2 golden）：
///
///   - cases[].id 必填 = 跳转目标（Python Branch(next=case["id"]) 硬编码 id，
///     cases[].next 不参与路由）；cases[].value 必填（null 合法，eq None
///     比较）；cases[].name 可选 = label。
///   - default 缺省或 null → 字面 "default" 节点 id；非空字符串 → 该 id。
///   - target 必须是表达式字符串或缺省：缺省/None 时条件恒 None，只有
///     value:null 的 case 可命中——desugar 以引擎的 $NULL 字面量承载；字面量
///     target 加载期拒绝（Python evaluate(字面量) 语义无部署意义）。
///   - cases 必须是数组；条目缺 id/value 加载期拒绝；空 cases 合法（只剩
///     兜底分支）。
fn translate_case(n: &mut Map<String, Value>) -> Result<(), String> {
    let target = match n.get("target") {
        None | Some(Value::Null) => Value::String("$NULL".into()),
        Some(v @ Value::String(_)) => v.clone(),
        Some(other) => {
            return Err(format!(
                "case target must be an expression string (or absent), got {}",
                type_name(other)
            ))
        }
    };
    let raw_cases: Vec<Value> = match n.get("cases") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => a.clone(),
        Some(other) => return Err(format!("cases must be an array, got {}", type_name(other))),
    };
    let mut branches: Vec<Value> = Vec::with_capacity(raw_cases.len() + 1);
    for (i, rc) in raw_cases.iter().enumerate() {
        let Some(c) = rc.as_object() else {
            return Err(format!(
                "cases[{i}] is not an object (got {}) — every case needs id/value, optional name",
                type_name(rc)
            ));
        };
        let id = c.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if id.is_empty() {
            return Err(format!(
                "cases[{i}] missing required \"id\" (the jump target) — every case needs \
                 id/value, optional name"
            ));
        }
        let Some(value) = c.get("value") else {
            return Err(format!("cases[{i}] ({id:?}) missing required \"value\""));
        };
        let mut b = Map::new();
        b.insert("name".into(), Value::String(id.to_string()));
        b.insert(
            "condition".into(),
            serde_json::json!({
                "field": target,
                "operator": "eq",
                "value": value,
            }),
        );
        b.insert("next".into(), Value::String(id.to_string()));
        if let Some(Value::String(name)) = c
            .get("name")
            .filter(|s| !s.as_str().unwrap_or("").is_empty())
        {
            b.insert("label".into(), Value::String(name.clone()));
        }
        branches.push(Value::Object(b));
    }
    let default_next = match n.get("default") {
        None | Some(Value::Null) => "default".to_string(),
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        Some(other) => {
            return Err(format!(
                "default must be a node id string, got {}",
                type_name(other)
            ))
        }
    };
    branches.push(serde_json::json!({
        "name": "default",
        "next": default_next,
        "isDefault": true,
    }));
    n.insert("type".into(), Value::String("switch".into()));
    n.insert("branches".into(), Value::Array(branches));
    n.remove("target");
    n.remove("cases");
    n.remove("default");
    let mut meta = as_meta(n);
    stash_into(
        &mut meta,
        "caseDialect",
        Value::String("desugared-to-switch".into()),
    );
    n.insert("metadata".into(), Value::Object(meta));
    Ok(())
}

/// 转正 plaita 的 reference 节点（child.py ReferenceFlow，引用逻辑）。plaita
/// 的调度器在运行前按 flow_id/flow_version 把 child_flow 注入节点；本引擎的
/// 对应物在加载期完成——经 FlowProvider 取回部署形态子流程文档注入
/// childFlow，随后与 inline childFlow 同路走嵌入归一化+校验（嵌套 reference
/// 在递归里继续解析，深度上限 32，环具名拒绝，菱形不误伤）。拍板依据
/// （plaita-go#80 r78 Python 实测 + golden）：
///
///   - inline childFlow 在 reference 上拒绝：plaita 运行时把它丢掉
///     （ReferenceFlow.setup_child_flow 与基类 FlowNode 同名遮蔽）并在执行期炸
///     "child_flow for reference flow node is required"。加载期具名拒绝并指路，
///     不给「本引擎能跑、plaita 跑不了」的跨运行时假象。
///   - flowId 缺省（且无 inline）拒绝；provider 未配置/解析失败/文档非法 →
///     装载失败（fail-closed）。
fn translate_reference(
    n: &mut Map<String, Value>,
    resolver: Option<&FlowResolver>,
    depth: u32,
) -> Result<(), String> {
    fold_key(n, "flow_id", "flowId");
    fold_key(n, "flow_version", "flowVersion");
    if let Some(cf) = n.get("childFlow") {
        if !cf.is_null() {
            return Err(
                "reference node takes no inline childFlow: plaita's runtime drops it \
                        (validator shadowing) and fails at run time with \"child_flow for \
                        reference flow node is required\"; inline the sub-flow in a child node, \
                        or declare flowId/flowVersion and load via ir::load_with_flow_provider"
                    .to_string(),
            );
        }
    }
    let flow_id = get_str(n, &["flowId"]).unwrap_or_default();
    if flow_id.is_empty() {
        return Err(
            "reference node requires flowId/flow_id (plaita's scheduler injects the \
                    referenced flow by id; this engine resolves it at load time)"
                .to_string(),
        );
    }
    let flow_version = get_str(n, &["flowVersion"]).unwrap_or_default();
    let Some(res) = resolver else {
        return Err(format!(
            "reference node {:?} references flow {:?}: no flow provider configured — use \
             ir::load_with_flow_provider (plaita injects reference children by flowId at \
             scheduling time)",
            n.get("id").and_then(|v| v.as_str()).unwrap_or("?"),
            flow_id
        ));
    };
    if depth >= MAX_REFERENCE_RESOLVE_DEPTH {
        return Err(format!(
            "reference node {:?}: reference chain deeper than {MAX_REFERENCE_RESOLVE_DEPTH} \
             while resolving flow {flow_id:?} (cycle?)",
            n.get("id").and_then(|v| v.as_str()).unwrap_or("?")
        ));
    }
    let sub = res
        .provider
        .load_flow_json(
            &flow_id,
            (!flow_version.is_empty()).then_some(flow_version.as_str()),
        )
        .map_err(|e| {
            format!(
                "reference node {:?}: resolving flow {flow_id:?} (version {flow_version:?}): {e}",
                n.get("id").and_then(|v| v.as_str()).unwrap_or("?")
            )
        })?;
    if !sub.is_object() {
        return Err(format!(
            "reference node {:?}: flow {flow_id:?} is not a JSON flow document",
            n.get("id").and_then(|v| v.as_str()).unwrap_or("?")
        ));
    }
    n.insert("childFlow".into(), sub);
    let mut meta = as_meta(n);
    stash_into(
        &mut meta,
        "resolvedFrom",
        Value::String(format!("{flow_id}@{flow_version}")),
    );
    n.insert("metadata".into(), Value::Object(meta));
    Ok(())
}

fn translate_http(n: &mut Map<String, Value>) -> Result<(), String> {
    fold_key(n, "content_type", "contentType");
    let mut meta = as_meta(n);
    let node_id = get_str(n, &["id"]).unwrap_or_default();
    // plaita SSRF hardening fields: not enforced by this engine → flows that
    // rely on them must not load (fail closed).
    for k in ["allowedHosts", "allowed_hosts"] {
        if let Some(v) = n.remove(k) {
            if v.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                return Err(format!(
                    "http {node_id:?}: {k} is not enforced by this engine; remove it or run this flow on the plaita runtime"
                ));
            }
        }
    }
    for k in ["deniedHosts", "denied_hosts"] {
        if let Some(v) = n.remove(k) {
            if v.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                return Err(format!(
                    "http {node_id:?}: {k} is not enforced by this engine; remove it or run this flow on the plaita runtime"
                ));
            }
        }
    }
    for k in ["blockPrivateNetworks", "block_private_networks"] {
        if let Some(v) = n.remove(k) {
            if v.as_bool().unwrap_or(false) {
                return Err(format!(
                    "http {node_id:?}: blockPrivateNetworks is not enforced by this engine; remove it or run this flow on the plaita runtime"
                ));
            }
        }
    }
    // plaita-go 携带 addressing/delegate（polaris 寻址/委托）在执行期使用；
    // 本引擎不实现——依赖它的 flow 不许静默打向裸 url（审计 F5）
    for k in ["addressing", "delegate"] {
        if let Some(v) = n.get(k) {
            let non_empty = match v {
                Value::Null => false,
                Value::Object(o) => !o.is_empty(),
                Value::String(s) => !s.is_empty(),
                _ => true,
            };
            if non_empty {
                return Err(format!(
                    "http {node_id:?}: {k} is not supported by this engine; remove it or run this flow on the plaita/go runtime"
                ));
            }
        }
    }
    // requestTimeout (seconds, float) folds into the node timeout when absent
    for k in ["requestTimeout", "request_timeout"] {
        if let Some(v) = n.remove(k) {
            let secs = as_float(&v).map_err(|e| format!("requestTimeout: {e}"))?;
            if secs > 0.0 {
                let has_timeout = n
                    .get("timeout")
                    .map(|t| t.as_str().map(|s| !s.is_empty()).unwrap_or(false))
                    .unwrap_or(false);
                if has_timeout {
                    stash_into(&mut meta, "requestTimeoutIgnored", v);
                } else {
                    n.insert("timeout".into(), Value::String(format!("PT{secs}S")));
                }
            }
        }
    }
    if let Some(v) = n.remove("maxRedirects") {
        stash_into(&mut meta, "maxRedirects", v);
    }
    fold_key(n, "max_redirects", "maxRedirects");
    n.insert("metadata".into(), Value::Object(meta));
    Ok(())
}

// -- 挂起族七类的装载面（plaita-go translate_server.go/translateEvent/
//    translateHitlAwait 移植）--------------------------------------------------
//
// 权威 = plaita/server/nodes/*_node.py 的 pydantic 模型层：必填字段、Literal
// 枚举、大小写归一（mode="before" validator）与缺省值都在模型校验期生效——
// 部署 flow 带非法枚举/缺必填时 plaita 的 Flow.model_validate 直接拒绝，
// 本引擎同样在装载期拒绝（fail-closed 对齐，不给「这里能跑、plaita 拒载」
// 的跨运行时假象）。固定事件订阅契约：approval/redis_queue/kafka_queue/
// http_callback 的 event_type 由 validator 无条件覆盖（用户值不参与）；delay
// 不覆盖（可配，默认 delay_trigger）；event 的 event_type 是作者必填字段。

/// gt=0 约束共享（0 会让订阅一注册就超时）。数字或数字串可解析。
fn normalize_seconds(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Go %v 的 JSON 形态（枚举拒绝消息里的原值展示）。
fn plaita_expression_or_display(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| format!("{v}"))
}

fn validate_subscription_timeout(v: &Value) -> Result<(), String> {
    match normalize_seconds(v) {
        Some(seconds) if seconds > 0.0 => Ok(()),
        _ => Err(format!(
            "subscription_timeout must be a positive number of seconds, got {v}"
        )),
    }
}

fn event_filter_shape(n: &Map<String, Value>) -> Result<(), String> {
    if let Some(v) = n.get("event_filter") {
        if !v.is_null() && !v.is_object() {
            return Err("event_filter must be an object".to_string());
        }
    }
    Ok(())
}

/// event 节点（event_node.py）：event_type 必填非空（空类型在匹配器里是
/// 通配）、event_filter 对象、subscription_timeout > 0。
fn translate_event(n: &mut Map<String, Value>) -> Result<(), String> {
    fold_key(n, "eventType", "event_type");
    fold_key(n, "eventFilter", "event_filter");
    fold_key(n, "subscriptionTimeout", "subscription_timeout");
    let event_type = n.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
    if event_type.trim().is_empty() {
        return Err(
            "event node requires a non-empty event_type (an empty type is treated as a wildcard)"
                .to_string(),
        );
    }
    event_filter_shape(n)?;
    if let Some(v) = n.get("subscription_timeout") {
        if !v.is_null() {
            validate_subscription_timeout(v)?;
        }
    }
    Ok(())
}

/// hitl_await（plaita-nodes）：camel 别名折叠；timeout_secs/event_type 收
/// 表达式，不在装载期收窄（节点侧与 plaita 的 resolve_* 回退语义一致）。
fn translate_hitl_await(n: &mut Map<String, Value>) -> Result<(), String> {
    fold_key(n, "baseUrl", "base_url");
    fold_key(n, "timeoutSecs", "timeout_secs");
    fold_key(n, "eventType", "event_type");
    fold_key(n, "eventFilter", "event_filter");
    fold_key(n, "dryRun", "dry_run");
    event_filter_shape(n)
}

/// delay（delay_node.py）：delay_seconds 必填（int|float|str，`$` 表达式
/// 合法），delay_unit 大小写归一后限枚举；event_type 可配（缺省
/// delay_trigger）。
fn translate_delay(n: &mut Map<String, Value>) -> Result<(), String> {
    fold_key(n, "eventType", "event_type");
    fold_key(n, "eventFilter", "event_filter");
    fold_key(n, "subscriptionTimeout", "subscription_timeout");
    fold_key(n, "delaySeconds", "delay_seconds");
    fold_key(n, "delayUnit", "delay_unit");

    let seconds_shape = match n.get("delay_seconds") {
        None | Some(Value::Null) => None,
        Some(v @ (Value::String(_) | Value::Number(_))) => Some(v.clone()),
        Some(other) => {
            return Err(format!(
                "delay_seconds must be a number or expression string, got {}",
                type_name(other)
            ))
        }
    };
    let Some(_) = seconds_shape else {
        return Err("delay node requires delay_seconds (int/float/expression)".to_string());
    };
    match n.get("event_type") {
        Some(v) if !v.is_null() => {
            if v.as_str().is_none() || v.as_str().unwrap_or("").trim().is_empty() {
                return Err("delay event_type must be a non-empty string".to_string());
            }
        }
        _ => {
            // pydantic default：delay 是五节点里唯一 event_type 可配的。
            n.insert("event_type".into(), Value::String("delay_trigger".into()));
        }
    }
    if let Some(unit) = n.get("delay_unit").filter(|v| !v.is_null()) {
        let Some(s) = unit.as_str() else {
            return Err(format!(
                "delay_unit must be a string, got {}",
                type_name(unit)
            ));
        };
        let lowered = s.to_lowercase();
        if !matches!(lowered.as_str(), "seconds" | "minutes" | "hours" | "days") {
            return Err(format!(
                "delay_unit must be one of seconds/minutes/hours/days, got {lowered:?}"
            ));
        }
        n.insert("delay_unit".into(), Value::String(lowered));
    } else {
        n.insert("delay_unit".into(), Value::String("seconds".into()));
    }
    event_filter_shape(n)?;
    if let Some(v) = n.get("subscription_timeout") {
        if !v.is_null() {
            validate_subscription_timeout(v)?;
        }
    }
    Ok(())
}

/// approval（approval_node.py）：标题/内容/审批人必填，枚举大小写归一，
/// 缺省值按模型填充；event_type 强制覆盖为契约值；显式 null 的
/// subscription_timeout 折叠为 -1（无限等待哨兵）。
fn translate_approval(n: &mut Map<String, Value>) -> Result<(), String> {
    for (camel, snake) in [
        ("eventType", "event_type"),
        ("eventFilter", "event_filter"),
        ("subscriptionTimeout", "subscription_timeout"),
        ("approvalTitle", "approval_title"),
        ("approvalContent", "approval_content"),
        ("approvalType", "approval_type"),
        ("approvalStrategy", "approval_strategy"),
        ("autoEscalation", "auto_escalation"),
        ("escalationTimeoutHours", "escalation_timeout_hours"),
        ("escalationApprovers", "escalation_approvers"),
        ("formFields", "form_fields"),
        ("allowComments", "allow_comments"),
        ("requireComments", "require_comments"),
        ("notificationConfig", "notification_config"),
    ] {
        fold_key(n, camel, snake);
    }
    for field in ["approval_title", "approval_content", "approvers"] {
        let missing = n.get(field).map(|v| v.is_null()).unwrap_or(true);
        if missing {
            return Err(format!("approval node requires {field}"));
        }
    }
    if !n.get("approvers").map(|v| v.is_array()).unwrap_or(false) {
        return Err(format!(
            "approval approvers must be a list, got {}",
            type_name(n.get("approvers").unwrap_or(&Value::Null))
        ));
    }
    if let Some(v) = n.get("approval_type").filter(|v| !v.is_null()) {
        let Some(s) = v.as_str() else {
            return Err(format!(
                "approval_type must be a string, got {}",
                type_name(v)
            ));
        };
        let lowered = s.to_lowercase();
        if !matches!(lowered.as_str(), "manual" | "auto") {
            return Err(format!(
                "approval_type must be one of manual/auto, got {lowered:?}"
            ));
        }
        n.insert("approval_type".into(), Value::String(lowered));
    } else {
        n.insert("approval_type".into(), Value::String("manual".into()));
    }
    if let Some(v) = n.get("approval_strategy").filter(|v| !v.is_null()) {
        let Some(s) = v.as_str() else {
            return Err(format!(
                "approval_strategy must be a string, got {}",
                type_name(v)
            ));
        };
        let lowered = s.to_lowercase();
        if !matches!(lowered.as_str(), "any" | "all" | "majority") {
            return Err(format!(
                "approval_strategy must be one of any/all/majority, got {lowered:?}"
            ));
        }
        n.insert("approval_strategy".into(), Value::String(lowered));
    } else {
        n.insert("approval_strategy".into(), Value::String("any".into()));
    }
    if !n.contains_key("escalation_timeout_hours") {
        n.insert("escalation_timeout_hours".into(), json!(24));
    }
    if n.get("allow_comments").map(|v| v.is_null()).unwrap_or(true) {
        n.insert("allow_comments".into(), json!(true));
    }
    // 运行时契约：event_type 无条件覆盖（Python _normalize_enums）。
    n.insert(
        "event_type".into(),
        Value::String("approval_decision".into()),
    );
    event_filter_shape(n)?;
    if let Some(v) = n.get("subscription_timeout") {
        if v.is_null() {
            // gt=0 校验 + 显式 null → -1 哨兵（无限等待）。
            n.insert("subscription_timeout".into(), json!(-1));
        } else {
            validate_subscription_timeout(v)?;
        }
    }
    Ok(())
}

/// redis_queue（redis_queue_node.py）：queue_name 必填，
/// queue_type/message_format 限枚举，event_type 强制契约值。
fn translate_redis_queue(n: &mut Map<String, Value>) -> Result<(), String> {
    for (camel, snake) in [
        ("eventType", "event_type"),
        ("eventFilter", "event_filter"),
        ("subscriptionTimeout", "subscription_timeout"),
        ("redisHost", "redis_host"),
        ("redisPort", "redis_port"),
        ("redisDb", "redis_db"),
        ("redisPassword", "redis_password"),
        ("queueName", "queue_name"),
        ("queueType", "queue_type"),
        ("timeoutSeconds", "timeout_seconds"),
        ("batchSize", "batch_size"),
        ("messageFormat", "message_format"),
    ] {
        fold_key(n, camel, snake);
    }
    let missing = n.get("queue_name").map(|v| v.is_null()).unwrap_or(true);
    if missing {
        return Err("redis_queue node requires queue_name".to_string());
    }
    if let Some(v) = n.get("queue_type").filter(|v| !v.is_null()) {
        let ok = v
            .as_str()
            .is_some_and(|s| matches!(s, "list" | "stream" | "pubsub"));
        if !ok {
            return Err(format!(
                "queue_type must be one of list/stream/pubsub, got {}",
                plaita_expression_or_display(v)
            ));
        }
    } else {
        n.insert("queue_type".into(), Value::String("list".into()));
    }
    if let Some(v) = n.get("message_format").filter(|v| !v.is_null()) {
        let ok = v
            .as_str()
            .is_some_and(|s| matches!(s, "json" | "text" | "raw"));
        if !ok {
            return Err(format!(
                "message_format must be one of json/text/raw, got {}",
                plaita_expression_or_display(v)
            ));
        }
    } else {
        n.insert("message_format".into(), Value::String("json".into()));
    }
    n.insert("event_type".into(), Value::String("redis_message".into()));
    event_filter_shape(n)?;
    if let Some(v) = n.get("subscription_timeout") {
        if !v.is_null() {
            validate_subscription_timeout(v)?;
        }
    }
    Ok(())
}

/// kafka_queue（kafka_queue_node.py）：bootstrap_servers（str|list）/topic/
/// group_id 必填，协议/SASL/偏移/格式限枚举，event_type 强制契约值。
fn translate_kafka_queue(n: &mut Map<String, Value>) -> Result<(), String> {
    for (camel, snake) in [
        ("eventType", "event_type"),
        ("eventFilter", "event_filter"),
        ("subscriptionTimeout", "subscription_timeout"),
        ("bootstrapServers", "bootstrap_servers"),
        ("securityProtocol", "security_protocol"),
        ("saslMechanism", "sasl_mechanism"),
        ("saslUsername", "sasl_username"),
        ("saslPassword", "sasl_password"),
        ("autoOffsetReset", "auto_offset_reset"),
        ("enableAutoCommit", "enable_auto_commit"),
        ("maxPollRecords", "max_poll_records"),
        ("messageFormat", "message_format"),
        ("timeoutMs", "timeout_ms"),
    ] {
        fold_key(n, camel, snake);
    }
    let Some(bootstrap) = n.get("bootstrap_servers").filter(|v| !v.is_null()) else {
        return Err("kafka_queue node requires bootstrap_servers (string or list)".to_string());
    };
    match bootstrap {
        Value::String(_) => {}
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                if item.as_str().is_none() {
                    return Err(format!(
                        "bootstrap_servers[{i}] must be a string, got {}",
                        type_name(item)
                    ));
                }
            }
        }
        other => {
            return Err(format!(
                "bootstrap_servers must be a string or list of strings, got {}",
                type_name(other)
            ))
        }
    }
    for field in ["topic", "group_id"] {
        let missing = n.get(field).map(|v| v.is_null()).unwrap_or(true);
        if missing {
            return Err(format!("kafka_queue node requires {field}"));
        }
    }
    if let Some(v) = n.get("security_protocol").filter(|v| !v.is_null()) {
        let ok = v
            .as_str()
            .is_some_and(|s| matches!(s, "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"));
        if !ok {
            return Err(format!(
                "security_protocol must be one of PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL, got {}",
                plaita_expression_or_display(v)
            ));
        }
    } else {
        n.insert(
            "security_protocol".into(),
            Value::String("PLAINTEXT".into()),
        );
    }
    if let Some(v) = n.get("sasl_mechanism").filter(|v| !v.is_null()) {
        let ok = v
            .as_str()
            .is_some_and(|s| matches!(s, "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"));
        if !ok {
            return Err(format!(
                "sasl_mechanism must be one of PLAIN/SCRAM-SHA-256/SCRAM-SHA-512, got {}",
                plaita_expression_or_display(v)
            ));
        }
    }
    if let Some(v) = n.get("auto_offset_reset").filter(|v| !v.is_null()) {
        let ok = v
            .as_str()
            .is_some_and(|s| matches!(s, "earliest" | "latest"));
        if !ok {
            return Err(format!(
                "auto_offset_reset must be one of earliest/latest, got {}",
                plaita_expression_or_display(v)
            ));
        }
    } else {
        n.insert("auto_offset_reset".into(), Value::String("latest".into()));
    }
    if let Some(v) = n.get("message_format").filter(|v| !v.is_null()) {
        let ok = v.as_str().is_some_and(|s| matches!(s, "json" | "text"));
        if !ok {
            return Err(format!(
                "message_format must be one of json/text, got {}",
                plaita_expression_or_display(v)
            ));
        }
    } else {
        n.insert("message_format".into(), Value::String("json".into()));
    }
    if !n.contains_key("timeout_ms") {
        n.insert("timeout_ms".into(), json!(30000));
    }
    if n.get("enable_auto_commit")
        .map(|v| v.is_null())
        .unwrap_or(true)
    {
        n.insert("enable_auto_commit".into(), json!(true));
    }
    if n.get("max_poll_records")
        .map(|v| v.is_null())
        .unwrap_or(true)
    {
        n.insert("max_poll_records".into(), json!(1));
    }
    n.insert("event_type".into(), Value::String("kafka_message".into()));
    event_filter_shape(n)?;
    if let Some(v) = n.get("subscription_timeout") {
        if !v.is_null() {
            validate_subscription_timeout(v)?;
        }
    }
    Ok(())
}

/// http_callback（http_callback_node.py）：callback_method 大小写归一限
/// 枚举、auth_type 限枚举，其余全有缺省；event_type 强制契约值。
fn translate_http_callback(n: &mut Map<String, Value>) -> Result<(), String> {
    for (camel, snake) in [
        ("eventType", "event_type"),
        ("eventFilter", "event_filter"),
        ("subscriptionTimeout", "subscription_timeout"),
        ("callbackPath", "callback_path"),
        ("callbackMethod", "callback_method"),
        ("callbackTimeoutMinutes", "callback_timeout_minutes"),
        ("requireAuth", "require_auth"),
        ("authType", "auth_type"),
        ("authToken", "auth_token"),
        ("validateHeaders", "validate_headers"),
        ("validateParams", "validate_params"),
        ("successResponse", "success_response"),
        ("errorResponse", "error_response"),
    ] {
        fold_key(n, camel, snake);
    }
    if let Some(v) = n.get("callback_method").filter(|v| !v.is_null()) {
        let Some(s) = v.as_str() else {
            return Err(format!(
                "callback_method must be a string, got {}",
                type_name(v)
            ));
        };
        let upper = s.to_uppercase();
        if !matches!(upper.as_str(), "GET" | "POST" | "PUT" | "DELETE") {
            return Err(format!(
                "callback_method must be one of GET/POST/PUT/DELETE, got {upper:?}"
            ));
        }
        n.insert("callback_method".into(), Value::String(upper));
    } else {
        n.insert("callback_method".into(), Value::String("POST".into()));
    }
    if let Some(v) = n.get("auth_type").filter(|v| !v.is_null()) {
        let ok = v
            .as_str()
            .is_some_and(|s| matches!(s, "token" | "basic" | "signature"));
        if !ok {
            return Err(format!(
                "auth_type must be one of token/basic/signature, got {}",
                plaita_expression_or_display(v)
            ));
        }
    } else {
        n.insert("auth_type".into(), Value::String("token".into()));
    }
    if n.get("require_auth").map(|v| v.is_null()).unwrap_or(true) {
        n.insert("require_auth".into(), json!(true));
    }
    if !n.contains_key("callback_timeout_minutes") {
        n.insert("callback_timeout_minutes".into(), json!(60));
    }
    n.insert("event_type".into(), Value::String("http_callback".into()));
    event_filter_shape(n)?;
    if let Some(v) = n.get("subscription_timeout") {
        if !v.is_null() {
            validate_subscription_timeout(v)?;
        }
    }
    Ok(())
}

/// The code node's admitted languages. python runs through the python3
/// subprocess sidecar (subprocess-form backend only — restricted/docker/
/// unsafe isolation tiers are refused fail-closed, never silently
/// downgraded). javascript has no in-process interpreter in plaita-rs and is
/// refused with the same fail-closed discipline.
fn translate_code(n: &mut Map<String, Value>) -> Result<(), String> {
    let lang = get_str(n, &["language"]).unwrap_or_default();
    let lang = if lang.is_empty() {
        // plaita: language None → LANGUAGE_PYTHON
        n.insert("language".into(), Value::String("python".into()));
        "python".to_string()
    } else {
        lang.to_string()
    };
    match lang.as_str() {
        "python" => {
            fold_key(n, "sandboxBackend", "sandbox_backend");
            match n
                .get("sandbox_backend")
                .and_then(|v| v.as_str())
                .unwrap_or("")
            {
                "" | "subprocess" => {
                    let v = n.remove("sandbox_backend").unwrap_or(Value::Null);
                    // keep the folded camel field so the engine node carries
                    // the declaration
                    let empty = Value::String(String::new());
                    let keep = if v.is_null() { &empty } else { &v };
                    n.insert("sandboxBackend".into(), keep.clone());
                }
                backend @ ("restricted" | "docker" | "unsafe") => {
                    return Err(format!(
                        "code node sandboxBackend {backend:?} has no plaita-rs implementation: \
                         the engine runs python code only in the subprocess form (fresh python3 \
                         process, wall-clock timeout, process-group kill — not an in-process or \
                         container isolation boundary). Set sandbox_backend to \"subprocess\" or \
                         drop it; flows that need in-process (restricted) or container (docker) \
                         isolation must run on the plaita runtime"
                    ));
                }
                other => {
                    return Err(format!(
                        "code node sandbox_backend value {other:?} is invalid for language python (expected absent or \"subprocess\")"
                    ));
                }
            }
        }
        "javascript" => {
            for k in ["sandboxBackend", "sandbox_backend"] {
                if let Some(v) = n.remove(k) {
                    let empty = !v.is_null() && !(matches!(&v, Value::String(s) if s.is_empty()));
                    if empty {
                        return Err(format!(
                            "code node sandboxBackend {v} is not supported in this engine (the javascript interpreter is in-process and has no sandbox backend tiers)"
                        ));
                    }
                }
            }
            // plaita-rs has no in-process JS engine: refuse at load instead
            // of failing at execute (documented divergence from plaita-go,
            // which embeds goja).
            return Err(
                "code node language \"javascript\" is not supported by plaita-rs \
                 (no in-process JS interpreter); use language \"python\" (subprocess sidecar) \
                 or run the flow on the plaita/go runtime"
                    .to_string(),
            );
        }
        other => {
            return Err(format!(
                "code node language {other:?} is not supported (this engine runs python)"
            ));
        }
    }
    Ok(())
}

fn translate_end(n: &mut Map<String, Value>) -> Result<(), String> {
    let rt = match n.get("resultType") {
        // Go 对 resultType 做 interface switch：非字符串非 null 是装载错误，
        // 不能静默归一成 success（审计 F1）
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        Some(Value::Null) | None => {
            let s = get_str(n, &["result_type"]).unwrap_or_default();
            if !s.is_empty() {
                n.insert("resultType".into(), Value::String(s.clone()));
                s
            } else {
                String::new()
            }
        }
        Some(other) => {
            return Err(format!("end resultType {other} is invalid (success|error)"));
        }
    };
    n.remove("result_type");
    match rt.as_str() {
        "" | "success" | "error" => {}
        "nop" => return Err(
            "end resultType \"nop\" (silent end) is not supported; use success and drop the output"
                .to_string(),
        ),
        other => {
            return Err(format!(
                "end resultType {other:?} is invalid (success|error)"
            ));
        }
    }
    if let Some(e) = n.get_mut("error").and_then(|e| e.as_object_mut()) {
        if let Some(code) = e.get("code") {
            // Go fmt.Sprint(nil) == "<nil>"
            let s = match code {
                Value::String(s) => s.clone(),
                Value::Null => "<nil>".to_string(),
                other => other.to_string(),
            };
            e.insert("code".into(), Value::String(s));
        }
    }
    Ok(())
}

/// Folds plaita property spellings into the engine schema, recursively.
/// Every property of a deployed flow is marked strict (no cast coercion).
pub fn normalize_property(p: &mut Value) -> Result<(), String> {
    let Some(po) = p.as_object_mut() else {
        return Err("property must be an object".to_string());
    };
    po.insert("strict".into(), Value::Bool(true));
    if !po.contains_key("dataType") {
        if let Some(t) = po.get("type").cloned() {
            po.insert("dataType".into(), t);
        }
    }
    po.remove("type"); // Go 无条件删旧键，双拼写不残留
    fold_key(po, "title", "label");
    fold_key(po, "description", "desc");
    fold_key(po, "is_required", "isRequired");
    fold_key(po, "default_value", "defaultValue");
    if !po.contains_key("defaultValue") {
        if let Some(d) = po.get("default").cloned() {
            po.insert("defaultValue".into(), d);
        }
    }
    po.remove("default");
    fold_key(po, "item_type", "itemType");
    if let Some(it) = po.get_mut("itemType") {
        if it.is_object() {
            normalize_property(it)?;
        }
    }
    // nested object properties: engine key is properties; plaita internal
    // spelling is children; console exports use properties directly
    if !po.contains_key("properties") {
        if let Some(c) = po.get("children").cloned() {
            po.insert("properties".into(), c);
        }
    }
    po.remove("children");
    // plaita Property.required: bool → isRequired; list form marks the named
    // children required
    match po.remove("required") {
        Some(Value::Bool(b)) => {
            if !po.contains_key("isRequired") {
                po.insert("isRequired".into(), Value::Bool(b));
            }
        }
        Some(Value::Array(names)) => {
            for name in names {
                let Some(s) = name.as_str() else { continue };
                if let Some(child) = po
                    .get_mut("properties")
                    .and_then(|props| props.get_mut(s))
                    .and_then(|c| c.as_object_mut())
                {
                    if !child.contains_key("isRequired") {
                        child.insert("isRequired".into(), Value::Bool(true));
                    }
                }
            }
        }
        _ => {}
    }
    if let Some(props) = po.get_mut("properties").and_then(|x| x.as_object_mut()) {
        for (_, v) in props.iter_mut() {
            if v.is_object() {
                normalize_property(v)?;
            }
        }
    }
    Ok(())
}

/// plaita _safe_environment entry rules at load time: exact env names only —
/// no wildcards, no blank or whitespace-padded entries.
fn validate_expose_env(v: &Value) -> Result<(), String> {
    let Some(list) = v.as_array() else {
        return Err(
            "exposeEnv must be an array of environment variable names, got json value".to_string(),
        );
    };
    let mut bad: Vec<String> = Vec::new();
    for item in list {
        match item {
            Value::String(s) => {
                if s.is_empty() || s.trim() != s || s.contains('*') {
                    bad.push(s.clone());
                }
            }
            other => bad.push(other.to_string()),
        }
    }
    if !bad.is_empty() {
        return Err(format!(
            "exposeEnv contains invalid entries {bad:?}: no wildcards, no blank or \
             whitespace-padded keys; list exact environment variable names"
        ));
    }
    Ok(())
}

/// plaita timeout spellings → ISO 8601. Accepted: ""/null (absent = no
/// budget), millisecond number / numeric string, "PT..." ISO string.
/// Returns `None` when the timeout is absent/empty.
pub fn normalize_timeout(v: &Value) -> Result<Option<String>, String> {
    match v {
        Value::Null => Ok(None),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return Ok(None);
            }
            if s.starts_with('P') {
                let d = duration::parse_iso8601(s).map_err(|e| format!("timeout {s:?}: {e}"))?;
                check_instant_range(s, d)?;
                Ok(Some(s.to_string()))
            } else {
                let ms: f64 = s
                    .parse()
                    .map_err(|_| format!("timeout {s:?} is neither ISO 8601 nor milliseconds"))?;
                normalize_millis(ms)
            }
        }
        Value::Number(n) => normalize_millis(n.as_f64().unwrap_or(f64::NAN)),
        other => Err(format!("timeout has unsupported type json {:?}", other)),
    }
}

/// 「Duration 可表示但超出平台 Instant 值域」的预算在这里具名拒载
///（r1 移交的运行期 panic——`Instant::now() + t` 裸加在 release/debug 都
/// 是进程级 panic，errorHandler/$ERROR 一律接不到）：fail-closed 收在装载
/// 期，flow/node/嵌入流共用本出口。e.g. PT100000000000S（≈3170 年）能被
/// Duration 表示，但构不成任何运行期 deadline。
fn check_instant_range(iso: &str, d: std::time::Duration) -> Result<(), String> {
    if Instant::now().checked_add(d).is_none() {
        return Err(format!(
            "timeout {iso:?} exceeds the platform instant range: the budget is representable \
             but no runtime deadline can be formed from it (fail-closed at load; reduce the \
             timeout)"
        ));
    }
    Ok(())
}

fn normalize_millis(ms: f64) -> Result<Option<String>, String> {
    if ms.is_infinite() || ms.is_nan() {
        return Err(format!("timeout {ms} must be a finite number"));
    }
    if ms < 0.0 {
        return Err(format!("timeout {ms} must be a non-negative number"));
    }
    if ms == 0.0 {
        return Ok(Some("PT0S".to_string()));
    }
    let iso = millis_to_iso(ms);
    let d = duration::parse_iso8601(&iso).map_err(|e| format!("timeout {iso:?}: {e}"))?;
    check_instant_range(&iso, d)?;
    Ok(Some(iso))
}

fn millis_to_iso(ms: f64) -> String {
    if ms.fract() == 0.0 && (ms as i64) % 1000 == 0 {
        return format!("PT{}S", ms as i64 / 1000);
    }
    format!("PT{}S", ms / 1000.0)
}

// ---------------------------------------------------------------------------
// small helpers over generic JSON objects
// ---------------------------------------------------------------------------

pub(crate) fn fold_key(m: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(v) = m.remove(from) {
        m.entry(to.to_string()).or_insert(v);
    }
}

pub(crate) fn as_meta(doc: &Map<String, Value>) -> Map<String, Value> {
    match doc.get("metadata") {
        Some(Value::Object(m)) => m
            .iter()
            .map(|(k, v)| {
                let s = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (k.clone(), Value::String(s))
            })
            .collect(),
        _ => Map::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timeout_spellings() {
        assert_eq!(normalize_timeout(&json!("3000")).unwrap().unwrap(), "PT3S");
        assert_eq!(normalize_timeout(&json!(1500)).unwrap().unwrap(), "PT1.5S");
        assert_eq!(normalize_timeout(&json!("PT2S")).unwrap().unwrap(), "PT2S");
        assert_eq!(normalize_timeout(&json!("")).unwrap(), None);
        assert!(normalize_timeout(&json!("soon")).is_err());
        assert_eq!(normalize_timeout(&json!(0)).unwrap().unwrap(), "PT0S");
        assert!(normalize_timeout(&json!(-1)).is_err());
    }

    #[test]
    fn property_legacy_keys() {
        let mut p = json!({
            "type": "object",
            "children": {"x": {"type": "string", "default": "s", "is_required": true}}
        });
        normalize_property(&mut p).unwrap();
        assert_eq!(p["dataType"], json!("object"));
        let inner = &p["properties"]["x"];
        assert_eq!(inner["dataType"], json!("string"));
        assert_eq!(inner["defaultValue"], json!("s"));
        assert_eq!(inner["isRequired"], json!(true));
        assert!(inner.get("default").is_none());
    }

    #[test]
    fn expose_env_rules() {
        assert!(validate_expose_env(&json!(["HOME", "PATH"])).is_ok());
        // inner spaces are legal; only blank / whitespace-PADDED / wildcard
        assert!(validate_expose_env(&json!(["A", "B C"])).is_ok());
        assert!(validate_expose_env(&json!(["A", " B"])).is_err());
        assert!(validate_expose_env(&json!(["*"])).is_err());
        assert!(validate_expose_env(&json!([""])).is_err());
        assert!(validate_expose_env(&json!("HOME")).is_err());
    }
}
