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

use serde_json::{Map, Value};

use std::time::Instant;

use crate::duration;
use crate::ir::support::{
    as_float, get_str, is_object, next_is_empty, obj, stash_into, take, type_name,
};
use crate::ir::{scope::Scope, validate};

/// Node type accept-list (strict, fail-closed): types this engine implements.
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
    // The remainder of plaita-go's accept list is NOT implemented by
    // plaita-rs yet; each gets a targeted hint in KNOWN_UNSUPPORTED.
];

/// plaita-go's accept-list entries plaita-rs has not ported, with hints.
pub const KNOWN_UNSUPPORTED: &[(&str, &str)] = &[
    (
        "map",
        "collection variant, not ported yet (base loop is supported)",
    ),
    (
        "filter",
        "collection variant, not ported yet (base loop is supported)",
    ),
    (
        "find",
        "collection variant, not ported yet (base loop is supported)",
    ),
    (
        "reduce",
        "collection variant, not ported yet (base loop is supported)",
    ),
    (
        "while",
        "condition loop variant, not ported yet (base loop is supported)",
    ),
    ("case", "legacy switch dialect, use switch"),
    ("calculate", "calculate node is not ported yet"),
    ("mock", "mock node is not ported yet"),
    (
        "reference",
        "provider-based child flow reference is not ported yet (use inline childFlow)",
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
        "event",
        "event node (suspending) is not ported by plaita-rs yet",
    ),
    (
        "hitl_await",
        "hitl_await node (suspending) is not ported by plaita-rs yet",
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

pub fn normalize_doc(doc: &mut Value, scope: Scope) -> Result<(), String> {
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
        normalize_node(n).map_err(|e| {
            format!(
                "ir: flow {flow_id:?}: node {:?}: {e}",
                n.get("id").and_then(|v| v.as_str()).unwrap_or("?")
            )
        })?;
        normalize_embedded(&flow_id, raw)?;
    }

    validate::validate_doc(doc, scope).map_err(|e| e.to_string())?;
    validate::stash_unknown_node_fields(doc);
    validate::stash_unknown_flow_fields(doc);
    Ok(())
}

/// Recursively normalizes + validates the flow documents embedded in
/// loop/child/parallel nodes. The engine requires a non-blank workflow id,
/// which plaita's embedded flows never carry — one is synthesized.
fn normalize_embedded(flow_id: &str, node: &mut Value) -> Result<(), String> {
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
        normalize_doc(sub, Scope::Child)
            .map_err(|e| format!("ir: flow {flow_id:?}: node {node_id:?} {name}: {e}"))
    };

    match node_type.as_str() {
        "loop" | "child" => {
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

fn normalize_node(n: &mut Map<String, Value>) -> Result<(), String> {
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
        "child" => translate_child(n),
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
