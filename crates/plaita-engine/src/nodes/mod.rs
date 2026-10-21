//! Builtin node execution (the `pkg/workflow/nodes` port): start / end /
//! assignment / switch / if / loop / parallel / child / code (python
//! sidecar) / http. Unported plaita node families never reach here — the
//! ir loader rejects them at load (fail-closed).

mod code;
mod condition;
#[cfg(feature = "http")]
mod http;
#[cfg(not(feature = "http"))]
mod http_stub;

use serde_json::{json, Value};

use crate::context::{RuntimeCtx, SharedCtx};
use crate::error::NodeError;
use crate::exec::Host;
use crate::model::{Collection, Flow, Node, NodeKind};
use std::time::Instant;

pub use condition::Condition;

/// What a node body produces: the result value (lands in `$NODE`) and the
/// next node id.
pub struct StepOutcome {
    pub result: Value,
    pub next: Option<String>,
}

/// Extra keys of the per-node current context (mirrors the Go constants).
pub const EXTRA_RESULT_KEY: &str = "node.extra.result";
const LOOP_ITEM_KEY: &str = "LOOP-ITEM";
const LOOP_INDEX_KEY: &str = "LOOP-INDEX";
const LOOP_RESULT_KEY: &str = "LOOP-RESULT";

/// Dispatches a node body. Errors here are node errors; the engine's retry
/// and error policies wrap around this.
pub fn execute_node_body(
    node: &Node,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    match &node.kind {
        NodeKind::Start => Ok(StepOutcome {
            result: Value::Null,
            next: node.next.clone(),
        }),
        NodeKind::End { result_type, error } => execute_end(node, result_type, error.as_ref(), rc),
        NodeKind::Assignment { upstream_output } => execute_assignment(node, upstream_output, rc),
        NodeKind::Switch { branches } => execute_switch(node, branches, rc),
        NodeKind::If {
            condition,
            else_next,
        } => execute_if(node, condition.as_ref(), else_next, rc),
        NodeKind::Loop {
            collection,
            condition,
            item_type,
            child_flow,
        } => execute_loop(
            node,
            collection,
            condition.as_ref(),
            item_type.as_ref(),
            child_flow,
            rc,
            host,
            deadline,
        ),
        NodeKind::Parallel {
            branches,
            is_conditional,
            join_branches,
            ..
        } => execute_parallel(
            node,
            branches,
            *is_conditional,
            join_branches,
            rc,
            host,
            deadline,
        ),
        NodeKind::Child { child_flow, input } => {
            execute_child(node, child_flow, input.as_ref(), rc, host, deadline)
        }
        NodeKind::Code {
            language,
            code,
            input,
            sandbox_backend,
        } => code::execute_code(
            node,
            language,
            code,
            input.as_ref(),
            sandbox_backend,
            rc,
            deadline,
        ),
        NodeKind::Http { .. } => {
            #[cfg(feature = "http")]
            {
                if let NodeKind::Http {
                    method,
                    content_type,
                    url,
                    query,
                    headers,
                    body,
                } = &node.kind
                {
                    http::execute_http(
                        node,
                        method,
                        content_type,
                        url,
                        query.as_ref(),
                        headers.as_ref(),
                        body.as_ref(),
                        rc,
                    )
                } else {
                    unreachable!()
                }
            }
            #[cfg(not(feature = "http"))]
            {
                http_stub::unavailable()
            }
        }
    }
}

// -- helpers -----------------------------------------------------------------

/// Evaluates a value through the expression facade as a node error source.
pub(crate) fn eval_value(
    node: &Node,
    what: &str,
    v: &Value,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    plaita_expression::eval_value(v, rc.as_ref()).map_err(|e| {
        NodeError::transient(format!("node {:?}: failed to eval {what}: {e}", node.id))
    })
}

pub(crate) fn validate_output_pub(node: &Node, what: &str, value: &Value) -> Result<(), NodeError> {
    validate_output(node, what, value)
}

fn validate_output(node: &Node, what: &str, value: &Value) -> Result<(), NodeError> {
    match &node.output_type {
        Some(p) => p.validate_value(value).map_err(|e| {
            NodeError::transient(format!(
                "node {:?}: failed to validate {what}: {e}",
                node.id
            ))
        }),
        None => Ok(()),
    }
}

// -- end ---------------------------------------------------------------------

fn execute_end(
    node: &Node,
    result_type: &str,
    error: Option<&crate::model::EndError>,
    rc: &SharedCtx,
) -> Result<StepOutcome, NodeError> {
    match result_type {
        "error" => {
            // 不求值 output——Python 在 error 分支直接 raise，output 不参与
            let (code, message) = match error {
                Some(e) => (e.code.clone(), e.message.clone()),
                None => (String::new(), String::new()),
            };
            // FlowResultError 的 code/message 随错误文本上行（CLI/宿主按
            // FlowResultError 前缀解析）；不落 current——那会泄漏成表达式
            // 可见的额外根，Go 版没有这个面
            Err(NodeError {
                message: format!("FlowResultError: code={code}, message={message}"),
                class: crate::error::ErrorClass::Deterministic,
                code: crate::error::RUNTIME_ERROR_CODE,
            })
        }
        // success（含未知值的默认归一，对齐 Python setup_end）：求值并校验
        // output，作为流程返回值
        _ => {
            let Some(output) = &node.output else {
                return Ok(StepOutcome {
                    result: Value::Null,
                    next: None,
                });
            };
            let evaluated = eval_value(node, "end node output", output, rc).map_err(|e| {
                NodeError::transient(format!("failed to evaluate end node output: {}", e.message))
            })?;
            validate_output(node, "end node output", &evaluated).map_err(|e| {
                NodeError::transient(format!("failed to validate end node output: {}", e.message))
            })?;
            Ok(StepOutcome {
                result: evaluated,
                next: None,
            })
        }
    }
}

// -- assignment --------------------------------------------------------------

fn execute_assignment(
    node: &Node,
    upstream_output: &[crate::model::UpstreamValue],
    rc: &SharedCtx,
) -> Result<StepOutcome, NodeError> {
    let value: Value = if !upstream_output.is_empty() {
        // upstream pick-up: the LAST completed node decides the value
        let last_node = rc
            .snapshot_json()
            .get("last_node")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let found = upstream_output
            .iter()
            .find(|u| u.upstream == last_node)
            .map(|u| u.value.clone());
        let Some(v) = found else {
            return Err(NodeError::transient(format!(
                "upstream value not found for last node id: {last_node}"
            )));
        };
        v
    } else {
        node.output.clone().unwrap_or(Value::Null)
    };

    let evaluated = eval_value(node, "assignment value", &value, rc)?;
    validate_output(node, "value", &evaluated)?;
    Ok(StepOutcome {
        result: evaluated,
        next: node.next.clone(),
    })
}

// -- switch ------------------------------------------------------------------

fn execute_switch(
    node: &Node,
    branches: &[crate::model::BranchDef],
    rc: &SharedCtx,
) -> Result<StepOutcome, NodeError> {
    // priority 降序排序——排副本：branches 属于共享的 Workflow 定义，原地
    // sort 会永久改写节点状态（issue #17 数据竞争教训）
    let mut sorted: Vec<&crate::model::BranchDef> = branches.iter().collect();
    sorted.sort_by_key(|b| std::cmp::Reverse(b.priority));

    let mut selected: Option<&crate::model::BranchDef> = None;
    let mut match_infos: Vec<Value> = Vec::new();
    for branch in &sorted {
        if branch.is_default {
            continue;
        }
        let cond = Condition::from_value(branch.condition.as_ref().unwrap_or(&Value::Null));
        let (matched, info) = cond.match_context(rc)?;
        if matched {
            if selected.is_none() {
                selected = Some(branch);
            }
            break;
        }
        // Go 对表达式条件不命中/组短路（info=nil）仍 append 占位——保持形状一致
        match_infos.push(info.unwrap_or(Value::Null));
    }

    if selected.is_none() {
        selected = branches.iter().find(|b| b.is_default);
    }
    let Some(selected) = selected else {
        return Err(NodeError::transient(format!(
            "execute switch node ({:?}): no branch matched and no default branch",
            node.id
        )));
    };

    // plaita 语义：命中分支的目标作为节点输出，供下游以 $NODE.<switch> 引用
    //（resolve_branch_target：next 优先，空则回落 branch name）。无目标显式
    // 报错，不静默跳空。
    let mut target = selected.next.clone();
    if target.is_empty() {
        target = selected.name.clone();
    }
    if target.is_empty() {
        return Err(NodeError::transient(format!(
            "execute switch node ({:?}): branch {:?} has no next target",
            node.id, selected.name
        )));
    }

    rc.set_current(EXTRA_RESULT_KEY, json!({ "matchInfos": match_infos }));
    Ok(StepOutcome {
        result: Value::String(target.clone()),
        next: Some(target),
    })
}

// -- if ----------------------------------------------------------------------

/// plaita decide.py Bool：condition 为 None 时恒走假分支（`if
/// branch.condition and ...` 短路——不走 loop/switch 零值条件的"恒真"回退）。
fn execute_if(
    node: &Node,
    condition: Option<&Value>,
    else_next: &str,
    rc: &SharedCtx,
) -> Result<StepOutcome, NodeError> {
    let matched = match condition {
        None => false,
        Some(c) => {
            let (m, _info) = Condition::from_value(c).match_context(rc)?;
            m
        }
    };
    let (target, branch) = if matched {
        (node.next.clone().unwrap_or_default(), "true")
    } else {
        (else_next.to_string(), "false")
    };
    if target.is_empty() {
        return Err(NodeError::transient(format!(
            "execute if node ({:?}): {branch} branch has no next target (next={:?}, else_next={else_next:?})",
            node.id,
            node.next
        )));
    }
    Ok(StepOutcome {
        result: Value::String(target.clone()),
        next: Some(target),
    })
}

// -- loop --------------------------------------------------------------------

/// ports plaita loop._coerce_collection (issue #27): a string is parsed as a
/// JSON array literal first; a non-JSON string is a single-element
/// collection (never iterated character-wise); nil → empty; dict → keys
/// (Python list(dict)); other scalars are a TypeError.
fn coerce_loop_collection(v: &Value) -> Result<Vec<Value>, NodeError> {
    match v {
        Value::Null => Ok(Vec::new()),
        Value::String(s) => {
            let stripped = s.trim();
            if stripped.starts_with('[') {
                if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(stripped) {
                    return Ok(arr);
                }
            }
            Ok(vec![Value::String(s.clone())])
        }
        Value::Array(a) => Ok(a.clone()),
        Value::Object(o) => Ok(o.keys().map(|k| Value::String(k.clone())).collect()),
        other => Err(NodeError::transient(format!(
            "loop collection: {} is not iterable",
            plaita_expression::py_repr(other)
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_loop(
    node: &Node,
    collection: &Collection,
    condition: Option<&Value>,
    item_type: Option<&crate::property::Property>,
    child_flow: &Flow,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let elements: Vec<Value> = match collection {
        Collection::Array(a) => a.clone(),
        Collection::Expr(expr) => {
            let v = eval_value(node, "loop collection", &Value::String(expr.clone()), rc).map_err(
                |e| {
                    NodeError::transient(format!(
                        "load loop collection elements error: {}",
                        e.message
                    ))
                },
            )?;
            coerce_loop_collection(&v)?
        }
    };

    let cond = Condition::from_value(condition.unwrap_or(&Value::Null));
    let mut last_result: Value = Value::Null;
    for (index, item) in elements.iter().enumerate() {
        if let (Collection::Array(_), Some(it)) = (collection, item_type) {
            if it.validate_value(item).is_err() {
                return Err(NodeError::transient(format!(
                    "invalid loop item value: {item}"
                )));
            }
        }
        rc.set_current(LOOP_INDEX_KEY, json!(index));
        rc.set_current(LOOP_ITEM_KEY, item.clone());

        // Run the child flow with the current item and index
        let input = json!({"item": item, "index": index});
        let child_rc = RuntimeCtx::child(rc, input.clone(), &child_flow.id, child_flow, deadline);
        let child_result = child_flow
            .execute_on(&child_rc, host, &input)
            .map_err(|e| NodeError::transient(e.to_string()))?;
        rc.set_current(LOOP_RESULT_KEY, child_result.clone());
        last_result = child_result;

        // Check the loop condition AFTER each iteration (Go semantics; the
        // zero-value condition always matches → a plain for-in loop)
        let (matched, _) = cond.match_context(rc)?;
        if !matched {
            break;
        }
    }

    if node.output.is_none() {
        // plaita semantics: the loop node's output is the last child result
        // ($NODE.<loop>), nil when the collection was empty
        return Ok(StepOutcome {
            result: last_result,
            next: node.next.clone(),
        });
    }
    let output = eval_value(node, "loop output", node.output.as_ref().unwrap(), rc)?;
    validate_output(node, "output", &output)?;
    Ok(StepOutcome {
        result: output,
        next: node.next.clone(),
    })
}

// -- child -------------------------------------------------------------------

fn execute_child(
    node: &Node,
    child_flow: &Flow,
    input: Option<&Value>,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let input = match input {
        Some(v) => eval_value(node, "child input", v, rc).map_err(|e| {
            NodeError::transient(format!(
                "failed to evaluate input for child node: {}",
                e.message
            ))
        })?,
        None => Value::Null,
    };
    if let Some(it) = &child_flow.input_type {
        it.validate_value(&input).map_err(|e| {
            NodeError::transient(format!(
                "failed to validate input for children workflow ({:?}): {e}",
                child_flow.id
            ))
        })?;
    }
    let child_rc = RuntimeCtx::child(rc, input.clone(), &child_flow.id, child_flow, deadline);
    let result = child_flow
        .execute_on(&child_rc, host, &input)
        .map_err(|e| {
            NodeError::transient(format!(
                "failed to exec children workflow ({:?}) : {e}",
                child_flow.id
            ))
        })?;
    validate_output(node, "child output", &result)?;
    Ok(StepOutcome {
        result,
        next: node.next.clone(),
    })
}

// -- parallel ----------------------------------------------------------------

/// plaita concurrent.py 语义：只有显式列进 joinBranches 的分支阻塞等待；
/// joinBranches 为空 = 全部后台 fire-and-forget，节点立即返回空结果 {}
/// （与"空列表=全部 join"的旧语义正好相反，issue #7）。
fn execute_parallel(
    node: &Node,
    branches: &[crate::model::ParallelBranchDef],
    is_conditional: bool,
    join_branches: &[String],
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let matched: Vec<&crate::model::ParallelBranchDef> = if !is_conditional {
        branches.iter().collect()
    } else {
        let mut out = Vec::new();
        for b in branches {
            let cond = Condition::from_value(b.branch.condition.as_ref().unwrap_or(&Value::Null));
            let (m, _) = cond.match_context(rc)?;
            if m {
                out.push(b);
            }
        }
        out
    };

    let (join, background): (
        Vec<&crate::model::ParallelBranchDef>,
        Vec<&crate::model::ParallelBranchDef>,
    ) = matched
        .into_iter()
        .partition(|b| join_branches.contains(&b.branch.name));

    // Background branches run fire-and-forget on detached threads; failures
    // are logged, never silently dropped (plaita done_callback alignment).
    // The base context is cloned (snapshot-pinned) in the parent thread
    // BEFORE dispatch (plaita#72), so background branches see parent state
    // as of the parallel node.
    for b in &background {
        let base_rc = rc.snapshot_arc();
        let b = (*b).clone();
        let host_thread = host.clone();
        let node_id = node.id.clone();
        let handle = std::thread::spawn(move || {
            if let Err(err) = run_branch_flow(&b, &base_rc, &host_thread, deadline) {
                eprintln!(
                    "parallel node {node_id:?} background branch {:?} failed: {err}",
                    b.branch.name
                );
            }
        });
        // 后台分支留 join 面：CLI/宿主可在退出前 drain（Go worker 长存无此
        // 需求；短命宿主不 drain 会在进程退出时杀掉在途分支）
        host.track_background(handle);
    }

    // Join branches run concurrently; a branch failure does not fail the
    // node — its slot gets the error sentinel (concurrent.py
    // _process_future_result).
    let mut results = serde_json::Map::new();
    if !join.is_empty() {
        let outcomes: Vec<Result<Value, String>> = std::thread::scope(|scope| {
            let handles: Vec<_> = join
                .iter()
                .map(|b| {
                    let b = (*b).clone();
                    let base_rc = rc.snapshot_arc();
                    let host = host.clone();
                    scope.spawn(move || run_branch_flow(&b, &base_rc, &host, deadline))
                })
                .collect();
            handles
                .into_iter()
                .map(|h| match h.join() {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(e.to_string()),
                    Err(_) => Err("branch thread panicked".to_string()),
                })
                .collect()
        });
        for (b, outcome) in join.iter().zip(outcomes) {
            match outcome {
                Ok(v) => {
                    results.insert(b.branch.name.clone(), v);
                }
                Err(err) => {
                    eprintln!(
                        "parallel node {:?} join branch {:?} failed, result set to the error sentinel: {err}",
                        node.id, b.branch.name
                    );
                    results.insert(
                        b.branch.name.clone(),
                        json!({
                            "__parallel_error__": err,
                            "__branch__": b.branch.name,
                        }),
                    );
                }
            }
        }
    }

    Ok(StepOutcome {
        result: Value::Object(results),
        next: node.next.clone(),
    })
}

/// Resolves the branch input and executes the branch sub-flow (shared by
/// join and background paths). A branch without an explicit input inherits
/// the parent $INPUT (concurrent.py _resolve_branch_input) — the inherited
/// value is the clone's snapshot, so in-place writes inside the branch never
/// reach the parent runtime.
fn run_branch_flow(
    b: &crate::model::ParallelBranchDef,
    base_rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<Value, NodeError> {
    let Some(flow) = &b.flow else {
        return Err(NodeError::transient("parallel branch has no flow"));
    };
    let input = match &b.input {
        Some(v) => plaita_expression::eval_value(v, base_rc.as_ref()).map_err(|e| {
            NodeError::transient(format!(
                "parallel branch {:?} resolve input error: {e}",
                b.branch.name
            ))
        })?,
        None => base_rc
            .snapshot_json()
            .get("input")
            .cloned()
            .unwrap_or(Value::Null),
    };
    let branch_rc = RuntimeCtx::child(base_rc, input.clone(), &flow.id, flow, deadline);
    flow.execute_on(&branch_rc, host, &input)
        .map_err(|e| NodeError::transient(e.to_string()))
}
