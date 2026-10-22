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
        NodeKind::Map {
            collection,
            item_type,
            concurrent,
            max_concurrent,
            child_flow,
        } => execute_map(
            node,
            collection,
            item_type.as_ref(),
            *concurrent,
            *max_concurrent,
            child_flow,
            rc,
            host,
            deadline,
        ),
        NodeKind::Filter {
            collection,
            item_type,
            child_flow,
        } => execute_filter(
            node,
            collection,
            item_type.as_ref(),
            child_flow,
            rc,
            host,
            deadline,
        ),
        NodeKind::Find {
            collection,
            item_type,
            child_flow,
        } => execute_find(
            node,
            collection,
            item_type.as_ref(),
            child_flow,
            rc,
            host,
            deadline,
        ),
        NodeKind::Reduce {
            collection,
            item_type,
            initial,
            child_flow,
        } => execute_reduce(
            node,
            collection,
            item_type.as_ref(),
            initial.as_ref(),
            child_flow,
            rc,
            host,
            deadline,
        ),
        NodeKind::While {
            condition,
            max_iterations,
            child_flow,
        } => execute_while(
            node,
            condition.as_ref(),
            *max_iterations,
            child_flow,
            rc,
            host,
            deadline,
        ),
        NodeKind::Mock { value } => Ok(StepOutcome {
            // plaita mock.py：value 原样透传不求值（pin 语义即字面固定），
            // output 字段忽略（golden mock_output_ignored 钉）。
            result: value.clone().unwrap_or(Value::Null),
            next: node.next.clone(),
        }),
        NodeKind::Reference { child_flow, input } => {
            execute_reference(node, child_flow, input.as_ref(), rc, host, deadline)
        }
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
            host.cancel_event_flag(),
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
                        deadline,
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

/// loop 家族共享：求值 `collection` 字段并 coerce 成元素列表。
fn eval_collection_elements(
    node: &Node,
    collection: &Collection,
    rc: &SharedCtx,
    what: &str,
) -> Result<Vec<Value>, NodeError> {
    match collection {
        Collection::Array(a) => Ok(a.clone()),
        Collection::Expr(expr) => {
            let v = eval_value(node, what, &Value::String(expr.clone()), rc).map_err(|e| {
                NodeError::transient(format!(
                    "load {what} collection elements error: {}",
                    e.message
                ))
            })?;
            coerce_loop_collection(&v)
        }
    }
}

/// loop 家族共享：一轮子流程执行——子流程的 `$INPUT` 只含注入形状
/// （{item,index} / {first,second} / 位置数组），`$PARENT` 是调用方执行；
/// 节点内部零 try/except，子流程错误直接冒泡交引擎按策略分流。
fn run_collection_child(
    child_flow: &Flow,
    input: Value,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<Value, NodeError> {
    let child_rc = RuntimeCtx::child(rc, input.clone(), &child_flow.id, child_flow, deadline);
    child_flow
        .execute_on(&child_rc, host, &input)
        .map_err(|e| NodeError::transient(e.to_string()))
}

/// loop 家族共享的输出尾（Go collectionExecuteResult 同形）：无显式 output →
/// 计算出的集合值本身就是节点结果（下游 $NODE.<id>）；否则求值 output 并按
/// outputType 校验。
fn collection_result_tail(
    node: &Node,
    result: Value,
    rc: &SharedCtx,
) -> Result<StepOutcome, NodeError> {
    if node.output.is_none() {
        return Ok(StepOutcome {
            result,
            next: node.next.clone(),
        });
    }
    let output = eval_value(node, "output", node.output.as_ref().unwrap(), rc)?;
    validate_output(node, "output", &output)?;
    Ok(StepOutcome {
        result: output,
        next: node.next.clone(),
    })
}

/// 字面量数组的 itemType 闸（Go checkCollectionItem 同形）：只在 collection
/// 是字面量数组且声明了 itemType 时生效（编译器产出的 collection 恒为表达式，
/// 这是手写 flow 的护栏，家族统一）。
fn check_collection_item(
    collection: &Collection,
    item_type: Option<&crate::property::Property>,
    node_type: &str,
    item: &Value,
) -> Result<(), NodeError> {
    if let (Collection::Array(_), Some(it)) = (collection, item_type) {
        if it.validate_value(item).is_err() {
            return Err(NodeError::transient(format!(
                "invalid {node_type} item value: {item}"
            )));
        }
    }
    Ok(())
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
    let elements = eval_collection_elements(node, collection, rc, "loop")?;

    let cond = Condition::from_value(condition.unwrap_or(&Value::Null));
    let mut last_result: Value = Value::Null;
    for (index, item) in elements.iter().enumerate() {
        check_collection_item(collection, item_type, "loop", item)?;
        rc.set_current(LOOP_INDEX_KEY, json!(index));
        rc.set_current(LOOP_ITEM_KEY, item.clone());

        // Run the child flow with the current item and index
        let input = json!({"item": item, "index": index});
        let child_result = run_collection_child(child_flow, input, rc, host, deadline)?;
        rc.set_current(LOOP_RESULT_KEY, child_result.clone());
        last_result = child_result;

        // Check the loop condition AFTER each iteration (Go semantics; the
        // zero-value condition always matches → a plain for-in loop)
        let (matched, _) = cond.match_context(rc)?;
        if !matched {
            break;
        }
    }

    collection_result_tail(node, last_result, rc)
}

// -- map / filter / find / reduce（loop.py 集合并五类，plaita-go#80 对齐）----

#[allow(clippy::too_many_arguments)]
fn execute_map(
    node: &Node,
    collection: &Collection,
    item_type: Option<&crate::property::Property>,
    concurrent: bool,
    max_concurrent: Option<i64>,
    child_flow: &Flow,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let elements = eval_collection_elements(node, collection, rc, "map")?;
    let mut results: Vec<Value> = vec![Value::Null; elements.len()];

    // Python `_build_executor`：concurrent=True 且已在共享池 worker 线程上时
    // 降级串行（向同一池再提交会 starving 死锁）——plaita-go#80 同款守卫。
    if concurrent && !elements.is_empty() && !in_branch_pool() {
        execute_map_concurrent(
            collection,
            item_type,
            max_concurrent,
            child_flow,
            &elements,
            &mut results,
            rc,
            host,
            deadline,
        )?;
    } else {
        for (index, item) in elements.iter().enumerate() {
            check_collection_item(collection, item_type, "map", item)?;
            let input = json!({"item": item, "index": index});
            results[index] = run_collection_child(child_flow, input, rc, host, deadline)?;
        }
    }

    collection_result_tail(node, Value::Array(results), rc)
}

/// Python ThreadParallelExecutor 的对应物：共享池 worker 数 = 进程级分支
/// 许可池（缺省 8，`PLAITA_BG_THREAD_WORKERS` 可调，r7 对齐面）；显式
/// `maxConcurrent`（Python `max_workers`）给定时用同款 semaphore gate——
/// 缺省/0/negative 都按 Python `or None` 语义回落池宽。结果按集合顺序落位；
/// 错误取最小失败下标（ThreadPoolExecutor.map 按序产出，最早失败先冒泡）。
#[allow(clippy::too_many_arguments)]
fn execute_map_concurrent(
    collection: &Collection,
    item_type: Option<&crate::property::Property>,
    max_concurrent: Option<i64>,
    child_flow: &Flow,
    elements: &[Value],
    results: &mut [Value],
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<(), NodeError> {
    let gate: usize = match max_concurrent {
        Some(n) if n > 0 => (n as usize).min(elements.len()),
        _ => branch_permits().max,
    };
    let next_index = std::sync::atomic::AtomicUsize::new(0);
    let (tx, rx) = std::sync::mpsc::channel::<(usize, Result<Value, String>)>();
    let next_index = &next_index;
    std::thread::scope(|scope| {
        for _ in 0..gate {
            let tx = tx.clone();
            scope.spawn(move || loop {
                let index = next_index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if index >= elements.len() {
                    break;
                }
                IN_BRANCH_POOL.with(|c| c.set(true));
                let outcome = (|| -> Result<Value, NodeError> {
                    let item = &elements[index];
                    check_collection_item(collection, item_type, "map", item)?;
                    let input = json!({"item": item, "index": index});
                    run_collection_child(child_flow, input, rc, host, deadline)
                })();
                IN_BRANCH_POOL.with(|c| c.set(false));
                let _ = tx.send((index, outcome.map_err(|e| e.to_string())));
            });
        }
    });
    drop(tx);
    // 最小失败下标先冒泡（Python map 的按序语义）。
    let mut errs: Vec<Option<String>> = vec![None; elements.len()];
    for (index, outcome) in rx {
        match outcome {
            Ok(v) => results[index] = v,
            Err(e) => errs[index] = Some(e),
        }
    }
    if let Some(err) = errs.into_iter().flatten().next() {
        return Err(NodeError::transient(err));
    }
    Ok(())
}

fn execute_filter(
    node: &Node,
    collection: &Collection,
    item_type: Option<&crate::property::Property>,
    child_flow: &Flow,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let elements = eval_collection_elements(node, collection, rc, "filter")?;
    let mut results: Vec<Value> = Vec::new();
    for (index, item) in elements.iter().enumerate() {
        check_collection_item(collection, item_type, "filter", item)?;
        let input = json!({"item": item, "index": index});
        let result = run_collection_child(child_flow, input, rc, host, deadline)?;
        if plaita_expression::truthy(&result) {
            results.push(item.clone());
        }
    }
    collection_result_tail(node, Value::Array(results), rc)
}

fn execute_find(
    node: &Node,
    collection: &Collection,
    item_type: Option<&crate::property::Property>,
    child_flow: &Flow,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let elements = eval_collection_elements(node, collection, rc, "find")?;
    for (index, item) in elements.iter().enumerate() {
        check_collection_item(collection, item_type, "find", item)?;
        let input = json!({"item": item, "index": index});
        let result = run_collection_child(child_flow, input, rc, host, deadline)?;
        if plaita_expression::truthy(&result) {
            return collection_result_tail(node, item.clone(), rc);
        }
    }
    // 无真值元素 → None（Python 返回 None）。
    collection_result_tail(node, Value::Null, rc)
}

#[allow(clippy::too_many_arguments)]
fn execute_reduce(
    node: &Node,
    collection: &Collection,
    item_type: Option<&crate::property::Property>,
    initial: Option<&Value>,
    child_flow: &Flow,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let elements = eval_collection_elements(node, collection, rc, "reduce")?;

    // 累积链（plaita loop.py Reduce）：initial 缺省 → 首元素作初值、从第二个
    // 元素开始；JSON null 视同缺省（Python `self.initial is not None` 判据）。
    // 空集合且无 initial → Python collection[0] 的 IndexError（确定性错误，
    // 不重试）；空集合但有 initial → 返回 initial。
    let (mut acc, items): (Value, &[Value]) = match initial {
        Some(expr) => {
            let v = eval_value(node, "reduce initial", expr, rc).map_err(|e| {
                NodeError::transient(format!("eval reduce initial error: {}", e.message))
            })?;
            (v, &elements[..])
        }
        None => match elements.split_first() {
            Some((first, rest)) => (first.clone(), rest),
            None => return Err(NodeError::deterministic(
                "reduce on empty collection without initial: IndexError: list index out of range",
            )),
        },
    };

    // 子流程输入两形态（plaita _child_is_array_input）：子流程
    // inputType.dataType == "array" → 位置参数 [acc, item]（@flow 编译器固定
    // 产此形态）；否则 object {first, second}。不注入 index。
    let array_input = child_flow
        .input_type
        .as_ref()
        .and_then(|p| p.data_type)
        .map(|d| d == crate::property::DataType::Array)
        .unwrap_or(false);
    for item in items {
        check_collection_item(collection, item_type, "reduce", item)?;
        let input = if array_input {
            json!([acc, item])
        } else {
            json!({"first": acc, "second": item})
        };
        acc = run_collection_child(child_flow, input, rc, host, deadline)?;
    }

    collection_result_tail(node, acc, rc)
}

/// loop.py While（条件循环）：先判断后执行——条件满足才继续下一轮。条件
/// 上下文可见 $LOOP-ITEM（上一轮子流程结果，首轮 None）与 $LOOP-INDEX（当前
/// 轮次，从 0 起）——Python While **不**注入 $LOOP-RESULT（Loop 专属）。
/// condition 缺省/null → Python condition=None：只执行一轮。
/// max_iterations 缺省 1000，触顶仅告警不报错（Python _logger.warning 同款，
/// 且只有真正跑满的循环才报——0/负上限零轮静默）。
#[allow(clippy::too_many_arguments)]
fn execute_while(
    node: &Node,
    condition: Option<&Value>,
    max_iterations: Option<i64>,
    child_flow: &Flow,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let max_it = max_iterations.unwrap_or(1000);
    let cond = Condition::from_value(condition.unwrap_or(&Value::Null));
    let single_round = condition.is_none();
    let mut index: i64 = 0;
    let mut result: Value = Value::Null;
    let mut stopped_by_limit = false;
    while index < max_it {
        if single_round && index > 0 {
            break;
        }
        rc.set_current(LOOP_ITEM_KEY, result.clone());
        rc.set_current(LOOP_INDEX_KEY, json!(index));
        let (matched, _) = cond.match_context(rc)?;
        if !matched {
            break;
        }
        let input = json!({"item": result, "index": index});
        result = run_collection_child(child_flow, input, rc, host, deadline)?;
        index += 1;
        stopped_by_limit = index >= max_it;
    }
    if stopped_by_limit {
        eprintln!(
            "while 节点 {:?} 达到 max_iterations={max_it} 上限，已强制停止",
            node.id
        );
    }
    collection_result_tail(node, result, rc)
}

// -- reference ----------------------------------------------------------------

/// child.py ReferenceFlow（引用逻辑）：childFlow 已由 FlowProvider 在装载期
/// 注入。子流程隔离父上下文——`$PARENT.<x>` 求值为 null（隔离是实测口径），
/// `$GLOBAL`/`$ENV` 与执行身份继承调用方；input 缺省（求值 None）→ 子流程
/// 继承调用方 $INPUT（golden ref_no_input_inherits_caller 钉）。
fn execute_reference(
    node: &Node,
    child_flow: &Flow,
    input: Option<&Value>,
    rc: &SharedCtx,
    host: &Host,
    deadline: Option<Instant>,
) -> Result<StepOutcome, NodeError> {
    let input = match input {
        Some(v) => eval_value(node, "reference input", v, rc).map_err(|e| {
            NodeError::transient(format!(
                "failed to evaluate input for reference node: {}",
                e.message
            ))
        })?,
        None => rc
            .snapshot_json()
            .get("input")
            .cloned()
            .unwrap_or(Value::Null),
    };
    if let Some(it) = &child_flow.input_type {
        it.validate_value(&input).map_err(|e| {
            NodeError::transient(format!(
                "failed to validate input for children workflow ({:?}): {e}",
                child_flow.id
            ))
        })?;
    }
    let child_rc =
        RuntimeCtx::child_isolated(rc, input.clone(), &child_flow.id, child_flow, deadline);
    let result = child_flow
        .execute_on(&child_rc, host, &input)
        .map_err(|e| {
            NodeError::transient(format!(
                "failed to exec children workflow ({:?}) : {e}",
                child_flow.id
            ))
        })?;
    validate_output(node, "reference output", &result)?;
    Ok(StepOutcome {
        result,
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

/// Python `parallel_executor.py` 后台池的 worker 数旋钮：
/// `_DEFAULT_BG_THREAD_WORKERS = int(os.environ.get("PLAITA_BG_THREAD_WORKERS", "8"))`
/// ——thread/process 两池共用、模块级单例，因此**所有** parallel 节点的并发
/// 分支流共享同一预算（16 分支默认峰值 8，超出者排队）。Rust 侧以进程级
/// 信号量等价实现（见 `branch_permits`），同名 env 同默认 8；非法值回退
/// 默认并告警（Python 是 import 期 int() 直接崩——这里按「降级必须可观测」
/// 处理，健壮性分歧记录在案）。进程生命周期内取值一次（Python 池同形）。
fn branch_pool_size() -> usize {
    const DEFAULT: usize = 8;
    match std::env::var("PLAITA_BG_THREAD_WORKERS") {
        Err(_) => DEFAULT,
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(n) if n >= 1 => n,
            _ => {
                eprintln!(
                    "[parallel] PLAITA_BG_THREAD_WORKERS={raw:?} is not a positive integer; \
                     falling back to the default branch pool of {DEFAULT}"
                );
                DEFAULT
            }
        },
    }
}

/// 进程级分支许可池——plaita 模块级单例后台池的等价物：join 与后台分支、
/// 嵌套与不嵌套的 parallel 全都从这里取并发名额。
struct BranchPermits {
    max: usize,
    live: std::sync::Mutex<usize>,
    cv: std::sync::Condvar,
}

fn branch_permits() -> &'static BranchPermits {
    static POOL: std::sync::OnceLock<BranchPermits> = std::sync::OnceLock::new();
    POOL.get_or_init(|| BranchPermits {
        max: branch_pool_size(),
        live: std::sync::Mutex::new(0),
        cv: std::sync::Condvar::new(),
    })
}

impl BranchPermits {
    fn acquire(&self) {
        let mut live = self.live.lock().expect("branch permit lock");
        while *live >= self.max {
            live = self.cv.wait(live).expect("branch permit condvar");
        }
        *live += 1;
    }

    fn release(&self) {
        let mut live = self.live.lock().expect("branch permit lock");
        *live -= 1;
        self.cv.notify_one();
    }
}

/// RAII 名额：分支流 panic 也不漏还。
struct BranchPermit(&'static BranchPermits);

impl BranchPermit {
    fn acquire() -> BranchPermit {
        let pool = branch_permits();
        pool.acquire();
        BranchPermit(pool)
    }
}

impl Drop for BranchPermit {
    fn drop(&mut self) {
        self.0.release();
    }
}

thread_local! {
    /// 当前线程是否正在（持名额）执行一条 parallel 分支流——Python
    /// `parallel_executor._pool_tls.in_pool` 的移植面。
    static IN_BRANCH_POOL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn in_branch_pool() -> bool {
    IN_BRANCH_POOL.with(|c| c.get())
}

/// plaita concurrent.py 语义：只有显式列进 joinBranches 的分支阻塞等待；
/// joinBranches 为空 = 全部后台 fire-and-forget，节点立即返回空结果 {}
/// （与"空列表=全部 join"的旧语义正好相反，issue #7）。
///
/// 并发上限（B4 r7 对齐 Python 后台池）：join 与后台分支共享进程级
/// [`branch_permits`] 名额，默认 8、`PLAITA_BG_THREAD_WORKERS` 可调；
/// 超出名额的分支在各自线程上排队等号。分支线程持名额期间
/// [`IN_BRANCH_POOL`] 置位——嵌套 parallel 在分支内降级为**串行内联**
/// （concurrent.py:233-237 `_execute_nested_inline` 的移植面）：持号线程
/// 不再取子号、不再开分支线程，杜绝「持号等子号」循环等待。
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

    // 分支线程持名额运行分支流；名额 RAII 保证 panic 不漏还。
    fn run_under_permit(
        b: &crate::model::ParallelBranchDef,
        base_rc: &SharedCtx,
        host: &Host,
        deadline: Option<Instant>,
    ) -> Result<Value, NodeError> {
        let _permit = BranchPermit::acquire();
        IN_BRANCH_POOL.with(|c| c.set(true));
        let out = run_branch_flow(b, base_rc, host, deadline);
        IN_BRANCH_POOL.with(|c| c.set(false));
        out
    }

    // 嵌套内联守卫：本线程已在池内（本身是一条分支流），嵌套并行改为
    // 串行内联执行——正确性优先，仅嵌套场景放弃并发（Python 同款降级，
    // 消防 concurrent.py 注释里的「分支数 >= max_workers 全员互等」死锁）。
    if in_branch_pool() {
        let mut results = serde_json::Map::new();
        for b in &background {
            let base_rc = rc.snapshot_arc();
            if let Err(err) = run_branch_flow(b, &base_rc, host, deadline) {
                eprintln!(
                    "parallel node {node_id:?} background branch {name:?} failed (inline): {err}",
                    node_id = node.id,
                    name = b.branch.name
                );
            }
        }
        for b in &join {
            let base_rc = rc.snapshot_arc();
            match run_branch_flow(b, &base_rc, host, deadline) {
                Ok(v) => {
                    results.insert(b.branch.name.clone(), v);
                }
                Err(err) => {
                    eprintln!(
                        "parallel node {:?} join branch {:?} failed (inline), result set to the error sentinel: {err}",
                        node.id, b.branch.name
                    );
                    results.insert(
                        b.branch.name.clone(),
                        json!({
                            "__parallel_error__": err.to_string(),
                            "__branch__": b.branch.name,
                        }),
                    );
                }
            }
        }
        return Ok(StepOutcome {
            result: Value::Object(results),
            next: node.next.clone(),
        });
    }

    // Background branches run fire-and-forget on detached threads; failures
    // are logged, never silently dropped (plaita done_callback alignment).
    // The base context is cloned (snapshot-pinned) in the parent thread
    // BEFORE dispatch (plaita#72), so background branches see parent state
    // as of the parallel node. Permits draw from the same process-wide pool
    // as join branches (plaita's single shared background pool).
    for b in &background {
        let base_rc = rc.snapshot_arc();
        let b = (*b).clone();
        let host_thread = host.clone();
        let node_id = node.id.clone();
        let handle = std::thread::spawn(move || {
            if let Err(err) = run_under_permit(&b, &base_rc, &host_thread, deadline) {
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

    // Join branches run concurrently up to the shared permit budget; a
    // branch failure does not fail the node — its slot gets the error
    // sentinel (concurrent.py _process_future_result).
    let mut results = serde_json::Map::new();
    if !join.is_empty() {
        let outcomes: Vec<Result<Value, String>> = std::thread::scope(|scope| {
            let handles: Vec<_> = join
                .iter()
                .map(|b| {
                    let b = (*b).clone();
                    let base_rc = rc.snapshot_arc();
                    let host = host.clone();
                    scope.spawn(move || run_under_permit(&b, &base_rc, &host, deadline))
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
