//! NS-M4 嵌套挂起差分 runner（隐藏子命令 `ns`）：单进程内执行一份用例——
//! `execute → suspend → resume(…)…`——并把统一形状的结果 JSON 打到 stdout，
//! 供 `tools/nested_suspend_diff/diffcheck.py` 与 go/plaita 两腿对拍。
//!
//! 用例形状与结果形状见 `tools/nested_suspend_diff/README.md`。比较面钉在
//! 语义等价类：结果值 JSON 相等；拒绝按类别（loop_frame/parallel_frame/
//! resume_guard）而非逐字文案——三仓文案是各自家方言。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use plaita_engine::exec::{ResumeKind, SubscriptionRegistry, SubscriptionRequest};
use plaita_engine::ir;
use plaita_engine::FlowError;

/// 私有订阅绑定键（rs `RESUME_BINDING_KEY`；go/python 同名键）：三仓都接受
/// 该键承载绑定且准入后剥掉——用同一键保证恢复载荷里不残留引擎方言。
const BINDING_KEY: &str = "__plaita_subscription_id";

/// 用例 `flows` 注册表：reference 节点的 flowId 在装载期经此解析（与 go
/// `ir.LoadWithFlowProvider`、plaita 调度器注入同职能）。
struct MapProvider {
    flows: HashMap<String, Value>,
}

impl ir::FlowProvider for MapProvider {
    fn load_flow_json(&self, flow_id: &str, _v: Option<&str>) -> Result<Value, String> {
        self.flows
            .get(flow_id)
            .cloned()
            .ok_or_else(|| format!("case flow registry: flow {flow_id:?} not found"))
    }
}

/// 内存订阅注册表：顺序派发 `sub-N`（与三仓测试的假件同形）。
#[derive(Default)]
struct SeqRegistry {
    seq: Mutex<usize>,
}

impl SubscriptionRegistry for SeqRegistry {
    fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String> {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        req.subscription_id = format!("sub-{}", *seq);
        Ok(())
    }

    fn unregister(&self, _subscription_id: &str, _tenant_id: &str) -> Result<(), String> {
        Ok(())
    }
}

/// 拒绝文案 → 具名类别（比较面）。rs 方言：loop 闸消息点名 `loop-iteration`、
/// parallel 静态闸点名 `contains a suspending node`、订阅绑定守卫点名
/// `not bound to the suspended subscription`。
fn reject_category(message: &str) -> &'static str {
    if message.contains("loop-iteration") {
        "loop_frame"
    } else if message.contains("contains a suspending node") {
        "parallel_frame"
    } else if message.contains("not bound to the suspended subscription")
        || message.contains("event type mismatch")
    {
        "resume_guard"
    } else {
        "error"
    }
}

/// reference 节点引用键的仓间方言归一：用例文件按 plaita 生产者方言写
/// `flowID`（plaita `ReferenceFlow.LEGACY_KEYS` 同款），而 rs/go 装载层只认
/// `flowId`/`flow_id`（差异已记 README「分歧清单」）。runner 侧改名，引擎
/// 装载路径保持原样。
/// 执行一份用例并把统一结果 JSON 打到 stdout。
pub fn run(case_path: &str) -> Result<(), String> {
    let text = std::fs::read_to_string(case_path).map_err(|e| format!("read {case_path}: {e}"))?;
    let case: Value = serde_json::from_str(&text).map_err(|e| format!("parse case: {e}"))?;
    let name = case
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("?")
        .to_string();

    let mut flows = HashMap::new();
    if let Some(map) = case.get("flows").and_then(|v| v.as_object()) {
        for (k, v) in map {
            let doc = v.clone();
            flows.insert(k.clone(), doc);
        }
    }

    let flow_doc = case.get("flow").cloned().unwrap_or(Value::Null);
    let flow = ir::load_with_flow_provider(
        &serde_json::to_vec(&flow_doc).map_err(|e| format!("render flow: {e}"))?,
        &MapProvider { flows },
    )
    .map_err(|e| format!("load flow: {e}"))?;

    let input = case.get("input").cloned().unwrap_or(json!({}));
    // 决策载荷序：pop 自尾，顺序消费。
    let mut resumes: Vec<Value> = match case.get("resume") {
        Some(Value::Array(a)) => a.clone(),
        Some(v) if v.is_object() => vec![v.clone()],
        _ => vec![],
    };
    resumes.reverse();

    let host = plaita_engine::Host::with_execution_id("exec-ns")
        .with_tenant_id("t1")
        .with_subscriptions(Arc::new(SeqRegistry::default()));

    let mut suspended_nodes: Vec<String> = Vec::new();
    // 哨兵类别：resume 序耗尽仍挂起 → outcome=suspended（非拒绝）。
    const STILL_SUSPENDED: &str = "__still_suspended__";

    let outcome: Result<Value, (&'static str, String)> = match flow.execute(&input, &host) {
        Ok(v) => Ok(v),
        Err(FlowError::Suspended(s)) => {
            let mut s = s;
            loop {
                suspended_nodes.push(s.suspended_node.clone());
                let Some(step) = resumes.pop() else {
                    break Err((STILL_SUSPENDED, String::new()));
                };
                let mut data = step.get("data").cloned().unwrap_or(json!({}));
                if let Value::Object(map) = &mut data {
                    let bind = step.get("bind").and_then(|b| b.as_str()).unwrap_or("auto");
                    let sub = if bind == "wrong" {
                        "sub-wrong".to_string()
                    } else {
                        s.node_state()
                            .get("subscription_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string()
                    };
                    map.insert(BINDING_KEY.to_string(), json!(sub));
                }
                let kind = match step.get("type").and_then(|t| t.as_str()) {
                    Some("timeout") => ResumeKind::Timeout,
                    Some("cancel") => ResumeKind::Cancel,
                    _ => ResumeKind::Event,
                };
                match s.resume(kind, data) {
                    Ok(v) => break Ok(v),
                    Err(FlowError::Suspended(inner)) => s = inner,
                    Err(e) => break Err((reject_category(&e.to_string()), e.to_string())),
                }
            }
        }
        Err(e) => Err((reject_category(&e.to_string()), e.to_string())),
    };

    let suspended_node = suspended_nodes.first().cloned().map(Value::String);
    let result = match outcome {
        Ok(output) => json!({
            "name": name,
            "outcome": "completed",
            "suspended_node": suspended_node,
            "suspended_nodes": suspended_nodes,
            "output": output,
        }),
        Err((STILL_SUSPENDED, _)) => json!({
            "name": name,
            "outcome": "suspended",
            "suspended_node": suspended_node,
            "suspended_nodes": suspended_nodes,
        }),
        Err((category, detail)) => json!({
            "name": name,
            "outcome": "rejected",
            "suspended_node": suspended_node,
            "suspended_nodes": suspended_nodes,
            "reject_reason": category,
            "detail": detail,
        }),
    };
    println!(
        "{}",
        serde_json::to_string(&result).map_err(|e| e.to_string())?
    );
    Ok(())
}
