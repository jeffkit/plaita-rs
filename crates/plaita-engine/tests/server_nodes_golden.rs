//! server_nodes golden 对拍（plaita-go#80 phase 3 的 Rust 侧验收）：testdata/
//! server_nodes_golden.json 由 plaita-go 的导出脚本从 **plaita Python 真跑**
//! 产出——delay/approval/http_callback 的恢复载荷由**真实 service**（Delay
//! 排程/ApprovalService.submit_approval_decision/HttpCallbackService 认领）
//! 发布，redis_queue/kafka_queue 依赖真实 broker、载荷按 service 源码的事件
//! 构造分支手建。本测试用同一份 IR 走 ir::load + Flow::execute（挂起）+
//! SuspendedExecution::resume（恢复+续跑）逐例对拍。
//!
//! 对拍口径与 plaita-go 相同：plaita 的分布式 _handle_resume 只处理被恢复
//! 节点本身（不续跑后继图），本引擎的 resume 恢复+续跑一步到位——两侧共有
//! 的可比较面 = **恢复后节点状态**（end 节点 output=$NODE.<server> 恰为该
//! 状态）。挂起态对 $NODE[suspend_node] 对拍。
//!
//! 规范化（plaita-go 测试同款）：剔键 subscription_id/subscription_tenant/
//! created_time/trigger_timestamp/actual_trigger_timestamp/
//! planned_trigger_timestamp/timestamp/recorded_at；串内 32hex→<hex32>、
//! 13 位毫秒→<ms13>。执行现场注入 32hex execution id（httpcb_auto_path 的
//! 自动路径打码对齐依赖它）。恢复载荷的订阅绑定键由本测试注入各自执行
//! 现场的 subscription_id（两侧 RESUME_BINDING_KEY 准入同款）。

use std::sync::{Arc, Mutex};

use plaita_engine::error::FlowError;
use plaita_engine::exec::{Host, ResumeKind, SubscriptionRegistry, SubscriptionRequest};
use plaita_engine::ir;
use serde_json::Value;

/// 假订阅注册表：顺序分配 golden-sub-a/b/...（plaita-go
/// goldenSubscriptionRegistry 同形）。
#[derive(Default)]
struct FakeRegistry {
    seq: Mutex<usize>,
}

impl SubscriptionRegistry for FakeRegistry {
    fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String> {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        req.subscription_id = format!("golden-sub-{}", char::from(b'a' + *seq as u8 - 1));
        Ok(())
    }

    fn unregister(&self, _subscription_id: &str, _tenant_id: &str) -> Result<(), String> {
        Ok(())
    }
}

#[derive(serde::Deserialize)]
struct Case {
    name: String,
    flow: Value,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    suspend_node: String,
    #[serde(default)]
    resume: Option<Value>,
    expected: Expected,
}

#[derive(serde::Deserialize)]
struct Expected {
    #[serde(default)]
    suspend: Option<Value>,
    #[serde(default, deserialize_with = "deserialize_output")]
    output: Option<Value>,
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_contains: String,
}

fn deserialize_output<'de, D>(de: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;
    Ok(Some(Value::deserialize(de)?))
}

#[test]
fn server_nodes_golden() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/server_nodes_golden.json"
    );
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    assert!(!cases.is_empty(), "no cases");

    let mut failures: Vec<String> = Vec::new();
    for tc in &cases {
        if let Err(e) = run_case(tc) {
            failures.push(format!("[{}] {e}", tc.name));
        }
    }
    assert!(
        failures.is_empty(),
        "server_nodes_golden: {} of {} cases diverge from the Python oracle:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

fn run_case(tc: &Case) -> Result<(), String> {
    let flow_json = serde_json::to_vec(&tc.flow).expect("serialize flow");

    // 拒绝对拍：pydantic 模型层拒绝的 flow（缺必填/非法枚举），本引擎在
    // ir::load 同拒（error_contains 钉字段名）。
    if !tc.expected.error.is_empty() {
        let load_err = ir::load(&flow_json)
            .expect_err("python raised a model rejection; engine must reject at load");
        if !tc.expected.error_contains.is_empty() {
            assert_contains(&tc.name, &load_err, &tc.expected.error_contains);
        }
        return Ok(());
    }

    let flow = ir::load(&flow_json).map_err(|e| format!("golden flow must load: {e}"))?;
    let exec_id = "a".repeat(32);
    let host = Host::with_execution_id(exec_id.clone())
        .with_tenant_id("default")
        .with_subscriptions(Arc::new(FakeRegistry::default()));

    let exec_result = flow.execute(&tc.input, &host);
    let suspended = match exec_result {
        Err(FlowError::Suspended(s)) => s,
        other => {
            return Err(format!(
                "server node must suspend, got: {:?}",
                other.map(|v| v.to_string()).err()
            ))
        }
    };
    assert_eq!(
        suspended.suspended_node, tc.suspend_node,
        "suspended at the wrong node"
    );

    // 挂起态对拍（$NODE 表项）。
    let state = suspended.node_state();
    let want_suspend = tc
        .expected
        .suspend
        .as_ref()
        .ok_or_else(|| "case has neither suspend nor error expectation".to_string())?;
    if !json_eq(&normalize(want_suspend), &normalize(&state)) {
        return Err(format!(
            "suspend state mismatch:\n  python: {}\n  engine: {}",
            serde_json::to_string(&normalize(want_suspend)).unwrap_or_default(),
            serde_json::to_string(&normalize(&state)).unwrap_or_default()
        ));
    }

    // 无恢复用例（错误凭据 stays suspended）：断言面 = 挂起态本身（已比）。
    let Some(resume_payload) = &tc.resume else {
        return Ok(());
    };

    // 恢复：注入本执行现场的订阅绑定（两侧 RESUME_BINDING_KEY 准入同款）。
    let mut payload = resume_payload.clone();
    if let Value::Object(map) = &mut payload {
        let sub_id = state.get("subscription_id").cloned().unwrap_or(Value::Null);
        map.insert(plaita_engine::exec::RESUME_BINDING_KEY.to_string(), sub_id);
    }
    let output = suspended
        .resume(ResumeKind::Event, payload)
        .map_err(|e| format!("resume failed: {e}"))?;

    let want_output = tc
        .expected
        .output
        .as_ref()
        .ok_or_else(|| "resumable case must carry expected.output".to_string())?;
    if !json_eq(&normalize(want_output), &normalize(&output)) {
        return Err(format!(
            "output mismatch:\n  python: {}\n  engine: {}",
            serde_json::to_string(&normalize(want_output)).unwrap_or_default(),
            serde_json::to_string(&normalize(&output)).unwrap_or_default()
        ));
    }
    Ok(())
}

fn assert_contains(name: &str, haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "[{name}] load error {haystack:?} does not contain the pin {needle:?}"
    );
}

// -- normalization（plaita-go normalizeGolden 同形）---------------------------

fn denylist_key(k: &str) -> bool {
    matches!(
        k,
        "subscription_id"
            | "subscription_tenant"
            | "created_time"
            | "trigger_timestamp"
            | "actual_trigger_timestamp"
            | "planned_trigger_timestamp"
            | "timestamp"
            | "recorded_at"
    )
}

fn normalize(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                if denylist_key(k) {
                    continue;
                }
                out.insert(k.clone(), normalize(val));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(normalize).collect()),
        Value::String(s) => Value::String(mask(s)),
        other => other.clone(),
    }
}

/// 串内 32 位十六进制 → <hex32>；13 位十进制毫秒 → <ms13>。线性扫描，
/// 任意位置命中（Go regex ReplaceAllString 语义）。注意先试 32hex 再试
/// 13ms：纯数字且恰为 32 位的时间戳会先被 32hex 吃掉——两侧同序即无分歧。
fn mask(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let hex_run = (0..32)
            .take_while(|&j| i + j < bytes.len() && (bytes[i + j] as char).is_ascii_hexdigit())
            .count();
        if hex_run == 32 {
            out.push_str("<hex32>");
            i += 32;
            continue;
        }
        let dec_run = (0..13)
            .take_while(|&j| i + j < bytes.len() && bytes[i + j].is_ascii_digit())
            .count();
        if dec_run == 13 {
            out.push_str("<ms13>");
            i += 13;
            continue;
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// JSON value 相等（键序无关、数值按值）。
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(u, v)| json_eq(u, v))
        }
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(u), Some(v)) => u == v,
            _ => x == y,
        },
        _ => a == b,
    }
}
