//! Node-parity golden 对拍（plaita-go#80 phase 1+2 的 Rust 侧移植验收）。
//!
//! `collection_golden.json`（39 例）与 `node_parity2_golden.json`（28 例）由
//! plaita-go 的导出脚本从 **plaita Python 真跑**产出（deployment IR →
//! Flow.model_validate → 调度器按 flow_registry 注入 reference 子流程 →
//! flow.run），每例记录期望输出或错误。本测试用同一份 IR 走
//! `ir::load`（reference 用例走 `ir::load_with_flow_provider`，registry 即
//! provider）+ `Flow::execute` 逐例对拍。
//!
//! 对拍口径与 plaita-go 相同：输出按 JSON 值相等比较（键序无关、数值按值）；
//! 错误用例钉发生性，带 `error_contains` 的（两侧逐字共有的确定性文案）加钉
//! 子串。reject_calculate / reject_ignore 是「两侧都拒载」的对拍：Python 报
//! unRecognized node type，本引擎在 ir::load 拒绝（错误里的同款子串已钉）。

use std::collections::BTreeMap;

use plaita_engine::exec::Host;
use plaita_engine::ir::{self, FlowProvider};
use serde_json::Value;

/// A golden case's `flow_registry` acting as the FlowProvider — the same
/// role plaita's scheduler plays before flow.run.
struct RegistryProvider {
    flows: BTreeMap<String, Value>,
}

impl FlowProvider for RegistryProvider {
    fn load_flow_json(&self, flow_id: &str, _flow_version: Option<&str>) -> Result<Value, String> {
        self.flows
            .get(flow_id)
            .cloned()
            .ok_or_else(|| format!("flow {flow_id:?} not in registry"))
    }
}

#[derive(serde::Deserialize)]
struct Case {
    name: String,
    flow: Value,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    flow_registry: Option<BTreeMap<String, Value>>,
    expected: Expected,
}

#[derive(serde::Deserialize)]
struct Expected {
    // 显式 null 是合法期望输出（空集合 loop/find、mock 缺省值都是 null）——
    // Option<Value> 会把 null 与「字段缺席」都折叠成 None，这里区分两者。
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
fn collection_golden() {
    run_file("collection_golden.json");
}

#[test]
fn node_parity2_golden() {
    run_file("node_parity2_golden.json");
}

fn run_file(file: &str) {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/").to_string() + file;
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    assert!(!cases.is_empty(), "{file}: no cases");

    let mut failures: Vec<String> = Vec::new();
    for tc in &cases {
        if let Err(e) = run_case(tc) {
            failures.push(format!("[{}] {e}", tc.name));
        }
    }
    assert!(
        failures.is_empty(),
        "{file}: {} of {} cases diverge from the Python oracle:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

fn run_case(tc: &Case) -> Result<(), String> {
    let flow_json = serde_json::to_vec(&tc.flow).expect("serialize flow");

    // 拒载对拍：Python 模型层拒绝的 flow（节点类型未注册 / SwitchLegacy 结构
    // 错误），本引擎必须在 ir::load 同样拒绝。
    let is_model_reject = tc.expected.error.starts_with("ValidationError")
        || tc.expected.error.contains("unRecognized node type");
    if is_model_reject {
        let load_err = load_for(tc, &flow_json).expect_err("python raised; engine must reject");
        if !tc.expected.error_contains.is_empty() {
            assert_contains(&tc.name, &load_err, &tc.expected.error_contains);
        }
        return Ok(());
    }

    let flow = load_for(tc, &flow_json).map_err(|e| format!("golden flow must load: {e}"))?;
    let exec_result = flow.execute(&tc.input, &Host::new());

    if !tc.expected.error.is_empty() {
        let err = exec_result
            .err()
            .ok_or_else(|| format!("python raised {:?}; engine succeeded", tc.expected.error))?;
        let text = err.to_string();
        if !tc.expected.error_contains.is_empty() {
            assert_contains(&tc.name, &text, &tc.expected.error_contains);
        }
        return Ok(());
    }

    let got = exec_result.map_err(|e| format!("python succeeded; engine raised: {e}"))?;
    let want = tc
        .expected
        .output
        .as_ref()
        .ok_or_else(|| "case has neither output nor error expectation".to_string())?;
    if !json_eq(want, &got) {
        return Err(format!(
            "output mismatch:\n  python: {want}\n  engine: {}",
            serde_json::to_string(&got).unwrap_or_default()
        ));
    }
    Ok(())
}

fn load_for(tc: &Case, flow_json: &[u8]) -> Result<plaita_engine::Flow, String> {
    match &tc.flow_registry {
        Some(reg) => {
            let provider = RegistryProvider { flows: reg.clone() };
            ir::load_with_flow_provider(flow_json, &provider)
        }
        None => ir::load(flow_json),
    }
}

fn assert_contains(name: &str, haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "[{name}] error text {haystack:?} does not contain the shared pin {needle:?}"
    );
}

/// JSON value equality the way assert.JSONEq sees it: key order ignored,
/// numbers compared by value (6 == 6.0).
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
