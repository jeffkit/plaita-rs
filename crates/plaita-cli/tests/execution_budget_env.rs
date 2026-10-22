//! 执行级缺省预算（plaita#267 对齐，B4 r7）：env 旋钮走**子进程**验证——
//! 进程内改全局 env 会和并行测试互踩，只有 CLI 子进程能安全钉住
//! `PLAITA_EXECUTION_DEADLINE_SECONDS`。权威语义 = Python
//! `core/deadline.py`（1800s 缺省、env 可调、非法回默认并告警）。

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CLI: &str = env!("CARGO_BIN_EXE_plaita-rs");

/// 无 timeout 的自环流（if 真分支自指）：缺省预算缺位时引擎无限自旋
/// （负控 RED：5s 看门狗杀），env=2s 后必须 ~2s 终态 FlowTimeout（exit 1）。
#[test]
fn execution_deadline_env_bounds_an_unbounded_flow() {
    let dir = std::env::temp_dir().join("plaita-rs-budget-env-test");
    std::fs::create_dir_all(&dir).unwrap();
    let flow = dir.join("selfloop.json");
    std::fs::write(
        &flow,
        r#"{
        "flow_id": "selfloop-no-budget",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "q", "else_next": "e",
             "condition": {"field": "$INPUT.go", "operator": "eq", "value": 1}},
            {"type": "end", "id": "e", "resultType": "success", "output": "done"}
        ]}"#,
    )
    .unwrap();

    let t0 = Instant::now();
    let out = Command::new(CLI)
        .arg("run")
        .arg(&flow)
        .arg("--input")
        .arg(r#"{"go":1}"#)
        .env("PLAITA_EXECUTION_DEADLINE_SECONDS", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    let elapsed = t0.elapsed();

    assert!(!out.status.success(), "a budget expiry must exit non-zero");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("flow budget exceeded"),
        "expected the terminal FlowTimeout dialect, got: {err}"
    );
    assert!(
        elapsed >= Duration::from_millis(1500) && elapsed <= Duration::from_secs(15),
        "expected ~2s enforcement, got {elapsed:?}"
    );
}

/// 非法 env（非数字）：回退默认并**告警**（Python deadline.py 的 WARNING
/// 同口径——绝不静默变成无限额），快流不受影响照常完成。
#[test]
fn invalid_execution_deadline_env_warns_but_does_not_break_fast_flows() {
    let dir = std::env::temp_dir().join("plaita-rs-budget-env-test");
    std::fs::create_dir_all(&dir).unwrap();
    let flow = dir.join("hello.json");
    std::fs::write(
        &flow,
        r#"{
        "flow_id": "hello",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "Hello, world!"}
        ]}"#,
    )
    .unwrap();

    let out = Command::new(CLI)
        .arg("run")
        .arg(&flow)
        .env("PLAITA_EXECUTION_DEADLINE_SECONDS", "abc")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("Hello, world!"));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("is not a number") && err.contains("NOT silently unlimited"),
        "expected the fallback warning, got: {err}"
    );
}
