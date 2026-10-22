//! parallel 分支池旋钮与嵌套守卫（B4 r7）：env 走**子进程**验证——
//! `PLAITA_BG_THREAD_WORKERS` 在进程内取值一次（OnceLock，镜像 Python
//! 模块级单例池），只有 CLI 子进程能安全钉住它。
//! 权威 = Python `parallel_executor.py`：默认 8、env 可调、嵌套共享池
//! 降级串行内联（concurrent.py `_execute_nested_inline`）。

use std::process::{Command, Stdio};

const CLI: &str = env!("CARGO_BIN_EXE_plaita-rs");

const BRANCH_CODE: &str =
    "import time\ndef run(input):\n    s=time.time()\n    time.sleep(0.6)\n    return {'start': s, 'end': time.time()}\n";

fn cap_flow_file(n: usize) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("plaita-rs-parallel-cap-test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("cap{n}.json"));
    let branches: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{"name": "b{i}", "flow": {{
                    "flow_id": "br{i}",
                    "nodes": [
                        {{"type": "start", "id": "s", "next": "c"}},
                        {{"type": "code", "id": "c", "language": "python",
                          "sandbox_backend": "subprocess", "code": {code:?}, "next": "e"}},
                        {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}}
                    ]}}}}"#,
                i = i,
                code = BRANCH_CODE
            )
        })
        .collect();
    let joins: Vec<String> = (0..n).map(|i| format!("\"b{i}\"")).collect();
    std::fs::write(
        &path,
        format!(
            r#"{{
            "flow_id": "cap-probe",
            "nodes": [
                {{"type": "start", "id": "s", "next": "p"}},
                {{"type": "parallel", "id": "p", "next": "e",
                  "branches": [{}],
                  "joinBranches": [{}]}},
                {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.p"}}
            ]}}"#,
            branches.join(","),
            joins.join(",")
        ),
    )
    .unwrap();
    path
}

fn peak_overlap(result: &serde_json::Value) -> usize {
    let mut iv: Vec<(f64, f64)> = result
        .as_object()
        .expect("join results object")
        .values()
        .map(|v| (v["start"].as_f64().unwrap(), v["end"].as_f64().unwrap()))
        .collect();
    iv.sort_by(|a, b| a.0.total_cmp(&b.0));
    iv.iter()
        .map(|&(s, _)| iv.iter().filter(|&&(s2, e2)| s2 <= s && s < e2).count())
        .max()
        .unwrap_or(0)
}

/// env=2：6 分支的峰值重叠必须 ≤2（Python 同名 env 同值实测峰值 2）。
#[test]
fn branch_pool_env_knob_bounds_concurrency() {
    let flow = cap_flow_file(6);
    let out = Command::new(CLI)
        .arg("run")
        .arg(&flow)
        .env("PLAITA_BG_THREAD_WORKERS", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(result.as_object().unwrap().len(), 6);
    let peak = peak_overlap(&result);
    assert!(
        peak <= 2,
        "env=2 must cap peak concurrency at 2, got {peak}"
    );
}

/// env=1（极端压池）：分支内嵌套 parallel 降级串行内联、不死锁
/// （Python `_execute_nested_inline` 同款：持号线程不再取子号）。
/// 守卫若失效，嵌套分支持唯一名额等子号 → 本测试永久挂起（CI 超时兜底）。
#[test]
fn nested_parallel_inside_a_branch_runs_inline_instead_of_deadlocking() {
    let dir = std::env::temp_dir().join("plaita-rs-parallel-cap-test");
    std::fs::create_dir_all(&dir).unwrap();
    let flow = dir.join("nested.json");
    let inner_code = "def run(input):\n    return {'inner': input.get('marker')}\n";
    std::fs::write(
        &flow,
        format!(
            r#"{{
            "flow_id": "nested-under-cap",
            "nodes": [
                {{"type": "start", "id": "s", "next": "p1"}},
                {{"type": "parallel", "id": "p1", "next": "e",
                  "branches": [
                    {{"name": "outer", "flow": {{
                        "flow_id": "outer-branch",
                        "nodes": [
                            {{"type": "start", "id": "os", "next": "p2"}},
                            {{"type": "parallel", "id": "p2", "next": "oe",
                              "branches": [
                                {{"name": "i1", "input": {{"marker": "one"}}, "flow": {{
                                    "flow_id": "i1",
                                    "nodes": [
                                        {{"type": "start", "id": "s", "next": "c"}},
                                        {{"type": "code", "id": "c", "language": "python",
                                          "sandbox_backend": "subprocess", "code": {inner:?},
                                          "input": "$INPUT", "next": "e"}},
                                        {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}}
                                    ]}}}},
                                {{"name": "i2", "input": {{"marker": "two"}}, "flow": {{
                                    "flow_id": "i2",
                                    "nodes": [
                                        {{"type": "start", "id": "s", "next": "c"}},
                                        {{"type": "code", "id": "c", "language": "python",
                                          "sandbox_backend": "subprocess", "code": {inner:?},
                                          "input": "$INPUT", "next": "e"}},
                                        {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}}
                                    ]}}}}
                              ],
                              "joinBranches": ["i1", "i2"]}},
                            {{"type": "end", "id": "oe", "resultType": "success", "output": "$NODE.p2"}}
                        ]}}}}
                  ],
                  "joinBranches": ["outer"]}},
                {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.p1"}}
            ]}}"#,
            inner = inner_code
        ),
    )
    .unwrap();
    let out = Command::new(CLI)
        .arg("run")
        .arg(&flow)
        .env("PLAITA_BG_THREAD_WORKERS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("one") && stdout.contains("two"),
        "nested branches must both complete: {stdout}"
    );
}
