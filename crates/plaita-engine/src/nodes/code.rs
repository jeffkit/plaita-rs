//! The `code` node — python via a python3 subprocess sidecar that mirrors
//! the plaita CodeNode "subprocess" backend (plaita/node/code.py
//! run_python_subprocess + _RUNNER_TEMPLATE) contract-for-contract, the
//! same shape plaita-go's code_python.go keeps:
//!
//! - the runner script embeds the user code with plain string replacement
//!   (never templating), input arrives as JSON on stdin and the result
//!   leaves as a `{ok, result|error, type}` envelope on stdout;
//! - wall-clock timeout from `PLAITA_SANDBOX_TIMEOUT` (seconds, default 10;
//!   must be a positive integer — 0/negative/non-integers are rejected
//!   fail-closed); the child runs in its own process group so the timeout
//!   kill takes the whole tree;
//! - the child environment is the whitelisted rebuild
//!   (PATH/HOME/TMPDIR/LANG/LC_ALL/LC_CTYPE/PYTHONIOENCODING + extra), so
//!   host env secrets are not handed over;
//! - NOT an isolation boundary: operator-trusted code with host file system
//!   and network access; restricted/docker/unsafe backends are refused at
//!   load (fail-closed, never silently downgraded).
//!
//! Divergence from plaita-go (documented): javascript code nodes are
//! rejected at load (no in-process JS interpreter in plaita-rs).

use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::context::SharedCtx;
use crate::error::NodeError;
use crate::model::Node;
use crate::nodes::StepOutcome;

const PY_SANDBOX_TIMEOUT_ENV: &str = "PLAITA_SANDBOX_TIMEOUT";
const PY_SANDBOX_MEMORY_ENV: &str = "PLAITA_SANDBOX_MEMORY_MB";
const PY_DEFAULT_TIMEOUT_SECS: u64 = 10;
const PY_BACKEND_SUBPROCESS: &str = "subprocess";

/// verbatim port of plaita/node/code.py `_RUNNER_TEMPLATE`; placeholders are
/// substituted with plain string replacement so arbitrary user code passes
/// through untouched.
const PY_RUNNER_TEMPLATE: &str = r#"import sys as _sys, json as _json

# ---- resource limits (Linux only) ------------------------------------
try:
    import resource as _resource
    _mem_bytes = __MEM_BYTES__
    if _mem_bytes > 0:
        _resource.setrlimit(_resource.RLIMIT_AS, (_mem_bytes, _mem_bytes))
except Exception:
    pass

# ---- user code -------------------------------------------------------
__USER_CODE__
# ---- run -------------------------------------------------------------
_input = _json.loads(_sys.stdin.read())
try:
    _result = run(_input)
    _sys.stdout.write(_json.dumps({"ok": True, "result": _result}))
except Exception as _e:
    _sys.stdout.write(_json.dumps({"ok": False, "error": str(_e), "type": type(_e).__name__}))
_sys.stdout.flush()
"#;

fn build_python_runner_script(user_code: &str, mem_bytes: i64) -> String {
    PY_RUNNER_TEMPLATE
        .replace("__USER_CODE__", user_code)
        .replace("__MEM_BYTES__", &mem_bytes.to_string())
}

/// plaita's parse-time stand-in for the AST check: a `def run(` must exist.
fn py_defines_run_function(code: &str) -> bool {
    for line in code.lines() {
        let t = line.trim_start();
        if let Some(rest) = t.strip_prefix("def ") {
            let rest = rest.trim_start();
            if let Some(after) = rest.strip_prefix("run") {
                let after = after.trim_start();
                if after.starts_with('(') {
                    return true;
                }
            }
        }
    }
    false
}

/// plaita `_positive_int_env_seconds`: a positive integer number of seconds;
/// 0/negatives/non-integers are named failures (fail-closed).
fn positive_int_env_seconds(name: &str, fallback: u64) -> Result<u64, NodeError> {
    match std::env::var(name) {
        Err(_) => Ok(fallback),
        Ok(raw) if raw.trim().is_empty() => Ok(fallback),
        Ok(raw) => {
            let v: i64 = raw.trim().parse().map_err(|_| {
                NodeError::deterministic(format!(
                    "{name} must be a positive integer number of seconds (got {raw:?})"
                ))
            })?;
            if v <= 0 {
                return Err(NodeError::deterministic(format!(
                    "{name} must be a positive integer number of seconds (got {raw:?}); \
                     0 is a zero budget, not unlimited"
                )));
            }
            Ok(v as u64)
        }
    }
}

fn sandbox_memory_mb() -> Result<i64, NodeError> {
    match std::env::var(PY_SANDBOX_MEMORY_ENV) {
        Err(_) => Ok(0),
        Ok(raw) if raw.trim().is_empty() => Ok(0),
        Ok(raw) => raw.trim().parse::<i64>().map_err(|_| {
            NodeError::deterministic(format!(
                "{PY_SANDBOX_MEMORY_ENV} must be an integer number of megabytes (got {raw:?})"
            ))
        }),
    }
}

/// The whitelisted child environment (gate.go gateEnvAllowlist + extra).
fn build_subprocess_env(extra: &[(&str, String)]) -> Vec<(String, String)> {
    const ALLOWLIST: &[&str] = &[
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "PATH",
        "PYTHONIOENCODING",
        "TMPDIR",
    ];
    let mut env: Vec<(String, String)> = Vec::new();
    for key in ALLOWLIST {
        if let Ok(v) = std::env::var(key) {
            env.push((key.to_string(), v));
        }
    }
    for (k, v) in extra {
        env.push((k.to_string(), v.clone()));
    }
    env
}

fn clip_stderr(stderr: &str) -> String {
    if stderr.is_empty() {
        return "(empty)".to_string();
    }
    let chars: Vec<char> = stderr.chars().collect();
    if chars.len() > 500 {
        chars[..500].iter().collect()
    } else {
        stderr.to_string()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_code(
    node: &Node,
    language: &str,
    code: &str,
    input: Option<&Value>,
    sandbox_backend: &str,
    rc: &SharedCtx,
    deadline: Option<Instant>,
    cancel_event: &std::sync::atomic::AtomicBool,
) -> Result<StepOutcome, NodeError> {
    match language {
        "python" => {
            if !sandbox_backend.is_empty() && sandbox_backend != PY_BACKEND_SUBPROCESS {
                // direct-engine construction path; the loader refuses earlier
                return Err(NodeError::deterministic(format!(
                    "code node: sandboxBackend {sandbox_backend:?} is not supported in this engine: \
                     the sidecar runs python code only in the subprocess form; drop the field, set \
                     \"subprocess\", or run such flows on the plaita runtime"
                )));
            }
            if !py_defines_run_function(code) {
                return Err(NodeError::deterministic(
                    "code node: python code validation failed: No run function found",
                ));
            }
            let arguments = match input {
                Some(v) => plaita_expression::eval_value(v, rc.as_ref()).map_err(|e| {
                    NodeError::transient(format!("code node: failed to evaluate input: {e}"))
                })?,
                None => Value::Null,
            };
            let result =
                run_python_subprocess(code, arguments, deadline, cancel_event).map_err(|e| {
                    // 代码错误按确定性分类（plaita ValueError 语义，不烧重试预算）
                    // ——但 Timeout 类必须保留：引擎的节点/流程 timeoutHandler
                    // 以 class==Timeout 为闸，抹掉它会让超时策略对 code 节点失哑
                    if e.class == crate::error::ErrorClass::Timeout {
                        e
                    } else {
                        NodeError::deterministic(format!(
                            "code node: failed to interpret code: {}",
                            e.message
                        ))
                    }
                })?;
            crate::nodes::validate_output_pub(node, "code output", &result)?;
            Ok(StepOutcome {
                result,
                next: node.next.clone(),
            })
        }
        "javascript" => Err(NodeError::deterministic(
            "code node: javascript is not supported by plaita-rs (no in-process JS interpreter); \
             use language \"python\" or run the flow on the plaita/go runtime",
        )),
        "go" => Err(NodeError::deterministic(
            "code node: in-process go (yaegi) execution is not supported by this engine",
        )),
        other => Err(NodeError::deterministic(format!(
            "code node: unsupported language type: {other}"
        ))),
    }
}

/// The Go port of plaita run_python_subprocess: JSON via stdin/stdout,
/// wall-clock timeout with process-group kill.
fn run_python_subprocess(
    code: &str,
    input: Value,
    deadline: Option<Instant>,
    cancel_event: &std::sync::atomic::AtomicBool,
) -> Result<Value, NodeError> {
    let timeout_secs = positive_int_env_seconds(PY_SANDBOX_TIMEOUT_ENV, PY_DEFAULT_TIMEOUT_SECS)?;
    let mem_mb = sandbox_memory_mb()?;
    // budget = the smaller of the sidecar timeout and the node deadline
    let mut budget = Duration::from_secs(timeout_secs);
    if let Some(dl) = deadline {
        let remaining = dl.saturating_duration_since(Instant::now());
        budget = budget.min(remaining);
    }
    if budget.is_zero() {
        return Err(NodeError::timeout(
            "Subprocess sandbox timed out before spawn (node deadline already passed)",
        ));
    }

    let input_json = serde_json::to_vec(&input).map_err(|e| {
        NodeError::deterministic(format!(
            "python sidecar: input is not JSON-serialisable: {e}"
        ))
    })?;
    let runner = build_python_runner_script(code, mem_mb * 1024 * 1024);

    use std::io::Write;
    use std::os::unix::process::CommandExt;
    let mut child = Command::new("python3")
        .arg("-c")
        .arg(&runner)
        .env_clear()
        .envs(build_subprocess_env(&[]))
        // 独立进程组（plaita start_new_session=True 同款）：超时/取消可以
        // 杀掉整个树，用户代码 spawn 的孙子进程不残留
        .process_group(0)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| NodeError::deterministic(format!("python sidecar: {e}")))?;

    // stdin 写入移出主等待路径（#15，镜像 plaita _popen_wait_cancellable
    // 的 communicate-in-deadloop）：先前阻塞 write_all 在 wait_with_timeout
    // 之前执行、游离于全部预算之外——子进程不读 stdin（runner 模板里用户
    // 代码先于 stdin.read() 执行）且载荷超过管道容量时无限挂死，节点/流/
    // 沙箱三个预算全失效，还留孤儿进程组。写入交给独立线程（写完 drop
    // 关 stdin，等价先前的 take()），寿命由 wait_with_timeout 的 deadline
    // 统一管辖：killpg 后管道关闭，write_all 以 EPIPE 返回。
    let writer: Option<std::thread::JoinHandle<()>> = child.stdin.take().map(|mut stdin| {
        std::thread::spawn(move || {
            let _ = stdin.write_all(&input_json);
        })
    });

    let timeout_result = wait_with_timeout(child, budget, writer, cancel_event);
    match timeout_result {
        WaitOutcome::TimedOut => Err(NodeError::timeout(format!(
            "Subprocess sandbox timed out after {}s (process tree killed). \
             Increase the timeout env var or optimise the code.",
            budget.as_secs_f64()
        ))),
        WaitOutcome::Cancelled => Err(NodeError::transient_with_code(
            "python sidecar: execution cancelled (process tree killed)",
            crate::error::RUNTIME_ERROR_CODE,
        )),
        WaitOutcome::Failed(e) => Err(NodeError::deterministic(format!("python sidecar: {e}"))),
        WaitOutcome::Done(status, stdout, stderr) => {
            let stdout_text = String::from_utf8_lossy(&stdout).to_string();
            let stderr_text = String::from_utf8_lossy(&stderr).to_string();
            let exit_ok = status.success();
            if !exit_ok && stdout_text.trim().is_empty() {
                return Err(NodeError::deterministic(format!(
                    "Subprocess exited with code {}. Stderr: {}",
                    status.code().unwrap_or(-1),
                    clip_stderr(&stderr_text)
                )));
            }
            // non-zero exit WITH stdout output: fall through to envelope
            // decoding, mirroring run_python_subprocess
            decode_python_runner_output(&stdout_text, &stderr_text)
        }
    }
}

enum WaitOutcome {
    Done(std::process::ExitStatus, Vec<u8>, Vec<u8>),
    TimedOut,
    /// 执行级取消：进程组已杀；exec.rs 的取消优先臂把它改判为
    /// FlowError::Cancelled（code=-4），不经任何节点策略链。
    Cancelled,
    Failed(String),
}

/// Waits with a wall clock; on timeout kills the child's whole process
/// group (`process_group(0)` made the sidecar its own group leader, so the
/// signal takes any grandchildren the user code spawned too). The stdin
/// writer thread (issue #15) is joined on every exit path — after the
/// group kill its write target is closed, so `write_all` returns (EPIPE)
/// and the join cannot outlive the budget.
fn wait_with_timeout(
    mut child: std::process::Child,
    budget: Duration,
    writer: Option<std::thread::JoinHandle<()>>,
    cancel_event: &std::sync::atomic::AtomicBool,
) -> WaitOutcome {
    let deadline = Instant::now() + budget;
    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                if let Some(mut out) = child.stdout.take() {
                    use std::io::Read;
                    let _ = out.read_to_end(&mut stdout);
                }
                if let Some(mut err) = child.stderr.take() {
                    use std::io::Read;
                    let _ = err.read_to_end(&mut stderr);
                }
                break WaitOutcome::Done(status, stdout, stderr);
            }
            Ok(None) => {
                // 执行级取消（plaita#267 波次③；Python `_popen_wait_cancellable`
                // 观察到 cancel_event 当场收尸同形）：置位即杀整个进程组。
                // 该错误会被 exec.rs 的取消优先臂拦下改判为具名取消终态，
                // 永远到不了 errorHandler/timeout 链。
                if cancel_event.load(std::sync::atomic::Ordering::SeqCst) {
                    kill_process_group(&mut child);
                    break WaitOutcome::Cancelled;
                }
                if Instant::now() >= deadline {
                    kill_process_group(&mut child);
                    break WaitOutcome::TimedOut;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                // try_wait 失败 = 子进程状态不可知；与超时同款收尸再退出，
                // 否则写线程（与子进程共享管道）可能把宿主一并拖住
                kill_process_group(&mut child);
                break WaitOutcome::Failed(e.to_string());
            }
        }
    };
    if let Some(writer) = writer {
        let _ = writer.join();
    }
    outcome
}

/// SIGKILLs the sidecar's process group, then reaps the direct child.
#[cfg(unix)]
fn kill_process_group(child: &mut std::process::Child) {
    // process_group(0) made the child a group leader, so -pid addresses the
    // whole tree
    let pgid = -(child.id() as i32);
    // SAFETY: kill(2) with a negative pid is the process-group signal form;
    // no invariants beyond a live pid are required.
    let killed = unsafe { libc::kill(pgid, libc::SIGKILL) };
    if killed != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
        // ESRCH means the tree already exited between try_wait and the kill;
        // any other failure still takes the direct child down
        let _ = child.kill();
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn kill_process_group(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Envelope `type` values whose `ok=false` failure is deterministic — the
/// same decision a retry re-runs identical user code on identical input, so
/// these can never succeed on attempt 2. Mirrors plaita
/// `runner.DETERMINISTIC_ERROR_TYPES` and plaita-go
/// `pyDeterministicEnvelopeTypes` (#43); everything else — out-of-set types
/// (`KeyError`), a missing/bare type, and infra-level failures (no output /
/// bad JSON) — keeps the retryable default (plaita's unknown-error posture).
fn py_deterministic_envelope_type(exc_type: &str) -> bool {
    matches!(
        exc_type,
        "ValueError" | "TypeError" | "NameError" | "SyntaxError"
    )
}

/// Parses the JSON envelope written by the runner — verbatim port of plaita
/// `_decode_runner_output`.
fn decode_python_runner_output(raw: &str, stderr: &str) -> Result<Value, NodeError> {
    if raw.trim().is_empty() {
        return Err(NodeError::transient(format!(
            "Sandbox produced no output. Stderr: {}",
            clip_stderr(stderr)
        )));
    }
    let envelope: Value = serde_json::from_str(raw).map_err(|_| {
        NodeError::transient(format!(
            "Sandbox output is not valid JSON: {:.200}. Stderr: {}",
            raw,
            clip_stderr(stderr)
        ))
    })?;
    let ok = envelope
        .get("ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !ok {
        let exc_type = envelope
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("Error");
        let exc_msg = envelope
            .get("error")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("(unknown)");
        let message = format!("{exc_type}: {exc_msg}");
        if py_deterministic_envelope_type(exc_type) {
            return Err(NodeError::deterministic(message));
        }
        return Err(NodeError::transient(message));
    }
    Ok(envelope.get("result").cloned().unwrap_or(Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_script_shape() {
        let script = build_python_runner_script("def run(input):\n    return input\n", 0);
        assert!(script.contains("def run(input):"));
        assert!(script.contains("_mem_bytes = 0"));
        assert!(!script.contains("__USER_CODE__"));
        assert!(!script.contains("__MEM_BYTES__"));
        // braces and %-formatting in user code pass through untouched
        let script =
            build_python_runner_script("X = '%s=%d'\ndef run(input):\n    return {'a': {1}}", 0);
        assert!(script.contains("{'a': {1}}"));
    }

    #[test]
    fn run_function_detection() {
        assert!(py_defines_run_function("def run(input):\n    return input"));
        assert!(py_defines_run_function("  def run(input): pass"));
        assert!(!py_defines_run_function("def runx(input): pass"));
        assert!(!py_defines_run_function("x = 'def run(input):'"));
    }

    #[test]
    fn envelope_decoding() {
        let v = decode_python_runner_output(r#"{"ok": true, "result": 5}"#, "").unwrap();
        assert_eq!(v, serde_json::json!(5));
        let e = decode_python_runner_output(
            r#"{"ok": false, "error": "boom", "type": "ValueError"}"#,
            "",
        )
        .unwrap_err();
        assert!(e.message.contains("ValueError: boom"));
        let e = decode_python_runner_output("", "(empty)").unwrap_err();
        assert!(e.message.contains("no output"));
        let e = decode_python_runner_output("not json", "x").unwrap_err();
        assert!(e.message.contains("not valid JSON"));
    }

    /// 信封重试分类 = py runner.is_retryable_error / go
    /// pyDeterministicEnvelopeTypes 四类白名单的 rs 镜像（rs#18 / A137-01）：
    /// 只有四类确定性类型不重试；集外类型（KeyError）、缺型（裸 `Error`）与
    /// 基础设施级失败（无输出/坏 JSON）维持可重试缺省。
    #[test]
    fn envelope_retry_classification_mirrors_go_whitelist() {
        for t in ["ValueError", "TypeError", "NameError", "SyntaxError"] {
            let e = decode_python_runner_output(
                &format!(r#"{{"ok": false, "error": "boom", "type": "{t}"}}"#),
                "",
            )
            .unwrap_err();
            assert!(!e.is_retryable(), "{t} must be deterministic");
        }
        // 集外确定性集：可重试缺省（A137-01 主红格）
        let e =
            decode_python_runner_output(r#"{"ok": false, "error": "'k'", "type": "KeyError"}"#, "")
                .unwrap_err();
        assert!(e.is_retryable(), "KeyError must keep the retryable default");
        // 缺型信封（runner 未捕获/无类型名）→ 裸 `Error`：可重试缺省
        let e = decode_python_runner_output(r#"{"ok": false, "error": "boom"}"#, "").unwrap_err();
        assert!(
            e.is_retryable(),
            "missing type must keep the retryable default"
        );
        // 基础设施级失败：sidecar OOM/被杀 → 无输出；输出截断 → 坏 JSON
        let e = decode_python_runner_output("", "(empty)").unwrap_err();
        assert!(
            e.is_retryable(),
            "no output must keep the retryable default"
        );
        let e = decode_python_runner_output("not json", "x").unwrap_err();
        assert!(e.is_retryable(), "bad JSON must keep the retryable default");
    }

    #[test]
    fn env_budget_fail_closed() {
        std::env::set_var(PY_SANDBOX_TIMEOUT_ENV, "0");
        assert!(positive_int_env_seconds(PY_SANDBOX_TIMEOUT_ENV, 10).is_err());
        std::env::set_var(PY_SANDBOX_TIMEOUT_ENV, "3.5");
        assert!(positive_int_env_seconds(PY_SANDBOX_TIMEOUT_ENV, 10).is_err());
        std::env::set_var(PY_SANDBOX_TIMEOUT_ENV, "3");
        assert_eq!(
            positive_int_env_seconds(PY_SANDBOX_TIMEOUT_ENV, 10).unwrap(),
            3
        );
        std::env::remove_var(PY_SANDBOX_TIMEOUT_ENV);
        assert_eq!(
            positive_int_env_seconds(PY_SANDBOX_TIMEOUT_ENV, 10).unwrap(),
            10
        );
        std::env::set_var(PY_SANDBOX_MEMORY_ENV, "-1");
        assert_eq!(sandbox_memory_mb().unwrap(), -1);
        std::env::remove_var(PY_SANDBOX_MEMORY_ENV);
    }

    /// stdin 写入必须在预算内（issue #15 负控）：大载荷（> 管道容量）+
    /// 子进程模块级阻塞（runner 模板里用户代码先于 stdin.read() 执行）时，
    /// 先前的阻塞 write_all 让节点/流/沙箱三个预算全部失效——永久挂死
    /// 且留孤儿进程组。修后 2s 节点预算兜底：具名 Timeout、无孤儿。
    #[test]
    fn stdin_write_is_inside_the_budget() {
        let marker = format!("plaita_rs_stdin_{}", std::process::id());
        let data = format!(
            r#"{{
        "flow_id": "p2-stdin-budget",
        "nodes": [
            {{"type": "start", "id": "s", "next": "c"}},
            {{"type": "code", "id": "c", "language": "python", "next": "e",
             "timeout": "PT2S",
             "code": "import time\ntime.sleep(60)  # {marker}\ndef run(input):\n    return input",
             "input": {{"blob": "$INPUT.blob"}}}},
            {{"type": "end", "id": "e", "resultType": "success", "output": "'ok'"}}
        ]}}"#
        );
        let flow = crate::ir::load(data.as_bytes()).unwrap();
        // 200KB 压过 64KB 管道容量，写必然阻塞在子进程 read 之前
        let big = "x".repeat(200_000);

        let (tx, rx) = std::sync::mpsc::channel();
        let t0 = std::time::Instant::now();
        std::thread::spawn(move || {
            let r = flow.execute(
                &serde_json::json!({ "blob": big }),
                &crate::exec::Host::new(),
            );
            let _ = tx.send((t0.elapsed(), r));
        });
        // 30s 只兜底「预算被绕过」的失败窗口；预算内应收在 ~2s
        let (elapsed, result) = rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap_or_else(|_| {
                pkill_marker(&marker);
                panic!(
                    "stdin write escaped every budget: the flow never returned \
                     (blocking write_all outside the deadline loop)"
                )
            });
        let err = result.expect_err("blocked-forever sidecar must fail, not succeed");
        assert!(
            err.to_string().contains("timed out"),
            "expected the named Timeout class, got: {err}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "budget must fire at ~2s, took {elapsed:?}"
        );

        // 无孤儿：sidecar 进程组内标记进程必须消失（SIGKILL 即时，2s 宽限）
        let gone = (0..40).all(|_| {
            let out = std::process::Command::new("ps")
                .args(["-eo", "command"])
                .output()
                .unwrap();
            let text = String::from_utf8_lossy(&out.stdout);
            !text.lines().any(|l| l.contains(&marker)) || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                false
            }
        });
        assert!(gone, "sidecar process group must not outlive the timeout");
    }

    fn pkill_marker(marker: &str) {
        let _ = std::process::Command::new("pkill")
            .args(["-9", "-f", marker])
            .output();
    }
}
