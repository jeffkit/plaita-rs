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
            let result = run_python_subprocess(code, arguments, deadline).map_err(|e| {
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

    if let Some(stdin) = child.stdin.as_mut() {
        let _ = stdin.write_all(&input_json);
    }
    child.stdin.take(); // close stdin so the runner's read() returns

    let timeout_result = wait_with_timeout(child, budget);
    match timeout_result {
        WaitOutcome::TimedOut => Err(NodeError::timeout(format!(
            "Subprocess sandbox timed out after {}s (process tree killed). \
             Increase the timeout env var or optimise the code.",
            budget.as_secs_f64()
        ))),
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
    Failed(String),
}

/// Waits with a wall clock; on timeout kills the child's whole process
/// group (`process_group(0)` made the sidecar its own group leader, so the
/// signal takes any grandchildren the user code spawned too).
fn wait_with_timeout(mut child: std::process::Child, budget: Duration) -> WaitOutcome {
    let deadline = Instant::now() + budget;
    loop {
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
                return WaitOutcome::Done(status, stdout, stderr);
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    kill_process_group(&mut child);
                    return WaitOutcome::TimedOut;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return WaitOutcome::Failed(e.to_string()),
        }
    }
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

/// Parses the JSON envelope written by the runner — verbatim port of plaita
/// `_decode_runner_output`.
fn decode_python_runner_output(raw: &str, stderr: &str) -> Result<Value, NodeError> {
    if raw.trim().is_empty() {
        return Err(NodeError::deterministic(format!(
            "Sandbox produced no output. Stderr: {}",
            clip_stderr(stderr)
        )));
    }
    let envelope: Value = serde_json::from_str(raw).map_err(|_| {
        NodeError::deterministic(format!(
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
        return Err(NodeError::deterministic(format!("{exc_type}: {exc_msg}")));
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
}
