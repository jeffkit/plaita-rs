//! http 响应帽可调（plaita#31 对齐，B4 r8）：`PLAITA_HTTP_MAX_RESPONSE_BYTES`
//! 旋钮走**子进程**验证——进程内改全局 env 会和并行测试互踩。权威语义 =
//! Python `node/http.py`（默认 10MiB、流式分块计数、超限抛具名
//! ResponseTooLargeError → code 1003、非法值 WARNING + 回默认）。
//!
//! 负控 RED（加旋钮前）：env=1024 时 2KB 响应应被具名拒绝，但 ureq
//! `into_string` 的固定 10MiB 帽管不着它——flow 照常成功。

use std::io::{Read, Write};
use std::process::{Command, Stdio};

const CLI: &str = env!("CARGO_BIN_EXE_plaita-rs");
const BODY: &str = "x"; // 下面按字节数凑 2KB

/// 起一个回 2KB 响应体的一次性本地 HTTP 服务，返回其地址。
fn serve_2kb() -> String {
    let body = BODY.repeat(2048);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            // 请求可能分多段到达：读到头完结（\r\n\r\n）才应答——
            // 带未读数据关闭会让 macOS 回 RST 掐断客户端读响应
            let mut acc = Vec::new();
            let mut chunk = [0u8; 1024];
            while !acc.ends_with(b"\r\n\r\n") {
                match sock.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => acc.extend_from_slice(&chunk[..n]),
                }
            }
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes());
            let _ = sock.flush();
            // 排空读对端关闭，确保 FIN 而非 RST
            let _ = sock.read(&mut chunk);
        }
    });
    format!("http://{addr}/x")
}

fn run_flow(tag: &str, url: &str, env_val: Option<&str>) -> (bool, String, String) {
    // 三测并行：flow 文件按测试名分开，否则互相覆盖 URL 撞进对方的一次性 server
    let flow = std::env::temp_dir().join(format!("plaita-rs-http-cap-{tag}.json"));
    std::fs::write(
        &flow,
        format!(
            r#"{{
        "flow_id": "http-cap",
        "nodes": [
            {{"type": "start", "id": "s", "next": "h"}},
            {{"type": "http", "id": "h", "url": "{url}", "next": "e"}},
            {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.h.status"}}
        ]}}"#
        ),
    )
    .unwrap();
    let mut cmd = Command::new(CLI);
    cmd.arg("run")
        .arg(&flow)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(v) = env_val {
        cmd.env("PLAITA_HTTP_MAX_RESPONSE_BYTES", v);
    }
    let out = cmd.output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// env=1024、响应 2KB → 超限具名错误（Python: NodeException code=1003,
/// "HTTP response body exceeds maxResponseBytes=1024; raise
/// PLAITA_HTTP_MAX_RESPONSE_BYTES to allow larger bodies"）。
#[test]
fn http_max_response_env_caps_the_body() {
    let url = serve_2kb();
    let (ok, stdout, stderr) = run_flow("cap1024", &url, Some("1024"));
    assert!(
        !ok,
        "a 2KB body under env=1024 must fail; stdout: {stdout} stderr: {stderr}"
    );
    let err = format!("{stdout}{stderr}");
    assert!(
        err.contains("maxResponseBytes=1024"),
        "expected the named over-cap message, got: {err}"
    );
    assert!(
        err.contains("PLAITA_HTTP_MAX_RESPONSE_BYTES"),
        "the error must point at the knob, got: {err}"
    );
}

/// 不设 env：默认帽 10MiB 罩得住 2KB → 照常成功（HTTP 200 落下游）。
#[test]
fn default_cap_passes_a_small_body() {
    let url = serve_2kb();
    let (ok, stdout, stderr) = run_flow("default", &url, None);
    assert!(
        ok,
        "2KB under the default 10MiB cap must pass; stderr: {stderr}"
    );
    assert!(
        stdout.contains("200"),
        "status must flow downstream: {stdout}"
    );
}

/// 非法 env（Python optional_int_env：WARNING + 回默认，不静默、不拒载）：
/// 快流照常完成，stderr 必须有告警。
#[test]
fn invalid_cap_env_warns_and_falls_back() {
    let url = serve_2kb();
    let (ok, stdout, stderr) = run_flow("invalid", &url, Some("abc"));
    assert!(
        ok,
        "invalid env falls back to the default; stderr: {stderr}"
    );
    assert!(
        stderr.contains("PLAITA_HTTP_MAX_RESPONSE_BYTES"),
        "the fallback must be observable (WARNING naming the knob), got: {stderr}"
    );
    assert!(
        stdout.contains("200"),
        "status must flow downstream: {stdout}"
    );
}
