//! 执行级取消面（plaita#267 波次③对齐，B4 r8）：Python 权威契约 =
//! `FlowExecution.cancel()` 幂等置位粘滞 `cancel_requested`（节点边界拒绝，
//! 先于重试/errorHandler，不被 continue 吞）+ 节点级 `cancel_event`（code
//! 沙箱等待循环观察到即 killpg 整树）；退避按 0.5s 分片轮询可打断；父子/
//! 分支共享同一对信号（对链上任一执行 cancel 即传播整链）。
//!
//! 负控 RED（补齐前）：Host 无 cancel API——本文件编译不过
//! （"no method named `cancel`"），即取消面在 Rust 侧整体缺位的红证。

use std::sync::Arc;
use std::time::{Duration, Instant};

use plaita_engine::exec::Host;
use serde_json::json;

fn code_node(id: &str, code: &str, next: &str) -> String {
    format!(
        r#"{{"type": "code", "id": "{id}", "language": "python", "next": "{next}",
         "timeout": "PT20S", "code": {code:?}, "input": {{}}}}"#,
    )
}

fn flow_doc(nodes: &[String]) -> String {
    format!(
        r#"{{"flow_id": "cancel-probe", "timeout": "PT15S", "nodes": [
        {{"type": "start", "id": "s", "next": "n0"}},
        {},
        {{"type": "end", "id": "e", "resultType": "success", "output": "done"}}]}}"#,
        nodes.join(",\n"),
    )
}

/// `delay` 后对（克隆共享的）Host 置位 cancel——Python `cancel()` 的
/// Rust 对应调用面。
fn cancel_after(host: &Host, delay: Duration) {
    let host = host.clone();
    std::thread::spawn(move || {
        std::thread::sleep(delay);
        host.cancel();
    });
}

/// 等到 ps 里不再出现 marker（取消杀掉沙箱进程组的实证），给 2s 轮询宽限。
fn process_gone(marker: &str) -> bool {
    (0..40).all(|_| {
        let out = std::process::Command::new("ps")
            .args(["-eo", "command"])
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        if !text.lines().any(|l| l.contains(marker)) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
        false
    })
}

/// 沙箱在途时 cancel：等待循环观察到 cancel_event 当场 killpg 整树，
/// flow 以具名取消终态结束（不是等 30s sleep 跑完）。
#[test]
fn cancel_interrupts_sandbox_wait_and_kills_the_tree() {
    let marker = format!("plaita_rs_cancel_{}", std::process::id());
    let code = format!("import time\ndef run(input):\n    time.sleep(30)  # {marker}");
    let data = flow_doc(&[code_node("n0", &code, "e")]);
    let flow = plaita_engine::ir::load(data.as_bytes()).unwrap();
    let host = Host::new();
    cancel_after(&host, Duration::from_millis(300));

    let t0 = Instant::now();
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "cancel must interrupt the in-flight sandbox wait, took {elapsed:?}"
    );
    let text = err.to_string();
    assert!(
        text.contains("Execution cancelled"),
        "named cancel terminal, got: {text}"
    );
    // code=-4 取消专属错误码（Python FlowCancelledException）
    assert!(
        text.contains("-4"),
        "cancel carries the -4 code, got: {text}"
    );
    // 沙箱进程组已被收尸
    assert!(
        process_gone(&marker),
        "sandbox process tree must be killed on cancel"
    );
}

/// 步界取消（默认语义）：在途**非沙箱**节点跑完（code 节点本身是沙箱、
/// 等待循环可被打断——那由上一测覆盖），下一个节点入口被具名拒绝
/// （"Execution cancelled before running node"，先于重试/errorHandler）。
#[test]
fn cancel_stops_at_the_next_node_boundary() {
    // 慢 http 服务：1s 后才应答——cancel 在应答前（300ms）置位
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        if let Ok((mut sock, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf);
            std::thread::sleep(Duration::from_secs(1));
            let _ = sock.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\nok",
            );
        }
    });

    let data = format!(
        r#"{{
        "flow_id": "cancel-boundary", "timeout": "PT15S",
        "nodes": [
            {{"type": "start", "id": "s", "next": "h"}},
            {{"type": "http", "id": "h", "url": "http://{addr}/x", "next": "n1"}},
            {},
            {{"type": "end", "id": "e", "resultType": "success", "output": "done"}}
        ]}}"#,
        code_node(
            "n1",
            "import time\ndef run(input):\n    time.sleep(30)",
            "e"
        ),
    );
    let flow = plaita_engine::ir::load(data.as_bytes()).unwrap();
    let host = Host::new();
    // 在 http 在途期间（300ms < 1s 应答）置位
    cancel_after(&host, Duration::from_millis(300));

    let t0 = Instant::now();
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let elapsed = t0.elapsed();

    // http 跑完（~1s）后立即停在 n1 入口，而不是跑 n1 的 30s
    assert!(
        elapsed < Duration::from_secs(10),
        "must stop at the boundary instead of running n1's 30s, took {elapsed:?}"
    );
    let text = err.to_string();
    assert!(
        text.contains("Execution cancelled before running node"),
        "boundary rejection dialect, got: {text}"
    );
}

/// 取消优先于任何节点错误/重试分类：瞬态失败的节点在退避中被取消时，
/// error_policy=continue 不得把取消洗成成功值（Python #267：取消检查
/// 先于重试与 errorHandler 分发）。
#[test]
fn cancel_is_not_swallowed_by_continue_policy() {
    // 连接拒绝 = 瞬态 1002 → 进 1s/2s 退避重试；errorHandler continue
    let data = r#"{
        "flow_id": "cancel-vs-policy", "timeout": "PT15S",
        "nodes": [
            {"type": "start", "id": "s", "next": "h"},
            {"type": "http", "id": "h", "url": "http://127.0.0.1:9/x", "next": "e",
             "errorHandler": {"strategy": "continue", "retryTimes": 3}},
            {"type": "end", "id": "e", "resultType": "success", "output": "done"}
        ]}"#;
    let flow = plaita_engine::ir::load(data.as_bytes()).unwrap();
    let host = Host::new();
    cancel_after(&host, Duration::from_millis(300));

    let t0 = Instant::now();
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "cancel must cut the retry backoff, took {elapsed:?}"
    );
    let text = err.to_string();
    assert!(
        text.contains("Execution cancelled"),
        "cancel must win over the continue policy, got: {text}"
    );
}

/// 取消传播整条执行链：parallel 分支共享同一 Host，对根执行 cancel
/// （任一执行对象）后各分支在节点边界停止——非沙箱在途节点跑完，
/// 后续节点具名拒绝。
#[test]
fn cancel_propagates_into_parallel_branches() {
    let branch = |name: &str| {
        format!(
            r#"{{"name": "{name}", "flow": {{"nodes": [
                {{"type": "start", "id": "s", "next": "c"}},
                {},
                {{"type": "end", "id": "e", "resultType": "success", "output": "b"}}
            ]}}, "next": "e"}}"#,
            // 各分支是独立子 flow 文档，节点 id 不跨分支冲突——都用 "c"
            code_node("c", "import time\ndef run(input):\n    time.sleep(1)", "e"),
        )
    };
    let data = format!(
        r#"{{
        "flow_id": "cancel-parallel", "timeout": "PT15S",
        "nodes": [
            {{"type": "start", "id": "s", "next": "p"}},
            {{"type": "parallel", "id": "p", "next": "e",
              "joinBranches": ["b1", "b2"],
              "branches": [{}, {}]}},
            {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.p"}}
        ]}}"#,
        branch("b1"),
        branch("b2"),
    );
    let flow = plaita_engine::ir::load(data.as_bytes()).unwrap();
    let host = Arc::new(Host::new());
    cancel_after(&host, Duration::from_millis(300));

    let t0 = Instant::now();
    let err = flow.execute(&json!({}), &host).unwrap_err();
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "branches must stop at boundaries, took {elapsed:?}"
    );
    let text = err.to_string();
    assert!(text.contains("Execution cancelled"), "got: {text}");
}
