//! 程序化构造 Flow 的运行期防御面：绕过装载闸（normalize/validate）的构造
//! 在超平台 Instant 值域的预算上必须得具名错误，而不是 `Instant::now() + t`
//! 裸加的进程级 panic（r1 移交点 exec.rs:119）。

use plaita_engine::{Flow, Host};
use serde_json::json;

#[test]
fn over_range_flow_timeout_is_named_error_not_panic() {
    // PT9900000000000000000S（9.9e18 s）可被 Duration 表示（de_timeout
    // 放行），但超平台 Instant 值域——execute 必须具名拒绝
    let flow: Flow = serde_json::from_value(json!({
        "id": "huge-timeout",
        "timeout": "PT9900000000000000000S",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e"}
        ]
    }))
    .expect("programmatic flow builds");
    let host = Host::new();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        flow.execute(&json!({}), &host)
    }));
    let r = r.expect("execute must not panic on an over-range flow timeout");
    let err = r.expect_err("over-range budget must fail the flow");
    assert!(
        err.to_string()
            .contains("exceeds the platform instant range"),
        "{err}"
    );
}
