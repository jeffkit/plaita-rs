//! End-to-end tests over the REAL plaita artifacts vendored from
//! plaita-go's `pkg/ir/testdata/`: `assignment_flow.json` is a console
//! export; `loop_flow` / `parallel_flow` / `child_flow` are compiled by
//! plaita's codeflow compiler from @flow sources. The expected outputs are
//! the same the Go engine's `ir_test.go` asserts — a cross-runtime
//! acceptance check.

use plaita_engine::{ir, Host};
use serde_json::{json, Value};

fn load(name: &str) -> plaita_engine::Flow {
    let data = std::fs::read(format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR")))
        .unwrap_or_else(|e| panic!("read {name}: {e}"));
    ir::load(&data).unwrap_or_else(|e| panic!("load {name}: {e}"))
}

fn run(flow: &plaita_engine::Flow, input: Value) -> Result<Value, plaita_engine::FlowError> {
    flow.execute(&input, &Host::new())
}

#[test]
fn hello_world() {
    let flow = load("hello_world.json");
    assert_eq!(flow.id, "hello_world_go");
    let got = run(&flow, json!({})).unwrap();
    assert_eq!(got, json!("Hello, world!"));
}

#[test]
fn assignment_fixture() {
    let flow = load("assignment_flow.json");

    // condition branch: $INPUT.bb eq "123" (priority 1) beats default
    let got = run(&flow, json!({"bb": "123"})).unwrap();
    assert_eq!(got, json!({"dd": "456"}));

    // default branch passes the input through
    let got = run(&flow, json!({"bb": "zzz"})).unwrap();
    assert_eq!(got, json!({"dd": "zzz"}));
}

#[test]
fn loop_fixture() {
    let flow = load("loop_flow.json");
    // plaita semantics: loop output is the last child result
    let got = run(&flow, json!({"items": [1, 2, 3]})).unwrap();
    assert_eq!(got, json!(6));
}

#[test]
fn parallel_fixture() {
    let flow = load("parallel_flow.json");
    let got = run(&flow, json!({"x": 5})).unwrap();
    assert_eq!(got, json!({"a": 10, "b": 15}));
}

#[test]
fn child_fixture() {
    let flow = load("child_flow.json");
    let got = run(&flow, json!({"name": "plaita"})).unwrap();
    assert_eq!(got, json!("hello plaita"));
}

// -- fail-closed load rejections (mirrors ir_test.go TestLoadRejects) -------

#[test]
fn load_rejects() {
    let cases: Vec<(&str, &str, &str)> = vec![
        (
            "unsupported known type",
            r#"{"nodes":[{"type":"start","id":"s","next":"d"},{"type":"mock","id":"d"}]}"#,
            "mock",
        ),
        (
            "unknown type",
            r#"{"nodes":[{"type":"start","id":"s","next":"x"},{"type":"wat","id":"x"}]}"#,
            "not supported",
        ),
        (
            "missing start",
            r#"{"nodes":[{"type":"end","id":"e"}]}"#,
            "exactly one start",
        ),
        (
            "dangling next",
            r#"{"nodes":[{"type":"start","id":"s","next":"gone"},{"type":"end","id":"e"}]}"#,
            "missing node",
        ),
        (
            "unreachable node",
            r#"{"nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e"},{"type":"end","id":"orphan"}]}"#,
            "not reachable",
        ),
        (
            "python code node with restricted backend",
            r#"{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"code","id":"c","language":"python","sandbox_backend":"restricted","code":"def run(input):\n    return input"}]}"#,
            "restricted",
        ),
        (
            "nop end",
            r#"{"nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e","resultType":"nop"}]}"#,
            "nop",
        ),
        (
            "http ssrf guard",
            r#"{"nodes":[{"type":"start","id":"s","next":"h"},{"type":"http","id":"h","url":"http://x","blockPrivateNetworks":true}]}"#,
            "blockPrivateNetworks",
        ),
        (
            "reference child",
            r#"{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"child","id":"c","flowId":"abc"}]}"#,
            "reference-style",
        ),
        (
            "unknown join branch",
            r#"{"nodes":[{"type":"start","id":"s","next":"p"},{"type":"parallel","id":"p","branches":[{"name":"a","flow":{"nodes":[{"type":"start","id":"start","next":"e"},{"type":"end","id":"e"}]}}],"joinBranches":["typo"]}]}"#,
            "unknown branch",
        ),
        (
            "embedded flow missing end",
            r#"{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"child","id":"c","childFlow":{"nodes":[{"type":"start","id":"start","next":"a"},{"type":"assignment","id":"a"}]}}]}"#,
            "never reaches an end",
        ),
        // plaita-rs-specific fail-closed surfaces
        (
            "javascript code",
            r#"{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"code","id":"c","language":"javascript","code":"function run(input){return input}"}]}"#,
            "javascript",
        ),
        (
            "agentrun not ported",
            r#"{"nodes":[{"type":"start","id":"s","next":"r"},{"type":"agentrun","id":"r","agent":"a"}]}"#,
            "agentrun",
        ),
    ];
    for (name, doc, want) in cases {
        let err = ir::load(doc.as_bytes()).expect_err(name);
        assert!(err.contains(want), "{name}: expected {want:?} in {err}");
    }
}

#[test]
fn switch_without_default_branch_loads() {
    let data = r#"{
        "nodes": [
            {"type": "start", "id": "s", "next": "sw"},
            {"type": "switch", "id": "sw", "branches": [
                {"name": "yes", "priority": 1, "condition": {"field": "$INPUT.v", "operator": "eq", "value": "1"}, "next": "e1"},
                {"name": "no", "priority": 0, "condition": {"field": "$INPUT.v", "operator": "eq", "value": "0"}, "next": "e0"}
            ]},
            {"type": "end", "id": "e1", "resultType": "success", "output": "one"},
            {"type": "end", "id": "e0", "resultType": "success", "output": "zero"}
        ]
    }"#;
    let flow = ir::load(data.as_bytes()).unwrap();

    let got = run(&flow, json!({"v": "1"})).unwrap();
    assert_eq!(got, json!("one"));

    let err = run(&flow, json!({"v": "42"})).unwrap_err();
    assert!(err.to_string().contains("no branch matched"), "{err}");
}

/// plaita condition operators: contains/notContains/truthy/falsy.
#[test]
fn plaita_condition_operators() {
    let flow_with = |op: &str, value: Value| {
        let mut cond = json!({"field": "$INPUT.v", "operator": op});
        if !value.is_null() {
            cond["value"] = value;
        }
        let data = json!({
            "nodes": [
                {"type": "start", "id": "s", "next": "sw"},
                {"type": "switch", "id": "sw", "branches": [
                    {"name": "hit", "priority": 1, "condition": cond, "next": "hit"},
                    {"name": "miss", "isDefault": true, "next": "miss"},
                ]},
                {"type": "end", "id": "hit", "resultType": "success", "output": "hit"},
                {"type": "end", "id": "miss", "resultType": "success", "output": "miss"},
            ]
        });
        ir::load(serde_json::to_vec(&data).unwrap().as_slice()).unwrap()
    };

    let run = |flow: &plaita_engine::Flow, input: Value| -> String {
        let got = flow.execute(&input, &Host::new()).unwrap();
        got.as_str().unwrap().to_string()
    };

    assert_eq!(
        run(&flow_with("contains", json!("ell")), json!({"v": "hello"})),
        "hit"
    );
    assert_eq!(
        run(&flow_with("contains", json!("xyz")), json!({"v": "hello"})),
        "miss"
    );
    assert_eq!(
        run(
            &flow_with("notContains", json!("xyz")),
            json!({"v": "hello"})
        ),
        "hit"
    );
    assert_eq!(
        run(&flow_with("contains", json!("k")), json!({"v": {"k": 1}})),
        "hit"
    );
    assert_eq!(
        run(&flow_with("truthy", Value::Null), json!({"v": "x"})),
        "hit"
    );
    assert_eq!(
        run(&flow_with("truthy", Value::Null), json!({"v": ""})),
        "miss"
    );
    assert_eq!(
        run(&flow_with("falsy", Value::Null), json!({"v": 0})),
        "hit"
    );
}

/// the go examples run too (addition flow with input validation). The two
/// documents are vendored from plaita-go `examples/` (same-contents copies,
/// do not hand-edit) so the suite has no sibling-checkout dependency.
#[test]
fn go_examples() {
    for name in ["go-example-add", "go-example-hello_world"] {
        let data = std::fs::read(format!(
            "{}/testdata/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap_or_else(|e| panic!("read {name}: {e}"));
        let flow = ir::load(&data).unwrap();
        let input = if name == "go-example-add" {
            json!({"a": 2, "b": 3})
        } else {
            json!({})
        };
        let got = flow.execute(&input, &Host::new()).unwrap();
        // $F.add(2, 3) stays an int under Python semantics (int×int → int);
        // the float outputType only validates, it does not coerce
        let want = if name == "go-example-add" {
            json!(5)
        } else {
            json!("Hello, world!")
        };
        assert_eq!(got, want, "{name}");
    }
}

/// aliases: flow_id wins over id; source_line lands in metadata.
#[test]
fn top_level_aliases_and_metadata() {
    let data = r#"{
        "id": "legacy",
        "flow_id": "canonical",
        "nodes": [
            {"type": "start", "id": "s", "next": "e", "source_line": 7},
            {"type": "end", "id": "e", "resultType": "success", "output": 1}
        ]
    }"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    assert_eq!(flow.id, "canonical");
    let start = flow.find_node("s").unwrap();
    assert_eq!(
        start.metadata.get("sourceLine").and_then(|v| v.as_str()),
        Some("7")
    );
}

// -- engine policies ---------------------------------------------------------

#[test]
fn error_policy_continue_records_dollar_error() {
    // a switch with no matching branch fails; the flow continues on the
    // next node with $ERROR populated and a nil node result
    let data = r#"{
        "flow_id": "err_policy",
        "nodes": [
            {"type": "start", "id": "s", "next": "sw"},
            {"type": "switch", "id": "sw", "next": "after", "errorHandler": {"strategy": "continue"}, "branches": [
                {"name": "yes", "condition": {"field": "$INPUT.v", "operator": "eq", "value": "1"}, "next": "dead_end"}
            ]},
            {"type": "end", "id": "after", "resultType": "success", "output": "$ERROR.sw.message"}
        ]}"#;
    let mut doc: Value = serde_json::from_str(data).unwrap();
    // "dead_end" must exist for the loader; add a reachable dead end
    doc["nodes"][2]["next"] = json!("terminus");
    let mut nodes = doc["nodes"].as_array().unwrap().clone();
    nodes[1]["branches"][0]["next"] = json!("terminus");
    // give the default (missing) branch path an end: switch falls to error,
    // continue policy moves to "after" via the node's linear next
    nodes.push(json!({"type": "end", "id": "terminus", "resultType": "success", "output": "dead"}));
    nodes[1]["next"] = json!("after");
    doc["nodes"] = Value::Array(nodes);

    let flow = ir::load(&serde_json::to_vec(&doc).unwrap()).unwrap();
    let got = flow.execute(&json!({"v": "42"}), &Host::new()).unwrap();
    assert!(
        got.as_str().unwrap().contains("no branch matched"),
        "expected the $ERROR message, got {got}"
    );
}

#[test]
fn error_policy_continue_with_supplies_default() {
    let data = r#"{
        "flow_id": "cw",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "language": "python", "next": "e",
             "code": "def run(input):\n    raise ValueError('boom')",
             "input": "$INPUT",
             "errorHandler": {"strategy": "continue-with", "defaultValue": "fallback"}},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let got = flow.execute(&json!({"k": 1}), &Host::new()).unwrap();
    assert_eq!(got, json!("fallback"));
}

#[test]
fn end_error_result_raises_flow_result() {
    let data = r#"{
        "flow_id": "fe",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "error",
             "error": {"code": "400", "message": "bad input"}}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let err = flow.execute(&json!({}), &Host::new()).unwrap_err();
    assert!(err.to_string().contains("FlowResultError"), "{err}");
    assert!(err.to_string().contains("bad input"), "{err}");
}

#[test]
fn deterministic_errors_never_retry() {
    // python sidecar raising counts the attempts via events
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let host = Host::new().listen(Arc::new(move |e: &plaita_engine::Event| {
        if let plaita_engine::Event::NodeStart { node_id, .. } = e {
            if node_id == "c" {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
    let data = r#"{
        "flow_id": "retry",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "language": "python", "next": "e",
             "code": "def run(input):\n    raise ValueError('nope')",
             "input": "$INPUT",
             "errorHandler": {"strategy": "continue-with", "defaultValue": "x", "retryTimes": 5}},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let got = flow.execute(&json!({}), &host).unwrap();
    assert_eq!(got, json!("x"));
    // deterministic: exactly ONE attempt despite retryTimes=5
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// 运行期产出的表达式形状字符串是数据（契约 2026-10-21）：code 节点返回的
/// `"$F.add(2,3)"` 由 `$NODE.c.expr` 原样读出，不再二次解析；要值就在 code
/// 里算掉。与 plaita-go 同语义（跨引擎差分用例见 tools/diffcheck.py）。
#[test]
fn runtime_expression_shaped_strings_stay_data() {
    let data = r#"{
        "flow_id": "runtime_data",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "language": "python", "next": "e",
             "code": "def run(input):\n    return {'expr': '$F.add(2,3)', 'text': 'see $INPUT.x?'}",
             "input": {}},
            {"type": "end", "id": "e", "resultType": "success",
             "output": {"expr": "$NODE.c.expr", "text": "$NODE.c.text"}}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let got = flow.execute(&json!({}), &Host::new()).unwrap();
    assert_eq!(
        got,
        json!({
            "expr": "$F.add(2,3)",
            "text": "see $INPUT.x?",
        })
    );
}

/// 子流自有 timeout 必须与父预算取 min（审计 [高]：先前「父 1s + 子 1h」
/// 会跑满子预算才在父层炸）。
#[test]
fn child_timeout_escapes_are_capped_by_parent_budget() {
    let data = r#"{
        "flow_id": "escape",
        "timeout": "PT1S",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "child", "id": "c", "next": "e",
             "childFlow": {
                "flow_id": "slow_child",
                "timeout": "PT1M",
                "nodes": [
                    {"type": "start", "id": "start", "next": "k"},
                    {"type": "code", "id": "k", "language": "python", "next": "e2",
                     "code": "import time\ndef run(input):\n    time.sleep(3)\n    return 1",
                     "input": {}},
                    {"type": "end", "id": "e2", "resultType": "success", "output": "$NODE.k"}
                ]}},
            {"type": "end", "id": "e", "resultType": "success", "output": "done"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let t0 = std::time::Instant::now();
    let err = flow.execute(&json!({}), &Host::new()).unwrap_err();
    let elapsed = t0.elapsed();
    assert!(err.to_string().contains("timed out"), "{err}");
    // 父预算 1s 生效：远小于子流声明的 1 分钟与代码睡眠 3s
    assert!(
        elapsed < std::time::Duration::from_millis(2500),
        "elapsed {elapsed:?}"
    );
}

/// code 节点的超时必须能被节点 timeoutHandler 接住（审计 [中]：先前
/// blanket deterministic 抹掉 Timeout 类，continue/continue-with 失效）。
#[test]
fn code_node_timeout_policy_is_reachable() {
    let data = r#"{
        "flow_id": "code_timeout",
        "nodes": [
            {"type": "start", "id": "s", "next": "k"},
            {"type": "code", "id": "k", "language": "python", "next": "e",
             "timeout": "PT1S",
             "timeoutHandler": {"strategy": "continue-with", "defaultValue": "soft-landing"},
             "code": "import time\ndef run(input):\n    time.sleep(5)\n    return 1",
             "input": {}},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.k"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let got = flow.execute(&json!({}), &Host::new()).unwrap();
    assert_eq!(got, json!("soft-landing"));
}

/// 指数退避：1s·2^n cap 30s（审计 [高]：先前 1^attempt 恒 1s）。
/// retryTimes=2 的瞬态错误 → 3 次尝试，墙钟 ≈ 1+2 = 3s。
#[test]
fn exponential_backoff_between_retries() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let host = Host::new().listen(Arc::new(move |e: &plaita_engine::Event| {
        if let plaita_engine::Event::NodeStart { node_id, .. } = e {
            if node_id == "a" {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
    // $NOPE 是缺根 KeyError → 引擎错误码 10000 → 瞬态（可重试）
    let data = r#"{
        "flow_id": "backoff",
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e",
             "output": "$F.add($NOPE, 1)",
             "errorHandler": {"strategy": "continue-with", "defaultValue": "gave-up", "retryTimes": 2}},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.a"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let t0 = std::time::Instant::now();
    let got = flow.execute(&json!({}), &host).unwrap();
    let elapsed = t0.elapsed().as_secs_f64();
    assert_eq!(got, json!("gave-up"));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "1 + retryTimes attempts"
    );
    // 退避 1s + 2s = 3s（若退避失效恒 1s 则只有 ~2s）
    assert!(
        elapsed >= 2.8,
        "expected exponential backoff >= 2.8s, got {elapsed}"
    );
    assert!(elapsed < 8.0, "elapsed {elapsed}");
}

#[test]
fn if_node_routes_and_records_branch() {
    let data = r#"{
        "flow_id": "if_flow",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "else_next": "no",
             "condition": {"field": "$INPUT.v", "operator": "gt", "value": 10}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "$BRANCH"},
            {"type": "end", "id": "no", "resultType": "success", "output": "$BRANCH"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    // $BRANCH records the branch TARGET (plaita-go if.go: the target IS the
    // branch value — "yes"/"no", not "true"/"false")
    assert_eq!(
        flow.execute(&json!({"v": 11}), &Host::new()).unwrap(),
        json!("yes")
    );
    assert_eq!(
        flow.execute(&json!({"v": 5}), &Host::new()).unwrap(),
        json!("no")
    );
}

#[test]
fn template_interpolation_flows_through_nodes() {
    let data = r#"{
        "flow_id": "tpl",
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e",
             "output": "hi {%$INPUT.name%}, n={%$F.add($INPUT.n, 1)%}"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.a"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    let got = flow
        .execute(&json!({"name": "rs", "n": 2}), &Host::new())
        .unwrap();
    assert_eq!(got, json!("hi rs, n=3"));
}

#[test]
fn loop_condition_breaks_early() {
    let data = r#"{
        "flow_id": "loopc",
        "nodes": [
            {"type": "start", "id": "s", "next": "l"},
            {"type": "loop", "id": "l", "next": "e",
             "collection": "[1, 2, 3, 4, 5]",
             "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 3},
             "childFlow": {
                "nodes": [
                    {"type": "start", "id": "start", "next": "e"},
                    {"type": "end", "id": "e", "resultType": "success", "output": "$F.mul($INPUT.item, 10)"}
                ]}},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.l"}
        ]}"#;
    let flow = ir::load(data.as_bytes()).unwrap();
    // iteration 0..3 run; after index 3 the condition fails → last result 40
    let got = flow.execute(&json!({}), &Host::new()).unwrap();
    assert_eq!(got, json!(40));
}
