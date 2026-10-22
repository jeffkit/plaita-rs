//! ir 契约边界测试——对应 plaita-go `pkg/ir/validate_test.go` / `if_test.go` /
//! `wave_eng_test.go` / `code_python_test.go` 的装载断言面（2026-10-21 审计
//! 缺测试对账表补齐），外加本轮修复的回归锚。

use plaita_engine::ir;
use serde_json::{json, Value};

fn load(doc: Value) -> Result<plaita_engine::Flow, String> {
    ir::load(&serde_json::to_vec(&doc).unwrap())
}

fn err_of(doc: Value) -> String {
    load(doc).expect_err("expected load error")
}

fn flow_with_end(output: Value) -> Value {
    json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": output}
        ]
    })
}

// -- R5: $NODE 引用扫描 -------------------------------------------------------

#[test]
fn r5_unknown_node_ref_rejected_with_hint() {
    let err = err_of(json!({
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.ennd.x"}
        ]
    }));
    assert!(err.contains("unknown node id(s)"), "{err}");
    assert!(err.contains("did you mean"), "{err}");
}

#[test]
fn r5_parent_qualified_refs_and_boundaries() {
    // $PARENT.$NODE 前缀场景不误报；紧邻标识符字符/点/$ 的引用不扫
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success",
             "output": {"a": "$PARENT.$NODE.s", "b": "pre$NODE.s is text", "c": 1}}
        ]
    });
    assert!(load(doc).is_ok());
}

#[test]
fn r5_nested_containers_scanned_and_code_body_skipped() {
    // 嵌套容器里的 $NODE 引用照样扫
    let err = err_of(json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "language": "python", "next": "e",
             "code": "def run(input):\n    return '$NODE.also_text'",
             "input": {"deep": ["$NODE.missing_nested"]}},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    }));
    assert!(err.contains("missing_nested"), "{err}");
    // code 体不是 plaita 表达式，不扫（上面若扫 code 体早就报 also_text 了——
    // 深层 missing_nested 先炸，本条只锚 code 豁免见下一条独立用例）
}

#[test]
fn r5_code_body_itself_not_scanned() {
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "language": "python", "next": "e",
             "code": "def run(input):\n    return '$NODE.not_a_ref'",
             "input": {}},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    assert!(load(doc).is_ok());
}

#[test]
fn r5_end_error_key_exempt_but_nested_error_scanned() {
    // end 的 error 载荷是字面数据（节点级豁免，#144 split 的 error 键例外）
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "error",
             "error": {"code": "$NODE.s is just text", "message": "x"}}
        ]
    });
    assert!(load(doc).is_ok());
}

// -- R6: $F 注册表扫描 --------------------------------------------------------

#[test]
fn r6_unregistered_function_rejected_with_hint() {
    let err = err_of(flow_with_end(json!("$F.uppr('x')")));
    assert!(err.contains("unregistered function(s)"), "{err}");
    assert!(err.contains("did you mean"), "{err}");
}

#[test]
fn r6_lookbehind_and_whitespace_boundaries() {
    // 'v$F.nope(1)'：$ 前是标识符字符 → 不是根作用域引用，不拒（审计 F2）
    assert!(load(flow_with_end(json!("v$F.nope(1)"))).is_ok());
    // '$F.nope\\n(1)'：Go 的 \\s* 认换行 → 装载期拒（审计 F3）
    let err = err_of(flow_with_end(json!("$F.nope\n(1)")));
    assert!(err.contains("unregistered function(s)"), "{err}");
}

// -- R7: output 字面量 vs outputType ------------------------------------------

#[test]
fn r7_static_literal_mismatch_rejected_dynamic_skipped() {
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e",
             "outputType": {"dataType": "string"},
             "output": 42},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let err = err_of(doc);
    assert!(err.contains("does not match declared outputType"), "{err}");

    // 表达式输出（非静态字面量）跳过预检
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e",
             "outputType": {"dataType": "string"},
             "output": "$INPUT.x"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    assert!(load(doc).is_ok());
}

// -- warn_unknown_keys 落盘（审计 F6） ----------------------------------------

#[test]
fn unknown_fields_stash_into_metadata() {
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "e", "typo_field": 1},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let flow = load(doc).unwrap();
    let start = flow.find_node("s").unwrap();
    assert_eq!(
        start.metadata.get("unknownFields").and_then(|v| v.as_str()),
        Some("typo_field")
    );
}

// -- exposeEnv 装载面 ----------------------------------------------------------

#[test]
fn expose_env_load_surface() {
    // snake 折叠 + camel 胜出 + 顶层透传
    let doc = json!({
        "flow_id": "ee",
        "expose_env": ["A_SNAKE"],
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let flow = load(doc).unwrap();
    assert_eq!(
        flow.expose_env.as_ref().unwrap(),
        &vec!["A_SNAKE".to_string()]
    );

    // camel 与 snake 并存：camel 胜出
    let doc = json!({
        "flow_id": "ee",
        "exposeEnv": ["A_CAMEL"],
        "expose_env": ["A_SNAKE"],
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let flow = load(doc).unwrap();
    assert_eq!(
        flow.expose_env.as_ref().unwrap(),
        &vec!["A_CAMEL".to_string()]
    );

    // 声明空数组合法（fail-closed 的「什么都不暴露」）
    let doc = json!({
        "flow_id": "ee",
        "exposeEnv": [],
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    assert!(load(doc).unwrap().expose_env.as_ref().unwrap().is_empty());

    // 非字符串项拒绝
    let doc = json!({
        "flow_id": "ee",
        "exposeEnv": [1],
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    assert!(load(doc).is_err());

    // 嵌入流程上的 exposeEnv 也校验
    let doc = json!({
        "flow_id": "ee",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "child", "id": "c", "next": "e",
             "childFlow": {
                "exposeEnv": ["BAD*"],
                "nodes": [
                    {"type": "start", "id": "start", "next": "e2"},
                    {"type": "end", "id": "e2", "resultType": "success", "output": "x"}
                ]}},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let err = err_of(doc);
    assert!(
        err.contains("wildcards") || err.contains("invalid entries"),
        "{err}"
    );
}

// -- if 节点结构面 --------------------------------------------------------------

#[test]
fn if_else_next_camel_alias_folds() {
    let doc = json!({
        "flow_id": "if_camel",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "elseNext": "no",
             "condition": {"field": "$INPUT.v", "operator": "truthy", "value": null}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    });
    let flow = load(doc).unwrap();
    let got = flow
        .execute(&json!({"v": 1}), &plaita_engine::Host::new())
        .unwrap();
    assert_eq!(got, json!("y"));
}

#[test]
fn if_condition_groups_and_no_condition_takes_else() {
    // 条件组：and 关系嵌套 + truthy 算子
    let doc = json!({
        "flow_id": "if_group",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "else_next": "no",
             "condition": {"relation": "and", "conditions": [
                {"field": "$INPUT.v", "operator": "truthy"},
                {"relation": "or", "conditions": [
                    {"field": "$INPUT.n", "operator": "gt", "value": 10},
                    {"field": "$INPUT.n", "operator": "lt", "value": -10}
                ]}
             ]}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    });
    let flow = load(doc).unwrap();
    assert_eq!(
        flow.execute(&json!({"v": 1, "n": 20}), &plaita_engine::Host::new())
            .unwrap(),
        json!("y")
    );
    assert_eq!(
        flow.execute(&json!({"v": 1, "n": 0}), &plaita_engine::Host::new())
            .unwrap(),
        json!("n")
    );
    // condition 缺省/null → 恒走假分支（decide.py Bool 语义）
    let doc = json!({
        "flow_id": "if_null_cond",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "else_next": "no", "condition": null},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    });
    let flow = load(doc).unwrap();
    assert_eq!(
        flow.execute(&json!({"v": 1}), &plaita_engine::Host::new())
            .unwrap(),
        json!("n")
    );
}

#[test]
fn if_broken_structures_rejected() {
    // 缺真分支目标
    let err = err_of(json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "else_next": "no",
             "condition": {"field": "$INPUT.v", "operator": "truthy"}},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    }));
    assert!(err.contains("true-branch target"), "{err}");
    // 缺假分支目标
    let err = err_of(json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes",
             "condition": {"field": "$INPUT.v", "operator": "truthy", "value": null}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"}
        ]
    }));
    assert!(err.contains("false-branch target"), "{err}");
    // 残缺条件（partial condition 会恒走假分支 → fail-closed）
    let err = err_of(json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "else_next": "no",
             "condition": {"field": "$INPUT.v", "operator": "truthy"}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    }));
    // field+operator 无 value：单条件形状要求三键齐——value 缺失即残缺
    assert!(err.contains("malformed"), "{err}");
    // {relation} 无 conditions 拒绝
    let err = err_of(json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "else_next": "no",
             "condition": {"relation": "and"}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    }));
    assert!(err.contains("malformed"), "{err}");
}

// -- 超时预算执行（Go LoadTimeoutBudgets 对应物） ------------------------------

#[test]
fn zero_budget_fails_immediately_and_continue_yields_nil() {
    // 显式零预算 = 最严预算（plaita#138）：立即失败，不放行节点体
    let doc = json!({
        "flow_id": "zero_budget",
        "timeout": "PT0S",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let flow = load(doc).unwrap();
    let err = flow
        .execute(&json!({}), &plaita_engine::Host::new())
        .unwrap_err();
    assert!(err.to_string().contains("deadline"), "{err}");

    // 零预算 + flow 级 timeoutHandler continue：预算到期终态（issue #14）
    // ——流级 handler 不再软着陆（先前返回 Ok(null)，把预算违规洗成
    // 成功值；Python 权威侧预算到期恒 ExecutionDeadlineError）
    let doc = json!({
        "flow_id": "zero_budget_continue",
        "timeout": "PT0S",
        "timeoutHandler": {"strategy": "continue"},
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let flow = load(doc).unwrap();
    let err = flow
        .execute(&json!({}), &plaita_engine::Host::new())
        .unwrap_err();
    assert!(err.to_string().contains("deadline"), "{err}");
}

// -- handler code/message 折叠（Go NormalizeHandlerCodeMessage 对应物） ---------

#[test]
fn error_handler_code_message_folding_visible_on_abort() {
    let doc = json!({
        "flow_id": "handler_fold",
        "nodes": [
            {"type": "start", "id": "s", "next": "sw"},
            {"type": "switch", "id": "sw", "branches": [
                {"name": "yes", "condition": {"field": "$INPUT.v", "operator": "eq", "value": "1"}, "next": "dead"}
             ],
             "errorHandler": {"strategy": "abort", "error_code": 4001, "error_message": "配置错误"}},
            {"type": "end", "id": "dead", "resultType": "success", "output": "dead"}
        ]
    });
    let flow = load(doc).unwrap();
    let err = flow
        .execute(&json!({"v": "42"}), &plaita_engine::Host::new())
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("4001"), "{text}");
    assert!(text.contains("配置错误"), "{text}");
}

// -- Property required list 形态 ------------------------------------------------

#[test]
fn property_required_list_marks_named_children() {
    let doc = json!({
        "flow_id": "req_list",
        "inputType": {
            "dataType": "object",
            "required": ["k"],
            "properties": {"k": {"dataType": "string"}}
        },
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let flow = load(doc).unwrap();
    // 缺 k → isRequired 命中
    let err = flow.execute(&json!({}), &plaita_engine::Host::new());
    // inputType 校验不在根入口做（Go 同样只在 child/code 显式校验），本条锚
    // 的是装载后 Property 形态：required list 已折成 isRequired
    let prop = flow.input_type.as_ref().unwrap();
    assert!(prop.properties.as_ref().unwrap()["k"].is_required);
    let _ = err;
}

// -- code 节点装载变体 -----------------------------------------------------------

#[test]
fn code_node_load_variants() {
    // language 缺省 → python
    let doc = json!({
        "flow_id": "code_def",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "next": "e",
             "code": "def run(input):\n    return input"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}
        ]
    });
    let flow = load(doc).unwrap();
    match &flow.find_node("c").unwrap().kind {
        plaita_engine::NodeKind::Code {
            language,
            sandbox_backend,
            ..
        } => {
            assert_eq!(language, "python");
            assert_eq!(sandbox_backend, "");
        }
        other => panic!("expected code node, got {other:?}"),
    }

    for backend in ["docker", "unsafe", "magic"] {
        let doc = json!({
            "flow_id": "code_backend",
            "nodes": [
                {"type": "start", "id": "s", "next": "c"},
                {"type": "code", "id": "c", "language": "python", "next": "e",
                 "sandbox_backend": backend, "code": "def run(input):\n    return input"},
                {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}
            ]
        });
        let err = err_of(doc);
        assert!(
            err.contains("sandbox_backend") || err.contains("sandboxBackend"),
            "{err}"
        );
    }

    // 未知语言拒绝
    let doc = json!({
        "flow_id": "code_lang",
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "code", "id": "c", "language": "ruby", "next": "e", "code": "x"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}
        ]
    });
    let err = err_of(doc);
    assert!(err.contains("ruby"), "{err}");
}

// -- 本轮修复的回归锚 ------------------------------------------------------------

#[test]
fn end_result_type_non_string_rejected() {
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": 123, "output": "x"}
        ]
    });
    let err = err_of(doc);
    assert!(err.contains("invalid (success|error)"), "{err}");
}

#[test]
fn global_context_explicit_context_wins() {
    let doc = json!({
        "flow_id": "gc",
        "globalContext": {"g": 1},
        "context": {"global": {"g": 2}},
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$GLOBAL.g"}
        ]
    });
    let flow = load(doc).unwrap();
    // 显式 context.global 胜出（Go normalize 语义，审计 F4）
    assert_eq!(
        flow.execute(&json!({}), &plaita_engine::Host::new())
            .unwrap(),
        json!(2)
    );
}

#[test]
fn http_addressing_delegate_rejected() {
    let doc = json!({
        "flow_id": "addr",
        "nodes": [
            {"type": "start", "id": "s", "next": "h"},
            {"type": "http", "id": "h", "url": "http://x", "addressing": {"name": "polaris"}},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let err = err_of(doc);
    assert!(err.contains("addressing"), "{err}");
}

#[test]
fn retry_times_negative_rejected() {
    let doc = json!({
        "flow_id": "neg_retry",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x",
             "errorHandler": {"retryTimes": -1}}
        ]
    });
    let err = err_of(doc);
    assert!(err.contains("retryTimes"), "{err}");
}

#[test]
fn end_error_code_null_renders_nil() {
    let doc = json!({
        "flow_id": "nil_code",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "error", "error": {"code": null, "message": "m"}}
        ]
    });
    let flow = load(doc).unwrap();
    match &flow.find_node("e").unwrap().kind {
        plaita_engine::NodeKind::End { error, .. } => {
            assert_eq!(error.as_ref().unwrap().code, "<nil>");
        }
        other => panic!("expected end node, got {other:?}"),
    }
}

// -- 装载预算 cap（#8：plaita#267/#35 对齐，Go/Python 均拒载，本仓先前放行）--

fn budget_flow(handler: Value, node: Value) -> Value {
    // null = 不携带该键（errorHandler: null 本就被装载拒绝，fixture 别混入）
    let mut d = json!({
        "flowId": "caps1",
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e"},
            {"type": "end", "id": "e"}
        ]
    });
    if !handler.is_null() {
        d["errorHandler"] = handler;
    }
    if !node.is_null() {
        d["nodes"][1]["timeout"] = node;
    }
    d
}

#[test]
fn budget_node_retry_times_cap() {
    let f = |rt: Value| {
        let mut d = budget_flow(json!(null), json!(null));
        d["nodes"][1]["errorHandler"] = json!({"strategy": "continue", "retryTimes": rt});
        d
    };
    assert!(load(f(json!(10))).is_ok(), "cap boundary is inclusive");
    let err = err_of(f(json!(11)));
    assert!(
        err.contains(r#"node "a": errorHandler.retryTimes 11 exceeds the cap 10"#),
        "{err}"
    );
    // 1e9 的审计形状（retryTimes multiplies node attempts）
    let err = err_of(f(json!(1000000000)));
    assert!(
        err.contains("errorHandler.retryTimes 1000000000 exceeds the cap 10"),
        "{err}"
    );
}

#[test]
fn budget_flow_retry_times_cap() {
    let f = |rt: Value| {
        budget_flow(
            json!({"strategy": "continue", "retryTimes": rt}),
            json!(null),
        )
    };
    assert!(load(f(json!(10))).is_ok());
    let err = err_of(f(json!(11)));
    assert!(
        err.contains(r#"ir: flow "caps1": flow: errorHandler.retryTimes 11 exceeds the cap 10"#),
        "{err}"
    );
}

#[test]
fn budget_node_timeout_cap() {
    let f = |t: Value| budget_flow(json!(null), t);
    assert!(
        load(f(json!("PT86400S"))).is_ok(),
        "24h boundary is inclusive"
    );
    assert!(
        load(f(json!("PT0S"))).is_ok(),
        "explicit zero stays a set budget"
    );
    let err = err_of(f(json!("PT90000S")));
    assert!(
        err.contains(r#"node "a": timeout PT90000S exceeds the 24h cap"#),
        "{err}"
    );
    // 毫秒拼写走同一拒绝面（90000000 ms 归一化后即 25h）
    let err = err_of(f(json!(90000000u64)));
    assert!(err.contains("PT90000S exceeds the 24h cap"), "{err}");
}

#[test]
fn budget_flow_timeout_deliberately_uncapped() {
    // flow 级 timeout 不设上限是 Go/Python/本仓三家的共同决策（运营预算；
    // 运行期由 worker deadline 取严——worker 层不在本仓）
    assert!(load(budget_flow(json!(null), json!(null))).is_ok());
    let f = |t: &str| {
        json!({
            "flowId": "caps1",
            "timeout": t,
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e"}
            ]
        })
    };
    assert!(load(f("PT90000S")).is_ok());
    assert!(load(f("PT1000000S")).is_ok());
}

#[test]
fn budget_caps_cover_embedded_flows() {
    // normalize_embedded 递归进 childFlow —— 嵌入流内节点同受 cap
    let doc = json!({
        "nodes": [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "child", "id": "c", "next": "e", "childFlow": {
                "nodes": [
                    {"type": "start", "id": "s", "next": "a"},
                    {"type": "assignment", "id": "a", "next": "e",
                     "errorHandler": {"strategy": "continue", "retryTimes": 11}},
                    {"type": "end", "id": "e"}
                ]
            }},
            {"type": "end", "id": "e"}
        ]
    });
    let err = err_of(doc);
    assert!(err.contains("retryTimes 11 exceeds the cap 10"), "{err}");
}

#[test]
fn timeout_beyond_instant_range_rejected_at_load() {
    // r1 移交：Duration 可表示但超平台 Instant 值域（本机 Instant 是
    // timespec——tv_sec: i64 秒，值域窗口在 i64::MAX 秒 9.22e18 与
    // Duration::MAX 1.845e19 之间；ns-u64 平台窗口更窄）→ 裸加是进程级
    // panic，fail-closed 收在装载期。测试值 9.9e18 s 落在两个平台的
    // 溢出区间内。
    // 节点级：超 24h cap 先拦（巨值同为具名拒载，fail-closed 同效；
    // 24h 远小于任何平台的 Instant 值域，cap 内构不成此场景）
    let err = err_of(budget_flow(json!(null), json!("PT1844674407370955161S")));
    assert!(err.contains("exceeds the 24h cap"), "{err}");
    // flow 级不设 cap——超 Instant 值域预算由 normalize 的具名闸拒载；
    // ISO 与毫秒两个拼写同一出口
    let f = |t: Value| {
        json!({
            "flowId": "caps1",
            "timeout": t,
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e"}
            ]
        })
    };
    let err = err_of(f(json!("PT9900000000000000000S")));
    assert!(err.contains("exceeds the platform instant range"), "{err}");
    // 毫秒拼写：1e22 ms = 1e19 s，同样落溢出区间
    let err = err_of(f(json!(1e22)));
    assert!(err.contains("exceeds the platform instant range"), "{err}");
}

// -- typed 字段闸（#9：present 但类型不符 = 装载期具名拒，禁静默降级）------------
//
// pre-fix：joinBranches/branches/upstreamOutput 降级成空集合（parallel 变全后台
// 结果丢失/分支全消失），next/else_next 降级成 None/""（R1/R3 缺边全绿假象）——
// 校验与运行链路全绿。Go typed unmarshal / Python pydantic 都在装载期拒。

fn parallel_join_doc(join_branches: Value, branches: Value) -> Value {
    let mut d = json!({
        "flowId": "joinstr",
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            {"type": "parallel", "id": "p", "next": "e", "branches": [
                {"name": "b1", "flow": {"nodes": [
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": {"v": 1}}]}}]},
            {"type": "end", "id": "e", "resultType": "success",
             "output": {"parallel_result": "$NODE.p"}}
        ]
    });
    if !join_branches.is_null() {
        d["nodes"][1]["joinBranches"] = join_branches;
    }
    if !branches.is_null() {
        d["nodes"][1]["branches"] = branches;
    }
    d
}

#[test]
fn typed_field_wrong_type_rejected_at_load() {
    // joinBranches 漏方括号（issue 实测形状）：静默降级成 [] = 全后台、结果丢失
    let err = err_of(parallel_join_doc(json!("b1"), json!(null)));
    assert!(
        err.contains("joinBranches") && err.contains("must be an array"),
        "{err}"
    );

    // joinBranches 元素非字符串（Go []string 同拒；pre-fix 被 filter_map 丢掉）
    let err = err_of(parallel_join_doc(json!(["b1", 42]), json!(null)));
    assert!(
        err.contains("joinBranches[1]") && err.contains("must be"),
        "{err}"
    );

    // parallel.branches 非数组 → 分支全部消失、flow 照样 success 的静默面
    let err = err_of(parallel_join_doc(json!(null), json!("b1")));
    assert!(
        err.contains("branches") && err.contains("must be an array"),
        "{err}"
    );

    // switch.branches 非数组，同型
    let doc = json!({
        "flowId": "swstr",
        "nodes": [
            {"type": "start", "id": "s", "next": "sw"},
            {"type": "switch", "id": "sw", "next": "e", "branches": "b1"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let err = err_of(doc);
    assert!(
        err.contains("branches") && err.contains("must be an array"),
        "{err}"
    );

    // assignment.upstreamOutput 非数组 → 上游声明被静默忽略
    let doc = json!({
        "flowId": "upstr",
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e", "upstreamOutput": "x"},
            {"type": "end", "id": "e", "resultType": "success", "output": "x"}
        ]
    });
    let err = err_of(doc);
    assert!(
        err.contains("upstreamOutput") && err.contains("must be an array"),
        "{err}"
    );

    // next 非字符串：if 节点形状（else_next 保底让 R1 看不见缺边）——要求在
    // 装载/校验期就红，而不是 run 期 code 10000
    let doc = json!({
        "flowId": "nextnum",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": 42, "else_next": "no",
             "condition": {"field": "$INPUT.v", "operator": "truthy", "value": null}},
            {"type": "end", "id": "no", "resultType": "success", "output": "n"}
        ]
    });
    let err = err_of(doc);
    assert!(
        err.contains("\"next\"") && err.contains("must be a string"),
        "{err}"
    );

    // else_next 非字符串：不再被 get_str 折成 "" 误报「缺假分支目标」
    let doc = json!({
        "flowId": "elsenum",
        "nodes": [
            {"type": "start", "id": "s", "next": "q"},
            {"type": "if", "id": "q", "next": "yes", "else_next": 7,
             "condition": {"field": "$INPUT.v", "operator": "truthy", "value": null}},
            {"type": "end", "id": "yes", "resultType": "success", "output": "y"}
        ]
    });
    let err = err_of(doc);
    assert!(
        err.contains("else_next") && err.contains("must be a string"),
        "{err}"
    );
}

#[test]
fn typed_field_positive_controls_unchanged() {
    // joinBranches:["b1"]：装载 + join 语义生效（分支结果进 $NODE.p）
    let flow = load(parallel_join_doc(json!(["b1"]), json!(null))).unwrap();
    assert_eq!(
        flow.execute(&json!({}), &plaita_engine::Host::new())
            .unwrap(),
        json!({"parallel_result": {"b1": {"v": 1}}})
    );

    // 缺省 joinBranches：仍合法装载，全后台语义不变（节点立即返回 {}）
    let flow = load(parallel_join_doc(json!(null), json!(null))).unwrap();
    assert_eq!(
        flow.execute(&json!({}), &plaita_engine::Host::new())
            .unwrap(),
        json!({"parallel_result": {}})
    );

    // joinBranches:null 同缺省（Go null → 零值同语义）
    assert!(load(parallel_join_doc(json!(Value::Null), json!(null))).is_ok());

    // null / 空串 next 仍是合法「无后继」
    for next in [Value::Null, json!("")] {
        let doc = json!({
            "flowId": "nullnext",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "x", "next": next}
            ]
        });
        assert!(load(doc).is_ok());
    }

    // joinBranches:null 同缺省（Go null → 零值同语义）
    assert!(load(parallel_join_doc(json!(Value::Null), json!(null))).is_ok());
}

// -- ISO 时长严格度（#13：乱序/重复分量具名拒，预算不再被静默改写）--------------

#[test]
fn iso_timeout_bad_component_spelling_rejected_at_load() {
    // 五份 issue 实测文档：pre-fix 全部 {"ok":true}（Go/Python 侧装载即拒）
    for t in ["PT1S1H", "PT1H1H", "P1D1D", "PT1M1H", "PT1S1S"] {
        let doc = json!({
            "flowId": "tmo",
            "nodes": [
                {"type": "start", "id": "s", "next": "a"},
                {"type": "assignment", "id": "a", "next": "e", "timeout": t},
                {"type": "end", "id": "e"}
            ]
        });
        let err = err_of(doc);
        assert!(
            err.contains("could not parse duration string"),
            "{t}: {err}"
        );
    }

    // 正控：合法跳分量拼写装载通过
    let doc = json!({
        "flowId": "tmo",
        "nodes": [
            {"type": "start", "id": "s", "next": "a"},
            {"type": "assignment", "id": "a", "next": "e", "timeout": "PT1H1S"},
            {"type": "end", "id": "e"}
        ]
    });
    assert!(load(doc).is_ok());
}
