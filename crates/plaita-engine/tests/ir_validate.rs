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

    // 零预算 + flow 级 timeoutHandler continue → nil 结果、不报错
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
    let got = flow
        .execute(&json!({}), &plaita_engine::Host::new())
        .unwrap();
    assert_eq!(got, json!(null));
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
