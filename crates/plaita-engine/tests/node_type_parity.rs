//! Python↔Rust 节点清单差分门（防漂移；plaita-go#80 的 node_parity_test.go
//! 同形）。权威数据源 = plaita 的 NodeRegistry 现场提取（core 内建 +
//! `plaita.nodes` entry points），经 scripts/extract_python_node_types.py
//! 落成 JSON。`PLAITA_NODE_TYPES_JSON` 未设时 skip——普通 `cargo test` 没有
//! plaita checkout 也能跑；CI 里由 make node-parity / ci.yml 提供。
//!
//! 契约：`ir::SUPPORTED_NODE_TYPES` 与 Python 清单的双向差集必须恰等于下方
//! 显式例外表——Python 单侧新增（或本引擎补齐类型后例外未摘）即红。

use std::collections::BTreeSet;

use plaita_engine::ir::{capability_manifest, SUPPORTED_NODE_TYPES};

/// Python 有、本引擎不实现的类型——「不能补的明确列为例外」清单。补齐任一
/// 类型后必须摘条，摘条动作本身由过期红守着。
/// 挂起族 7（event/hitl_await/delay/approval/redis_queue/kafka_queue/
/// http_callback）已随 suspend/resume 架构转正（B4 r12），摘条；
/// 外部宿主族（21）需要 agentproc/凭据仓/队列/服务商等部署侧基础设施
/// （sql_query 的驱动选型前置 = plaita-go#84 同源评估）。
const PYTHON_ONLY_EXCEPTIONS: &[&str] = &[
    // 外部宿主族
    "agentrun",
    "api_request",
    "capture",
    "decision",
    "dingtalk_webhook",
    "email_send",
    "feishu_webhook",
    "gate",
    "generic_webhook",
    "git_publish",
    "github_comment",
    "hitl",
    "llm",
    "notify",
    "parse_json",
    "rate_limit",
    "report",
    "slack_webhook",
    "sql_query",
    "wecom_webhook",
    "writefile",
];

/// 本引擎有、Python 默认 registry 不列出的类型。code 是 Python 刻意 opt-in
/// 注册（execute 任意用户代码，无沙箱不默认注册）——模块在默认 registry 外
/// 是 Python 形态差异，不是缺陷。
const RUST_ONLY_EXCEPTIONS: &[&str] = &["code"];

#[test]
fn python_node_type_parity() {
    let Ok(path) = std::env::var("PLAITA_NODE_TYPES_JSON") else {
        eprintln!(
            "skip: PLAITA_NODE_TYPES_JSON is not set (run `make node-parity` with the \
             sibling plaita + plaita-nodes checkouts to exercise this gate)"
        );
        return;
    };
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let doc: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    let empty = Vec::new();
    let python: BTreeSet<String> = doc["node_types"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert!(
        !python.is_empty(),
        "{path}: empty python node-type list — a shrunk authority list is a false green"
    );

    let rust: BTreeSet<&str> = SUPPORTED_NODE_TYPES.iter().copied().collect();

    let mut python_only: Vec<&str> = python
        .iter()
        .map(String::as_str)
        .filter(|t| !rust.contains(t))
        .collect();
    python_only.sort_unstable();
    let mut rust_only: Vec<&str> = rust
        .iter()
        .copied()
        .filter(|t| !python.contains(*t))
        .collect();
    rust_only.sort_unstable();

    let mut expected_python_only: Vec<&str> = PYTHON_ONLY_EXCEPTIONS.to_vec();
    expected_python_only.sort_unstable();
    let mut expected_rust_only: Vec<&str> = RUST_ONLY_EXCEPTIONS.to_vec();
    expected_rust_only.sort_unstable();

    assert_eq!(
        python_only, expected_python_only,
        "python-only node types drifted: a new python node type must either be ported or \
         added to PYTHON_ONLY_EXCEPTIONS; a stale entry (the type is now ported) must be \
         removed"
    );
    assert_eq!(
        rust_only, expected_rust_only,
        "rust-only node types drifted: an engine-only type must be listed in \
         RUST_ONLY_EXCEPTIONS (python opt-in registrations) or removed"
    );
    // 例外条目必须都在 KNOWN_UNSUPPORTED 里有具名 hint（例外 ≠ 静默放过；
    // 加载期 fail-closed 文案逐类可查）。
    let hinted: BTreeSet<&str> = plaita_engine::ir::KNOWN_UNSUPPORTED
        .iter()
        .map(|(t, _)| *t)
        .collect();
    for t in PYTHON_ONLY_EXCEPTIONS {
        assert!(
            hinted.contains(t),
            "exception {t:?} has no entry in KNOWN_UNSUPPORTED — add a targeted hint there"
        );
    }
}

/// manifest 内部一致性（无外部 checkout 也跑）：supported/unsupported 互斥、
/// supported 与白名单常量一致、contract_version 在位。
#[test]
fn capability_manifest_internal_consistency() {
    let m = capability_manifest();
    let supported: Vec<&str> = m["supported_node_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    let unsupported: Vec<&str> = m["unsupported_node_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(supported.len(), SUPPORTED_NODE_TYPES.len());
    for t in &supported {
        assert!(
            !unsupported.contains(t),
            "node type {t:?} is both supported and unsupported"
        );
    }
    assert_eq!(m["engine"], "plaita-rs");
    assert!(m["contract_version"].is_string());
    // supported 必有白名单、unsupported 必有 hint（白名单即 manifest 的源）。
    for t in SUPPORTED_NODE_TYPES {
        assert!(supported.contains(t));
    }
}
