//! `ir::load` — the deployment-JSON contract between the plaita Python
//! frontend (@flow compile / console publish) and this engine (port of
//! plaita-go `pkg/ir`).
//!
//! Pipeline: normalize → validate → engine deserialization. The loader is a
//! strict accept-list by design (fail-closed): node types this engine has
//! not ported are rejected at load with an explicit message instead of being
//! silently skipped. The authoritative contract is plaita's codeflow
//! compiler output (top-level `flow_id`/`inputType`/`nodes`/`runtime`,
//! camelCase with tolerated snake residues).
//!
//! Scope notes versus plaita-go (documented divergences, both fail-closed):
//! - `javascript` code nodes are rejected (no in-process JS interpreter);
//! - agentrun + the plaita-nodes node families (gate/notify/…) are rejected
//!   with per-type hints until ported.

mod flow_hash;
mod normalize;
mod scope;
mod support;
mod validate;

pub use flow_hash::{
    canonical_definition_json, flow_definition_hash, verify_definition_hash, FLOW_HASH_ALGO,
    HASH_ALGO_FIELD, HASH_FIELD,
};

pub use normalize::{KNOWN_UNSUPPORTED, SUPPORTED_NODE_TYPES};
pub use scope::Scope;

use serde_json::Value;

use crate::duration;
use crate::model::{Collection, EndError, Flow, Node, NodeKind, ParallelBranchDef, UpstreamValue};
use crate::property::Property;

/// The reference-node flow source: resolves a `reference` node's
/// `flowId`/`flowVersion` to a deployment-form flow document at load time —
/// the same role plaita's scheduler plays before flow.run (plaita-go#80
/// phase 2 parity).
pub trait FlowProvider: Send + Sync {
    /// Returns the deployment-form flow JSON for the given id/version.
    /// A resolution failure (unknown id, storage error) fails the load
    /// (fail-closed).
    fn load_flow_json(&self, flow_id: &str, flow_version: Option<&str>) -> Result<Value, String>;
}

/// Parses plaita deployment-form flow JSON and builds the engine flow.
/// Structural problems (missing start, dangling refs, unreachable nodes,
/// unsupported node types) are reported as errors before execution.
pub fn load(data: &[u8]) -> Result<Flow, String> {
    let mut doc: Value =
        serde_json::from_slice(data).map_err(|e| format!("ir: parse flow json: {e}"))?;
    // 内嵌指纹门（plaita#60 语义，B4 r11）：定义带 `definitionHash` 时装载
    // 前校验——不匹配具名拒（`flow_hash_mismatch`，终态可判不静默）、无该
    // 键跳过（向后兼容旧定义）；校验通过后保留键被剥离，normalize 看到的
    // 是干净定义。必须在 normalize 之前：normalize 会改写文档，而指纹的
    // 计算域是存储原始定义。
    flow_hash::verify_definition_hash(data, &mut doc)?;
    normalize::normalize_doc(&mut doc, Scope::Root)?;
    let flow: Flow = serde_json::from_value(doc).map_err(|e| format!("ir: build flow: {e}"))?;
    flow.validate()
        .map_err(|e| format!("ir: build flow: {e}"))?;
    Ok(flow)
}

/// Capability contract version — bump whenever the supported/unsupported
/// node-type lists change, so consumers caching the manifest don't read a
/// stale list under an unchanged version (plaita-go#80 discipline).
pub const CONTRACT_VERSION: &str = "2026-10-23.2";

/// 挂起族节点的部署侧依赖（plaita-go#80 phase 3 的 node_dependencies 键）：
/// 节点引擎面（pending 载荷/决策载体/订阅契约）已实现，但这些设施在本引擎
/// 进程外——worker/控制面按同一协议接。
pub const NODE_DEPENDENCIES: &[(&str, &str)] = &[
    (
        "event",
        "external event publisher + subscription store (engine registers via the host SubscriptionRegistry)",
    ),
    (
        "hitl_await",
        "hitl-server (message send) + reply poller publishing hitl_reply events",
    ),
    (
        "delay",
        "delay service consuming {prefix}:delay:queue (Redis ZSET scheduling)",
    ),
    (
        "approval",
        "approval service consuming {prefix}:approval:queue (console interaction surface)",
    ),
    (
        "redis_queue",
        "redis queue consumer service publishing redis_message events",
    ),
    (
        "kafka_queue",
        "kafka consumer service publishing kafka_message events",
    ),
    (
        "http_callback",
        "callback ingress forwarding requests to the handle_callback_request family",
    ),
];

/// Capability manifest（plaita#300 五键形态，plaita-go#80 parity）：console/
/// 上层 worker 可据此做 publish/start 能力门。`features` 恒空（checkpoint/
/// hitl/suspend 的宿主侧设施不在引擎面）；`node_dependencies` 逐节点标注
/// 挂起族的部署侧依赖（服务层/订阅存储）。
pub fn capability_manifest() -> Value {
    let mut supported: Vec<&str> = SUPPORTED_NODE_TYPES.to_vec();
    supported.sort_unstable();
    let mut unsupported: Vec<String> = KNOWN_UNSUPPORTED
        .iter()
        .map(|(t, _)| t.to_string())
        .collect();
    unsupported.sort();
    let mut dependencies = serde_json::Map::new();
    for (node, dep) in NODE_DEPENDENCIES {
        dependencies.insert((*node).to_string(), serde_json::json!(dep));
    }
    serde_json::json!({
        "engine": "plaita-rs",
        "contract_version": CONTRACT_VERSION,
        "supported_node_types": supported,
        "unsupported_node_types": unsupported,
        "features": [],
        "node_dependencies": dependencies,
    })
}

/// [`ir::load`] with a [`FlowProvider`] wired in: `reference` nodes resolve
/// their `flowId` through the provider at load time and the resolved
/// sub-flow enters the same normalize + validate pipeline as an inline
/// childFlow (nested references recurse; cycles fail the load with a named
/// error).
pub fn load_with_flow_provider(data: &[u8], provider: &dyn FlowProvider) -> Result<Flow, String> {
    let mut doc: Value =
        serde_json::from_slice(data).map_err(|e| format!("ir: parse flow json: {e}"))?;
    // 同 `load`：装载前内嵌指纹门（normalize 之前，指纹域 = 存储原始定义）。
    flow_hash::verify_definition_hash(data, &mut doc)?;
    let resolver = normalize::FlowResolver { provider };
    normalize::normalize_doc_with(&mut doc, Scope::Root, Some(&resolver), 0)?;
    let flow: Flow = serde_json::from_value(doc).map_err(|e| format!("ir: build flow: {e}"))?;
    flow.validate()
        .map_err(|e| format!("ir: build flow: {e}"))?;
    Ok(flow)
}

impl Flow {
    /// Post-deserialization sanity: non-empty id, known runtime, a start and
    /// an end node (the structural rules already ran in validate_doc; these
    /// catch directly-constructed flows too).
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("workflow id is required".to_string());
        }
        if !matches!(
            self.runtime.as_str(),
            "" | "go" | "gengine" | "typescript" | "python" | "rust"
        ) {
            return Err(format!(
                "runtime must be one of [go gengine typescript python], got {:?}",
                self.runtime
            ));
        }
        if self.nodes.is_empty() {
            return Err("nodes is required".to_string());
        }
        if self.find_start().is_none() {
            return Err("start node not found".to_string());
        }
        if self.find_end().is_none() {
            return Err("end node not found".to_string());
        }
        Ok(())
    }
}

/// Typed-field gate (#9): a present-but-wrong-type field is a load error with
/// the same strictness as Go's typed unmarshal / Python's pydantic models.
/// The old `as_*().unwrap_or_default()` accessors silently degraded to empty
/// values — an empty `joinBranches` flips parallel into fire-and-forget
/// (results dropped), empty `branches` drops every branch — and none of it
/// was visible to the structural rules. `null` and absent stay legal (Go
/// unmarshal treats JSON null as "leave the zero value").
fn want_string_field(
    node_id: &str,
    field: &str,
    v: Option<&Value>,
) -> Result<Option<String>, String> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(format!(
            "node {node_id:?}: field {field:?} must be a string, got {}",
            support::type_name(other)
        )),
    }
}

fn want_array_field<'a>(
    node_id: &str,
    field: &str,
    v: Option<&'a Value>,
) -> Result<Option<&'a Vec<Value>>, String> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(a)) => Ok(Some(a)),
        Some(other) => Err(format!(
            "node {node_id:?}: field {field:?} must be an array, got {}",
            support::type_name(other)
        )),
    }
}

/// Builds a `Node` from a normalized node document (the type-tagged
/// deserialization `serde` cannot express with mixed spellings).
pub(crate) fn node_from_value(v: &Value) -> Result<Node, String> {
    let Some(obj) = v.as_object() else {
        return Err("node entry is not an object".to_string());
    };
    let get = |k: &str| obj.get(k);
    let get_str = |k: &str| get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let node_type = get_str("type");
    if node_type.is_empty() {
        return Err("missing type".to_string());
    }
    let node_id = get_str("id");

    let base = BaseFields {
        id: node_id.clone(),
        node_type: node_type.clone(),
        name: get_str("name"),
        desc: get_str("desc"),
        output: get("output").cloned().filter(|v| !v.is_null()),
        timeout: match get("timeout") {
            Some(Value::String(s)) if !s.is_empty() => {
                Some(duration::parse_iso8601(s).map_err(|e| format!("node {node_id:?}: {e}"))?)
            }
            _ => None,
        },
        error_policy: match get("errorHandler") {
            Some(h) => serde_json::from_value(h.clone()).map_err(|e| e.to_string())?,
            None => Default::default(),
        },
        timeout_policy: match get("timeoutHandler") {
            Some(h) => serde_json::from_value(h.clone()).map_err(|e| e.to_string())?,
            None => Default::default(),
        },
        next: want_string_field(&node_id, "next", get("next"))?.filter(|s| !s.is_empty()),
        output_type: match get("outputType") {
            Some(p) if p.is_object() && !p.as_object().unwrap().is_empty() => {
                Some(serde_json::from_value::<Property>(p.clone()).map_err(|e| e.to_string())?)
            }
            _ => None,
        },
        metadata: get("metadata")
            .and_then(|m| m.as_object().cloned())
            .unwrap_or_default(),
    };

    let kind = match node_type.as_str() {
        "start" => NodeKind::Start,
        "end" => NodeKind::End {
            result_type: get_str("resultType"),
            error: match get("error") {
                Some(e) if e.is_object() => {
                    Some(serde_json::from_value::<EndError>(e.clone()).map_err(|e| e.to_string())?)
                }
                _ => None,
            },
        },
        "assignment" => NodeKind::Assignment {
            upstream_output: match want_array_field(&node_id, "upstreamOutput", get("upstreamOutput"))? {
                Some(u) => u
                    .iter()
                    .map(|x| {
                        serde_json::from_value::<UpstreamValue>(x.clone())
                            .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            },
        },
        "switch" => NodeKind::Switch {
            branches: match want_array_field(&node_id, "branches", get("branches"))? {
                Some(a) => a
                    .iter()
                    .map(|x| {
                        serde_json::from_value::<BranchDefShim>(x.clone())
                            .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(|s| s.inner)
                    .collect(),
                None => Vec::new(),
            },
        },
        "if" => NodeKind::If {
            condition: get("condition").cloned().filter(|c| !c.is_null()),
            else_next: want_string_field(&node_id, "else_next", get("else_next"))?
                .unwrap_or_default(),
        },
        "loop" => NodeKind::Loop {
            collection: collection_of(obj),
            condition: get("condition").cloned().filter(|c| !c.is_null()),
            item_type: item_type_of(obj)?,
            child_flow: child_flow_of(obj)?,
        },
        "map" => NodeKind::Map {
            collection: collection_of(obj),
            item_type: item_type_of(obj)?,
            concurrent: obj
                .get("concurrent")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            max_concurrent: match obj.get("maxConcurrent") {
                Some(Value::Number(n)) => n.as_i64(),
                _ => None,
            },
            child_flow: child_flow_of(obj)?,
        },
        "filter" => NodeKind::Filter {
            collection: collection_of(obj),
            item_type: item_type_of(obj)?,
            child_flow: child_flow_of(obj)?,
        },
        "find" => NodeKind::Find {
            collection: collection_of(obj),
            item_type: item_type_of(obj)?,
            child_flow: child_flow_of(obj)?,
        },
        "reduce" => NodeKind::Reduce {
            collection: collection_of(obj),
            item_type: item_type_of(obj)?,
            initial: get("initial").cloned().filter(|v| !v.is_null()),
            child_flow: child_flow_of(obj)?,
        },
        "while" => NodeKind::While {
            condition: get("condition").cloned().filter(|c| !c.is_null()),
            max_iterations: match get("max_iterations") {
                Some(Value::Number(n)) => n.as_i64(),
                _ => None,
            },
            child_flow: child_flow_of(obj)?,
        },
        "mock" => NodeKind::Mock {
            value: get("value").cloned().filter(|v| !v.is_null()),
        },
        "reference" => NodeKind::Reference {
            child_flow: child_flow_of(obj)?,
            input: get("input").cloned().filter(|v| !v.is_null()),
        },
        "event" => NodeKind::Event {
            core: extended_core_of(obj)?,
        },
        "hitl_await" => NodeKind::HitlAwait {
            message: get("message").cloned().filter(|v| !v.is_null()),
            images: get("images").cloned().filter(|v| !v.is_null()),
            base_url: get("base_url").cloned().filter(|v| !v.is_null()),
            timeout_secs: get("timeout_secs").cloned().filter(|v| !v.is_null()),
            event_type: get_str("event_type"),
            event_filter: get("event_filter").cloned().filter(|v| !v.is_null()),
            dry_run: get("dry_run").and_then(|v| v.as_bool()).unwrap_or(false),
        },
        "delay" => NodeKind::Delay {
            core: extended_core_of(obj)?,
            delay_seconds: get("delay_seconds").cloned().unwrap_or(Value::Null),
            delay_unit: get_str("delay_unit"),
        },
        "approval" => NodeKind::Approval {
            approval_title: get("approval_title").cloned().unwrap_or(Value::Null),
            approval_content: get("approval_content").cloned().unwrap_or(Value::Null),
            approval_type: get_str("approval_type"),
            approvers: get("approvers")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            approval_strategy: get_str("approval_strategy"),
            auto_escalation: get("auto_escalation").and_then(|v| v.as_bool()).unwrap_or(false),
            escalation_timeout_hours: get("escalation_timeout_hours").and_then(|v| v.as_i64()),
            escalation_approvers: get("escalation_approvers")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            form_fields: get("form_fields")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            allow_comments: get("allow_comments").and_then(|v| v.as_bool()),
            require_comments: get("require_comments").and_then(|v| v.as_bool()).unwrap_or(false),
            notification_config: get("notification_config").cloned().filter(|v| !v.is_null()),
            core: extended_core_of(obj)?,
        },
        "redis_queue" => NodeKind::RedisQueue {
            core: extended_core_of(obj)?,
            redis_host: get("redis_host").cloned().filter(|v| !v.is_null()),
            redis_port: get("redis_port").and_then(|v| v.as_i64()),
            redis_db: get("redis_db").and_then(|v| v.as_i64()),
            redis_password: get("redis_password").cloned().filter(|v| !v.is_null()),
            queue_name: get("queue_name").cloned().unwrap_or(Value::Null),
            queue_type: get_str("queue_type"),
            timeout_seconds: get("timeout_seconds").and_then(|v| v.as_i64()),
            batch_size: get("batch_size").and_then(|v| v.as_i64()),
            message_format: get_str("message_format"),
        },
        "kafka_queue" => NodeKind::KafkaQueue {
            core: extended_core_of(obj)?,
            bootstrap_servers: get("bootstrap_servers").cloned().unwrap_or(Value::Null),
            security_protocol: get_str("security_protocol"),
            sasl_mechanism: get("sasl_mechanism").cloned().filter(|v| !v.is_null()),
            sasl_username: get("sasl_username").cloned().filter(|v| !v.is_null()),
            sasl_password: get("sasl_password").cloned().filter(|v| !v.is_null()),
            topic: get("topic").cloned().unwrap_or(Value::Null),
            partition: get("partition").and_then(|v| v.as_i64()),
            group_id: get("group_id").cloned().unwrap_or(Value::Null),
            auto_offset_reset: get_str("auto_offset_reset"),
            enable_auto_commit: get("enable_auto_commit").and_then(|v| v.as_bool()),
            max_poll_records: get("max_poll_records").and_then(|v| v.as_i64()),
            message_format: get_str("message_format"),
            timeout_ms: get("timeout_ms").and_then(|v| v.as_i64()),
        },
        "http_callback" => NodeKind::HttpCallback {
            core: extended_core_of(obj)?,
            callback_path: get("callback_path").cloned().filter(|v| !v.is_null()),
            callback_method: get_str("callback_method"),
            callback_timeout_minutes: get("callback_timeout_minutes").and_then(|v| v.as_i64()),
            require_auth: get("require_auth").and_then(|v| v.as_bool()),
            auth_type: get_str("auth_type"),
            auth_token: get("auth_token").cloned().filter(|v| !v.is_null()),
            validate_headers: get("validate_headers").cloned().filter(|v| !v.is_null()),
            validate_params: get("validate_params").cloned().filter(|v| !v.is_null()),
            success_response: get("success_response").cloned().filter(|v| !v.is_null()),
            error_response: get("error_response").cloned().filter(|v| !v.is_null()),
        },
        "parallel" => NodeKind::Parallel {
            branches: match want_array_field(&node_id, "branches", get("branches"))? {
                Some(a) => a
                    .iter()
                    .map(|x| {
                        serde_json::from_value::<ParallelBranchDef>(x.clone())
                            .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            },
            is_conditional: get("isConditional")
                .and_then(|b| b.as_bool())
                .unwrap_or(false),
            mode: get_str("mode"),
            join_branches: match want_array_field(&node_id, "joinBranches", get("joinBranches"))? {
                Some(a) => a
                    .iter()
                    .enumerate()
                    .map(|(i, x)| match x {
                        Value::String(s) => Ok(s.clone()),
                        other => Err(format!(
                            "node {node_id:?}: field \"joinBranches[{i}]\" must be a string, got {}",
                            support::type_name(other)
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            },
        },
        "child" => NodeKind::Child {
            child_flow: child_flow_of(obj)?,
            input: get("input").cloned().filter(|v| !v.is_null()),
        },
        "code" => NodeKind::Code {
            language: get_str("language"),
            code: get_str("code"),
            input: get("input").cloned().filter(|v| !v.is_null()),
            sandbox_backend: get_str("sandboxBackend"),
        },
        "http" => NodeKind::Http {
            method: get_str("method"),
            content_type: get_str("contentType"),
            url: get_str("url"),
            query: get("query").cloned().filter(|v| !v.is_null()),
            headers: get("headers").cloned().filter(|v| !v.is_null()),
            body: get("body").cloned().filter(|v| !v.is_null()),
        },
        other => {
            return Err(format!(
                "node type {other:?} is not supported by this engine"
            ));
        }
    };

    Ok(Node {
        id: base.id,
        node_type: base.node_type,
        name: base.name,
        desc: base.desc,
        output: base.output,
        timeout: base.timeout,
        error_policy: base.error_policy,
        timeout_policy: base.timeout_policy,
        next: base.next,
        output_type: base.output_type,
        metadata: base.metadata,
        kind,
    })
}

#[derive(serde::Deserialize)]
struct BranchDefShim {
    #[serde(flatten)]
    inner: crate::model::BranchDef,
}

fn child_flow_of(obj: &serde_json::Map<String, Value>) -> Result<Box<Flow>, String> {
    match obj.get("childFlow") {
        Some(cf) if cf.is_object() => {
            let flow: Flow = serde_json::from_value(cf.clone()).map_err(|e| e.to_string())?;
            flow.validate().map_err(|e| e.to_string())?;
            Ok(Box::new(flow))
        }
        _ => Err("child node requires an inline childFlow document".to_string()),
    }
}

/// The polymorphic `collection` field (loop 家族共享): expression string,
/// literal array, or absent/null (empty collection — plaita evaluates
/// None → _coerce_collection → []).
fn collection_of(obj: &serde_json::Map<String, Value>) -> Collection {
    match obj.get("collection") {
        Some(Value::String(s)) => Collection::Expr(s.clone()),
        Some(Value::Array(a)) => Collection::Array(a.clone()),
        _ => Collection::Expr(String::new()),
    }
}

/// 挂起族共享的事件核心字段（装载期 translate 已按契约/缺省填充）。
fn extended_core_of(
    obj: &serde_json::Map<String, Value>,
) -> Result<crate::model::ExtendedCore, String> {
    Ok(crate::model::ExtendedCore {
        event_type: obj
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        event_filter: obj.get("event_filter").cloned().filter(|v| !v.is_null()),
        subscription_timeout: obj.get("subscription_timeout").and_then(|v| v.as_f64()),
    })
}

/// The optional `itemType` schema (loop 家族共享).
fn item_type_of(obj: &serde_json::Map<String, Value>) -> Result<Option<Property>, String> {
    match obj.get("itemType") {
        Some(p) if p.is_object() && !p.as_object().unwrap().is_empty() => Ok(Some(
            serde_json::from_value::<Property>(p.clone()).map_err(|e| e.to_string())?,
        )),
        _ => Ok(None),
    }
}

impl<'de> serde::Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer)?;
        node_from_value(&v).map_err(serde::de::Error::custom)
    }
}

struct BaseFields {
    id: String,
    node_type: String,
    name: String,
    desc: String,
    output: Option<Value>,
    timeout: Option<std::time::Duration>,
    error_policy: crate::model::ErrorPolicy,
    timeout_policy: crate::model::TimeoutPolicy,
    next: Option<String>,
    output_type: Option<Property>,
    metadata: serde_json::Map<String, Value>,
}
