//! The engine flow model — the shape the ir loader normalizes into
//! (camelCase engine fields, plaita-nodes snake fields for node configs).

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use crate::property::Property;

/// Error/timeout handling strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum HandleStrategy {
    #[serde(rename = "abort")]
    Abort,
    #[serde(rename = "continue")]
    Continue,
    #[serde(rename = "continue-with")]
    ContinueWith,
}

/// Recoverable error handling policy (`errorHandler`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ErrorPolicy {
    #[serde(default)]
    pub strategy: Option<HandleStrategy>,
    #[serde(default, rename = "defaultValue")]
    pub default_value: Option<Value>,
    #[serde(default, rename = "retryTimes", deserialize_with = "de_u64_lenient")]
    pub retry_times: u64,
    #[serde(default)]
    pub code: Option<i64>,
    #[serde(default)]
    pub message: String,
}

/// Timeout handling policy (`timeoutHandler`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TimeoutPolicy {
    #[serde(default)]
    pub strategy: Option<HandleStrategy>,
    #[serde(default, rename = "defaultValue")]
    pub default_value: Option<Value>,
    #[serde(default)]
    pub code: Option<i64>,
    #[serde(default)]
    pub message: String,
}

fn de_u64_lenient<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Go json.Unmarshal 进 uint 的语义：整数照收，负数/小数/非数字报装载错，
    // null 落零值（审计 F11——先前把非法值静默截为 0）
    let v = Value::deserialize(de)?;
    match v {
        Value::Null => Ok(0),
        Value::Number(n) => n.as_u64().ok_or_else(|| {
            serde::de::Error::custom(format!(
                "retryTimes must be a non-negative integer, got {n}"
            ))
        }),
        other => Err(serde::de::Error::custom(format!(
            "retryTimes must be a non-negative integer, got {other}"
        ))),
    }
}

/// A switch branch / parallel branch header (the `Branch` fields).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BranchDef {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub desc: String,
    #[serde(default)]
    pub condition: Option<Value>,
    #[serde(default)]
    pub priority: i64,
    #[serde(default, rename = "isDefault")]
    pub is_default: bool,
    #[serde(default)]
    pub next: String,
}

/// A parallel branch: branch header + its flow + optional input.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ParallelBranchDef {
    #[serde(flatten)]
    pub branch: BranchDef,
    #[serde(default)]
    pub flow: Option<Box<Flow>>,
    #[serde(default)]
    pub input: Option<Value>,
}

/// An upstream pick-up entry of an assignment node.
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamValue {
    #[serde(rename = "upstream")]
    pub upstream: String,
    #[serde(default)]
    pub value: Value,
}

/// End node's error payload.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct EndError {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

/// Loop collection: either a literal array or an expression string.
#[derive(Debug, Clone)]
pub enum Collection {
    Expr(String),
    Array(Vec<Value>),
}

/// The kind-specific payload of a node.
#[derive(Debug, Clone)]
pub enum NodeKind {
    Start,
    End {
        result_type: String,
        error: Option<EndError>,
    },
    Assignment {
        upstream_output: Vec<UpstreamValue>,
    },
    Switch {
        branches: Vec<BranchDef>,
    },
    If {
        condition: Option<Value>,
        else_next: String,
    },
    Loop {
        collection: Collection,
        condition: Option<Value>,
        item_type: Option<Property>,
        child_flow: Box<Flow>,
    },
    Parallel {
        branches: Vec<ParallelBranchDef>,
        is_conditional: bool,
        mode: String,
        join_branches: Vec<String>,
    },
    Child {
        child_flow: Box<Flow>,
        input: Option<Value>,
    },
    Code {
        language: String,
        code: String,
        input: Option<Value>,
        sandbox_backend: String,
    },
    Http {
        method: String,
        content_type: String,
        url: String,
        query: Option<Value>,
        headers: Option<Value>,
        body: Option<Value>,
    },
}

/// A logic node: shared base fields plus the kind payload. Deserialized
/// manually from the normalized document (type-tagged, mixed spellings —
/// see `ir::node_from_value`).
#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub node_type: String,
    pub name: String,
    pub desc: String,
    pub output: Option<Value>,
    pub timeout: Option<Duration>,
    pub error_policy: ErrorPolicy,
    pub timeout_policy: TimeoutPolicy,
    pub next: Option<String>,
    pub output_type: Option<Property>,
    pub metadata: serde_json::Map<String, Value>,
    pub kind: NodeKind,
}

/// Workflow-level context config (`context.global` after normalization).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ContextConfig {
    #[serde(default)]
    pub global: Option<Value>,
}

/// The executable flow (root or embedded).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Flow {
    #[serde(default = "default_flow_id")]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub desc: String,
    #[serde(default)]
    pub runtime: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub nodes: Vec<Node>,
    #[serde(default, deserialize_with = "de_timeout")]
    pub timeout: Option<Duration>,
    #[serde(default, rename = "inputType")]
    pub input_type: Option<Property>,
    #[serde(default, rename = "outputType")]
    pub output_type: Option<Property>,
    // 流级 errorHandler/timeoutHandler：errorHandler 不是本引擎契约面
    // （plaita Flow 模型无该字段，装载期 warn+metadata 留痕后忽略，
    // 见 ir::validate::stash_unknown_flow_fields）；timeoutHandler 保留
    // 是 r6 #14 的裁决面（节点自有超时 abort 传播时咨询，Go 移植面）。
    #[serde(default, rename = "timeoutHandler")]
    pub timeout_handler: TimeoutPolicy,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub context: ContextConfig,
    /// $ENV allowlist (plaita 0.5.0 allowlist model, fail-closed): absent →
    /// inherit the parent execution's filtered env; declared → only these
    /// variables pass through; declared empty → nothing.
    #[serde(default, rename = "exposeEnv")]
    pub expose_env: Option<Vec<String>>,
}

fn default_flow_id() -> String {
    "plaita-flow".to_string()
}

fn de_timeout<'de, D>(de: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<String>::deserialize(de)?;
    match v {
        None => Ok(None),
        Some(s) => crate::duration::parse_iso8601(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

impl Flow {
    pub fn find_start(&self) -> Option<&Node> {
        self.nodes.iter().find(|n| n.node_type == "start")
    }

    pub fn find_end(&self) -> Option<&Node> {
        self.nodes.iter().find(|n| n.node_type == "end")
    }

    pub fn find_node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }
}
