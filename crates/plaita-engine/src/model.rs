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
    /// map（loop.py Map）：对集合每个元素执行子流程并按集合顺序返回全部结果。
    Map {
        collection: Collection,
        item_type: Option<Property>,
        concurrent: bool,
        max_concurrent: Option<i64>,
        child_flow: Box<Flow>,
    },
    /// filter（loop.py Filter）：子流程返回真值的元素进结果集。
    Filter {
        collection: Collection,
        item_type: Option<Property>,
        child_flow: Box<Flow>,
    },
    /// find（loop.py Find）：第一个真值元素；无则 null。
    Find {
        collection: Collection,
        item_type: Option<Property>,
        child_flow: Box<Flow>,
    },
    /// reduce（loop.py Reduce）：累积链；initial 缺省取首元素（空集合且无
    /// initial 是确定性错误，对齐 Python 的 IndexError）。
    Reduce {
        collection: Collection,
        item_type: Option<Property>,
        initial: Option<Value>,
        child_flow: Box<Flow>,
    },
    /// while（loop.py While）：先判断后执行的条件循环；condition 缺省只跑
    /// 一轮；max_iterations 缺省 1000，触顶仅告警不报错。
    While {
        condition: Option<Value>,
        max_iterations: Option<i64>,
        child_flow: Box<Flow>,
    },
    /// mock（mock.py Mock，数据固定 pin 占位）：value 原样透传不求值，
    /// output 字段忽略（golden mock_output_ignored 钉）。
    Mock {
        value: Option<Value>,
    },
    /// reference（child.py ReferenceFlow，引用逻辑）：childFlow 由
    /// FlowProvider 在装载期注入；子流程隔离父上下文（$PARENT.<x> → null，
    /// $GLOBAL/$ENV/执行身份继承），input 缺省继承调用方 $INPUT。
    Reference {
        child_flow: Box<Flow>,
        input: Option<Value>,
    },
    /// event（event_node.py EventNode，挂起型基类族）：等待外部事件后继续。
    /// execute 产出 pending 事件契约（event_id/status/event_type/event_filter/
    /// is_async），等待与恢复全在引擎外（订阅注册表是宿主面）。
    Event {
        core: ExtendedCore,
    },
    /// hitl_await（plaita-nodes hitl_await.py，挂起版人工确认）：execute 发
    /// 消息拿 session_id 即挂起（dry_run 短路）；回复经 hitl_reply 事件唤醒。
    HitlAwait {
        message: Option<Value>,
        images: Option<Value>,
        base_url: Option<Value>,
        timeout_secs: Option<Value>,
        event_type: String,
        event_filter: Option<Value>,
        dry_run: bool,
    },
    /// delay（server/nodes/delay_node.py）：解析时长产出 delay service_config
    /// 后挂起；真正的排程等待在外部 delay service（节点进程内不等待）。
    Delay {
        core: ExtendedCore,
        delay_seconds: Value,
        delay_unit: String,
    },
    /// approval（server/nodes/approval_node.py）：固定订阅 approval_decision；
    /// 缺省 24h 订阅超时（显式 null=无限等待，ir 层折叠为 -1 哨兵）。
    Approval {
        core: ExtendedCore,
        approval_title: Value,
        approval_content: Value,
        approval_type: String,
        approvers: Vec<Value>,
        approval_strategy: String,
        auto_escalation: bool,
        escalation_timeout_hours: Option<i64>,
        escalation_approvers: Vec<Value>,
        form_fields: Vec<Value>,
        allow_comments: Option<bool>,
        require_comments: bool,
        notification_config: Option<Value>,
    },
    /// redis_queue（server/nodes/redis_queue_node.py）：固定订阅
    /// redis_message；节点自身不连接 Redis。
    RedisQueue {
        core: ExtendedCore,
        redis_host: Option<Value>,
        redis_port: Option<i64>,
        redis_db: Option<i64>,
        redis_password: Option<Value>,
        queue_name: Value,
        queue_type: String,
        timeout_seconds: Option<i64>,
        batch_size: Option<i64>,
        message_format: String,
    },
    /// kafka_queue（server/nodes/kafka_queue_node.py）：固定订阅
    /// kafka_message；节点自身不连接 Kafka。
    KafkaQueue {
        core: ExtendedCore,
        bootstrap_servers: Value,
        security_protocol: String,
        sasl_mechanism: Option<Value>,
        sasl_username: Option<Value>,
        sasl_password: Option<Value>,
        topic: Value,
        partition: Option<i64>,
        group_id: Value,
        auto_offset_reset: String,
        enable_auto_commit: Option<bool>,
        max_poll_records: Option<i64>,
        message_format: String,
        timeout_ms: Option<i64>,
    },
    /// http_callback（server/nodes/http_callback_node.py）：固定订阅
    /// http_callback；节点自身不监听端口。
    HttpCallback {
        core: ExtendedCore,
        callback_path: Option<Value>,
        callback_method: String,
        callback_timeout_minutes: Option<i64>,
        require_auth: Option<bool>,
        auth_type: String,
        auth_token: Option<Value>,
        validate_headers: Option<Value>,
        validate_params: Option<Value>,
        success_response: Option<Value>,
        error_response: Option<Value>,
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

/// 挂起型节点共享的事件核心（Go extendedEventCore 同形）：event_type 装载期
/// 已按契约填充（approval/redis_queue/kafka_queue/http_callback 强制覆盖，
/// delay 缺省 delay_trigger 可配，event 为作者字段）；subscription_timeout 的
/// None=字段缺省、Some(-1)=显式 null（无限等待哨兵，ir 层折叠）。
#[derive(Debug, Clone)]
pub struct ExtendedCore {
    pub event_type: String,
    pub event_filter: Option<Value>,
    pub subscription_timeout: Option<f64>,
}

impl NodeKind {
    /// plaita `is_suspending` 类属性：执行本节点会把流程挂起（等待/恢复全在
    /// 引擎外）。与 [`crate::nodes::SUSPENDING_NODE_TYPES`] 保持一致。
    pub fn is_suspending(&self) -> bool {
        matches!(
            self,
            NodeKind::Event { .. }
                | NodeKind::HitlAwait { .. }
                | NodeKind::Delay { .. }
                | NodeKind::Approval { .. }
                | NodeKind::RedisQueue { .. }
                | NodeKind::KafkaQueue { .. }
                | NodeKind::HttpCallback { .. }
        )
    }
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
