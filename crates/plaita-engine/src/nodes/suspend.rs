//! Suspending-node bodies（plaita-go#80 phase 3 的 Rust 侧，权威 =
//! plaita/node/event_node.py + plaita/server/nodes/*_node.py +
//! plaita-nodes hitl_await.py）：event/hitl_await/delay/approval/
//! redis_queue/kafka_queue/http_callback 七类。
//!
//! 挂起/恢复契约（Python 实测钉死，Go 同形移植）：
//!   - execute 结果 = {event_type, event_filter, is_async:true, event_id,
//!     status:"pending", service_config?, node_subtype?}——extended 家族由
//!     BaseExtendedNode.execute 合并出后两个键；hitl_await 是自己的
//!     {status, session_id, replies, ...} 形状；
//!   - 恢复后状态 = 挂起态全量保留（service_config/node_subtype/event_id/
//!     subscription_id/subscription_tenant）+ status 切换 + event_data
//!     （EventNode.on_event 的 _get_node_state → 更新 → _create_result 语义）。
//!
//! 等待/监听/决策全在引擎外：订阅注册表是宿主面（Host::with_subscriptions），
//! 订阅超时的到期投递（resume_type=timeout）同属宿主（Python
//! SubscriptionTimeoutChecker 同位），五节点的 service 层（队列消费/延迟
//! 排程/审批记录/回调 ingress）是部署侧设施。

use serde_json::{json, Map, Value};

use crate::context::SharedCtx;
use crate::error::NodeError;
use crate::exec::{ResumeKind, SubscriptionRequest};
use crate::model::{ExtendedCore, Flow, Node, NodeKind};

/// BaseExtendedNode.get_default_retry_config 的固定形状。
fn retry_config() -> Value {
    json!({
        "max_retries": 3,
        "retry_delay_ms": 1000,
        "exponential_backoff": true,
    })
}

/// 当前 Unix 毫秒时间戳（Python time.time()*1000 的 int 口径）。
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 现场铸 uuid4().hex 同形凭据（32 位十六进制）。
fn random_hex32() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// pyHasValue：Python 真值在 Optional 字段上的口径——null 与空串都算 falsy
/// （`if self.callback_path:` / `if self.auth_token:`——显式空串触发自动生成）。
fn py_has_value(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

/// approval/redis_queue/kafka_queue/http_callback 共享的 `$` 表达式解析
/// （各 *_node.py _resolve_value）：`$` 开头走求值——结果 null 或求值失败时
/// 返回**原值**。delay 的时长解析是另一套语义（解析失败回退 60 秒），不共用。
fn resolve_extended_value(rc: &SharedCtx, value: &Value) -> Value {
    let Value::String(s) = value else {
        return value.clone();
    };
    if !s.starts_with('$') {
        return value.clone();
    }
    match plaita_expression::eval_str(s, rc.as_ref()) {
        Ok(v) if !v.is_null() => v,
        _ => value.clone(),
    }
}

/// *string 字段的缺省 + `$` 解析（resolveExtendedString）。
fn resolve_extended_str(rc: &SharedCtx, v: Option<&Value>, default: &str) -> Value {
    match v {
        None => Value::String(default.to_string()),
        Some(v) => resolve_extended_value(rc, v),
    }
}

/// 逐元素 `$` 解析；解析结果是 list 时摊平（Python _resolve_approval_config
/// 的 extend 语义）。
fn resolve_list_values(rc: &SharedCtx, values: &[Value]) -> Vec<Value> {
    let mut resolved = Vec::with_capacity(values.len());
    for value in values {
        let r = resolve_extended_value(rc, value);
        if let Value::Array(list) = r {
            resolved.extend(list);
        } else {
            resolved.push(r);
        }
    }
    resolved
}

/// 逐值 `$` 解析（Python _resolve_validation_config）。
fn resolve_map_values(rc: &SharedCtx, m: Option<&Value>) -> Value {
    let Some(Value::Object(map)) = m else {
        return json!({});
    };
    let mut out = Map::new();
    for (key, value) in map {
        out.insert(key.clone(), resolve_extended_value(rc, value));
    }
    Value::Object(out)
}

/// EventNode.execute 的 event_type 解析：`$` 表达式求值，非字符串/空结果/
/// 求值失败保留原值。
fn resolve_event_expression(rc: &SharedCtx, raw: &str) -> String {
    if !raw.starts_with('$') {
        return raw.to_string();
    }
    match plaita_expression::eval_str(raw, rc.as_ref()) {
        Ok(Value::String(s)) if !s.is_empty() => s,
        _ => raw.to_string(),
    }
}

/// event_filter 的 pydantic default_factory=dict 口径：字段缺省时是空对象
/// 而非 null。
fn non_nil_filter(v: Option<&Value>) -> Value {
    v.cloned()
        .filter(|v| v.is_object())
        .unwrap_or_else(|| json!({}))
}

fn filter_of(core: &ExtendedCore) -> Value {
    non_nil_filter(core.event_filter.as_ref())
}

/// 挂起边界的唤醒订阅规格（plaita-go `SuspendSpec` 移植面）：event_type 按
/// 契约（delay/event 支持 `$` 表达式，用 execute 已解析值），超时按各节点
/// 语义（approval 缺省 24h/显式 null=0；其余缺省 0=无限等待）。
pub(crate) fn suspend_spec(
    node: &Node,
    state: &Value,
    rc: &SharedCtx,
    flow: &Flow,
    _host: &crate::exec::Host,
) -> Result<SubscriptionRequest, String> {
    let _ = flow;
    let mut req = SubscriptionRequest {
        subscription_id: String::new(),
        event_type: String::new(),
        filter: json!({}),
        node_id: node.id.clone(),
        correlation_id: String::new(),
        flow_id: String::new(),
        tenant_id: String::new(),
        timeout_seconds: 0.0,
    };
    match &node.kind {
        NodeKind::Event { core } => {
            req.event_type = state
                .get("event_type")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
                .unwrap_or_else(|| core.event_type.clone());
            if req.event_type.is_empty() {
                return Err(
                    "event_type is required (an empty type would match every event)".to_string(),
                );
            }
            req.filter = filter_of(core);
            req.timeout_seconds = core.subscription_timeout.unwrap_or(0.0).max(0.0);
        }
        NodeKind::Delay { core, .. } => {
            req.event_type = state
                .get("event_type")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
                .unwrap_or_else(|| core.event_type.clone());
            req.filter = filter_of(core);
            req.timeout_seconds = core.subscription_timeout.unwrap_or(0.0).max(0.0);
        }
        NodeKind::HitlAwait {
            event_type,
            event_filter,
            ..
        } => {
            // SuspendSpec 的 event_type 走同款 `$` 解析；timeout_secs 读
            // execute 产出的**已解析**值（Go gateToFloat(result["timeout_secs"])
            // 同形——表达式/缺省回退都在节点体里完成）。
            req.event_type = resolve_event_expression(rc, &hitl_event_type(event_type));
            req.filter = non_nil_filter(event_filter.as_ref());
            req.timeout_seconds = state.get("timeout_secs").and_then(as_f64).unwrap_or(0.0);
        }
        NodeKind::Approval { core, .. } => {
            req.event_type = core.event_type.clone();
            req.filter = filter_of(core);
            // 缺省 24h（#56 兜底：无人审批落可观测终态）；显式 null 折叠为
            // -1 哨兵 → 0=无限等待。
            req.timeout_seconds = match core.subscription_timeout {
                None => 24.0 * 3600.0,
                Some(t) if t < 0.0 => 0.0,
                Some(t) => t,
            };
        }
        NodeKind::RedisQueue { core, .. } => {
            req.event_type = core.event_type.clone();
            req.filter = filter_of(core);
            req.timeout_seconds = core.subscription_timeout.unwrap_or(0.0).max(0.0);
        }
        NodeKind::KafkaQueue { core, .. } => {
            req.event_type = core.event_type.clone();
            req.filter = filter_of(core);
            req.timeout_seconds = core.subscription_timeout.unwrap_or(0.0).max(0.0);
        }
        NodeKind::HttpCallback { core, .. } => {
            req.event_type = core.event_type.clone();
            req.filter = filter_of(core);
            req.timeout_seconds = core.subscription_timeout.unwrap_or(0.0).max(0.0);
        }
        _ => {
            return Err(format!(
                "node type {:?} does not declare a wake-up subscription",
                node.node_type
            ))
        }
    }
    Ok(req)
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
}

/// extended 家族的挂起态（BaseExtendedNode.execute 的合并顺序：EventNode
/// 结果 + service_config + node_subtype）。
fn extended_pending_state(
    node_id: &str,
    event_type: String,
    filter: Value,
    service_config: Value,
    subtype: &str,
) -> Value {
    json!({
        "event_id": format!("event_{node_id}_{}", now_ms()),
        "status": "pending",
        "event_type": event_type,
        "event_filter": filter,
        "is_async": true,
        "service_config": service_config,
        "node_subtype": subtype,
    })
}

/// extended 家族共享的 `$` 兜底缺省（Go orDefaultStr）：Literal 枚举字段的
/// 节点层防御缺省——装载期已按 pydantic 缺省填充，直构流的空串回落同款。
fn or_default_str(s: &str, default: &str) -> String {
    if s.is_empty() {
        default.to_string()
    } else {
        s.to_string()
    }
}

fn or_default_i64(v: Option<i64>, default: i64) -> Value {
    match v {
        Some(v) => json!(v),
        None => json!(default),
    }
}

// -- event --------------------------------------------------------------------

pub(crate) fn execute_event(
    core: &ExtendedCore,
    node: &Node,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    if core.event_type.trim().is_empty() {
        return Err(NodeError::deterministic(format!(
            "event {:?}: event_type is required (an empty type would match every event)",
            node.id
        )));
    }
    let resolved = resolve_event_expression(rc, &core.event_type);
    Ok(json!({
        "event_id": format!("event_{}_{}", node.id, now_ms()),
        "status": "pending",
        "event_type": resolved,
        "event_filter": filter_of(core),
        "is_async": true,
    }))
}

// -- hitl_await ---------------------------------------------------------------

const HITL_REPLY_EVENT_TYPE: &str = "hitl_reply";
const HITL_DEFAULT_BASE_URL: &str = "http://127.0.0.1:8081";
const HITL_DEFAULT_TIMEOUT_SECS: i64 = 3600;

fn hitl_base_url(rc: &SharedCtx, base_url: Option<&Value>) -> String {
    match base_url {
        Some(v) if py_has_value(Some(v)) => {
            let resolved = resolve_extended_value(rc, v);
            let s = match &resolved {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            s.trim_end_matches('/').to_string()
        }
        _ => std::env::var("HITL_URL")
            .unwrap_or_else(|_| HITL_DEFAULT_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string(),
    }
}

/// hitl_await 的 event_type 节点层缺省（Go `h.eventType()` 同款：装载层不
/// 填缺省，空值回落契约事件 hitl_reply）。
fn hitl_event_type(raw: &str) -> String {
    if raw.trim().is_empty() {
        HITL_REPLY_EVENT_TYPE.to_string()
    } else {
        raw.to_string()
    }
}

fn hitl_timeout_secs(rc: &SharedCtx, timeout_secs: Option<&Value>) -> i64 {
    timeout_secs
        .map(|v| resolve_extended_value(rc, v))
        .and_then(|v| v.as_i64())
        .unwrap_or(HITL_DEFAULT_TIMEOUT_SECS)
}

pub(crate) fn execute_hitl_await(
    node: &Node,
    message: Option<&Value>,
    images: Option<&Value>,
    base_url: Option<&Value>,
    timeout_secs: Option<&Value>,
    dry_run: bool,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    // Python execute：message/images/base_url/timeout_secs 全走求值；
    // dry_run（字段或全局 $GLOBAL.dry_run）短路。
    let message_text = match message {
        Some(v) => match resolve_extended_value(rc, v) {
            Value::Null => String::new(),
            Value::String(s) => s,
            other => plaita_expression::py_repr(&other),
        },
        None => String::new(),
    };
    let image_list: Vec<String> = match images {
        Some(v) => match resolve_extended_value(rc, v) {
            Value::Array(items) => items
                .iter()
                .filter_map(|i| i.as_str().map(String::from))
                .collect(),
            _ => Vec::new(),
        },
        None => Vec::new(),
    };
    let timeout = hitl_timeout_secs(rc, timeout_secs);
    let dry = dry_run
        || rc
            .get_global("dry_run")
            .map(|v| plaita_expression::truthy(&v))
            .unwrap_or(false);

    if dry {
        return Ok(json!({
            "status": "pending",
            "session_id": "dryrun",
            "replies": [],
            "dry_run": true,
            "timeout_secs": timeout,
        }));
    }

    let base = hitl_base_url(rc, base_url);
    let session_id = hitl_send_message(&base, &message_text, &image_list, timeout, true)
        .map_err(|e| NodeError::transient(format!("hitl_await {:?}: {e}", node.id)))?;
    // 供外部 poller 定位（Python execution.set_state("$HITL_SESSION", ...)）。
    rc.set_current("HITL-SESSION", json!(session_id));
    Ok(json!({
        "status": "pending",
        "session_id": session_id,
        "replies": [],
        "timeout_secs": timeout,
    }))
}

/// hitl_send_message（_hitl_client.send_message 移植）：可选图片探路降级 +
/// 主发送。server 拒发（success=false）是 HitlError；传输/解码失败是普通错误。
#[cfg(feature = "http")]
fn hitl_send_message(
    base: &str,
    message: &str,
    images: &[String],
    timeout_secs: i64,
    wait_reply: bool,
) -> Result<String, String> {
    let mut message = message.to_string();
    if !images.is_empty() {
        let probe = hitl_post_json(
            &format!("{base}/api/send"),
            &json!({
                "message": "（图片见下）",
                "images": images,
                "wait_reply": false,
                "upstream": "ilink",
            }),
            15,
        );
        let image_failed = match probe {
            Err(_) => true,
            Ok(resp) => !resp
                .get("success")
                .map(plaita_expression::truthy)
                .unwrap_or(false),
        };
        if image_failed {
            message.push_str(
                "\n\n⚠️ 封面图发送失败（媒体窗口过期）：请先给 bot 发任意一条消息后说\"重发图\"。",
            );
        }
    }
    let payload = hitl_post_json(
        &format!("{base}/api/send"),
        &json!({
            "message": message,
            "wait_reply": wait_reply,
            "timeout": timeout_secs,
            "upstream": "ilink",
        }),
        30,
    )?;
    if !payload
        .get("success")
        .map(plaita_expression::truthy)
        .unwrap_or(false)
    {
        let detail = payload
            .get("error")
            .filter(|e| !e.is_null())
            .cloned()
            .unwrap_or(payload);
        return Err(format!(
            "hitl-server 发送失败: {}",
            plaita_expression::py_repr(&detail)
        ));
    }
    Ok(payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string())
}

#[cfg(feature = "http")]
fn hitl_post_json(url: &str, body: &Value, timeout_secs: u64) -> Result<Value, String> {
    let resp = ureq::post(url)
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .send_json(body.clone())
        .map_err(|e| format!("hitl-server 请求失败: {e}"))?;
    resp.into_json::<Value>()
        .map_err(|e| format!("hitl-server 响应解码失败: {e}"))
}

#[cfg(not(feature = "http"))]
fn hitl_send_message(
    _base: &str,
    _message: &str,
    _images: &[String],
    _timeout_secs: i64,
    _wait_reply: bool,
) -> Result<String, String> {
    Err(
        "hitl_await: this plaita-engine build was compiled without the \"http\" feature"
            .to_string(),
    )
}

// -- delay --------------------------------------------------------------------

pub(crate) fn execute_delay(
    node: &Node,
    core: &ExtendedCore,
    delay_seconds: &Value,
    delay_unit: &str,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    let seconds = resolve_delay_seconds(node, delay_seconds, rc)?;
    let filter = filter_of(core);
    let delay_ms = delay_to_milliseconds(seconds, delay_unit);
    let now = now_ms();
    let service_config = json!({
        "type": "delay",
        "delay_ms": delay_ms,
        // delay 的 service_config 携带**原始** event_type 字段（Python
        // generate_service_config 用 self.event_type，不做 $ 解析）。
        "trigger_timestamp": now + delay_ms,
        "node_id": node.id,
        "execution_id": rc.execution_id(),
        "flow_id": rc.flow_id(),
        "event_type": core.event_type,
        "event_filter": filter,
        "retry_config": retry_config(),
    });
    Ok(extended_pending_state(
        &node.id,
        resolve_event_expression(rc, &core.event_type),
        filter,
        service_config,
        "delay",
    ))
}

/// Python `_resolve_delay_time`：`$` 表达式解析出非数值/null/求值异常一律
/// 回退 60（秒）；非表达式串按 float 解析（Python float(...) 的 ValueError
/// 口径，逐字文案对拍）。
fn resolve_delay_seconds(
    node: &Node,
    delay_seconds: &Value,
    rc: &SharedCtx,
) -> Result<f64, NodeError> {
    if let Value::String(s) = delay_seconds {
        if s.starts_with('$') {
            if let Ok(resolved) = plaita_expression::eval_str(s, rc.as_ref()) {
                match &resolved {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            return Ok(f);
                        }
                    }
                    // Python isinstance(True, int) == True：布尔按 1/0 计。
                    Value::Bool(b) => return Ok(if *b { 1.0 } else { 0.0 }),
                    _ => {}
                }
            }
            return Ok(60.0);
        }
    }
    match delay_seconds {
        Value::Number(n) => n.as_f64().ok_or_else(|| {
            NodeError::deterministic(format!(
                "delay node {:?}: could not convert string to float: '{n}'",
                node.id
            ))
        }),
        Value::String(s) => s.trim().parse::<f64>().map_err(|_| {
            NodeError::deterministic(format!(
                "delay node {:?}: could not convert string to float: '{s}'",
                node.id
            ))
        }),
        Value::Null => Err(NodeError::deterministic(format!(
            "delay node {:?}: delay_seconds is required",
            node.id
        ))),
        other => Err(NodeError::deterministic(format!(
            "delay node {:?}: delay_seconds must be a number, got {}",
            node.id,
            plaita_expression::py_repr(other)
        ))),
    }
}

/// Python `_convert_to_milliseconds`：单位乘法表 + 未知单位兜底按秒
///（Literal 装载期已拦，这里是直构流的防御），int() 向零截断。
fn delay_to_milliseconds(value: f64, unit: &str) -> i64 {
    match unit.to_lowercase().as_str() {
        "minutes" => (value * 60.0 * 1000.0) as i64,
        "hours" => (value * 60.0 * 60.0 * 1000.0) as i64,
        "days" => (value * 24.0 * 60.0 * 60.0 * 1000.0) as i64,
        _ => (value * 1000.0) as i64,
    }
}

// -- approval -----------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_approval(
    node: &Node,
    core: &ExtendedCore,
    approval_title: &Value,
    approval_content: &Value,
    approval_type: &str,
    approvers: &[Value],
    approval_strategy: &str,
    auto_escalation: bool,
    escalation_timeout_hours: Option<i64>,
    escalation_approvers: &[Value],
    form_fields: &[Value],
    allow_comments: Option<bool>,
    require_comments: bool,
    notification_config: Option<&Value>,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    let now = now_ms();
    let filter = filter_of(core);
    let resolved_fields: Vec<Value> = form_fields
        .iter()
        .map(|field| match field {
            Value::Object(map) => {
                let mut out = Map::new();
                for (k, v) in map {
                    out.insert(k.clone(), resolve_extended_value(rc, v));
                }
                Value::Object(out)
            }
            other => other.clone(),
        })
        .collect();
    let resolved_notification = match notification_config {
        Some(Value::Object(map)) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), resolve_extended_value(rc, v));
            }
            Value::Object(out)
        }
        _ => json!({}),
    };
    let allow_comments = allow_comments.unwrap_or(true);
    let service_config = json!({
        "type": "approval",
        "node_id": node.id,
        "approval_id": format!("approval_{}_{}", node.id, now),
        "execution_id": rc.execution_id(),
        "flow_id": rc.flow_id(),
        "event_type": core.event_type,
        "event_filter": filter,
        "approval_config": {
            "title": resolve_extended_value(rc, approval_title),
            "content": resolve_extended_value(rc, approval_content),
            "type": or_default_str(approval_type, "manual"),
            "created_time": now,
        },
        "approver_config": {
            "approvers": resolve_list_values(rc, approvers),
            "strategy": or_default_str(approval_strategy, "any"),
            "auto_escalation": auto_escalation,
            "escalation_timeout_hours": or_default_i64(escalation_timeout_hours, 24),
            "escalation_approvers": resolve_list_values(rc, escalation_approvers),
        },
        "form_config": {
            "fields": resolved_fields,
            "allow_comments": allow_comments,
            "require_comments": require_comments,
        },
        "notification_config": resolved_notification,
        "retry_config": retry_config(),
    });
    Ok(extended_pending_state(
        &node.id,
        core.event_type.clone(),
        filter,
        service_config,
        "approval",
    ))
}

// -- redis_queue --------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_redis_queue(
    node: &Node,
    core: &ExtendedCore,
    redis_host: Option<&Value>,
    redis_port: Option<i64>,
    redis_db: Option<i64>,
    redis_password: Option<&Value>,
    queue_name: &Value,
    queue_type: &str,
    timeout_seconds: Option<i64>,
    batch_size: Option<i64>,
    message_format: &str,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    let filter = filter_of(core);
    let service_config = json!({
        "type": "redis_queue",
        "node_id": node.id,
        "execution_id": rc.execution_id(),
        "flow_id": rc.flow_id(),
        "event_type": core.event_type,
        "event_filter": filter,
        "redis_config": {
            "host": resolve_extended_str(rc, redis_host, "localhost"),
            "port": or_default_i64(redis_port, 6379),
            "db": or_default_i64(redis_db, 0),
            "password": redis_password.map(|v| resolve_extended_value(rc, v)).unwrap_or(Value::Null),
        },
        "queue_config": {
            "name": resolve_extended_value(rc, queue_name),
            "type": or_default_str(queue_type, "list"),
            "message_format": or_default_str(message_format, "json"),
        },
        "listen_config": {
            "timeout_seconds": or_default_i64(timeout_seconds, 0),
            "batch_size": or_default_i64(batch_size, 1),
        },
        "retry_config": retry_config(),
    });
    Ok(extended_pending_state(
        &node.id,
        core.event_type.clone(),
        filter,
        service_config,
        "redis_queue",
    ))
}

// -- kafka_queue --------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_kafka_queue(
    node: &Node,
    core: &ExtendedCore,
    bootstrap_servers: &Value,
    security_protocol: &str,
    sasl_mechanism: Option<&Value>,
    sasl_username: Option<&Value>,
    sasl_password: Option<&Value>,
    topic: &Value,
    partition: Option<i64>,
    group_id: &Value,
    auto_offset_reset: &str,
    enable_auto_commit: Option<bool>,
    max_poll_records: Option<i64>,
    message_format: &str,
    timeout_ms: Option<i64>,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    let filter = filter_of(core);
    // bootstrap_servers：字符串按逗号切分为列表（Python _resolve_kafka_config）；
    // 非 str 原样透传。
    let resolved_bootstrap = resolve_extended_value(rc, bootstrap_servers);
    let servers = match &resolved_bootstrap {
        Value::String(s) => Value::Array(
            s.split(',')
                .map(|p| Value::String(p.trim().to_string()))
                .collect(),
        ),
        other => other.clone(),
    };
    let service_config = json!({
        "type": "kafka_queue",
        "node_id": node.id,
        "execution_id": rc.execution_id(),
        "flow_id": rc.flow_id(),
        "event_type": core.event_type,
        "event_filter": filter,
        "kafka_config": {
            "bootstrap_servers": servers,
            "security_protocol": or_default_str(security_protocol, "PLAINTEXT"),
            "sasl_mechanism": sasl_mechanism.map(|v| resolve_extended_value(rc, v)).unwrap_or(Value::Null),
            "sasl_username": sasl_username.map(|v| resolve_extended_value(rc, v)).unwrap_or(Value::Null),
            "sasl_password": sasl_password.map(|v| resolve_extended_value(rc, v)).unwrap_or(Value::Null),
        },
        "topic_config": {
            "topic": resolve_extended_value(rc, topic),
            "partition": partition.map(|v| json!(v)).unwrap_or(Value::Null),
            "message_format": or_default_str(message_format, "json"),
        },
        "consumer_config": {
            "group_id": resolve_extended_value(rc, group_id),
            "auto_offset_reset": or_default_str(auto_offset_reset, "latest"),
            "enable_auto_commit": enable_auto_commit.unwrap_or(true),
            "max_poll_records": or_default_i64(max_poll_records, 1),
            "timeout_ms": or_default_i64(timeout_ms, 30000),
        },
        "retry_config": retry_config(),
    });
    Ok(extended_pending_state(
        &node.id,
        core.event_type.clone(),
        filter,
        service_config,
        "kafka_queue",
    ))
}

// -- http_callback ------------------------------------------------------------

const HTTP_CALLBACK_BASE_URL: &str = "http://localhost:8080";

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_http_callback(
    node: &Node,
    core: &ExtendedCore,
    callback_path: Option<&Value>,
    callback_method: &str,
    callback_timeout_minutes: Option<i64>,
    require_auth: Option<bool>,
    auth_type: &str,
    auth_token: Option<&Value>,
    validate_headers: Option<&Value>,
    validate_params: Option<&Value>,
    success_response: Option<&Value>,
    error_response: Option<&Value>,
    rc: &SharedCtx,
) -> Result<Value, NodeError> {
    let filter = filter_of(core);
    let path = callback_path_value(node, callback_path, rc);
    let path_str = path.as_str().unwrap_or("").to_string();
    let method = or_default_str(callback_method, "POST");
    let service_config = json!({
        "type": "http_callback",
        "node_id": node.id,
        "execution_id": rc.execution_id(),
        "flow_id": rc.flow_id(),
        "event_type": core.event_type,
        "event_filter": filter,
        "callback_config": {
            "path": path,
            "method": method,
            "timeout_minutes": or_default_i64(callback_timeout_minutes, 60),
            // full_url 与 path 同源（Python f"{base}{path}"）。
            "full_url": format!("{HTTP_CALLBACK_BASE_URL}{path_str}"),
        },
        "auth_config": http_callback_auth_config(auth_type, auth_token, require_auth, rc),
        "validation_config": {
            "headers": resolve_map_values(rc, validate_headers),
            "params": resolve_map_values(rc, validate_params),
        },
        "response_config": {
            "success_response": success_response.cloned().unwrap_or(json!({"status": "success"})),
            "error_response": error_response.cloned().unwrap_or(json!({"status": "error"})),
        },
        "retry_config": retry_config(),
    });
    Ok(extended_pending_state(
        &node.id,
        core.event_type.clone(),
        filter,
        service_config,
        "http_callback",
    ))
}

/// Python `_generate_callback_path`：真值路径（空串 falsy）走 `$` 解析；
/// 缺省/空串自动生成 `/callback/{flow_id}/{node_id}/{execution_id}`
///（execution_id 缺省现场铸 uuid hex，flow_id 缺省字面 "unknown"）。
fn callback_path_value(node: &Node, callback_path: Option<&Value>, rc: &SharedCtx) -> Value {
    if py_has_value(callback_path) {
        return resolve_extended_value(rc, callback_path.unwrap());
    }
    let mut execution_id = rc.execution_id();
    if execution_id.is_empty() {
        execution_id = random_hex32();
    }
    let flow_id = {
        let id = rc.flow_id();
        if id.is_empty() {
            "unknown".to_string()
        } else {
            id
        }
    };
    Value::String(format!("/callback/{flow_id}/{}/{}", node.id, execution_id))
}

/// Python `_generate_auth_config`：require_auth=false → {"enabled":false}；
/// token/basic/signature 三型各按字段生成，凭据缺省现场铸 uuid hex。
fn http_callback_auth_config(
    auth_type: &str,
    auth_token: Option<&Value>,
    require_auth: Option<bool>,
    rc: &SharedCtx,
) -> Value {
    if !require_auth.unwrap_or(true) {
        return json!({"enabled": false});
    }
    let auth_type = or_default_str(auth_type, "token");
    let mut config = json!({"enabled": true, "type": auth_type});
    let obj = config.as_object_mut().expect("auth config object");
    match auth_type.as_str() {
        "basic" => {
            // username 用显式 auth_token（真值判伪：null/空串都走生成缺省），
            // password 现场生成。
            if py_has_value(auth_token) {
                obj.insert(
                    "username".into(),
                    resolve_extended_value(rc, auth_token.unwrap()),
                );
            }
            obj.insert("password".into(), Value::String(random_hex32()));
        }
        "signature" => {
            let secret = match auth_token {
                Some(v) if py_has_value(Some(v)) => resolve_extended_value(rc, v),
                _ => Value::String(random_hex32()),
            };
            obj.insert("secret".into(), secret);
            obj.insert("algorithm".into(), json!("HMAC-SHA256"));
        }
        // "token"（装载期 Literal 已拦未知类型，直构流兜底同 token）
        _ => {
            let token = match auth_token {
                Some(v) if py_has_value(Some(v)) => resolve_extended_value(rc, v),
                _ => Value::String(random_hex32()),
            };
            obj.insert("token".into(), token);
        }
    }
    config
}

// -- resume（EventNode.resume 多态分发）---------------------------------------

/// 对挂起节点施加决策载体，产出恢复后的节点状态（plaita-go `extendedResume`
/// /`Event.Resume`/`HitlAwait.Resume` 的移植面）。挂起态字段从 `$NODE` 结果
/// 回填（引擎已校验 pending 状态与订阅绑定）。
pub(crate) fn resume_node_body(
    node: &Node,
    pending: &Value,
    resume_kind: ResumeKind,
    data: &Value,
    _rc: &SharedCtx,
) -> Result<Value, NodeError> {
    match &node.kind {
        NodeKind::Event { core } => {
            // Python on_event：state = 前一节点结果（整体，含解析后的
            // event_type/event_id）+ status 切换 + event_data——恢复后
            // event_type 保持 execute 解析值（dict.update 覆盖序）。
            let mut state = match pending {
                Value::Object(map) => map.clone(),
                _ => Map::new(),
            };
            state
                .entry("event_filter".to_string())
                .or_insert_with(|| filter_of(core));
            state.entry("is_async".to_string()).or_insert(json!(true));
            apply_status(&mut state, resume_kind, data);
            Ok(Value::Object(state))
        }
        NodeKind::HitlAwait { event_filter, .. } => {
            // Go HitlAwait.Resume：data["event_data"] 嵌套合并（缺席不覆盖）。
            let mut merged = match data {
                Value::Object(map) => map.clone(),
                _ => Map::new(),
            };
            if let Some(Value::Object(nested)) = data.get("event_data") {
                for (k, v) in nested {
                    merged.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            let merged = Value::Object(merged);
            let session_id = merged
                .get("session_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let status = match resume_kind {
                ResumeKind::Cancel => "cancel".to_string(),
                ResumeKind::Timeout => "timeout".to_string(),
                ResumeKind::Event => {
                    let status = merged.get("status").and_then(|v| v.as_str()).unwrap_or("");
                    if status.is_empty() {
                        "replied".to_string()
                    } else {
                        status.to_string()
                    }
                }
            };
            let replies = merged
                .get("replies")
                .cloned()
                .filter(|v| v.is_array())
                .map(|v| {
                    v.as_array()
                        .unwrap()
                        .iter()
                        .map(|r| match r {
                            Value::String(s) => Value::String(s.clone()),
                            other => Value::String(plaita_expression::py_repr(other)),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let _ = event_filter;
            Ok(json!({
                "status": status,
                "replies": replies,
                "session_id": session_id,
            }))
        }
        NodeKind::Delay { core, .. }
        | NodeKind::Approval { core, .. }
        | NodeKind::RedisQueue { core, .. }
        | NodeKind::KafkaQueue { core, .. }
        | NodeKind::HttpCallback { core, .. } => {
            // extendedResume：base = 契约 event_type + filter + is_async，
            // 挂起态字段全量回填，仅 status 翻转 + event_data 落地。
            let mut state = Map::new();
            state.insert("event_type".into(), json!(core.event_type));
            state.insert("event_filter".into(), filter_of(core));
            state.insert("is_async".into(), json!(true));
            if let Value::Object(prev) = pending {
                for key in [
                    "event_id",
                    "service_config",
                    "node_subtype",
                    "subscription_id",
                    "subscription_tenant",
                ] {
                    if let Some(v) = prev.get(key) {
                        state.insert(key.to_string(), v.clone());
                    }
                }
            }
            apply_status(&mut state, resume_kind, data);
            Ok(Value::Object(state))
        }
        other => Err(NodeError::deterministic(format!(
            "node type {:?} has no resume handler",
            other_kind_name(other)
        ))),
    }
}

fn other_kind_name(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::Start => "start",
        NodeKind::End { .. } => "end",
        NodeKind::Assignment { .. } => "assignment",
        NodeKind::Switch { .. } => "switch",
        NodeKind::If { .. } => "if",
        NodeKind::Loop { .. } => "loop",
        NodeKind::Map { .. } => "map",
        NodeKind::Filter { .. } => "filter",
        NodeKind::Find { .. } => "find",
        NodeKind::Reduce { .. } => "reduce",
        NodeKind::While { .. } => "while",
        NodeKind::Mock { .. } => "mock",
        NodeKind::Reference { .. } => "reference",
        NodeKind::Parallel { .. } => "parallel",
        NodeKind::Child { .. } => "child",
        NodeKind::Code { .. } => "code",
        NodeKind::Http { .. } => "http",
        NodeKind::Event { .. } => "event",
        NodeKind::HitlAwait { .. } => "hitl_await",
        NodeKind::Delay { .. } => "delay",
        NodeKind::Approval { .. } => "approval",
        NodeKind::RedisQueue { .. } => "redis_queue",
        NodeKind::KafkaQueue { .. } => "kafka_queue",
        NodeKind::HttpCallback { .. } => "http_callback",
    }
}

fn apply_status(state: &mut Map<String, Value>, resume_kind: ResumeKind, data: &Value) {
    match resume_kind {
        ResumeKind::Cancel => {
            state.insert("status".into(), json!("cancelled"));
        }
        ResumeKind::Timeout => {
            state.insert("status".into(), json!("timeout"));
        }
        ResumeKind::Event => {
            state.insert("status".into(), json!("completed"));
            state.insert("event_data".into(), data.clone());
        }
    }
}
