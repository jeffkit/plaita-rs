//! Error taxonomy of the engine — the Rust analogue of plaita-go's
//! `error.go`/`errs.go` split.
//!
//! The retry/timeout machinery classifies every failure:
//! - [`NodeError`] carries a [`ErrorClass`] that decides retries
//!   (deterministic never retry; transient retry per `retryTimes`);
//! - timeout errors are attributed to the flow budget or the node budget
//!   (plaita FLOW_ERROR vs NODE_ERROR);
//! - handler aborts wrap their cause with the configured code/message;
//! - an `end` node with `resultType: "error"` raises [`FlowError::FlowResult`]
//!   (plaita FlowResultError — a business error, not an engine fault).

use std::fmt;

/// plaita-go engine error codes (subset).
pub const RUNTIME_ERROR_CODE: i64 = 10000;
/// deterministic request-generation error: never retried
pub const NODE_GEN_REQUEST_ERROR_CODE: i64 = 1001;
/// transient node codes: retried
pub const NODE_DO_REQUEST_ERROR_CODE: i64 = 1002;
pub const NODE_EXEC_ERROR_CODE: i64 = 1003;

/// plaita ErrorHandler.error_code's default abort code.
pub const DEFAULT_HANDLER_ERROR_CODE: i64 = -9527;

/// Execution-level cancellation (plaita#267 波次③; Python
/// `FlowCancelledException`). Distinct from the generic -500 / node -520 /
/// timeout -1 families so the upper layer can recognize a cancelled
/// terminal state.
pub const CANCELLED_ERROR_CODE: i64 = -4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// bad configuration/parameters — a retry cannot fix it
    Deterministic,
    /// sending/processing errors — retried per the policy's retryTimes
    Transient,
    /// a deadline fired
    Timeout,
    /// 挂起作用域闸拒绝（嵌套挂起 v1：迭代帧/分支帧上的挂起）：fail-closed
    /// 的具名拒绝——不可重试、不可被策略吞（join 哨兵必须让位于它）。
    SuspendRejected,
}

#[derive(Debug, Clone)]
pub struct NodeError {
    pub message: String,
    pub class: ErrorClass,
    /// Engine error code carried into $ERROR entries (Go errs.Internal).
    pub code: i64,
    /// 挂起穿透载体（嵌套挂起 v1）：子执行挂起时由 child/reference 节点体
    /// 携带上抛——execute_node 必须先于一切策略链原样转发（与取消同待遇），
    /// advance_from 捕获后把外层帧压入其调用栈。
    pub suspension: Option<Box<crate::exec::SuspendedExecution>>,
}

impl NodeError {
    pub fn deterministic(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Deterministic,
            code: NODE_GEN_REQUEST_ERROR_CODE,
            suspension: None,
        }
    }

    /// 嵌套挂起穿透载体（不进任何策略链；advance_from 捕获压帧）。
    pub fn suspension(inner: Box<crate::exec::SuspendedExecution>) -> Self {
        let node_id = inner.suspended_node.clone();
        NodeError {
            message: format!("flow suspended at node {node_id:?} (nested)"),
            class: ErrorClass::Deterministic,
            code: NODE_GEN_REQUEST_ERROR_CODE,
            suspension: Some(inner),
        }
    }
    pub fn transient(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Transient,
            code: RUNTIME_ERROR_CODE,
            suspension: None,
        }
    }
    pub fn transient_with_code(message: impl Into<String>, code: i64) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Transient,
            code,
            suspension: None,
        }
    }
    pub fn timeout(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Timeout,
            code: RUNTIME_ERROR_CODE,
            suspension: None,
        }
    }

    /// 挂起穿透判定：载体在场时不进 timeout/retry/errorHandler 任何策略链。
    pub fn is_suspension(&self) -> bool {
        self.suspension.is_some()
    }

    /// 挂起作用域闸拒绝。
    pub fn suspend_rejected(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::SuspendRejected,
            code: RUNTIME_ERROR_CODE,
            suspension: None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self.class {
            ErrorClass::Deterministic | ErrorClass::SuspendRejected => false,
            // plaita runner.is_retryable_error: 1001 never; 1002/1003 and
            // unknown codes retry
            ErrorClass::Transient | ErrorClass::Timeout => self.code != NODE_GEN_REQUEST_ERROR_CODE,
        }
    }
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "code: {}, message: {}", self.code, self.message)
    }
}

/// Everything a flow execution can fail with.
#[derive(Debug, Clone)]
pub enum FlowError {
    /// A node failed and its (abort) policy let the error propagate.
    Node {
        node_id: String,
        node_type: String,
        error: NodeError,
    },
    /// An abort handler with a configured code/message wrapped the cause.
    Handler {
        code: i64,
        message: String,
        cause: Box<FlowError>,
    },
    /// The flow-level budget fired. Terminal by contract (issue #14,
    /// mirroring plaita's ExecutionDeadlineError): node timeout/error
    /// policies and the flow-level timeoutHandler never soften it.
    FlowTimeout { message: String },
    /// Execution-level cancellation (plaita#267 波次③; Python
    /// `FlowCancelledException`, code=-4). The sticky `cancel_requested`
    /// intent is honored at node boundaries — BEFORE any retry/errorHandler
    /// classification, so no continue policy can swallow it — and never
    /// flows through handler chains: cancellation is an execution-level
    /// intent, not a node error. Terminal, like FlowTimeout.
    Cancelled { message: String },
    /// The flow parked at a suspending node (plaita-go `SuspendError`). This
    /// is deliberately an error variant: nested flow nodes wrap and propagate
    /// errors, so a suspension physically cannot be mistaken for flow output
    /// on the way up. The execution state travels inside; apply a decision
    /// carrier with `SuspendedExecution::resume`.
    Suspended(Box<crate::exec::SuspendedExecution>),
    /// 挂起作用域闸拒绝（嵌套挂起 v1：迭代帧/分支帧）。终态直抛，等价 Other
    /// 但带专属类型——parallel 的 join 哨兵语义必须让它失败而不是吞成数据。
    SuspendRejected { message: String },
    /// plaita `ResumeGuardError`: the resume carrier cannot be applied to the
    /// current execution state (no suspended node, non-pending status,
    /// unsupported resume type, stale subscription binding). The execution
    /// stays suspended; the carrier is not a valid decision.
    ResumeGuard { message: String },
    /// plaita `ResumeError`: the carrier was admissible but the node's resume
    /// handler failed.
    ResumeError { message: String },
    /// Engine-level failures (reentry, missing node, ...).
    Other(String),
}

impl FlowError {
    pub fn node(node_id: &str, node_type: &str, error: NodeError) -> Self {
        FlowError::Node {
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            error,
        }
    }

    /// Wraps with handler code/message unless both are unset (Go
    /// wrapHandlerError: handlers without code/message keep the raw error;
    /// an already-wrapped error is never wrapped twice).
    pub fn wrap_handler(self, code: Option<i64>, message: &str) -> FlowError {
        if code.is_none() && message.is_empty() {
            return self;
        }
        if let FlowError::Handler { .. } = self {
            return self;
        }
        let c = code.unwrap_or(DEFAULT_HANDLER_ERROR_CODE);
        let msg = if message.is_empty() {
            self.to_string()
        } else {
            message.to_string()
        };
        FlowError::Handler {
            code: c,
            message: msg,
            cause: Box::new(self),
        }
    }

    /// True when the failure is a deadline firing (possibly wrapped by a
    /// handler) — the flow-level timeout policy's input.
    pub fn is_flow_budget_timeout(&self) -> bool {
        match self {
            FlowError::FlowTimeout { .. } => true,
            FlowError::Node { error, .. } => error.class == ErrorClass::Timeout,
            FlowError::Handler { cause, .. } => cause.is_flow_budget_timeout(),
            _ => false,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            FlowError::Node { error, .. } => error.is_retryable(),
            FlowError::Handler { cause, .. } => cause.is_retryable(),
            _ => false,
        }
    }
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowError::Node {
                node_id,
                node_type,
                error,
            } => write!(f, "node {node_id:?} ({node_type}) failed: {error}"),
            FlowError::Handler {
                code,
                message,
                cause,
            } => {
                write!(f, "code: {code}, message: {message}, cause: {cause}")
            }
            FlowError::FlowTimeout { message } => write!(f, "{message}"),
            // Python FlowCancelledException 同族方言：code=-4 是取消专属
            // 错误码，上层按它识别取消终态
            FlowError::Cancelled { message } => {
                write!(f, "code: {CANCELLED_ERROR_CODE}, message: {message}")
            }
            FlowError::Suspended(suspended) => {
                write!(f, "flow suspended at node {:?}", suspended.suspended_node)
            }
            FlowError::SuspendRejected { message } => write!(f, "{message}"),
            FlowError::ResumeGuard { message } => write!(f, "{message}"),
            FlowError::ResumeError { message } => write!(f, "{message}"),
            FlowError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for FlowError {}
