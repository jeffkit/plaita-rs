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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// bad configuration/parameters — a retry cannot fix it
    Deterministic,
    /// sending/processing errors — retried per the policy's retryTimes
    Transient,
    /// a deadline fired
    Timeout,
}

#[derive(Debug, Clone)]
pub struct NodeError {
    pub message: String,
    pub class: ErrorClass,
    /// Engine error code carried into $ERROR entries (Go errs.Internal).
    pub code: i64,
}

impl NodeError {
    pub fn deterministic(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Deterministic,
            code: NODE_GEN_REQUEST_ERROR_CODE,
        }
    }
    pub fn transient(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Transient,
            code: RUNTIME_ERROR_CODE,
        }
    }
    pub fn transient_with_code(message: impl Into<String>, code: i64) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Transient,
            code,
        }
    }
    pub fn timeout(message: impl Into<String>) -> Self {
        NodeError {
            message: message.into(),
            class: ErrorClass::Timeout,
            code: RUNTIME_ERROR_CODE,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self.class {
            ErrorClass::Deterministic => false,
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
            FlowError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for FlowError {}
