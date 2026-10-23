//! Worker error taxonomy (rs-worker-design §11.2, port of plaita-go
//! errors.go): every error names its disposition family —
//!
//! | family                  | disposition                                   |
//! |-------------------------|-----------------------------------------------|
//! | transient               | park (leave pending), Warn + counter          |
//! | lease conflict / lost   | §3.5/§3.4 disposition (decision carriers never acked) |
//! | deterministic terminal  | error terminal row + ack (flow_not_found …)   |
//! | invalid payload/schema  | DLQ + ack                                     |
//! | drain aborted           | park, NO terminal write                       |
//!
//! fail-closed 总则：探针失败一律按最保守分支（park 留 pending）。

use std::fmt;

use crate::backend::BackendError;

/// The disposition family of a worker failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Redis/IO transient failure — warn + leave the message pending.
    Transient,
    /// Another holder owns the execution lease (§3.5): decision resume
    /// carriers are NEVER acked in the conflict window; other carriers ack
    /// only with live-holder proof (M1: no registry — conservative park).
    LeaseConflict,
    /// The lease was lost mid-run (renew false / fence CAS mismatch).
    LeaseLost,
    /// Fence key matched but the never-cleared fencegen counter disagrees
    /// (#54): partial restore suspected — the stale token must not re-hit
    /// the CAS. Same "do not ack" semantics as LeaseLost.
    FenceGenerationInconsistent,
    /// The state row could not be persisted — never ack.
    StatePersist,
    /// A present state row / checkpoint / subscription is unreadable —
    /// never silently "missing" (§5.2 read-side contract).
    StateLoad,
    /// Transient read failure while loading a flow definition — park.
    FlowLoad,
    /// XGROUP CREATE hit BUSYGROUP (tolerated by EnsureGroup).
    BusyGroup,
    /// The in-flight step was abandoned by a drain force-stop (§9.3 D2):
    /// no terminal write, message stays pending.
    DrainAborted,
    /// The resume carrier was permanently rejected by the engine's
    /// admission gates (FlowError::ResumeGuard — stale carrier, non-pending
    /// node, binding mismatch): the execution STAYS SUSPENDED and the
    /// carrier is acked (#68 — leaving it pending only feeds the
    /// reclaim→DLQ→clone self-feeding loop).
    ResumeGuardRejected,
    /// Deterministic: the flow definition does not exist / is not loadable
    /// for this build → flow_not_found error terminal.
    FlowNotFound,
    /// The stream payload is not a JSON object → DLQ + ack (§4.4).
    InvalidPayload,
    /// Envelope schema_version above this build's write version → DLQ + ack.
    UnsupportedSchema,
    /// Reclaim delivery ceiling hit (§4.2): guarded dead-letter.
    MaxDeliveries,
    /// A retryable node failure below the §9.1 budget (retry.rs): the
    /// consumer loop owes the backoff + enqueue→ack choreography. Never
    /// leaks to a disposition family of its own.
    NodeRetryable,
}

impl ErrorKind {
    /// Transient family: the message must stay pending (park semantics).
    pub fn is_pending(&self) -> bool {
        matches!(
            self,
            ErrorKind::Transient
                | ErrorKind::LeaseConflict
                | ErrorKind::LeaseLost
                | ErrorKind::FenceGenerationInconsistent
                | ErrorKind::StatePersist
                | ErrorKind::StateLoad
                | ErrorKind::FlowLoad
                | ErrorKind::DrainAborted
        )
    }

    /// The carrier-rejected family: ack, execution stays suspended.
    pub fn is_carrier_rejected(&self) -> bool {
        matches!(self, ErrorKind::ResumeGuardRejected)
    }

    /// Deterministic missing-flow family (error terminal + ack).
    pub fn is_deterministic_terminal(&self) -> bool {
        matches!(self, ErrorKind::FlowNotFound)
    }

    /// DLQ family (dead-letter + ack at the queue layer).
    pub fn is_dlq(&self) -> bool {
        matches!(
            self,
            ErrorKind::InvalidPayload | ErrorKind::UnsupportedSchema | ErrorKind::MaxDeliveries
        )
    }
}

/// A worker failure with its disposition family.
#[derive(Debug, Clone)]
pub struct WorkerError {
    pub kind: ErrorKind,
    pub message: String,
    /// §9.1 payload for the NodeRetryable control-flow error: what the
    /// consumer loop's re-enqueue choreography needs (retry.go
    /// NodeRetryableError).
    pub node_retry: Option<NodeRetryPayload>,
}

/// The retryable-failure data carrier (§9.1).
#[derive(Debug, Clone)]
pub struct NodeRetryPayload {
    pub execution_id: String,
    pub failed_node: String,
    pub attempt: i64,
    pub budget: i64,
}

impl WorkerError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> WorkerError {
        WorkerError {
            kind,
            message: message.into(),
            node_retry: None,
        }
    }

    pub fn transient(message: impl Into<String>) -> WorkerError {
        WorkerError::new(ErrorKind::Transient, message)
    }

    pub fn node_retryable(p: NodeRetryPayload) -> WorkerError {
        WorkerError {
            kind: ErrorKind::NodeRetryable,
            message: format!(
                "retryable node failure below budget (execution_id={}, node={}, attempt={}, \
                 budget={})",
                p.execution_id, p.failed_node, p.attempt, p.budget
            ),
            node_retry: Some(p),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The NodeRetryable payload, when this error is one.
    pub fn node_retry(&self) -> Option<&NodeRetryPayload> {
        self.node_retry.as_ref()
    }

    /// Terminal error name for worker-judged terminals (§5.4 error_type);
    /// the state row's error.error_type is this string for worker-judged
    /// paths, `code` falls back to -500 (flowErrorCode).
    pub fn worker_error_type(&self) -> Option<&'static str> {
        match self.kind {
            ErrorKind::FlowNotFound => Some("flow_not_found"),
            _ => None,
        }
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for WorkerError {}

impl From<BackendError> for WorkerError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::BusyGroup(m) => WorkerError::new(ErrorKind::BusyGroup, m),
            other => WorkerError::transient(other.to_string()),
        }
    }
}

pub use plaita_engine::error::CANCELLED_ERROR_CODE;

/// plaita's `_FLOW_ERROR_CODE` fallback (§5.4: code=-500 when neither a
/// node-level nor handler error code is available).
pub const FLOW_ERROR_CODE: i64 = -500;

/// The terminal `state.error` dict (errors.go terminalErrorState shape):
/// `{message, code, error_type, details?, failed_node?}`.
pub fn terminal_error_state(
    error_type: &str,
    code: i64,
    message: &str,
    details: Option<serde_json::Value>,
    failed_node: Option<&str>,
) -> serde_json::Value {
    let mut out = serde_json::json!({
        "message": message,
        "code": code,
        "error_type": error_type,
    });
    if let Some(d) = details {
        out["details"] = d;
    }
    if let Some(node) = failed_node {
        if !node.is_empty() {
            out["failed_node"] = serde_json::Value::String(node.to_string());
        }
    }
    out
}
