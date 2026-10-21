//! The flow execution engine: step loop, node retry/timeout/error policies,
//! event callbacks (port of plaita-go `pkg/workflow/workflow.go` + retry.go
//! + timeout.go + event.go).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::context::{RuntimeCtx, SharedCtx};
use crate::error::{ErrorClass, FlowError, NodeError};
use crate::model::{Flow, HandleStrategy, Node};
use crate::nodes::{self, StepOutcome};

/// Engine events (in-process callbacks, mirroring plaita-go event.go).
#[derive(Debug, Clone)]
pub enum Event {
    FlowStart {
        flow_id: String,
    },
    FlowEnd {
        flow_id: String,
        result: Option<Value>,
        error: Option<String>,
    },
    NodeStart {
        flow_id: String,
        node_id: String,
        node_type: String,
    },
    NodeEnd {
        flow_id: String,
        node_id: String,
        node_type: String,
        result: Option<Value>,
        error: Option<String>,
    },
}

pub type EventSink = Arc<dyn Fn(&Event) + Send + Sync>;

/// Host-side wiring for one execution tree: the distributed execution id and
/// the event listeners. Shared (by `Arc`) across flow/branch threads.
#[derive(Clone, Default)]
pub struct Host {
    pub execution_id: Option<String>,
    events: Arc<Vec<EventSink>>,
}

impl Host {
    pub fn new() -> Host {
        Host::default()
    }

    pub fn with_execution_id(execution_id: impl Into<String>) -> Host {
        Host {
            execution_id: Some(execution_id.into()),
            events: Arc::new(Vec::new()),
        }
    }

    /// Registers an event listener (builder style).
    pub fn listen(mut self, sink: EventSink) -> Host {
        let mut sinks = (*self.events).clone();
        sinks.push(sink);
        self.events = Arc::new(sinks);
        self
    }

    pub(crate) fn emit(&self, event: Event) {
        for sink in self.events.iter() {
            sink(&event);
        }
    }
}

const FLOW_RETRY_DELAY: Duration = Duration::from_millis(10);
const NODE_BACKOFF_BASE: Duration = Duration::from_secs(1);
const NODE_BACKOFF_CAP: Duration = Duration::from_secs(30);

impl Flow {
    /// Executes the flow with the given input.
    ///
    /// The flow's own error policy `retryTimes` multiplies WHOLE-FLOW
    /// re-executions (fixed 10ms delay) for transient faults only — the
    /// plaita exponential backoff and per-node retry classification live at
    /// the node level. An explicit zero budget (or an already-expired
    /// deadline) fails immediately instead of letting budget-ignoring node
    /// bodies run to completion (plaita#138).
    pub fn execute(&self, input: &Value, host: &Host) -> Result<Value, FlowError> {
        let flow_deadline = self.timeout.map(|t| Instant::now() + t);
        let mut attempt: u64 = 0;
        loop {
            let rc = RuntimeCtx::new_execution(
                None,
                input.clone(),
                Some(&self.id),
                self.context
                    .global
                    .as_ref()
                    .and_then(|g| g.as_object().cloned()),
                self.expose_env.as_ref(),
                host.execution_id.as_deref(),
                flow_deadline,
            );
            host.emit(Event::FlowStart {
                flow_id: self.id.clone(),
            });
            let result: Result<Value, FlowError> = match flow_deadline {
                Some(dl) if Instant::now() >= dl => Err(FlowError::FlowTimeout {
                    message: "context deadline exceeded".to_string(),
                }),
                _ => self.run_nodes(&rc, host, flow_deadline),
            };
            let (result_value, error_text) = match &result {
                Ok(v) => (Some(v.clone()), None),
                Err(e) => (None, Some(e.to_string())),
            };
            host.emit(Event::FlowEnd {
                flow_id: self.id.clone(),
                result: result_value,
                error: error_text,
            });
            match result {
                Ok(v) => return Ok(v),
                Err(e) if e.is_flow_budget_timeout() => {
                    // The flow budget fired: the flow-level timeout policy
                    // decides (continue → nil result, continue-with →
                    // default, abort/none → the error propagates).
                    match self.timeout_handler.strategy {
                        Some(HandleStrategy::Continue) => return Ok(Value::Null),
                        Some(HandleStrategy::ContinueWith) => {
                            return Ok(self
                                .timeout_handler
                                .default_value
                                .clone()
                                .unwrap_or(Value::Null))
                        }
                        _ => {
                            return Err(e.wrap_handler(
                                self.timeout_handler.code,
                                &self.timeout_handler.message,
                            ))
                        }
                    }
                }
                Err(e) if e.is_retryable() && attempt < self.error_handler.retry_times => {
                    if sleep_respecting(flow_deadline, FLOW_RETRY_DELAY).is_err() {
                        return Err(FlowError::FlowTimeout {
                            message: "context deadline exceeded".to_string(),
                        });
                    }
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Sub-flow entry: executes with an already-prepared context (child /
    /// loop body / parallel branch), keeping the flow-level retry loop.
    pub fn execute_on(
        &self,
        rc: &SharedCtx,
        host: &Host,
        _input: &Value,
    ) -> Result<Value, FlowError> {
        let flow_deadline = rc.flow_deadline;
        let mut attempt: u64 = 0;
        loop {
            host.emit(Event::FlowStart {
                flow_id: self.id.clone(),
            });
            let result: Result<Value, FlowError> = match flow_deadline {
                Some(dl) if Instant::now() >= dl => Err(FlowError::FlowTimeout {
                    message: "context deadline exceeded".to_string(),
                }),
                _ => self.run_nodes(rc, host, flow_deadline),
            };
            let (result_value, error_text) = match &result {
                Ok(v) => (Some(v.clone()), None),
                Err(e) => (None, Some(e.to_string())),
            };
            host.emit(Event::FlowEnd {
                flow_id: self.id.clone(),
                result: result_value,
                error: error_text,
            });
            match result {
                Ok(v) => return Ok(v),
                Err(e) if e.is_flow_budget_timeout() => match self.timeout_handler.strategy {
                    Some(HandleStrategy::Continue) => return Ok(Value::Null),
                    Some(HandleStrategy::ContinueWith) => {
                        return Ok(self
                            .timeout_handler
                            .default_value
                            .clone()
                            .unwrap_or(Value::Null))
                    }
                    _ => {
                        return Err(e.wrap_handler(
                            self.timeout_handler.code,
                            &self.timeout_handler.message,
                        ))
                    }
                },
                Err(e) if e.is_retryable() && attempt < self.error_handler.retry_times => {
                    if sleep_respecting(flow_deadline, FLOW_RETRY_DELAY).is_err() {
                        return Err(FlowError::FlowTimeout {
                            message: "context deadline exceeded".to_string(),
                        });
                    }
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Advances the flow from the start node (the shared step bookkeeping
    /// for the start path; the engine has no resume/checkpoint entry — the
    /// worker layer owns that in plaita-go).
    fn run_nodes(
        &self,
        rc: &SharedCtx,
        host: &Host,
        flow_deadline: Option<Instant>,
    ) -> Result<Value, FlowError> {
        let Some(mut node) = self.find_start() else {
            return Err(FlowError::Other("start node not found".to_string()));
        };
        loop {
            let outcome = self.execute_node(node, rc, host, flow_deadline)?;
            if node.node_type == "end" {
                return Ok(outcome.result);
            }
            rc.set_node_result(&node.id, outcome.result);
            rc.set_last_node(&node.id);
            let next = outcome
                .next
                .ok_or_else(|| FlowError::Other("workflow node not found".to_string()))?;
            node = self
                .find_node(&next)
                .ok_or_else(|| FlowError::Other("workflow node not found".to_string()))?;
        }
    }

    /// One node: timeout budget, retry loop, error/timeout policies, `$ERROR`
    /// bookkeeping and `$BRANCH` recording for branching nodes.
    fn execute_node(
        &self,
        node: &Node,
        rc: &SharedCtx,
        host: &Host,
        flow_deadline: Option<Instant>,
    ) -> Result<StepOutcome, FlowError> {
        let node_deadline: Option<Instant> = match (flow_deadline, node.timeout) {
            (Some(fd), Some(nt)) => {
                Some(Instant::now() + nt.min(fd.saturating_duration_since(Instant::now())))
            }
            (Some(fd), None) => Some(fd),
            (None, Some(nt)) => Some(Instant::now() + nt),
            (None, None) => None,
        };

        let mut attempt: u64 = 0;
        let mut outcome: Option<StepOutcome> = None;
        let mut final_err: Option<NodeError> = None;
        loop {
            rc.reset_current();
            host.emit(Event::NodeStart {
                flow_id: self.id.clone(),
                node_id: node.id.clone(),
                node_type: node.node_type.clone(),
            });
            let body: Result<StepOutcome, NodeError> = match node_deadline {
                Some(dl) if Instant::now() >= dl => {
                    Err(NodeError::timeout("context deadline exceeded"))
                }
                _ => nodes::execute_node_body(node, rc, host, node_deadline),
            };
            let (result_value, error_text) = match &body {
                Ok(o) => (Some(o.result.clone()), None),
                Err(e) => (None, Some(e.to_string())),
            };
            host.emit(Event::NodeEnd {
                flow_id: self.id.clone(),
                node_id: node.id.clone(),
                node_type: node.node_type.clone(),
                result: result_value,
                error: error_text,
            });

            match body {
                Ok(o) => {
                    outcome = Some(o);
                    break;
                }
                Err(ne) if ne.class == ErrorClass::Timeout => {
                    if flow_budget_binding(rc, node_deadline) {
                        // The flow-level budget is the binding constraint —
                        // a FLOW_ERROR, not the node's timeout to handle.
                        final_err = Some(ne);
                        break;
                    }
                    match node.timeout_policy.strategy {
                        Some(HandleStrategy::Continue) => {
                            outcome = Some(StepOutcome {
                                result: Value::Null,
                                next: node.next.clone(),
                            });
                            break;
                        }
                        Some(HandleStrategy::ContinueWith) => {
                            outcome = Some(StepOutcome {
                                result: node
                                    .timeout_policy
                                    .default_value
                                    .clone()
                                    .unwrap_or(Value::Null),
                                next: node.next.clone(),
                            });
                            break;
                        }
                        _ => {
                            final_err = Some(ne);
                            break;
                        }
                    }
                }
                Err(ne) if !ne.is_retryable() => {
                    final_err = Some(ne);
                    break;
                }
                Err(ne) if attempt >= node.error_policy.retry_times => {
                    final_err = Some(ne);
                    break;
                }
                Err(_) => {
                    // plaita's exponential backoff: 1s × 2^n capped at 30s
                    let delay = NODE_BACKOFF_BASE
                        .saturating_mul(1u32.saturating_pow(attempt as u32))
                        .min(NODE_BACKOFF_CAP);
                    if sleep_respecting(node_deadline, delay).is_err() {
                        final_err = Some(NodeError::timeout("context deadline exceeded"));
                        break;
                    }
                    attempt += 1;
                }
            }
        }

        if let Some(ne) = final_err {
            let flow_err = FlowError::node(&node.id, &node.node_type, ne.clone());
            // 继续执行策略：把错误信息存到 $ERROR 供下游引用
            if matches!(
                node.error_policy.strategy,
                Some(HandleStrategy::Continue) | Some(HandleStrategy::ContinueWith)
            ) {
                rc.reset_current();
                rc.set_node_error(
                    &node.id,
                    serde_json::json!({
                        "code": ne.code,
                        "message": ne.message,
                        "info": Value::Null,
                    }),
                );
            }
            return match node.error_policy.strategy {
                Some(HandleStrategy::Continue) => Ok(StepOutcome {
                    result: Value::Null,
                    next: node.next.clone(),
                }),
                Some(HandleStrategy::ContinueWith) => Ok(StepOutcome {
                    result: node
                        .error_policy
                        .default_value
                        .clone()
                        .unwrap_or(Value::Null),
                    next: node.next.clone(),
                }),
                _ => Err(flow_err.wrap_handler(node.error_policy.code, &node.error_policy.message)),
            };
        }

        let o = outcome.expect("loop exits with either outcome or final_err");
        // $BRANCH: the branch taken by the most recent branching node
        // (plaita runner: only on a successful attempt; the switch/if result
        // IS the resolved branch target).
        if matches!(node.node_type.as_str(), "switch" | "if") {
            if let Some(branch) = o.result.as_str() {
                rc.set_last_branch(Value::String(branch.to_string()));
            }
        }
        Ok(o)
    }
}

/// Reports whether a context-deadline error was caused by the flow-level
/// budget rather than the node's own: the recorded flow deadline has passed
/// and the node deadline is not strictly tighter (plaita
/// time_limit_by_flow → FLOW_ERROR).
fn flow_budget_binding(rc: &SharedCtx, node_deadline: Option<Instant>) -> bool {
    let Some(fd) = rc.flow_deadline else {
        return false;
    };
    if Instant::now() < fd {
        return false;
    }
    if let Some(nd) = node_deadline {
        if nd < fd {
            return false;
        }
    }
    true
}

/// Sleeps for `d`, returning Err if the deadline would be exceeded.
pub(crate) fn sleep_respecting(deadline: Option<Instant>, d: Duration) -> Result<(), ()> {
    if let Some(dl) = deadline {
        let now = Instant::now();
        if now >= dl {
            return Err(());
        }
        let d = d.min(dl.saturating_duration_since(now));
        std::thread::sleep(d);
        Ok(())
    } else {
        std::thread::sleep(d);
        Ok(())
    }
}
