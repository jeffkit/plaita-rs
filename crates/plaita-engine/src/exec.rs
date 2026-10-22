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

/// Host-side wiring for one execution tree: the distributed execution id,
/// the event listeners and the fire-and-forget branch join handles. Shared
/// (by `Arc`) across flow/branch threads.
#[derive(Clone, Default)]
pub struct Host {
    pub execution_id: Option<String>,
    events: Arc<Vec<EventSink>>,
    background: Arc<std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>>,
}

impl Host {
    pub fn new() -> Host {
        Host::default()
    }

    pub fn with_execution_id(execution_id: impl Into<String>) -> Host {
        Host {
            execution_id: Some(execution_id.into()),
            events: Arc::new(Vec::new()),
            background: Default::default(),
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

    /// Registers a fire-and-forget parallel branch thread so the host can
    /// drain before exiting (Go's worker is long-lived so goroutine death at
    /// process exit never mattered; a short-lived CLI host kills in-flight
    /// background branches unless it drains).
    pub(crate) fn track_background(&self, handle: std::thread::JoinHandle<()>) {
        self.background
            .lock()
            .expect("background lock")
            .push(handle);
    }

    /// Waits for all fire-and-forget branches to settle. Call before process
    /// exit when background branch side effects matter.
    pub fn join_background(&self) {
        let handles: Vec<_> =
            std::mem::take(&mut *self.background.lock().expect("background lock"));
        for h in handles {
            let _ = h.join();
        }
    }
}

const NODE_BACKOFF_BASE: Duration = Duration::from_secs(1);
const NODE_BACKOFF_CAP: Duration = Duration::from_secs(30);

impl Flow {
    /// Executes the flow with the given input.
    ///
    /// The flow runs EXACTLY ONCE: plaita has no flow-level error handler —
    /// the authoritative `Flow` model (core/flow.py) has no such field and a
    /// top-level `errorHandler` key in a flow document is an unknown key
    /// (warning + ignored). The Go whole-flow `retryTimes` re-run (×(1+n)
    /// re-executions, replaying every side-effecting node, letting a child's
    /// transient error burn the flow budget) is a deliberate Go-ism this
    /// engine does not implement (B4 r7). Node-level retry/backoff/error
    /// policies are unaffected; an explicit zero budget (or an
    /// already-expired deadline) fails immediately instead of letting
    /// budget-ignoring node bodies run to completion (plaita#138).
    pub fn execute(&self, input: &Value, host: &Host) -> Result<Value, FlowError> {
        // 有意分歧（与 Go 对照）：Go 在 Execute 入口 prepare 一次运行时上下文、
        // 流级重试轮复用（上一轮的 $NODE/$ERROR 残留进重试轮）；本引擎每轮
        // 重建干净上下文。依赖该残留的 flow 属于反模式（见 exec 模块注释）。
        //
        // 执行级缺省预算（plaita#267，Python strategies.py:144
        // `execution_deadline_ms`）：显式 timeout 缺省时补 1800s 总预算
        // （PLAITA_EXECUTION_DEADLINE_SECONDS 可调，非法值回退默认并告警），
        // 无 timeout 的 flow 不再无限占用执行线程。显式 `PT0S` 仍是显式零
        // 预算（最严，plaita#138），不会被当成缺省。
        //
        // 装载期（normalize_timeout）已拒掉超平台 Instant 值域的预算；这里的
        // checked_add 是程序化构造 Flow 的防御面——裸加在两种 profile 都是
        // 进程级 panic（r1 移交点），具名错误 fail-closed。
        let budget = self
            .timeout
            .unwrap_or_else(crate::budget::default_execution_deadline);
        let flow_deadline = match Instant::now().checked_add(budget) {
            Some(dl) => Some(dl),
            None => {
                return Err(FlowError::Other(format!(
                    "flow timeout {} exceeds the platform instant range: the budget is \
                     representable but no runtime deadline can be formed from it",
                    crate::duration::to_iso(budget)
                )))
            }
        };
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
        // 流级预算到期 = 终态（#14）：timeoutHandler 不拦截。
        // Python 权威侧无流级 handler，预算到期恒 ExecutionDeadlineError；
        // continue/continue-with 会把预算违规洗成成功值（p1c 负控面），
        // execute_on 同。
        match result {
            Ok(v) => Ok(v),
            Err(e) if matches!(e, FlowError::FlowTimeout { .. }) => Err(e),
            // 节点自有超时按 abort 传播时仍走流级 timeoutHandler（Go 移植
            // 面；#14 只裁「预算到期」这一面）
            Err(e) if e.is_flow_budget_timeout() => {
                // The node's own timeout propagated: the flow-level timeout
                // policy decides (continue → nil result, continue-with →
                // default, abort/none → the error propagates).
                match self.timeout_handler.strategy {
                    Some(HandleStrategy::Continue) => Ok(Value::Null),
                    Some(HandleStrategy::ContinueWith) => Ok(self
                        .timeout_handler
                        .default_value
                        .clone()
                        .unwrap_or(Value::Null)),
                    _ => {
                        Err(e
                            .wrap_handler(self.timeout_handler.code, &self.timeout_handler.message))
                    }
                }
            }
            // 其余错误一律终态直抛（无流级重试——单次执行契约，见上）
            Err(e) => Err(e),
        }
    }

    /// Sub-flow entry: executes with an already-prepared context (child /
    /// loop body / parallel branch). Single run, like [`Flow::execute`] —
    /// no flow-level retryTimes re-dispatch (the Go-ism is not implemented;
    /// a branch failure surfaces to its node once).
    pub fn execute_on(
        &self,
        rc: &SharedCtx,
        host: &Host,
        _input: &Value,
    ) -> Result<Value, FlowError> {
        let flow_deadline = rc.flow_deadline;
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
            Ok(v) => Ok(v),
            // 流级预算到期 = 终态（#14），同 execute：timeoutHandler
            // 不拦截预算违规。
            Err(e) if matches!(e, FlowError::FlowTimeout { .. }) => Err(e),
            // 节点自有超时按 abort 传播时仍走流级 timeoutHandler（Go
            // 移植面；#14 只裁「预算到期」这一面）
            Err(e) if e.is_flow_budget_timeout() => match self.timeout_handler.strategy {
                Some(HandleStrategy::Continue) => Ok(Value::Null),
                Some(HandleStrategy::ContinueWith) => Ok(self
                    .timeout_handler
                    .default_value
                    .clone()
                    .unwrap_or(Value::Null)),
                _ => Err(e.wrap_handler(self.timeout_handler.code, &self.timeout_handler.message)),
            },
            // 其余错误一律终态直抛（无流级重试——单次执行契约，见 execute）
            Err(e) => Err(e),
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
        // (Some(fd), Some(nt)) 的 min 项受 fd 自身可表示约束，裸加安全；
        // (None, Some(nt)) 走 checked——节点预算在装载期已被 24h cap 关死，
        // 这里只防程序化构造：溢出意味着预算大到等效无界，回落无 deadline
        // 与 min 语义一致。
        // 单点取 now（#14 归属修正）：先前两次 Instant::now() 使
        // nd = t1 + (fd - t2) = fd - ε（ε 为取时差，恒正），归属判定
        // 的 nd < fd 恒判成节点预算、节点 timeoutHandler 被咨询。单点
        // 取时流预算更紧则 nd 与 fd 精确相等，归属退化为静态预算比较
        // （plaita runner.py:631-634：更紧的配置预算决定归属）。
        let node_deadline: Option<Instant> = {
            let now = Instant::now();
            match (flow_deadline, node.timeout) {
                (Some(fd), Some(nt)) => Some(now + nt.min(fd.saturating_duration_since(now))),
                (Some(fd), None) => Some(fd),
                (None, Some(nt)) => now.checked_add(nt),
                (None, None) => None,
            }
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
                    //（底数是 2——先前写成 1^attempt 恒等于 1s，退避失效）
                    let delay = NODE_BACKOFF_BASE
                        .saturating_mul(2u32.saturating_pow((attempt as u32).min(31)))
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
            // 流级预算到期是终态（#14）：先于节点 timeout/error 策略链
            // 直抛。Python 权威（runner.py:655-679）里预算到期具名且不可
            // 吞——「否则一个 continue 策略就能把 deadline 变成摆设」。
            // 归属由 flow_budget_binding 判定（流预算更紧 ⇒ nd==fd，见上）。
            if ne.class == ErrorClass::Timeout && flow_budget_binding(rc, node_deadline) {
                return Err(FlowError::FlowTimeout {
                    message: format!(
                        "flow execution timed out: flow budget exceeded while running node \
                         {node_id:?} ({node_type})",
                        node_id = node.id,
                        node_type = node.node_type,
                    ),
                });
            }
            let mut flow_err = FlowError::node(&node.id, &node.node_type, ne.clone());
            // 超时 abort 的包裹来源是 timeoutHandler（Go 先以它包裹，
            // errorHandler 的包裹遇已包裹错误会跳过）；两处都没配则维持裸错
            //（预算到期的 Timeout 已在上面终态返回，走到这里的 Timeout
            // 一律归属节点自有预算）
            if ne.class == ErrorClass::Timeout {
                flow_err =
                    flow_err.wrap_handler(node.timeout_policy.code, &node.timeout_policy.message);
            }
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
