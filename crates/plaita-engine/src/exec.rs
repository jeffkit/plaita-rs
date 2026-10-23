//! The flow execution engine: step loop, node retry/timeout/error policies,
//! event callbacks (port of plaita-go `pkg/workflow/workflow.go` + retry.go
//! + timeout.go + event.go).

use std::sync::atomic::{AtomicBool, Ordering};
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
/// the event listeners, the fire-and-forget branch join handles and the
/// cancellation signals. Shared (by `Arc`) across flow/branch threads —
/// clones see the same flags, so cancelling any handle in the execution
/// tree propagates to the whole chain (Python ExecutionContext 父子共享
/// 同一对 Event 的对应面).
#[derive(Clone, Default)]
pub struct Host {
    pub execution_id: Option<String>,
    events: Arc<Vec<EventSink>>,
    background: Arc<std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>>,
    /// 节点级协作取消信号（Python `cancel_event`）：作用域=当前节点，进入
    /// 新节点前复位；code 沙箱等待循环观察到置位当场杀进程组。
    cancel_event: Arc<AtomicBool>,
    /// 执行级取消意图（Python `cancel_requested`）：粘滞、永不复位，
    /// 节点边界消费。
    cancel_requested: Arc<AtomicBool>,
    /// 挂起型节点的唤醒订阅注册表（宿主面；Go `SuspendOptions.Subscriptions`
    /// 同形）。声明了事件订阅的节点挂起时**必须**有注册表——「无唤醒源的
    /// 挂起」被具名拒绝（Python 订阅失败禁止挂起同款）。
    subscriptions: Option<Arc<dyn SubscriptionRegistry>>,
    /// 多租户路由键：订阅注册时携带，恢复注销时优先用挂起态记录值。
    tenant_id: String,
}

/// 一次挂起的唤醒订阅请求（plaita EventSubscription 核心字段；Go
/// `SubscriptionRequest` 同形）。registry 在 register 时填 `subscription_id`。
#[derive(Debug, Clone)]
pub struct SubscriptionRequest {
    pub subscription_id: String,
    pub event_type: String,
    pub filter: Value,
    pub node_id: String,
    pub correlation_id: String,
    pub flow_id: String,
    pub tenant_id: String,
    /// >0 = 订阅超时（到期由宿主投递 resume_type=timeout 载荷；引擎不自动触发）。
    pub timeout_seconds: f64,
}

/// 宿主侧唤醒订阅存储（Redis 版在 worker 层；引擎测试用内存假件）。
pub trait SubscriptionRegistry: Send + Sync {
    fn register(&self, req: &mut SubscriptionRequest) -> Result<(), String>;
    fn unregister(&self, subscription_id: &str, tenant_id: &str) -> Result<(), String>;
}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host")
            .field("execution_id", &self.execution_id)
            .field("tenant_id", &self.tenant_id)
            .field("registry_configured", &self.subscriptions.is_some())
            .finish_non_exhaustive()
    }
}

impl Host {
    pub fn new() -> Host {
        Host::default()
    }

    pub fn with_execution_id(execution_id: impl Into<String>) -> Host {
        Host {
            execution_id: Some(execution_id.into()),
            ..Host::default()
        }
    }

    /// Registers the wake-up subscription store (builder style). Required
    /// before executing a flow whose suspending nodes declare subscriptions.
    pub fn with_subscriptions(mut self, registry: Arc<dyn SubscriptionRegistry>) -> Host {
        self.subscriptions = Some(registry);
        self
    }

    /// Sets the tenant routing key carried into subscription registrations.
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Host {
        self.tenant_id = tenant_id.into();
        self
    }

    pub(crate) fn subscription_registry(&self) -> Option<&Arc<dyn SubscriptionRegistry>> {
        self.subscriptions.as_ref()
    }

    pub(crate) fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Requests cancellation of this execution (idempotent — Python
    /// `FlowExecution.cancel()` 同形). Sets both signals:
    /// - `cancel_event`: the node-scoped cooperative signal — the code
    ///   sandbox wait loop observes it and kills the process tree on sight;
    /// - `cancel_requested`: the sticky execution-level intent — the engine
    ///   refuses to continue at node boundaries (named cancel error,
    ///   bypassing every retry/error policy). Default semantics: stop at the
    ///   step boundary; in-flight non-sandbox nodes run to completion.
    pub fn cancel(&self) {
        self.cancel_event.store(true, Ordering::SeqCst);
        self.cancel_requested.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::SeqCst)
    }

    /// The node-scoped cooperative flag (consumed by the code sandbox wait
    /// loop); cleared on every node entry so a leftover signal never kills
    /// the next node's sidecar (Python runner.py:592-600).
    pub(crate) fn cancel_event_flag(&self) -> &AtomicBool {
        &self.cancel_event
    }

    pub(crate) fn clear_cancel_event(&self) {
        self.cancel_event.store(false, Ordering::SeqCst);
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
        // 流上下文一次性新建（B4 r7 起无流级重试轮——「重试轮复用残留」的
        // Go 对照面随整流重投撤除而消失；子流/分支走 execute_on 共享 rc）。
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
        // 挂起不是错误：Go suspend 路径的 FlowEnd 以 nil result/nil error
        // 上报（"a suspension is reported without an error"）。
        let (result_value, error_text) = match &result {
            Ok(v) => (Some(v.clone()), None),
            Err(FlowError::Suspended(_)) => (None, None),
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
            // 执行级取消 = 终态（plaita#267 波次③）：节点 errorHandler/
            // timeoutHandler 与流级 timeoutHandler 均不拦截——取消是执行级
            // 意图，不是节点错误（Python FlowCancelledException 不经任何
            // handler 分发）。
            Err(e) if matches!(e, FlowError::Cancelled { .. }) => Err(e),
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
        let flow_deadline = rc.flow_deadline();
        host.emit(Event::FlowStart {
            flow_id: self.id.clone(),
        });
        let Some(start) = self.find_start() else {
            return Err(FlowError::Other("start node not found".to_string()));
        };
        self.advance_and_finalize(start, rc, host, flow_deadline)
    }

    /// 从给定节点推进到 end 并套用本 flow 的收尾链（FlowTimeout 终态 /
    /// 节点自有超时 abort 的 timeoutHandler 咨询 / FlowEnd 发射）——execute_on
    /// 的主体，也被恢复续跑循环逐帧复用（验证报告 Q8.1：恢复会话的内层失败
    /// 必须走与首跑相同的本帧收尾链，不再直抛宿主）。不发 FlowStart（恢复
    /// 会话约定：无 FlowStart，FlowEnd 可重复、最后一次为准）。
    pub(crate) fn advance_and_finalize(
        &self,
        start: &Node,
        rc: &SharedCtx,
        host: &Host,
        flow_deadline: Option<Instant>,
    ) -> Result<Value, FlowError> {
        let result: Result<Value, FlowError> = match flow_deadline {
            Some(dl) if Instant::now() >= dl => Err(FlowError::FlowTimeout {
                message: "context deadline exceeded".to_string(),
            }),
            _ => self.advance_from(start, rc, host, flow_deadline),
        };
        let (result_value, error_text) = match &result {
            Ok(v) => (Some(v.clone()), None),
            Err(FlowError::Suspended(_)) => (None, None),
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
            // 执行级取消 = 终态，同 execute：不经任何 handler 分发
            //（plaita#267 波次③）。
            Err(e) if matches!(e, FlowError::Cancelled { .. }) => Err(e),
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
        let Some(node) = self.find_start() else {
            return Err(FlowError::Other("start node not found".to_string()));
        };
        self.advance_from(node, rc, host, flow_deadline)
    }

    /// 共享步进体：从给定节点推进到 end/挂起/失败（execute 的 start 入口与
    /// resume 的后继入口共用同一套步进记账、取消门与挂起检测——plaita-go
    /// runNodes 三入口同形注释的移植面）。
    fn advance_from(
        &self,
        start: &Node,
        rc: &SharedCtx,
        host: &Host,
        flow_deadline: Option<Instant>,
    ) -> Result<Value, FlowError> {
        let mut node = start;
        loop {
            // 节点边界取消门（plaita#267 波次③；Python runner.py:602-613
            // 同形）：粘滞的 cancel_requested 在本节点入口直接拒绝执行，
            // 先于重试与 errorHandler——取消不被任何节点策略吞掉。默认
            // 语义=步界取消：在途非沙箱节点跑完，下一节点具名拒绝。
            if host.is_cancel_requested() {
                return Err(cancelled_at(node, "before running"));
            }
            let outcome = match self.execute_node(node, rc, host, flow_deadline) {
                Ok(o) => o,
                // 嵌套挂起穿透（v1）：内层挂起沿子节点体上抛到本层——把本帧
                // 压入其调用栈后继续向上。挂起不是本层节点的失败。
                Err(FlowError::Suspended(mut inner)) => {
                    inner.push_frame(self.clone(), rc.clone(), node.id.clone());
                    return Err(FlowError::Suspended(inner));
                }
                Err(e) => return Err(e),
            };
            // plaita `is_suspending`：挂起检查先于 end 判断与推进（挂起节
            // 点不可能是 end；顺序是 Go runNodes 的移植面）。节点结果
            // （status pending + 订阅绑定）由 suspend_flow 落进 $NODE，
            // 恢复准入读的正是它。
            if node.kind.is_suspending() {
                return Err(self.suspend_flow(rc, host, node, outcome));
            }
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

    /// 挂起边界（plaita-go `Workflow.suspend` 移植）：校验 pending 状态、
    /// 注册唤醒订阅（宿主注册表必需——无唤醒源的挂起具名拒绝）、把
    /// subscription_id/subscription_tenant 写进挂起态并落 $NODE，返回
    /// 携带执行现场的 [`FlowError::Suspended`]。只有根执行可挂起（嵌套
    /// child/loop/parallel 里的挂起点其恢复位置无法用执行状态表达）。
    fn suspend_flow(
        &self,
        rc: &SharedCtx,
        host: &Host,
        node: &Node,
        outcome: StepOutcome,
    ) -> FlowError {
        // 作用域闸（嵌套挂起 v1）：child/reference 调用链允许（恢复位置由
        // 调用栈表达）；迭代帧/分支帧具名拒绝（恢复需要迭代状态快照，v2）。
        if let Some(blocker) = rc.frame_chain_blocker() {
            return FlowError::SuspendRejected {
                message: format!(
                    "node {:?} suspends inside a {:?} frame: suspension v1 supports only \
                     child/reference call chains (iteration-state snapshotting is a future \
                     milestone; see docs/PROGRAM.md)",
                    node.id,
                    blocker.as_str()
                ),
            };
        }
        let mut state = outcome.result;
        let status = state.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "pending" {
            return FlowError::Other(format!(
                "suspending node {:?} returned status {:?}, want pending",
                node.id, status
            ));
        }
        let Some(registry) = host.subscription_registry() else {
            return FlowError::Other(format!(
                "node {:?} declares an event subscription but no subscription registry is \
                 configured (attach one with Host::with_subscriptions; plaita refuses to \
                 suspend without an active wake-up source)",
                node.id
            ));
        };
        let mut req = match crate::nodes::suspend_spec(node, &state, rc, self, host) {
            Ok(req) => req,
            Err(e) => return FlowError::Other(format!("suspend spec for node {:?}: {e}", node.id)),
        };
        if req.node_id.is_empty() {
            req.node_id = node.id.clone();
        }
        if req.correlation_id.is_empty() {
            req.correlation_id = rc.execution_id();
        }
        if req.flow_id.is_empty() {
            req.flow_id = self.id.clone();
        }
        if req.tenant_id.is_empty() {
            req.tenant_id = host.tenant_id().to_string();
        }
        if let Err(e) = registry.register(&mut req) {
            return FlowError::Other(format!(
                "event subscription failed for node {:?}; refusing to suspend without an \
                 active subscription: {e}",
                node.id
            ));
        }
        if req.subscription_id.is_empty() {
            return FlowError::Other(format!(
                "subscription registry did not assign an id for node {:?}",
                node.id
            ));
        }
        if let Value::Object(map) = &mut state {
            map.insert(
                "subscription_id".into(),
                Value::String(req.subscription_id.clone()),
            );
            map.insert(
                "subscription_tenant".into(),
                Value::String(req.tenant_id.clone()),
            );
        }
        // 只落 $NODE（恢复准入读它）；$LAST_NODE 维持前一个完成节点
        //（Go runNodes 挂起路径不写 last node 的移植面）。
        rc.set_node_result(&node.id, state);
        FlowError::Suspended(Box::new(SuspendedExecution {
            suspended_node: node.id.clone(),
            flow: self.clone(),
            rc: rc.clone(),
            host: host.clone(),
            call_stack: Vec::new(),
        }))
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
        // cancel_event 的作用域是当前节点（Python runner.py:592-600 同形）：
        // 上一个节点遗留的取消信号在进入本节点前复位——否则后续每个 code
        // 节点的沙箱都会被误杀。
        host.clear_cancel_event();

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
            // 每次尝试入口的取消门（Python runner.py:684-686 同形：挡住
            // 「取消恰好发生在退避结束与下一次尝试之间」的窗口；首轮在
            // run_nodes 边界门已拦，此处仅在重试轮可达）。
            if host.is_cancel_requested() {
                return Err(cancelled_at(node, "while running"));
            }
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
                // 取消优先于任何节点错误/重试分类（Python #267：节点在取消
                // 后抛出的异常不得被包装成节点错误或再重试一轮）——置于
                // Timeout/可重试分类之前，errorHandler 的 continue 策略
                // 永远接不到取消。
                Err(_ne) if host.is_cancel_requested() => {
                    return Err(cancelled_at(node, "while running"));
                }
                // 嵌套挂起穿透（v1）：先于一切策略链（预算归属/超时/重试/
                // errorHandler）原样上抛，与取消门同待遇——挂起不是节点失败。
                Err(ne) if ne.is_suspension() => {
                    return Err(FlowError::Suspended(
                        ne.suspension.expect("suspension carrier"),
                    ))
                }
                // 作用域闸拒绝（验证报告修正清单 #3）：同样不可被 errorHandler
                // continue 吞（error.rs 的 fail-closed 承诺）。
                Err(ne) if ne.class == ErrorClass::SuspendRejected => {
                    return Err(FlowError::SuspendRejected {
                        message: ne.message,
                    })
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
                    // 退避可被取消打断（Python _backoff_sleep_cancellable：
                    // 0.5s 分片轮询 cancel_requested，cancel 在 ≤1s 内打断
                    // 重试/退避）；deadline 语义与 sleep_respecting 一致。
                    match sleep_backoff_cancellable(host, node_deadline, delay) {
                        Backoff::Slept => attempt += 1,
                        Backoff::Expired => {
                            final_err = Some(NodeError::timeout("context deadline exceeded"));
                            break;
                        }
                        Backoff::Cancelled => {
                            return Err(cancelled_at(node, "while running"));
                        }
                    }
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

// -- suspend / resume（plaita-go suspend.go + plaita DistributedStrategy
//    _handle_resume 的移植面）------------------------------------------------

/// Resume carrier 类型（plaita `ResumeType` 的事件/超时/取消三态；continue/
/// retry 是非挂起状态的步进载体，对挂起节点具名拒绝）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeKind {
    Event,
    Timeout,
    Cancel,
}

impl ResumeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResumeKind::Event => "event",
            ResumeKind::Timeout => "timeout",
            ResumeKind::Cancel => "cancel",
        }
    }

    pub fn parse(s: &str) -> Option<ResumeKind> {
        match s {
            "event" => Some(ResumeKind::Event),
            "timeout" => Some(ResumeKind::Timeout),
            "cancel" => Some(ResumeKind::Cancel),
            _ => None,
        }
    }
}

/// 私有订阅绑定键（plaita `RESUME_BINDING_KEY`）：手动 resume 可用它把载体
/// 绑定到挂起期登记的那条订阅，不污染用户载荷——准入校验后剥掉。
pub const RESUME_BINDING_KEY: &str = "__plaita_subscription_id";

/// 一次挂起的执行现场：flow 定义 + 活上下文（内存中）+ 宿主接线 + 恢复
/// 位置。[`checkpoint_json`] 供观测/持久化取材；JSON 回放入口属 worker 层
///（本引擎只做进程内 resume，plaita-go 的 Checkpoint 序列化恢复是 worker
/// 接线面对应物）。
#[derive(Clone, Debug)]
pub struct SuspendedExecution {
    /// 挂起节点 id（最内层帧）。
    pub suspended_node: String,
    pub(crate) flow: Flow,
    pub(crate) rc: SharedCtx,
    pub(crate) host: Host,
    /// 调用栈（嵌套挂起 v1）：待返回的父帧，index 0 = 最内父帧，递增到根。
    /// 空栈 = 挂起发生在根流程。
    pub(crate) call_stack: Vec<CallFrame>,
}

/// 调用栈的一帧：child/reference 子节点完成后，其值回填的父执行现场。
#[derive(Clone, Debug)]
pub struct CallFrame {
    /// 父 flow 中的子节点 id。
    pub node_id: String,
    pub(crate) flow: Flow,
    pub(crate) rc: SharedCtx,
}

impl SuspendedExecution {
    /// 传播层（advance_from）压帧：本执行作为「待返回的调用帧」入栈。压入序
    /// = 传播序（最内父帧先入），恢复时按同序消费（最深帧先返回）。
    pub(crate) fn push_frame(&mut self, flow: Flow, rc: SharedCtx, node_id: String) {
        self.call_stack.push(CallFrame { node_id, flow, rc });
    }

    /// 调用栈路径（观测/持久化素材）：从最内父帧到根的 flow_id/node_id 链。
    pub fn call_stack_path(&self) -> Vec<(String, String)> {
        self.call_stack
            .iter()
            .map(|f| (f.flow.id.clone(), f.node_id.clone()))
            .collect()
    }
}

impl SuspendedExecution {
    /// Rebuild entry for the worker layer's checkpoint recovery (rs-worker
    /// design §7.4 E2): reassembles a suspended execution from the persisted
    /// parts — the flow (rebuilt via `ir::load`), the context (rebuilt via
    /// [`RuntimeCtx::from_snapshot_json`] + `shared()`), a freshly wired host
    /// and the suspended node id recorded in the checkpoint.
    ///
    /// Recovery itself is NOT re-implemented here: [`SuspendedExecution::resume`]
    /// (admission gates, `RESUME_BINDING_KEY` stripping, fresh budget,
    /// subscription unregister, NodeEnd, advance) is the single resume chain
    /// and the worker must reuse it instead of forking recovery logic. The
    /// flow budget stays unset — `resume` issues the fresh session budget.
    pub fn from_parts(
        flow: Flow,
        rc: SharedCtx,
        host: Host,
        suspended_node: impl Into<String>,
    ) -> SuspendedExecution {
        SuspendedExecution {
            suspended_node: suspended_node.into(),
            flow,
            rc,
            host,
        }
    }

    /// 挂起节点的存储状态（$NODE 表项：status pending + 唤醒契约 + 订阅绑定）。
    pub fn node_state(&self) -> Value {
        self.rc
            .snapshot_json()
            .get("nodes")
            .and_then(|n| n.get(&self.suspended_node))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// 全量上下文快照（诊断/持久化素材；`$NODE`/`$GLOBAL`/`$ENV`/...）。
    /// 嵌套挂起时附带 `call_stack`：从最内父帧到根的 flow_id/node_id 链。
    pub fn checkpoint_json(&self) -> Value {
        let mut snap = self.rc.snapshot_json();
        if !self.call_stack.is_empty() {
            if let Value::Object(map) = &mut snap {
                map.insert(
                    "call_stack".into(),
                    serde_json::json!(self
                        .call_stack
                        .iter()
                        .map(|f| serde_json::json!({
                            "flow_id": f.flow.id,
                            "node_id": f.node_id,
                        }))
                        .collect::<Vec<_>>()),
                );
            }
        }
        snap
    }

    /// 挂起节点的订阅 id（注销/绑定对拍用）。
    pub fn subscription_id(&self) -> Option<String> {
        self.node_state()
            .get("subscription_id")
            .and_then(|v| v.as_str())
            .map(String::from)
    }

    /// 对挂起节点施加决策载体并从其后继继续流程（plaita
    /// `DistributedStrategy._handle_resume` + `EventNode.resume` 的多态分发；
    /// plaita-go `Workflow.Resume` 的恢复+续跑一步到位——Python 分布式侧
    /// 只处理被恢复节点，两侧共有可比较面 = 恢复后节点状态）。
    ///
    /// 准入（ResumeGuardError 族，执行保持挂起）：节点存在且挂起型、
    /// status=="pending"、载体类型合法、订阅绑定（plaita#221：event/
    /// timeout 必须绑定挂起期登记的订阅，event_type 还须一致）。
    /// 恢复会话的执行预算按 Python worker 语义新鲜发放（flow 自有 timeout
    /// 或执行级缺省）——否则 24h 审批的恢复会落在早已过期的原 deadline 上。
    pub fn resume(self, resume_kind: ResumeKind, mut data: Value) -> Result<Value, FlowError> {
        let guard = |message: String| FlowError::ResumeGuard { message };
        // 恢复入口取消门（验证报告建议修 #9）：挂起期间 host.cancel() 过的
        // 执行，恢复被拒（决策载体未消费即弃——worker 层不得在 cancel 后投递
        // 恢复；引擎侧 fail-closed：副作用不应在取消后照发）。
        if self.host.is_cancel_requested() {
            return Err(FlowError::Cancelled {
                message: format!(
                    "resume refused: cancel was requested while suspended at node {:?}; \
                     the carrier was not consumed",
                    self.suspended_node
                ),
            });
        }
        let node = self.flow.find_node(&self.suspended_node).ok_or_else(|| {
            guard(format!(
                "suspended node {:?} is not part of flow {:?}",
                self.suspended_node, self.flow.id
            ))
        })?;
        if !node.kind.is_suspending() {
            return Err(guard(format!(
                "node {:?} is not suspending",
                self.suspended_node
            )));
        }
        let pending = self.node_state();
        let pending_status = pending.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if pending_status != "pending" {
            return Err(guard(format!(
                "suspending node {:?} is not in pending status: {:?}",
                self.suspended_node, pending_status
            )));
        }
        check_resume_binding(&pending, resume_kind, &data).map_err(guard)?;

        // 绑定键只用于准入，不进节点载荷（plaita _handle_resume 同款剥离）。
        if let Value::Object(map) = &mut data {
            map.remove(RESUME_BINDING_KEY);
        }

        let budget = self
            .flow
            .timeout
            .unwrap_or_else(crate::budget::default_execution_deadline);
        self.rc
            .set_flow_deadline(Instant::now().checked_add(budget));

        let result = crate::nodes::resume_node_body(node, &pending, resume_kind, &data, &self.rc)
            .map_err(|e| FlowError::ResumeError {
            message: format!("resume node {:?}: {e}", self.suspended_node),
        })?;

        self.rc.set_node_result(&node.id, result.clone());
        self.rc.set_last_node(&node.id);
        // plaita 的 _handle_resume 对被恢复节点报 on_node_end（不重跑
        // on_node_start）；worker 用它闭合挂起节点的计时/时间线。
        self.host.emit(Event::NodeEnd {
            flow_id: self.flow.id.clone(),
            node_id: node.id.clone(),
            node_type: node.node_type.clone(),
            result: Some(result.clone()),
            error: None,
        });

        // 唤醒订阅已消费：注销（plaita _unregister_suspended_subscription；
        // 失败仅告警，不改变恢复结果）。
        if let Some(sub_id) = pending.get("subscription_id").and_then(|v| v.as_str()) {
            if !sub_id.is_empty() {
                if let Some(registry) = self.host.subscription_registry() {
                    let tenant = pending
                        .get("subscription_tenant")
                        .and_then(|v| v.as_str())
                        .unwrap_or(self.host.tenant_id());
                    if let Err(e) = registry.unregister(sub_id, tenant) {
                        eprintln!("[resume] unregister subscription {sub_id:?}: {e}");
                    }
                }
            }
        }

        // -- 续跑循环（嵌套挂起 v1）：从最内层帧开始推进；每层 flow 完成时
        //    作为「调用返回」把值交给父帧（$NODE[child]=返回值 + outputType
        //    校验 + 续跑 child.next），直到根流程完成。续跑途中再次挂起则
        //    合并调用栈（新现场的栈更深，在前）重新入挂起态。
        let mut current_flow = self.flow.clone();
        let mut current_rc = self.rc.clone();
        let mut current_next = node.next.clone();
        let mut stack: Vec<CallFrame> = self.call_stack;

        loop {
            let Some(next) = current_next.clone() else {
                return Err(FlowError::ResumeError {
                    message: format!(
                        "resume: node {:?} in flow {:?} has no next node",
                        self.suspended_node, current_flow.id
                    ),
                });
            };
            let Some(next_node) = current_flow.find_node(&next) else {
                return Err(FlowError::Other(format!(
                    "resume: next node {next:?} not found in flow {:?}",
                    current_flow.id
                )));
            };
            let deadline = current_rc.flow_deadline();
            let host = self.host.clone();
            // 逐帧走 advance_and_finalize（= execute_on 的收尾链）：内层失败
            // 与首跑同链（FlowTimeout 终态/本帧 timeoutHandler/FlowEnd），不
            // 直抛宿主（验证报告 Q8.1 修正）；FlowEnd 由 finalize 发射。
            match current_flow.advance_and_finalize(next_node, &current_rc, &host, deadline) {
                Ok(value) => {
                    // 返回步按传播序消费：最深帧（index 0）先返回。
                    let frame = if stack.is_empty() {
                        None
                    } else {
                        Some(stack.remove(0))
                    };
                    match frame {
                        // 根 flow 完成：恢复会话收尾（FlowEnd 已由 finalize 发）。
                        None => {
                            return Ok(value);
                        }
                        // 调用返回步：值回填父帧（$NODE[child] + outputType 校验
                        // + last_node + NodeEnd），从 child.next 续跑父 flow。
                        Some(frame) => {
                            let child_node =
                                frame.flow.find_node(&frame.node_id).ok_or_else(|| {
                                    FlowError::ResumeError {
                                        message: format!(
                                            "call frame node {:?} not found in flow {:?}",
                                            frame.node_id, frame.flow.id
                                        ),
                                    }
                                })?;
                            crate::nodes::validate_child_return_output(child_node, &value)
                                .map_err(|e| FlowError::ResumeError {
                                    message: format!("call frame node {:?}: {e}", frame.node_id),
                                })?;
                            // 返回步同样新鲜发放预算（帧的原租约已结束）。
                            let frame_budget = frame
                                .flow
                                .timeout
                                .unwrap_or_else(crate::budget::default_execution_deadline);
                            frame
                                .rc
                                .set_flow_deadline(Instant::now().checked_add(frame_budget));
                            frame.rc.set_node_result(&frame.node_id, value.clone());
                            frame.rc.set_last_node(&frame.node_id);
                            host.emit(Event::NodeEnd {
                                flow_id: frame.flow.id.clone(),
                                node_id: frame.node_id.clone(),
                                node_type: child_node.node_type.clone(),
                                result: Some(value.clone()),
                                error: None,
                            });
                            // 内层 flow 的 FlowEnd 已由 finalize 发射。
                            current_flow = frame.flow.clone();
                            current_rc = frame.rc.clone();
                            current_next = child_node.next.clone();
                        }
                    }
                }
                Err(FlowError::Suspended(mut inner)) => {
                    // 续跑中再次挂起：新现场（更深）的调用栈在前，本会话剩余
                    // 栈在后拼接；FlowEnd(nil,nil) 已由 finalize 发射。
                    inner.call_stack.extend(stack);
                    return Err(FlowError::Suspended(inner));
                }
                Err(e) => {
                    // 本帧收尾链已套用（含 FlowEnd(error)）：失败按 finalize
                    // 的结论终态上抛（验证报告 Q8.1 修正——不再绕过本帧收尾链）。
                    return Err(e);
                }
            }
        }
    }
}

/// plaita#221 移植：event/timeout 载体必须绑定挂起期登记的订阅。挂起态没有
/// 订阅 id（宿主驱动的挂起/历史快照）无可比对，放行；event 还须 event_type
/// 一致（两侧都有值时）。
fn check_resume_binding(
    pending: &Value,
    resume_kind: ResumeKind,
    data: &Value,
) -> Result<(), String> {
    if !matches!(resume_kind, ResumeKind::Event | ResumeKind::Timeout) {
        return Ok(());
    }
    let expected = pending
        .get("subscription_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if expected.is_empty() {
        return Ok(());
    }
    let got = data
        .get("subscription_id")
        .and_then(|v| v.as_str())
        .or_else(|| data.get(RESUME_BINDING_KEY).and_then(|v| v.as_str()))
        .unwrap_or("");
    if got != expected {
        return Err(format!(
            "resume carrier is not bound to the suspended subscription (expected {expected:?}, got {got:?})"
        ));
    }
    if matches!(resume_kind, ResumeKind::Event) {
        let want_type = pending
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let got_type = data
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !want_type.is_empty() && !got_type.is_empty() && want_type != got_type {
            return Err(format!(
                "event type mismatch (suspended on {want_type:?}, carrier carries {got_type:?})"
            ));
        }
    }
    Ok(())
}

/// Reports whether a context-deadline error was caused by the flow-level
/// budget rather than the node's own: the recorded flow deadline has passed
/// and the node deadline is not strictly tighter (plaita
/// time_limit_by_flow → FLOW_ERROR).
fn flow_budget_binding(rc: &SharedCtx, node_deadline: Option<Instant>) -> bool {
    let Some(fd) = rc.flow_deadline() else {
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

/// 取消的具名错误（Python `_cancelled_error` 同形：标签取
/// `node.name or node.id`；「before running」=节点入口拒绝，「while
/// running」=重试尝试/退避中打断）。
fn cancelled_at(node: &Node, phase: &str) -> FlowError {
    let label = if node.name.is_empty() {
        &node.id
    } else {
        &node.name
    };
    FlowError::Cancelled {
        message: format!("Execution cancelled {phase} node {label}"),
    }
}

/// 取消轮询粒度（Python `_CANCEL_POLL_INTERVAL_SECONDS`=0.5s）：重试退避
/// 期间每片最多等这么久就复查一次 cancel_requested——cancel 在 ≤1s 内打断
/// 退避（plaita#267 验收口径）。
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// 退避睡眠的三种结局。
enum Backoff {
    /// 睡满（或被 deadline 截短——撞线由下一轮 body 入口判过期，与
    /// sleep_respecting 的原语义一致）
    Slept,
    /// 入口即已过 deadline
    Expired,
    /// 退避期间 cancel_requested 置位
    Cancelled,
}

/// 可被取消打断的退避（Python `_backoff_sleep_cancellable` 对应面）：按
/// [`CANCEL_POLL_INTERVAL`] 分片睡眠，每片后复查 cancel_requested；deadline
/// 只截短睡眠、不在此处报错（预算到期归 body 入口的既有判定）。
fn sleep_backoff_cancellable(host: &Host, deadline: Option<Instant>, d: Duration) -> Backoff {
    if deadline.is_some_and(|dl| Instant::now() >= dl) {
        return Backoff::Expired;
    }
    if host.is_cancel_requested() {
        return Backoff::Cancelled;
    }
    let mut remaining = d;
    while remaining > Duration::ZERO {
        let mut shard = remaining.min(CANCEL_POLL_INTERVAL);
        if let Some(dl) = deadline {
            let left = dl.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Backoff::Slept;
            }
            shard = shard.min(left);
        }
        std::thread::sleep(shard);
        remaining -= shard;
        if host.is_cancel_requested() {
            return Backoff::Cancelled;
        }
    }
    Backoff::Slept
}
