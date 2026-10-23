//! The per-execution runtime context — the home of the expression roots
//! (`$INPUT`, `$NODE`, `$GLOBAL`, `$ENV`, `$ERROR`, `$PARENT`, `$BRANCH`,
//! `$FLOW_ID`, `$LAST_NODE`, `$EXECUTION_ID` and the per-node extras such as
//! `LOOP-ITEM`).
//!
//! Concurrency mirrors plaita-go `internal/runtime/context.go`: a context is
//! shared between a flow and its background parallel branches, so every map
//! sits behind a read/write lock and readers take snapshots (or borrow under
//! the lock through [`Roots::with_root`]).

use std::sync::{Arc, RwLock};

use plaita_expression::Roots;
use serde_json::{Map, Value};

/// 执行帧类型（嵌套挂起 v1 的作用域闸依据）：挂起只允许发生在
/// Root / ChildCall / ReferenceCall 链上；任何 LoopIteration / ParallelBranch
/// 帧上的挂起具名拒绝（其恢复位置需要迭代状态快照，v2 候选）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrameKind {
    #[default]
    Root,
    /// child 节点的子执行帧。
    ChildCall,
    /// reference 节点的子执行帧（隔离上下文）。
    ReferenceCall,
    /// loop/map/filter/find/reduce/while 的迭代体帧。
    LoopIteration,
    /// parallel 分支帧。
    ParallelBranch,
}

impl FrameKind {
    /// 该帧是否可承载挂起穿透（调用链帧）。
    fn allows_suspension(self) -> bool {
        matches!(
            self,
            FrameKind::Root | FrameKind::ChildCall | FrameKind::ReferenceCall
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            FrameKind::Root => "root",
            FrameKind::ChildCall => "child",
            FrameKind::ReferenceCall => "reference",
            FrameKind::LoopIteration => "loop-iteration",
            FrameKind::ParallelBranch => "parallel-branch",
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    input: Value,
    nodes: Value,
    errors: Value,
    env: Value,
    global: Value,
    current: Value,
    last_node: Option<String>,
    last_branch: Option<Value>,
    flow_id: String,
    execution_id: String,
}

impl Inner {
    fn new() -> Self {
        Inner {
            input: Value::Object(Map::new()),
            nodes: Value::Object(Map::new()),
            errors: Value::Object(Map::new()),
            env: Value::Object(Map::new()),
            global: Value::Object(Map::new()),
            current: Value::Object(Map::new()),
            ..Default::default()
        }
    }
}

/// A shared, lock-guarded execution context.
#[derive(Debug)]
pub struct RuntimeCtx {
    inner: RwLock<Inner>,
    /// The execution that spawned this one (`$PARENT`). A root execution has
    /// none; a parallel branch's clone keeps the SOURCE execution as its
    /// parent (Go CloneContext semantics).
    parent: Option<Arc<RuntimeCtx>>,
    /// Deadline imposed by this flow's own timeout budget (used to attribute
    /// a timeout to the flow budget vs a node budget). Lock-guarded: the
    /// resume path issues a FRESH budget for the continued session
    /// ([`RuntimeCtx::set_flow_deadline`]).
    pub flow_deadline: std::sync::RwLock<Option<std::time::Instant>>,
    /// 本执行的帧类型（嵌套挂起作用域闸；snapshot 沿拷贝）。
    frame_kind: FrameKind,
}

pub type SharedCtx = Arc<RuntimeCtx>;

impl RuntimeCtx {
    fn new() -> Self {
        RuntimeCtx {
            inner: RwLock::new(Inner::new()),
            parent: None,
            flow_deadline: std::sync::RwLock::new(None),
            frame_kind: FrameKind::Root,
        }
    }

    /// Wraps in an Arc for sharing across the execution.
    pub fn shared(self) -> SharedCtx {
        Arc::new(self)
    }

    /// A fresh root execution context (no parent linkage).
    pub fn root() -> SharedCtx {
        RuntimeCtx::new().shared()
    }

    /// [`RuntimeCtx::snapshot`] wrapped for sharing.
    pub fn snapshot_arc(&self) -> SharedCtx {
        self.snapshot().shared()
    }

    /// Builds the child-execution context for a sub-flow (child / loop body
    /// / parallel branch): parent linkage, the flow's own globals, its
    /// exposeEnv and its own timeout budget.
    ///
    /// 预算合成（Go context.WithTimeout 语义）：子流自有 timeout 与外层
    /// deadline（父流/父节点预算经 `outer_deadline` 传入）取 min——否则
    /// 「父 1s + 子流声明 1h」会跑满 1h 才在父层炸（审计 [高] 实测 3s 逃逸）。
    pub fn child(
        parent: &SharedCtx,
        input: Value,
        flow_id: &str,
        flow: &crate::model::Flow,
        outer_deadline: Option<std::time::Instant>,
        frame_kind: FrameKind,
    ) -> SharedCtx {
        Self::child_with_kind(parent, input, flow_id, flow, outer_deadline, frame_kind)
    }

    fn child_with_kind(
        parent: &SharedCtx,
        input: Value,
        flow_id: &str,
        flow: &crate::model::Flow,
        outer_deadline: Option<std::time::Instant>,
        frame_kind: FrameKind,
    ) -> SharedCtx {
        // 装载期（normalize_timeout）已拒掉超平台 Instant 值域的预算；这里
        // checked_add 是程序化构造 Flow 的防御面（裸加是进程级 panic）。
        // 溢出意味着预算大到等效无界——数学上 own 晚于任何可表示的 outer，
        // min(own, outer) = outer，回落 None（仅 outer）语义不变。
        let own = flow
            .timeout
            .and_then(|t| std::time::Instant::now().checked_add(t));
        let deadline = match (own, outer_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let ctx = RuntimeCtx::prepare(
            Some(parent),
            None,
            input,
            Some(flow_id),
            flow.context
                .global
                .as_ref()
                .and_then(|g| g.as_object().cloned()),
            flow.expose_env.as_ref(),
            None,
            deadline,
            frame_kind,
        );
        ctx
    }

    /// Reference-node child context（child.py ReferenceFlow 的引用隔离）：
    /// `$GLOBAL`/`$ENV`/执行身份与预算合成都从真实父执行种子，但 `$PARENT`
    /// 链接指向一个**空的 ambient 执行**——子流程读不到父的任何状态，
    /// `$PARENT.<未知根>` 按表达式语义求值为 null（隔离实测口径，
    /// plaita-go#80 r78 golden `ref_parent_isolation` 钉）。
    pub fn child_isolated(
        parent: &SharedCtx,
        input: Value,
        flow_id: &str,
        flow: &crate::model::Flow,
        outer_deadline: Option<std::time::Instant>,
    ) -> SharedCtx {
        let own = flow
            .timeout
            .and_then(|t| std::time::Instant::now().checked_add(t));
        let deadline = match (own, outer_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        RuntimeCtx::prepare(
            Some(parent),
            Some(RuntimeCtx::new().shared()),
            input,
            Some(flow_id),
            flow.context
                .global
                .as_ref()
                .and_then(|g| g.as_object().cloned()),
            flow.expose_env.as_ref(),
            None,
            deadline,
            FrameKind::ReferenceCall,
        )
    }

    /// A deep snapshot of this context: every map copied under the read
    /// lock, scalars copied, the PARENT LINKAGE KEPT (a parallel branch is a
    /// child of the flow that contains the parallel node — plaita#72 pins
    /// the $PARENT/$INPUT snapshot at branch creation).
    pub fn snapshot(&self) -> RuntimeCtx {
        let inner = self.inner.read().expect("ctx lock");
        RuntimeCtx {
            inner: RwLock::new(Inner {
                input: inner.input.clone(),
                nodes: inner.nodes.clone(),
                errors: inner.errors.clone(),
                env: inner.env.clone(),
                global: inner.global.clone(),
                current: inner.current.clone(),
                last_node: inner.last_node.clone(),
                last_branch: inner.last_branch.clone(),
                flow_id: inner.flow_id.clone(),
                execution_id: inner.execution_id.clone(),
            }),
            parent: self.parent.clone(),
            flow_deadline: std::sync::RwLock::new(self.flow_deadline()),
            frame_kind: self.frame_kind,
        }
    }

    /// Builds the fresh execution context for a sub-flow (child / loop body
    /// / parallel branch), mirroring `prepareExecutionRuntimeContext`:
    ///
    /// - the parent execution stays reachable as `$PARENT`;
    /// - `$GLOBAL` starts from the parent's globals (plaita fallback), the
    ///   flow's own `context.global` overrides (`globals` argument);
    /// - `$FLOW_ID` is the flow's own id, a sub-flow without one keeps the
    ///   parent's;
    /// - `$EXECUTION_ID` is inherited (host injection wins for roots);
    /// - `$ENV` follows the exposeEnv allowlist model: declared non-empty →
    ///   filter the process environment; declared empty → nothing;
    ///   undeclared → the parent's already-filtered env.
    #[allow(clippy::too_many_arguments)]
    pub fn new_execution(
        parent: Option<&SharedCtx>,
        input: Value,
        flow_id: Option<&str>,
        own_globals: Option<Map<String, Value>>,
        expose_env: Option<&Vec<String>>,
        host_execution_id: Option<&str>,
        flow_deadline: Option<std::time::Instant>,
    ) -> SharedCtx {
        RuntimeCtx::prepare(
            parent,
            None,
            input,
            flow_id,
            own_globals,
            expose_env,
            host_execution_id,
            flow_deadline,
            FrameKind::Root,
        )
    }

    /// The shared construction path. `link` overrides the context reachable
    /// as `$PARENT` (`None` = the seeded parent); child_isolated passes an
    /// empty ambient execution for reference-node isolation.
    #[allow(clippy::too_many_arguments)]
    fn prepare(
        parent: Option<&SharedCtx>,
        link: Option<SharedCtx>,
        input: Value,
        flow_id: Option<&str>,
        own_globals: Option<Map<String, Value>>,
        expose_env: Option<&Vec<String>>,
        host_execution_id: Option<&str>,
        flow_deadline: Option<std::time::Instant>,
        frame_kind: FrameKind,
    ) -> SharedCtx {
        let mut ctx = RuntimeCtx::new();
        ctx.frame_kind = frame_kind;
        {
            let parent_inner = parent.map(|p| p.inner.read().expect("ctx lock"));
            let parent_env: Option<Value>;
            let parent_global: Option<Value>;
            if let Some(pi) = &parent_inner {
                // the parent chain carries at most two levels (Go reads the
                // grandparent through the seed's $PARENT link); the immediate
                // parent wins
                ctx.inner.write().unwrap().flow_id = pi.flow_id.clone();
                ctx.inner.write().unwrap().execution_id = pi.execution_id.clone();
                parent_env = Some(pi.env.clone());
                parent_global = Some(pi.global.clone());
            } else {
                parent_env = None;
                parent_global = None;
            }

            let mut inner = ctx.inner.write().unwrap();
            inner.input = input;
            if let Some(fid) = flow_id {
                if !fid.is_empty() {
                    inner.flow_id = fid.to_string();
                }
            }
            // $GLOBAL: parent's globals first, flow's own globals override
            if let Some(pg) = parent_global.and_then(|v| v.as_object().cloned()) {
                let mut g = pg;
                if let Some(own) = &own_globals {
                    for (k, v) in own {
                        g.insert(k.clone(), v.clone());
                    }
                }
                inner.global = Value::Object(g);
            } else if let Some(own) = own_globals {
                inner.global = Value::Object(own);
            }
            // $EXECUTION_ID: host injection wins; else inherited; else uuid
            if let Some(eid) = host_execution_id.filter(|s| !s.is_empty()) {
                inner.execution_id = eid.to_string();
            } else if inner.execution_id.is_empty() {
                inner.execution_id = uuid::Uuid::new_v4().to_string();
            }
            // $ENV (fail-closed allowlist model)
            let env = match expose_env {
                Some(keys) if !keys.is_empty() => {
                    let mut m = Map::new();
                    for (k, v) in std::env::vars() {
                        if keys.contains(&k) {
                            m.insert(k, Value::String(v));
                        }
                    }
                    Value::Object(m)
                }
                Some(_) => Value::Object(Map::new()),
                None => parent_env.unwrap_or_else(|| Value::Object(Map::new())),
            };
            inner.env = env;
            if let Some(pid) = flow_id {
                if !pid.is_empty() {
                    // $GLOBAL.flow_id (plaita setup_flow)
                    if let Value::Object(g) = &mut inner.global {
                        g.insert("flow_id".into(), Value::String(pid.to_string()));
                    }
                }
            }
        }
        ctx.parent = match link {
            Some(l) => Some(l),
            None => parent.cloned(),
        };
        let inherited = flow_deadline.or_else(|| parent.and_then(|p| p.flow_deadline()));
        *ctx.flow_deadline.write().unwrap() = inherited;
        ctx.shared()
    }

    // -- node results / errors ------------------------------------------

    pub fn set_node_result(&self, node_id: &str, result: Value) {
        let mut inner = self.inner.write().unwrap();
        if let Value::Object(m) = &mut inner.nodes {
            m.insert(node_id.to_string(), result);
        }
    }

    pub fn set_node_error(&self, node_id: &str, err: Value) {
        let mut inner = self.inner.write().unwrap();
        if let Value::Object(m) = &mut inner.errors {
            m.insert(node_id.to_string(), err);
        }
    }

    pub fn set_current(&self, key: &str, value: Value) {
        let mut inner = self.inner.write().unwrap();
        if let Value::Object(m) = &mut inner.current {
            m.insert(key.to_string(), value);
        }
    }

    pub fn reset_current(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.current = Value::Object(Map::new());
    }

    pub fn get_current(&self, key: &str) -> Option<Value> {
        let inner = self.inner.read().unwrap();
        inner.current.get(key).cloned()
    }

    // -- bookkeeping -----------------------------------------------------

    pub fn set_last_node(&self, node_id: &str) {
        self.inner.write().unwrap().last_node = Some(node_id.to_string());
    }

    pub fn set_last_branch(&self, branch: Value) {
        self.inner.write().unwrap().last_branch = Some(branch);
    }

    pub fn set_global(&self, key: &str, value: Value) {
        let mut inner = self.inner.write().unwrap();
        if let Value::Object(m) = &mut inner.global {
            m.insert(key.to_string(), value);
        }
    }

    pub fn get_global(&self, key: &str) -> Option<Value> {
        let inner = self.inner.read().unwrap();
        inner.global.get(key).cloned()
    }

    pub fn flow_id(&self) -> String {
        self.inner.read().unwrap().flow_id.clone()
    }

    pub fn execution_id(&self) -> String {
        self.inner.read().unwrap().execution_id.clone()
    }

    /// Root execution?（挂起边界的作用域闸：只有根流程可以挂起——嵌套
    /// child/loop/parallel 里的挂起点，其恢复位置无法用执行状态表达，
    /// 具名拒绝，Go checkpoint v1.5 同款范围规则。）
    pub fn is_root_execution(&self) -> bool {
        self.parent.is_none()
    }

    /// Updates the flow budget deadline (the resume path issues a fresh
    /// execution budget for the continued session — the suspended lease is
    /// over; a 24h approval resume would otherwise land on an expired
    /// deadline from the original run).
    pub fn set_flow_deadline(&self, deadline: Option<std::time::Instant>) {
        *self.flow_deadline.write().unwrap() = deadline;
    }

    /// Reads the flow budget deadline (method form for the lock; direct
    /// field access is a lock guard away).
    pub fn flow_deadline(&self) -> Option<std::time::Instant> {
        *self.flow_deadline.read().unwrap()
    }

    pub fn frame_kind(&self) -> FrameKind {
        self.frame_kind
    }

    /// 嵌套挂起作用域闸（v1）：沿本帧到根的帧链检查——全部帧都必须可承载
    /// 挂起穿透（Root/ChildCall/ReferenceCall）；返回第一个阻断帧的类型
    ///（None = 允许挂起）。
    pub fn frame_chain_blocker(&self) -> Option<FrameKind> {
        let mut cur = Some(self);
        while let Some(ctx) = cur {
            if !ctx.frame_kind.allows_suspension() {
                return Some(ctx.frame_kind);
            }
            cur = ctx.parent.as_deref();
        }
        None
    }

    /// A JSON snapshot of the whole context (diagnostics / tests).
    pub fn snapshot_json(&self) -> Value {
        let inner = self.inner.read().unwrap();
        serde_json::json!({
            "input": inner.input,
            "nodes": inner.nodes,
            "errors": inner.errors,
            "env": inner.env,
            "global": inner.global,
            "last_node": inner.last_node,
            "last_branch": inner.last_branch,
            "flow_id": inner.flow_id,
            "execution_id": inner.execution_id,
        })
    }

    /// Rebuilds a root context from a [`RuntimeCtx::snapshot_json`] value —
    /// the worker-layer checkpoint recovery entry (rs-worker-design §7.4
    /// E1; the `context_flavor: plaita-rs-snapshot-v1` gate lives in the
    /// worker's checkpoint store, this constructor only speaks the snapshot
    /// shape).
    ///
    /// All nine roots are REQUIRED: a missing or wrongly-typed root is a
    /// named error, never a silent default — a snapshot that cannot be
    /// rebuilt must not silently become an empty context "resuming from the
    /// wrong place". `parent` is `None` (suspension only happens in the root
    /// execution — the engine's root-flow gate makes the parent chain
    /// unnecessary in a snapshot) and the flow budget is left unset (the
    /// resume path issues a fresh budget).
    pub fn from_snapshot_json(snapshot: &Value) -> Result<RuntimeCtx, String> {
        const ROOTS: &str =
            "input/nodes/errors/env/global/last_node/last_branch/flow_id/execution_id";
        let obj = snapshot.as_object().ok_or_else(|| {
            format!("snapshot_json rebuild: expected a JSON object, got {snapshot}")
        })?;
        let missing = |name: &str| {
            format!("snapshot_json rebuild: missing root {name:?} (a snapshot must carry all nine roots: {ROOTS})")
        };
        let expect_object = |name: &str, v: &Value| {
            v.as_object().map(|_| ()).ok_or_else(|| {
                format!("snapshot_json rebuild: root {name:?} must be a JSON object, got {v}")
            })
        };
        let ctx = RuntimeCtx::new();
        {
            let mut inner = ctx.inner.write().expect("ctx lock");
            inner.input = obj.get("input").ok_or_else(|| missing("input"))?.clone();
            for name in ["nodes", "errors", "env", "global"] {
                let v = obj.get(name).ok_or_else(|| missing(name))?;
                expect_object(name, v)?;
            }
            inner.nodes = obj["nodes"].clone();
            inner.errors = obj["errors"].clone();
            inner.env = obj["env"].clone();
            inner.global = obj["global"].clone();
            inner.last_node = match obj.get("last_node").ok_or_else(|| missing("last_node"))? {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                other => {
                    return Err(format!(
                        "snapshot_json rebuild: root \"last_node\" must be null or a string, got {other}"
                    ))
                }
            };
            inner.last_branch = match obj
                .get("last_branch")
                .ok_or_else(|| missing("last_branch"))?
            {
                Value::Null => None,
                v => Some(v.clone()),
            };
            for name in ["flow_id", "execution_id"] {
                let v = obj.get(name).ok_or_else(|| missing(name))?;
                let s = v.as_str().ok_or_else(|| {
                    format!("snapshot_json rebuild: root {name:?} must be a string, got {v}")
                })?;
                if name == "flow_id" {
                    inner.flow_id = s.to_string();
                } else {
                    inner.execution_id = s.to_string();
                }
            }
        }
        Ok(ctx)
    }
}

impl Roots for RuntimeCtx {
    fn with_root(&self, name: &str, f: &mut dyn FnMut(&Value)) -> bool {
        let inner = self.inner.read().unwrap();
        match name {
            "INPUT" => {
                f(&inner.input);
                true
            }
            "NODE" => {
                f(&inner.nodes);
                true
            }
            "ERROR" => {
                f(&inner.errors);
                true
            }
            "ENV" => {
                f(&inner.env);
                true
            }
            "GLOBAL" => {
                f(&inner.global);
                true
            }
            "BRANCH" => {
                // None until a branching node has run (plaita None)
                let v = inner.last_branch.clone().unwrap_or(Value::Null);
                drop(inner);
                f(&v);
                true
            }
            "FLOW_ID" | "FLOW" => {
                let v = Value::String(inner.flow_id.clone());
                drop(inner);
                f(&v);
                true
            }
            "LAST_NODE" => {
                let v = match &inner.last_node {
                    Some(id) => Value::String(id.clone()),
                    None => Value::Null,
                };
                drop(inner);
                f(&v);
                true
            }
            "EXECUTION_ID" => {
                let v = Value::String(inner.execution_id.clone());
                drop(inner);
                f(&v);
                true
            }
            other => match inner.current.get(other) {
                Some(v) => {
                    f(v);
                    true
                }
                None => false,
            },
        }
    }

    fn parent(&self) -> Option<&dyn Roots> {
        self.parent.as_deref().map(|p| p as &dyn Roots)
    }

    fn root_names(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut names: Vec<String> = [
            "INPUT",
            "NODE",
            "ERROR",
            "ENV",
            "GLOBAL",
            "BRANCH",
            "FLOW_ID",
            "LAST_NODE",
            "EXECUTION_ID",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if let Value::Object(m) = &inner.current {
            names.extend(m.keys().cloned());
        }
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plaita_expression::eval_str;
    use serde_json::json;

    #[test]
    fn roots_resolve() {
        let ctx = RuntimeCtx::root();
        ctx.set_node_result("n1", json!({"out": "hello"}));
        ctx.set_last_node("n1");
        ctx.set_last_branch(json!("branch-a"));
        assert_eq!(eval_str("$NODE.n1.out", &*ctx).unwrap(), json!("hello"));
        assert_eq!(eval_str("$LAST_NODE", &*ctx).unwrap(), json!("n1"));
        assert_eq!(eval_str("$BRANCH", &*ctx).unwrap(), json!("branch-a"));
        assert!(eval_str("$MISSING", &*ctx).is_err());
        ctx.set_current("LOOP-ITEM", json!(7));
        assert_eq!(eval_str("$LOOP-ITEM", &*ctx).unwrap(), json!(7));
    }

    #[test]
    fn parent_chain() {
        let outer = RuntimeCtx::root();
        outer.set_node_result("n0", json!("outer-result"));
        let input = json!({"k": "v"});
        let inner = RuntimeCtx::new_execution(
            Some(&outer),
            input,
            Some("child-flow"),
            None,
            None,
            None,
            None,
        );
        assert_eq!(
            eval_str("$PARENT.$NODE.n0", &*inner).unwrap(),
            json!("outer-result")
        );
        assert_eq!(
            eval_str("$PARENT.NODE.n0", &*inner).unwrap(),
            json!("outer-result")
        );
        assert_eq!(eval_str("$FLOW_ID", &*inner).unwrap(), json!("child-flow"));
        assert_eq!(
            eval_str("$GLOBAL.flow_id", &*inner).unwrap(),
            json!("child-flow")
        );
        // undeclared exposeEnv inherits the parent's (empty) env
        assert_eq!(eval_str("$ENV", &*inner).unwrap(), json!({}));
    }

    #[test]
    fn child_isolated_reference_isolation() {
        // plaita-go#80 r78 golden（ref_parent_isolation / ref_global_inherit）
        // 的 Rust 侧钉：reference 子流程继承 $GLOBAL/$ENV 与执行身份，但
        // $PARENT 链接指向空 ambient 执行——父的任何根都读不到（未知根求值
        // null），父状态零泄漏。
        let parent = RuntimeCtx::root();
        parent.set_node_result("n", json!("parent-node"));
        parent.set_global("token", json!("top-token"));
        let flow = crate::model::Flow {
            id: "sub".into(),
            name: String::new(),
            desc: String::new(),
            runtime: String::new(),
            version: String::new(),
            nodes: Vec::new(),
            timeout: None,
            input_type: None,
            output_type: None,
            timeout_handler: Default::default(),
            metadata: Value::Null,
            context: Default::default(),
            expose_env: None,
        };
        let child = RuntimeCtx::child_isolated(&parent, json!({"z": 9}), "sub", &flow, None);
        // $GLOBAL 继承；执行身份继承
        assert_eq!(
            eval_str("$GLOBAL.token", &*child).unwrap(),
            json!("top-token")
        );
        assert_eq!(eval_str("$GLOBAL.flow_id", &*child).unwrap(), json!("sub"));
        // $PARENT 隔离：未知根 → null；已知根也读不到父的值（ambient 空）
        assert_eq!(eval_str("$PARENT.n", &*child).unwrap(), json!(null));
        assert_eq!(eval_str("$PARENT.$NODE.n", &*child).unwrap(), json!(null));
        // 自身 $INPUT 正常
        assert_eq!(eval_str("$INPUT.z", &*child).unwrap(), json!(9));
    }

    #[test]
    fn expose_env_allowlist() {
        std::env::set_var("PLAITA_RS_TEST_VAR", "1");
        let ctx = RuntimeCtx::root();
        let child = RuntimeCtx::new_execution(
            Some(&ctx),
            json!({}),
            Some("f"),
            None,
            Some(&vec!["PLAITA_RS_TEST_VAR".to_string()]),
            None,
            None,
        );
        assert_eq!(
            eval_str("$ENV.PLAITA_RS_TEST_VAR", &*child).unwrap(),
            json!("1")
        );
        // a declared EMPTY allowlist exposes nothing
        let child2 = RuntimeCtx::new_execution(
            Some(&ctx),
            json!({}),
            Some("f"),
            None,
            Some(&vec![]),
            None,
            None,
        );
        assert_eq!(eval_str("$ENV", &*child2).unwrap(), json!({}));
    }
}
