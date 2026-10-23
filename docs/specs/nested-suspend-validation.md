# 嵌套挂起 v1 设计稿——三仓代码深挖验证报告

- 验证席产出，2026-10-23。对象：《嵌套挂起 v1 设计稿》（rs 先行试点 NS-M1，go NS-M2，plaita NS-M3）。
- 行号口径（四个快照，均真实可查）：
  - **rs 基线**：plaita-rs-wt2 分支 `feat/nested-suspend`，HEAD `27d1add`（未改动的已提交代码，下称「基线」）；
  - **rs 在飞实现**：同一工作区 2026-10-23 的**未提交改动**（NS-M1 实现中，`git status` 含 exec.rs/context.rs/error.rs/nodes/mod.rs/tests 修改 + 未跟踪 tests/nested_suspend.rs）——设计稿的第一个实现标本，本报告对其按同规格验收；
  - **go**：plaita-go 分支 `fix/batch-52-50`（已提交态）；
  - **plaita**：plaita 分支 `main`（已提交态）。
- 未标注「在飞」的 rs 行号一律指基线；go/plaita 行号指其各自快照。

---

## ① 逐项验证

### Q1 rs：错误策略链的拦截位置、Cancelled 先例共存、NodeError→FlowError 转换点

**结论：拦截点唯一可行位置 = `execute_node` 的 `match body` 顶部——取消门之后、Timeout 归属/重试分类/errorHandler 尾链之前。设计稿写的 `NodeError::suspension(Box<SuspendedExecution>)` 在基线代码里**无处安放**（NodeError 是 struct 不是 enum），且不存在 `From<NodeError> for FlowError`，转换全部是显式点——其中三处 stringify 点正是「挂起被剥成文本」的元凶。**

证据：

1. 策略链全貌（基线 `crates/plaita-engine/src/exec.rs`）：
   - 尝试循环 `match body`：取消优先臂 578-580 → Timeout 归属 581-612 → 不可重试分类 613-616 → 重试次数 617-620 → 退避重试 621-640；
   - `final_err` 尾链 644-698：流预算终态 649-658 → `FlowError::node` 包装 **659**（唯一的 NodeError→FlowError 结构化转换点）→ timeoutHandler 包裹 664-667 → `$ERROR` 记账 669-682 → **errorPolicy match 683-697：`Continue`/`ContinueWith` 在这里把任意错误吞成 `Ok(null/default)`**。
   - 任何未被顶部拦截的 `NodeError::suspension`（哪怕 class=Deterministic 不可重试）都会流到 683-697 被子节点自身的 errorHandler continue 吞掉。**拦截必须发生在 581 之前**。
2. Cancelled 先例：取消在三个位置绕过全部策略——节点边界门 392-394、每尝试入口 542-544、body 错误后的取消优先臂 578-580、退避打断 636-638；`FlowError::Cancelled` 在 execute/execute_on 尾部终态直抛（282、344），不经任何 handler。挂起 carrier 应与之同形：`match body` 顶部加专属臂，直接 `return Err(FlowError::Suspended(..))`。注意相对次序：挂起臂放在取消臂**之后**意味着「挂起发生时若 cancel_requested 已置位」取消赢——合理，但规格要写明。
3. NodeError 的形状（基线 `crates/plaita-engine/src/error.rs:42-48`）：`struct NodeError { message, class, code }`——没有携带 `Box<SuspendedExecution>` 的位置。设计稿的记法隐含「加字段或改 enum」，在飞实现选择了加字段 `pub suspension: Option<Box<SuspendedExecution>>`（在飞 error.rs:54）+ 判定 `is_suspension()`（error.rs:103-106）+ 拦截臂（在飞 exec.rs:599-602，位于取消臂 594-596 之后、Timeout 臂 603 之前）——**位置正确**。
4. FlowError→NodeError 的 stringify 点（破坏 carrier 的位置，基线 `crates/plaita-engine/src/nodes/mod.rs`）：`run_collection_child` 677-678、`execute_reference` 1024-1031、`execute_child` 1066-1074、`run_branch_flow` 1368-1369（`map_err(|e| NodeError::transient(e.to_string()))`）。这些点把 `FlowError::Suspended`/`FlowError::SuspendRejected` 剥成纯文本 `NodeError::transient`（code=10000，**is_retryable()==true**，error.rs:80-88）——被父节点重试 retryTimes 次、再被 errorHandler continue 吞。**这正是基线「fail-closed 具名拒」的真实 degraded 形态**：闸在 `suspend_flow`（exec.rs:429-435）是 fail-closed，但拒绝错误一出内层 flow 就降级为可重试可吞的 transient。go 同病（见 Q5）。

**在飞实现遗留缺口**：`ErrorClass::SuspendRejected`（在飞 error.rs:42、108-114，`is_retryable` 已归入不可重试 117-125）**没有**对应的 execute_node 拦截臂——它从 `execute_parallel` 上抛后在飞 exec.rs:594-640 的臂序列里无人接，落到 final_err 尾链被子节点（此处是 parallel 节点）的 errorHandler continue 吞成 `Ok(null)`（基线 exec.rs:683-697 同位）。与 error.rs:39-41 注释「不可重试、不可被策略吞」的承诺相悖。→ 见修正清单 #3。

### Q2 rs：事件面发射序（挂起/恢复两方向）

**结论：挂起方向每帧发 `FlowStart + FlowEnd(nil,nil)`（现有不变量「execute_on 恰发一次 FlowEnd」自然推广到每帧）；恢复方向（基线只覆盖根流程）发 NodeEnd（无 NodeStart）+ 收尾 FlowEnd（无 FlowStart）——「FlowEnd 可重复、最后一次为准」是既有约定。嵌套恢复若中间帧完成不发 FlowEnd，宿主账目不对称：每帧在挂起时刻已收到过形似终态的 `FlowEnd(nil,nil)`，此后该 flow 真正完成时宿主永远收不到携带真实结果的事件——按 FlowEnd 收结果的宿主对内层帧全盲。**

证据与建议：

- 挂起方向：基线 `execute` exec.rs:259-270、`execute_on` 327-336 对 `Err(Suspended)` 发 `FlowEnd(result=nil, error=nil)`；go 同款：`workflow.go:202-206`（Execute retryFunc 的 AsSuspend 分支）与 `suspend.go:205-211`（Resume 的 defer）。
- 恢复方向（基线根流程）：resume() 只发被恢复节点的 NodeEnd（exec.rs:838-846，注释明说不重跑 on_node_start）+ 收尾 FlowEnd（879-889），无 FlowStart。
- **建议（并已在飞实现按此办理，应升格为规格条文）**：
  1. 恢复方向对每个完成的帧发 `FlowEnd(result=该帧返回值)`，顺序**最内帧→根**；FlowStart 恢复会话一律不发（与根流程先例一致）。在飞实现：返回步 NodeEnd+FlowEnd（在飞 exec.rs:975-1010 区域，FlowEnd 在 1009）、根完成 FlowEnd（969）、再次挂起 FlowEnd(nil,nil)（1024）、失败 FlowEnd(error)（1032）。
  2. 钉死两种 NodeEnd 形状都合法：直接挂起节点挂起时 NodeEnd 携带 result=pending 态（body Ok 后无条件发射，exec.rs:557-567）；穿越型子节点挂起时 NodeEnd 携带 error 文本（body Err 同一发射点）。恢复时各自再补一次 NodeEnd(result)。
  3. 顺序细则：挂起方向事件序为「内层帧 FlowEnd 先于外层帧 NodeEnd/FlowEnd」；恢复方向相反（最内先完成）。宿主不得假设 FlowStart/FlowEnd 配对。

### Q3 rs：advance_from 取消门在恢复循环的行为；预算 min 合成与「每帧新鲜发放」

**结论：取消门天然覆盖恢复循环（每帧共用同一 advance_from，每节点边界检查）；但恢复入口没有取消门——resume_node_body 先跑（副作用）后拦。预算方面「每帧新鲜发放」是 load-bearing：漏刷任何一帧，该帧的第一次 advance 就以 FlowTimeout 终态死掉；而「每帧独立新鲜发放」会丧失 min 合成（N 帧总预算=Σ），需要显式抉择。**

证据：

- 取消门：advance_from 节点边界（exec.rs:392-394）；Host 的 Arc 共享语义（exec.rs:43-66，克隆见同一对标志位）使全部帧共享一个 cancel_requested。缺口：`resume()`（exec.rs:863 在飞；基线 795）从准入到 `resume_node_body`（在飞 873；基线 831）之间**无** `is_cancel_requested` 检查——挂起期间 `host.cancel()` 过，恢复仍先把挂起节点 body 跑完（审批/回调副作用照发），之后 advance_from 才拒。建议：resume 入口加取消门（返回 `ResumeGuard` 族或 `Cancelled`——需钉类型），或明文声明「取消后恢复=决策已消费」。
- 预算 min 合成：子帧 deadline 在 rc 创建时合成——`RuntimeCtx::child`（context.rs:103-109）与 `child_isolated`（136-142）`min(own, outer_deadline)`，`outer_deadline` 实参是**父帧的 node_deadline**（=min(父节点 timeout, 父帧 flow deadline)，exec.rs:525-533，由 execute_node 555 下传）；兜底继承 context.rs:301。挂起期间所有帧的 deadline 继续走钟（无人暂停）。
- 恢复刷新：基线只刷最内帧（exec.rs:824-829 `set_flow_deadline`，context.rs:380-382）。若父帧不刷：父帧首个节点入口 `node_deadline = now + min(nt, fd.saturating_duration_since(now))` = now（exec.rs:528）→ body 入口即报 Timeout（552-553）→ `flow_budget_binding`（944-957：now≥fd 且 nd(now)<fd 不成立）判 **true** → `FlowError::FlowTimeout` 终态（649-658）——恢复会话当场死，非可吞错误。结论：设计稿④的「每帧新鲜发放」必须逐帧落到位，在飞实现已在恢复入口（最内帧）+ 返回步（每层父帧，`frame.rc.set_flow_deadline`）双点落实。
- 遗留语义抉择（规格缺口）：每帧独立发放 `own timeout 或缺省 1800s`（budget.rs:41 `DEFAULT_EXECUTION_DEADLINE`）意味着 (a) 深链总墙钟=Σ帧预算，可远超原执行的 min 合成预算；(b) 内层帧的新预算可能比父帧的更长（min 包含关系丢失）。二选一：维持独立发放（现状，需写明「恢复会话按帧重发」）或返回步按 `min(帧 own, 父帧剩余)` 重合成。另需写明：**子节点的 node.timeout（父帧对子执行的天花板）跨挂起后不再生效**——返回步只验 outputType（在飞 nodes/mod.rs:408-415），节点预算只覆盖首次执行。

### Q4 rs：suspend_flow 的 rc 写入、snapshot 隔离、child_isolated parent 链断裂（设计稿重大疑点）

**结论：疑点成立，且在飞实现已经踩中——`frame_chain_blocker` 沿 parent 链行走，在 reference 帧处被「指向空 ambient 的 parent 链接」截断，ambient 的 frame_kind 又是默认 Root，导致「loop → reference → 挂起」被闸放行；随后 carrier 在 loop 的转换点被毁成 transient，造成订阅孤儿+重试+可吞的完整失败链。修正方案：闸的重心从「链行走」移到「传播转换点的调用方帧 kind 检查」——调用方 rc 永远是真实帧，无 ambient 眼镜。**

证据：

1. suspend_flow 的写入面（基线 exec.rs:481-499）：pending 态只落**当前帧** rc 的 `$NODE`（493），`$LAST_NODE` 不动（491-492 注释，Go runNodes 同款）。各帧 `$NODE` 天然隔离：`child()`/`child_isolated()` 都构造全新 `Inner`（context.rs:239-287，仅种子 $GLOBAL/$ENV/执行身份），无 map 别名。`snapshot_arc` 只出现在 parallel 分支路径（基线 nodes/mod.rs:1228、1271、1298），child/reference 路径不经过 snapshot；且 `snapshot()` 保留 parent 链接（context.rs:162-180，177 行）——parallel 之外的路径与 snapshot 无交互。**这一半没有问题。**
2. 断链实锤：`child_isolated`（context.rs:129-156）把 `link = Some(RuntimeCtx::new().shared())`（143-145）传入 `prepare`，后者 `ctx.parent = link`（297-300）。ref 帧的 parent 链：`ref_rc → ambient(空执行) → None`——真实父帧不可达。基线闸 `is_root_execution()`（372-374，`parent.is_none()`）只看本帧，因此所有嵌套都能拒、不需要走链；设计稿的「沿 parent 链检查全部帧」是新语义引入的新依赖，恰好踩在 reference 的断链上。
3. 在飞实现的失败链（全部行号为 2026-10-23 工作区）：
   - ambient 的 kind：`RuntimeCtx::new()` 默认 `FrameKind::Root`（context.rs:20-49 的 enum 定义 + new 初始化），`child_isolated` 只给 **ref 帧本身**标 `ReferenceCall`（context.rs:210），ambient 未经标记；
   - `frame_chain_blocker`（context.rs:457-467）沿 `ctx.parent` 行走：从 ref 帧出发 `ReferenceCall(允许) → ambient(Root,允许) → None`——**loop/parallel 祖先帧被 ambient 遮蔽，闸放行**；
   - `execute_reference` 的转换点不检查调用方帧 kind（nodes/mod.rs:1041 `Err(FlowError::Suspended(inner)) => return Err(NodeError::suspension(inner))`，execute_child 同款 1094）——carrier 存活穿出 ref 帧；
   - ref 帧所在的 loop 体 flow 的 `advance_from` 捕获 carrier 并**把 loop 帧**压入 call_stack（在飞 exec.rs:399-403）——挂起被错误接纳；
   - carrier 继续上行，在 `run_collection_child` 被 stringify：`map_err(|e| NodeError::transient(e.to_string()))`（nodes/mod.rs:675）——`SuspendedExecution` 连同 `subscription_id` 一起 drop；
   - 后果：loop 节点按 transient **重试 retryTimes 次**——每次重试内层重新执行、`suspend_flow` 每次注册一条新订阅（注册在闸之后、carrier 被毁之前）——**N 条孤儿订阅**永不注销；重试耗尽后还可被 errorHandler continue 吞成 null，流程带着未消费的挂起继续走。
   - 现有测试未覆盖该组合：tests/nested_suspend.rs 只有 child 链（1/2 层）、reference 链（直连根）、再挂起拼接、深层绑定；无 loop→reference、无 loop 直连的 errorHandler/retryTimes 组合。
4. 修正方案（建议写进设计稿，rs/go/plaita 三仓同构）：
   - **主闸 = 传播转换点的调用方帧 kind 检查**：`execute_child`/`execute_reference` 在把 `Err(Suspended)` 转成 carrier 之前，检查**调用方 rc**（函数参数 `rc`，即子节点所在帧——真实帧，非 ambient）的 `frame_kind`：`LoopIteration`/`ParallelBranch` → 返回 `NodeError::suspend_rejected`（具名拒绝，进不重试分类）；`run_collection_child`/`run_branch_flow` 的帧自身就是阻断帧，无条件拒绝任何 carrier/SuspendRejected 穿越并把 `FlowError::SuspendRejected` 映射为 `NodeError::suspend_rejected`（不得 stringify）。
   - `suspend_flow` 里的 `frame_chain_blocker`（或等价的单帧检查）保留为**注册订阅前的快路径**——它保证「拒绝发生在订阅注册之前」（基线 exec.rs:429-435 的次序），防孤儿订阅；但它的链行走对 correctness 不再背书（advisory）。若想保留链检的兜底价值，可让 `child_isolated` 把调用方帧 kind 写进 ambient——但这只能修一层（ambient 再往上的祖先仍不可达），不能替代转换点闸。
   - go 侧同构点见 Q5；plaita 侧 parent 链是真实执行链（`ExecutionContext.parent`，context.py:258），无 ambient 断链问题，但为三仓规格一致仍应采用转换点闸。

### Q5 go：作用域闸、传播改造点、checkpoint 结构缺口

**结论：go 的闸（单层 parent 判定）与 rs 基线同构，能挡所有嵌套；新语义下的传播改造点共六处 + executeNode 两个插入位；IsRetryableError 对未知错误默认可重试，与 rs 同病；checkpoint 是扁平单帧结构，缺每帧路径；且 go 的恢复路径目前完全没有预算概念（FlowDeadline 不入快照、Resume 不重挂 timeout）——设计④在 go 是从无到有而非「跟进」。另一形态差异：go 的 Resume 以 `*Checkpoint` 为载体（无进程内活帧句柄），设计稿「v1 进程内句柄」在 go 侧没有直接对应物，需要补规格。**

证据：

1. 闸：`pkg/workflow/suspend.go:356-360` `rc.ParentWorkflowContext != nil` → 具名拒。任何嵌套帧的 rc 都有非空 parent link：child 种子（`pkg/workflow/nodes/child.go:58-61`）、loop 家族（`pkg/workflow/nodes/collection.go:23-32` runCollectionChild 同款种子）、parallel（`runtime.CloneContext`，`internal/runtime/context.go:566-582`，582 行 `ParentWorkflowContext = ctx`）。failing 测试 `pkg/workflow/checkpoint_test.go:355-371`。
2. reference 隔离（与 rs 同构的 ambient 断链）：`pkg/workflow/nodes/reference.go:91-106`——seed 的 `ParentWorkflowContext` 指向 ambientCtx（94-98：ambient 只带 global/env/FlowDeadline）。链：`seed → ambient → nil`。**go 若照抄「沿 parent 链的 frame_chain_blocker」会踩同一个坑**；同款修正（转换点调用方帧 kind）适用。
3. 传播改造点（go）：`nodes/child.go:62-65`、`nodes/reference.go:80-83`、`nodes/collection.go:23-32`、`nodes/loop.go:184-218`、`nodes/map|filter|find|reduce|while.go`（同 collection.go 助手）、`nodes/parallel.go:160-175`（分支流执行）与 `:243` 起（分支编排）——全部要把「stringify/wrap 后按普通错误走」改为识别 `AsSuspend(err)`（`%w` 包装保留 errors.As 链，child.go:64 的 wrap 不会破坏识别）。
4. executeNode 两个插入位（`pkg/workflow/workflow.go:476-553`）：
   - retryFunc 内、`contextTimeoutError`/`IsRetryableError` 分类（504/516-521）之前：`if AsSuspend(err) != nil { return err }`——**不带 RetryableError 标记返回**，否则 `IsRetryableError` 对未知错误默认 true（`pkg/workflow/error.go:253-274`，273 行 `return true`）会让 retrier 重跑整个子流；
   - `Retrier().Do` 返回后、`IsContinue`（526）之前：`if susp := AsSuspend(err); susp != nil { return nil, err }`——否则 errorHandler continue/continue-with 把挂起吞成默认值（537-539 HandleFinalError）。
   - 流程级已有正确先例：`workflow.go:202-205` SuspendError 不标记、不重试。
5. checkpoint 缺口（`pkg/workflow/checkpoint.go`）：`Checkpoint` 扁平单帧（73-86：SuspendedNode/NextNode/Context）。`SnapshotRuntimeContext` 会递归快照 `$PARENT` 链（303-314，`maxParentChainDepth=64`）——child 链的父帧 `$NODE` 因此**顺带**入快照；但 (a) reference 链被 ambient 掐断，父帧状态完全不在快照里；(b) `RestoreRuntimeContext`/`restoreRuntimeChain`（321-375）把 `$PARENT` 还原成表达式可见的静态链，**没有父 flow 定义、没有子节点 id、没有父帧继续位置**——嵌套恢复不可能从现有 Checkpoint 重建中间帧。缺的结构：每帧 `{child_node_id, flow 标识（+ flow 定义解析途径），以及 reference 帧的独立状态快照}`；schema version 需 bump 并定旧 checkpoint 的拒绝策略。
6. go 恢复预算现状：`Resume`（suspend.go:183-288）全程不挂 `context.WithTimeout`（对比 Execute 177-181）；`FlowDeadline` 不在快照键集里（259-301 无此键）、`RestoreRuntimeContext` 不还原（321-349）→ `flowBudgetBinding`（workflow.go:559-570）在恢复会话恒 false。设计④在 go 落地时要同时补「快照带预算或恢复时重挂」两件事，且每帧一份。

### Q6 plaita：NORMAL 子执行的 fail-open 机制面、最小侵入改造点、checkpoint 结构

**结论：fail-open 的机制面是「子执行的策略选择不看 mode + 步进体无 is_suspending 检查」两点；最小侵入的 fail-closed 化 = `_advance_one` 加挂起检查 + 专用 carrier 异常类型 + runner 异常链的一个提前 except 分支。checkpoint 是单帧扁平 dict（$NODE 单字典），父 to_dict 不含子帧、子的 $PARENT 是构造期快照——嵌套 checkpoint 必须逐帧另存。**

证据（fail-open 全链，plaita main）：

1. `plaita/node/child.py:77-83`（InlineFlow.arun）/ `126-132`（ReferenceFlow.arun）：`child_execution = execution.get_child_execution()` → `arun_compatible(self.child_flow, lazy, child_input)`。
2. `plaita/core/executor.py:397-402` `get_child_execution`：`child.mode = self.mode`——继承 distributed 也无用。
3. `plaita/core/executor.py:481-496` `_prepare_strategy`：`strategy = self._strategies[GENERATOR if lazy else NORMAL]`（493-495）——**子执行恒 NORMAL/GENERATOR，挂起语义整个缺席**。
4. `plaita/core/strategies.py:578-599` `_advance_one` 与 `140-177` `NormalStrategy.execute`：无任何 `is_suspending` 检查——挂起节点的 pending dict 被当普通结果写进子执行 `$NODE`（`plaita/core/runner.py:571` `update_node_result`），流程**越过挂起点继续走**，end 输出或 $NODE 表被当子节点返回值——静默误执行实锤。
5. loop 家族同路：`plaita/node/loop.py:270-271、293-294、361-386、419-430、451-461、503-506`；parallel 分支：`plaita/node/concurrent.py:190-193、461-463`（`run_compatible/arun_compatible(pb.flow, False, ...)`）。
6. 注意面：**根流程 NORMAL 模式同样 fail-open**（NormalStrategy 无挂起检查）——设计稿只写「修子执行挂起传播」，根流程 normal/generator 模式的同款洞需要一并定语义（修复 or 明文保留并告警）。

最小侵入改造点：

- `_advance_one`（strategies.py:578-599）run_node 返回后加：`if getattr(node, "is_suspending", False) is True: raise FlowSuspendedCarrier(...)`（携带子执行现场）——单点覆盖 normal/generator 的一切调用（子执行+根）；
- `plaita/core/runner.py:681` 通用 `except (Exception, NodeException)` 之前加 `except FlowSuspendedCarrier: raise`——现在的取消是靠 `_raise_if_cancel_requested` 复查逃出该分支的（runner.py:602-615 入口拒绝、682-683 取消优先重查），挂起 carrier 没有对应机制，不提前 except 就会进重试/errorHandler（被 `_handle_node_error` 包装）；
- `child.py` 的 FlowNode.arun 不捕获 carrier 原样上抛（现状的裸 `return await ...` 恰好满足，验收时钉住「不得包 try/except」）；
- 传播/恢复：checkpoint 逐帧化 + `_handle_resume`（strategies.py:388-460，现只认根帧 last_node_id）扩成沿帧路径恢复；订阅注销 `_unregister_suspended_subscription`（462-484）按最内帧执行。

checkpoint 结构：`context.to_dict()`（`plaita/core/context.py:612-618`）→ `CheckpointState.to_checkpoint_dict()`（`plaita/core/state.py:902-921`）= **单帧**扁平 dict；`$NODE` 是单一字典（state.py:647 `node_results`），**不分帧**。子执行有独立 FlowExecution/ExecutionContext——父 to_dict 不含子帧；子的 `$PARENT` 是**子上下文构造时刻**的惰性快照（context.py:307-308 `_LazyRootSnapshot`，482-487 setup_flow 消费），非活父状态。→ 嵌套 checkpoint = 每帧一份 to_dict + 帧元数据（child node id / flow 标识 / 帧序）。

### Q7 三仓通用：子节点自带 errorHandler/retryTimes；node timeout 与挂起的次序

**结论：三仓的现状基线里，内层挂起（及闸拒绝）都会流经父节点（child/reference 节点自身）的策略链——rs/go 是「可重试+可吞」，plaita 根本没有 carrier。v1 的「穿透不经策略链」在三仓都是「在策略链之前拦截」这一件事；timeout 与挂起的次序三仓一致：到达挂起节点前预算耗尽→超时赢；到达→挂起赢（deadline 不再复查）；恢复后父帧对子节点的 node.timeout 不再生效。**

- rs：拦截位=Q1（match body 顶部）；在飞实现已落地 suspension carrier 臂（exec.rs:599-602），SuspendRejected 臂缺失（见修正清单#3）。次序：node_deadline body 入口判过期（exec.rs:552-553）；内层帧预算=min(内层 timeout, 父 node_deadline)（context.rs:103-109）；到达挂起节点后 deadline 不再复查（advance_from 400-402 直接 suspend_flow）。
- go：插入位=Q5.4 两点；`IsRetryableError` 默认 true（error.go:273）+ `HandleFinalError` continue 吞（workflow.go:537-539）。次序：节点 ctx `WithTimeout`（workflow.go:477-481）+ body 前 `ctx.Err()` fail-fast（490-494）；子流 ctx 链式 min（Execute 177-181 派生 ctx 自动继承父 deadline）。
- plaita：挂起只发生在 distributed 逐步边界（strategies.py:368-384）；预算 per-step 新鲜发放（strategies.py:228-232 + `plaita/core/deadline.py` 默认 1800s）+ 节点 min 合成（runner.py:619-648）+ flow 级预算具名终态（656-676 ExecutionDeadlineError，不可被 handler 吞）。改造后子执行 carrier 应走 `FlowCancelledException` 的先例位置：入口拒绝（runner.py:602-615）+ 取消优先于重试分类（682-683）+ 退避打断（243-274）——carrier 的 except 分支加在 681 行的通用 except 之前。

### Q8 设计稿未列、验证席担心的三个交互

1. **恢复路径的错误语义黑洞（最严重）**：内层 flow 在恢复续跑中失败时——`FlowTimeout`、内层 end `resultType:"error"`（基线 nodes/mod.rs:429-443 的 FlowResultError 形 NodeError）、内层节点 abort 超时——基线执行路径本来会经过两级策略：内层 `execute_on` 尾（exec.rs:341-355：FlowTimeout 终态 / abort 超时交内层 timeoutHandler）→ 父帧子节点的 errorHandler/retry（exec.rs:659-697）。而恢复循环对 `Err(e)` 是直接上抛宿主（在飞 exec.rs:1030-1035）——**两级策略链全部绕过**。同一逻辑失败在首跑与恢复后走不同的处理路径（例：内层节点 abort 超时 + 内层 flow timeoutHandler=continue：首跑返回 null 继续，恢复会话直接整体失败）。必须二选一钉死：返回步失败逐级重放两级策略，或明文声明「恢复会话失败=终态直抛，两级行为不保」。
2. **resume 的单次消费与并发面**：(a) 恢复入口无取消门（Q3）；(b) 同一 `SuspendedExecution` 的防重入只有 pending 状态检查（基线 exec.rs:809-816）——它只挡「同一节点二次 resume」；返回步弹栈后（在飞 resume 循环 `stack.pop()`），半途崩溃/并发的旧 handle resume 会从半程栈再入，且各帧 rc 是**共享活对象**，无法从状态判断「已消费」；(c) 再挂起拼接（③）后旧 handle 的栈与新现场共享帧对象，旧 handle 二次 resume 会把同一父帧推进两次。rs 的 `Flow` 没有 reentry guard（go 有 `running atomic.Bool` workflow.go:92-94/168-171；plaita 有 `_begin_run` executor.py:465-467）——三仓规格需统一钉：handle 单次消费语义 + 挂起会话代数（generation）+ 恢复循环持有树级重入守卫。
3. **订阅生命周期与传播失败的交接**：闸拒绝发生在订阅注册前（基线 exec.rs:429-435 次序，好）；但 Q4 的 loop 转换点毁 carrier 时已注册的订阅成孤儿（SuspendedExecution drop 无 Drop 清理）；再挂起拼接时新旧订阅交接依赖「① 的注销（基线 exec.rs:850-862）先于新 suspend_flow 的注册」——该次序目前隐含在 resume() 的代码顺序里，未被测试钉住；若恢复准入成功但后续 advance 再挂起失败（如注册失败），会话悬在「旧订阅已注销、新订阅未立起」的中间态。规格需钉：订阅交接的原子性要求与失败时的可观测面（至少 WARNING，与「降级必须可观测」约定一致）。

---

## ② 设计稿修正清单

### 必须修

1. **作用域闸的机制改为「传播转换点检查调用方帧 kind」**（rs 在飞实现已踩坑）：
   - `execute_child`/`execute_reference` 转 carrier 前检查调用方 rc 的 `frame_kind`（`LoopIteration`/`ParallelBranch` → `SuspendRejected`）；
   - `run_collection_child`/`run_branch_flow` 无条件拒绝 carrier 穿越并把 `FlowError::SuspendRejected` 映射为 `NodeError::suspend_rejected`；
   - `suspend_flow` 的 `frame_chain_blocker` 降级为注册前的 advisory 快路径（防孤儿订阅），不再作为正确性保证；
   - go/plaita 跟进规格按同构写入（go 的 reference ambient 断链同在：reference.go:91-106）。
2. **rs：`run_collection_child` 的 stringify**（nodes/mod.rs:675 工作区行号）——`SuspendRejected`/`Suspended` 一律不得落 `NodeError::transient`；现状后果=闸拒绝被重试 N 次、N 条孤儿订阅、可被 errorHandler 吞。
3. **rs：`execute_node` 补 `SuspendRejected` 拦截臂**（在飞 exec.rs:594-640 无此臂）——否则 parallel join 路径（nodes/mod.rs:1270/1348 特判）上抛的拒绝仍会在子节点 errorHandler continue 处被吞（基线 exec.rs:683-697 尾链），违背 error.rs:40-41 的 fail-closed 承诺。
4. **rs：background 分支的拒绝不可只 eprintln**（nodes/mod.rs:1304-1307）——join 路径已特判 SuspendRejected（1348），后台路径只有日志，parallel 节点照常成功=具名拒被静默化。
5. **钉死恢复会话的错误语义**（Q8.1）：返回步/续跑失败是否重放「内层 execute_on 尾 + 父帧子节点策略链」。不钉就是第二处 fail-open。
6. **resume 单次消费 + 树级重入守卫**（Q8.2）：rs 无 reentry guard、resume 幂等性无定义；go/plaita 各有守卫但均不覆盖恢复会话。
7. **plaita 跟进范围补一条**：根流程 normal/generator 模式的 fail-open（NormalStrategy/_advance_one 无挂起检查）与子执行 fail-open 是同一个洞，一并修或具名保留。

### 建议修

8. 事件约定升格为规格条文（Q2）：恢复方向逐帧 FlowEnd（最内→根）、不重发 FlowStart、两种 NodeEnd 形状合法——在飞实现已按此实现，但约定只存在于代码里；go/plaita 跟进按此对拍。
9. resume 入口取消门（Q3）：`is_cancel_requested` 置位时的恢复行为（拒绝类型、决策是否算已消费）。
10. 预算细则显式化（Q3）：每帧新鲜发放的公式（own timeout or 1800s）、是否与父帧剩余 min 重合成、「子节点 node.timeout 只覆盖首次执行」的声明。
11. go 的 v1 载体形态补规格（Q5）：go Resume 以 `*Checkpoint` 为载体，无进程内活帧——要么 `SuspendError` 携带活帧（同 rs），要么扩展 Checkpoint 帧结构并从根 Workflow 图解析中间 flow；两者工作量与风险不同，设计稿未选。
12. go checkpoint schema bump 与旧格式拒绝策略（Q5.5）；FlowDeadline 入快照或恢复时重挂（含每帧）。
13. 验收测试补位（rs）：loop→reference 旁路（修后应具名拒）、loop 直连 + errorHandler continue / retryTimes>0 组合、parallel background 分支、恢复会话中途失败的两级策略行为、cancel 期间恢复。

### 存疑

14. `checkpoint_json` 的 `call_stack` 目前只有路径观测（flow_id/node_id 序列，在飞 exec.rs:804-811 与 824-832）——v1 够用；v2 JSON 回放需要各帧 rc 快照，而 reference 帧的父状态在「$PARENT 链快照」里永远缺失（ambient 隔离是特性不是 bug）——v2 规格需为 reference 帧设计显式快照，设计稿未提。
15. 每帧新鲜发放 vs min 重合成（#10 的抉择）——若选独立发放，深链墙钟无上界是否可接受需要 ops 口径。
16. 订阅 correlation_id 在嵌套下全帧共享 execution_id（rs context.rs:239-240 继承；go context.go:529-534 同款）——worker 侧按 correlation 投递时无歧义，但若未来按 (flow_id,node_id) 路由，内层 flow_id 入订阅（基线 exec.rs:462-464）是否会引起宿主路由混乱，属 worker 层议题，v1 建议只在规格里记一句。

---

## ③ v1 还应钉住的语义细则

1. **事件账目全表**：挂起方向（每帧 FlowStart + FlowEnd(nil,nil)，穿越节点 NodeEnd(error)）、恢复方向（无 FlowStart；每完成帧 NodeEnd+FlowEnd，最内→根；直接挂起节点 NodeEnd 不重发 NodeStart）——含 go `triggerFlowEnd`（suspend.go:205-211 defer）与 plaita `on_flow_suspend/on_flow_resume`（strategies.py:380-381、443-444）的对拍口径。
2. **返回步的回填细则**：`$NODE[child]=v`、`$LAST_NODE=child`、不写 `$ERROR`、outputType 校验失败=ResumeError（在飞 nodes/mod.rs:408-415 + exec.rs ResumeError 已如此）——写进规格；switch/if 的 `$BRANCH` 在返回步不受影响（子节点非分支节点）。
3. **恢复会话的预算表**：每帧发放公式、min 重合成抉择、node.timeout 跨挂起失效声明、恢复会话期间无 per-node 重挂（go 需从零补）。
4. **取消矩阵**：cancel 于挂起期间→resume 行为（#9）；cancel 于恢复中途→每帧步界取消生效（advance_from 门），已推进的帧如何落定（call_stack 保留再挂起？还是终态取消？）——v1 需二选一。
5. **resume 消费语义**：单次消费；重复/concurrent resume 的判定面（pending 检查 + generation）；拼接后旧 handle 全部失效；rs 补树级重入守卫（对齐 go `running`/plaita `_begin_run`）。
6. **订阅交接**：闸拒绝不注册（已成立，钉成验收条款）；传播/转换失败时已注册订阅的注销责任；再挂起时「旧注销先于新注册」的次序；交接失败可观测。
7. **$PARENT 可见性声明**：恢复会话中父帧经回填 `$NODE` 闭合对子结果的可见性（父不持子 rc，`$PARENT` 是子看父的单向链——rs context.rs:297-300、go CloneContext 注释 context.go:558-565、plaita `_parent_snapshot` context.py:307-308 三仓同向）；返回步后子帧 rc 即弃，无泄漏面。
8. **作用域闸的拒绝语义**：拒绝是终态错误（不挂起、不写 pending、无订阅）；错误类型（rs `FlowError::SuspendRejected`/go 专属 error/plaita 专属异常）与 `$ERROR` 是否记账。
9. **go checkpoint 兼容**：schema version bump、旧扁平 checkpoint 遇嵌套恢复的拒绝面（ResumeGuard，非静默）。
10. **plaita 对拍基线**：`is_suspending` 属性判定用 `is True`（strategies.py:286-288 的 bool 代理防御）在 `_advance_one` 新检查里同样适用；`callback_manager.on_node_suspend/on_flow_suspend` 在嵌套传播下的每帧发射口径。
