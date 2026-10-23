# NS-M4 嵌套挂起三引擎差分对拍集

程式一（`docs/PROGRAM.md` 程式一）NS-M4 收官门：**plaita（Python）/
plaita-go / plaita-rs** 三引擎在同一份嵌套挂起用例集上对拍——同 flow、同
决策载荷，比挂起点序列、最终输出与拒绝类别。

比较面钉在**语义等价类**（A1 `tools/diffcheck.py` 老规矩）：

- 结果值 JSON 相等（数字域精确比较：int 不塌 f64、float 1e-12 相对容差）；
- 挂起点序列（`suspended_nodes`）逐位相等；
- 拒绝按**类别**（`loop_frame` / `parallel_frame` / `resume_guard`）而非逐字
  文案——三仓文案是各自家方言，只展示不计分。

## 跑法

```bash
# 三腿全跑（推荐；缺席引擎自动 SKIP 并注明）
make ns-m4
# 等价：
#   cargo build -p plaita-cli && python3 tools/nested_suspend_diff/diffcheck.py

# 单腿 / 双腿
python3 tools/nested_suspend_diff/diffcheck.py --engine rs,go
python3 tools/nested_suspend_diff/diffcheck.py --engine plaita

# 过滤用例 / 详细输出
python3 tools/nested_suspend_diff/diffcheck.py --case grandchild -v

# 单腿直跑（一行一个结果 JSON）
python3 tools/nested_suspend_diff/run_rs.py                          # 需先 cargo build -p plaita-cli
python3 tools/nested_suspend_diff/run_go.py                          # 自动 go build gorunner（缓存 target/nsm4/）
../plaita/.venv/bin/python tools/nested_suspend_diff/run_plaita.py   # 需 plaita venv
```

退出码：`0` = 全部用例绿（含 known-red 形状保持）；`1` = 有 DIVERGENCE /
STALE-EXPECTED / known-red 漂移。腿不可用（缺二进制/工具链/plaita 仓）由各
runner 以 exit 3 报告，diffcheck 记 SKIP。

## 各腿驱动方式（单进程 execute→suspend→resume）

| 腿 | runner | 驱动面 |
|---|---|---|
| rs | `run_rs.py` → plaita-cli 隐藏子命令 `ns`（`crates/plaita-cli/src/ns.rs`） | `ir::load_with_flow_provider` + `flow.execute(&host)` → `FlowError::Suspended` → `suspended.resume(kind, data)` 循环 |
| go | `run_go.py` → `gorunner/main.go`（`go build`） | `ir.LoadWithFlowProvider` + `wf.Execute(ctx)` → `workflow.AsSuspend` → `wf.ResumeSuspension(ctx, susp, type, data)` 循环 |
| plaita | `run_plaita.py`（plaita venv） | `FlowExecution(mode=distributed)` 逐步 `run_distributed` + `saved_context` 注入恢复；恢复步未到根 end 时以 `resume_type="continue"` 推进（plaita 分布式逐步契约） |

订阅绑定：三 runner 都把挂起期登记的订阅 id 写进决策载荷的**私有绑定键**
`__plaita_subscription_id`（三仓同键同语义，准入后剥掉、不污染 event_data）。
`"bind": "wrong"` 时写 `sub-wrong`（深位绑定准入负例用）。

## 用例集（`cases/*.json`，13 例）

统一形状：

```json
{
  "name": "...",             // 语义锚点名
  "doc": "...", "source": "...",  // 语义与三仓测试蓝本出处
  "flow": {...},             // 主 flow（deployment IR）
  "flows": {"sub": {...}},   // 可选：reference 节点按 flowID 解析的注册表
  "input": {...},
  "resume": [ {"type": "event|timeout|cancel", "data": {...}, "bind": "auto|wrong"} ],
  "suspend_node": "ev",      // 首个挂起点（拒例为 null）
  "expected": {"outcome": "completed|rejected|suspended",
               "suspended_nodes": [...], "output"?: ..., "reject_reason"?: ...}
}
```

覆盖矩阵（用例 ← 三仓测试蓝本）：

| # | case | 语义 | rs | go | plaita |
|---|---|---|---|---|---|
| 01 | child_chain_suspend_resume | child 链挂起+恢复到根（单帧栈） | deep_binding* / 同族 | ChildChainParksAndResumes | ProbeScenario::event_resume |
| 02 | grandchild_chain_two_frames | 孙代双帧栈+恢复态沿 $NODE 链 | grandchild_chain | GrandchildChainTwoFrames | grandchild_double_frame |
| 03 | reference_chain_isolation | reference 链隔离（$PARENT 不可见）+穿透 | reference_chain_isolation | ReferenceChainIsolation | reference_chain_isolation |
| 04 | loop_direct_named_rejection | loop 直连具名拒（注册前拒） | loop_boundary | LoopDirectNamedRejected | loop_direct |
| 05 | loop_child_bypass_named_rejection | loop→child 旁路（转换点闸） | 转换点闸 | 同族 | loop_via_child |
| 06 | loop_reference_bypass_named_rejection | loop→reference 旁路（Q4 遮蔽） | loop_reference_bypass | LoopReferenceBypassNoOrphan | 同语义面 |
| 07 | loop_errorhandler_continue_not_swallowed | errorHandler continue 不吞闸拒 | not_swallowed_by_error_handler | LoopRejectionNotSwallowed | error_handler_continue |
| 08 | parallel_branch_static_rejection | parallel 分支派发前静态拒 | parallel_background_static | ParallelJoinBranchStaticReject | parallel_branch_static_gate |
| 09 | resuspension_merges_call_stacks | 恢复中再挂起栈合并（深位 child 内 event） | resume_continuation_re_suspension | ReSuspensionMergesCallStacks | resume_then_resuspend | ⚠️ 见分歧清单 |
| 10 | deep_binding_wrong_subscription_refused | 深位订阅绑定准入（错绑定拒、保持挂起） | deep_binding_enforcement | DeepBindingAndTimeoutAtDepth | stale_event_binding |
| 11 | deep_timeout_carrier | timeout 载体深位（节点翻 timeout 后续跑到根） | deep_binding timeout 臂 | 同族 | 同通道 |
| 12 | deep_cancel_carrier | cancel 载体深位（决议即继续） | resume_node_body Cancel 臂 | Event.Resume ResumeCancel | cancel_resume_resolves_innermost |
| 13 | middle_frame_resuspension | 恢复会话中间父帧续跑中再挂起（三层栈→合并） | 栈合并同面 | 同面 | suspension_inside_middle_frame |

首轮对拍结果（2026-10-23）：**12/13 三引擎一致**；1 例 recorded divergence
（known-red 持有，见下）。

## 已知良性差异（不计分的方言面）

- **错误文案**：同一拒绝三仓措辞各异（rs/go 点名 `loop-iteration` /
  `contains a suspending node` / `not bound to the suspended subscription`；
  plaita 点名 `frame boundary` / `parallel frames` / `Stale … 不匹配`）——
  比较面只取类别。
- **reference 节点引用键方言**：用例文件按 plaita 生产者方言写
  `"flowID"`（plaita `ReferenceFlow.LEGACY_KEYS` 同款）；**rs/go 装载层只认
  `flowId`/`flow_id`**（`ir::load`/`ir.Load` 遇 `flowID` 直接装载拒）。三仓
  runner 在装载前做 `flowID → flowId` 改名（`ns.rs::adapt_reference_keys` /
  `gorunner::adaptReferenceKeys`），引擎路径不动。这是对拍集顺带发现的
  **跨引擎装载面分歧**（plaita 侧模型产出 `flowID`、go/rs 拒收），引擎侧
  收口属后续轮。
- **恢复推进契约**：rs/go 的 resume 一路走到根完成；plaita 分布式是逐步
  契约（恢复步可能止于 child 回填，需 continue 续步）——runner 语义拉齐后
  比较最终根输出，非引擎分歧。
- **go 无进程内 cancel 面**（cancel-while-suspended 的恢复入口拒绝，rs
  `resume_during_cancel_is_refused` 独有）——不入对拍集；go 取消语义由宿主
  层（ctx 超时）表达，见 go 测试文件头注记。
- **订阅账目断言**（注册/注销计数、correlation_id 路由）是各仓测试的面，
  不进对拍集——三仓 registry 假件形状不同，结果形状里无处安放。

## 分歧清单（recorded divergences，引擎修复属后续轮）

### 1. `resuspension_merges_call_stacks`（known-red 持有）

- **现象**：恢复 evA 后内层续跑进入 child 节点 `c2` 时，plaita 崩溃：
  `AttributeError: 'ExecutionContext' object has no attribute
  'get_child_execution'`（outcome=engine_error，挂在首个恢复步）；rs/go 均
  completed（栈合并 `["evA","evB"]`、输出 `"done"`）。
- **归因初判**：`_advance_frame_to_end`（plaita `core/strategies.py:992`）
  构造恢复会话的帧推进 runner 时写的是 `NodeRunner(context)`，**未带
  `node_execution=<FlowExecution facade>`**；child/reference 节点的 arun 要
  经 facade 的 `get_child_execution()` 建子执行（裸 ExecutionContext 没有
  该方法——对照 `test_nested_suspend.py::test_carrier_escapes_normal_drive`
  的 runner 接线注释）。即：**恢复续跑途中再进 child/reference 节点**这条路
  plaita 未接 facade。
- **为何 NS-M3 没拦住**：plaita 的再挂起用例（`RESUSPEND_ROOT`、
  `middle_frame`）都在**同一帧的 event** 上再挂，或在首挂前就走完 child——
  「恢复会话内层续跑进入 child 节点」（rs
  `resume_continuation_re_suspension_merges_call_stacks` 的主路径）没有覆盖。
- **翻绿指引**：给 `_advance_frame_to_end` 补 facade 接线后，本例三引擎同绿
  → 从 `diffcheck.py` 的 `KNOWN_RED` 删除该条并重跑（升入正常对拍门）。
- **对照面**：case 13（middle_frame_resuspension）三引擎一致——其再挂起
  event `w2` 与已返回的 child `callb` 同帧并列，恢复续跑不进新 child。

## 维护约定

- 新增用例：放 `cases/NN_*.json`，`expected` 必须与三仓测试蓝本对得上
  （三引擎一致但与 expected 相悖 → 门报 STALE-EXPECTED，先修用例）。
- 新增 recorded 分歧：先在「分歧清单」写归因，再进 `KNOWN_RED` 钉形状；
  修复后删表项（翻绿进门）。永不为了绿灯改 expected 去迁就分歧。

## plaita 腿的源路径钉定

`run_plaita.py` 以 `PLAITA_CHECKOUT` env（缺省兄弟仓 `../plaita`）**显式钉定**
plaita 源路径并置于 `sys.path` 置顶——venv 的 editable 安装指向共享主仓工作树，
其提交位置由并行会话决定（case 09 曾因工作树停在修复前提交而假红
engine_error）。对拍门必须钉确定性源；本机跑法示例：

    PLAITA_CHECKOUT=$PWD/../plaita-wt-ns3 make ns-m4

（plaita-wt-ns3 = NS-M3/fix 分支 worktree，含嵌套挂起全部修复。）
