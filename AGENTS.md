# AGENTS.md — plaita-rs

> plaita 的 Rust 版引擎 + IR 解析。参考 [plaita-go](../plaita-go)（同仓群的 Golang 版）移植：
> **只做「解析 Plaita 的 Flow 并运行」的运行时**，不做 Flow Worker（队列/租约/checkpoint/事件桥
> 全部不在范围内，属于 worker 运行时层）。负责人：jeffkit | 创建：2026-10-21

## 定位与范围

- **在范围内**：部署形态 IR 装载（归一化 + 校验 + fail-closed 拒载）、plaita 表达式语言、
  执行引擎（变量根 / 条件 / 循环 / 并行 / 子流程 / 错误·超时·重试策略 / 事件回调）、
  内置节点（start / end / assignment / switch / if / loop / parallel / child /
  code〔python sidecar〕/ http；**#80 对齐转正**：map / filter / find / reduce /
  while / case〔脱糖 switch〕/ mock / reference〔provider 注入〕；
  **挂起族七类**〔suspend/resume 架构，B4 r12〕：event / hitl_await / delay /
  approval / redis_queue / kafka_queue / http_callback）。
- **不在范围内（刻意不做）**：`pkg/worker`（Redis Streams 消费、租约 Lua、fence、DLQ、
  checkpoint 恢复、drain）、console BFF、沙箱 driver、影子对账 harness、agentproc。
  plaita-go 的这些层按 queue/lease 协议接 worker，本仓接不进来也不需要。
- 表达式与 IR 的**语义权威**与 plaita-go 相同：plaita `plaita/core/expression_parser.py`
  文法 docstring + `test_expression_golden.py`（golden 用例文件逐字复用 Go 仓导出的
  `testdata/expression_golden.json`）+ plaita 的 `ir_validate DEFAULT_RULES`。

## 仓库结构

```
crates/
  plaita-expression/   表达式语言：手写递归下降文法解析器 / 求值器 / $F 函数库(~70) /
                       {% %} 模板 / PyStr·PyRepr / %-格式化 / strftime；tests/golden.rs 跑 43 条 plaita golden
  plaita-engine/       引擎：
    src/ir/            部署 JSON 装载——normalize（别名折叠/超时→ISO8601/metadata 暂存）
                       + validate（R1 唯一 start+可达 / R3 next 引用 / R2 嵌入流程达 end /
                       R4 joinBranches⊆分支名 / R5 $NODE 引用 / R6 $F 注册表 / R7 output 字面量）+ 拒载清单
    src/context.rs     运行时上下文：$INPUT/$NODE/$PARENT/$GLOBAL/$ENV/$ERROR/$BRANCH/$FLOW_ID/
                       $LAST_NODE/$EXECUTION_ID + current（LOOP-ITEM 等）；RwLock + 快照（并行分支并发读）
    src/exec.rs        执行循环：flow/node 两级 retry（flow 固定 10ms / node 指数 1s·2^n cap 30s）、
                       确定性错误不重试、超时归属（FLOW_ERROR vs NODE_ERROR）、错误/超时策略
                       （abort/continue/continue-with + code/message + $ERROR 记账）、事件回调
    src/exec.rs 末尾   suspend/resume：挂起边界（pending 校验/订阅注册/$NODE 落账/
                       FlowError::Suspended 载现场）+ SuspendedExecution::resume
                       （绑定准入/pending 校验/状态翻转/订阅注销/恢复+续跑一步到位）
    src/nodes/         节点体：condition（15 算子 + None 短路）、loop（末次子结果语义）、
                       suspend（挂起族七类执行体/恢复体 + hitl 客户端，http feature 门），
                       map/filter/find/reduce/while（集合并五类，loop.py 逐语义）、
                       mock（value 原样透传不求值）、reference（child_isolated 隔离上下文）、
                       parallel（joinBranches 空=全后台；分支失败=错误哨兵）、
                       code（python3 sidecar，镜像 plaita _RUNNER_TEMPLATE + env 白名单 + 进程组超时）、
                       http（{data,status,statusText,headers} 契约；非 2xx 不是错误）
  plaita-cli/          bin `plaita-rs`：run / validate / eval
scripts/
  extract_python_node_types.py   Python↔Rust 节点清单差分门的权威提取器
                                 （自 plaita-go 同名脚本 vendor；从 plaita NodeRegistry 现场取清单，
                                 entry points 不可见/插件加载失败即硬失败，绝不输出缩水清单）
tools/
  go-ref/              Go 参照 runner：直接 import ../plaita-go 的 pkg/ir 执行同一 flow
  go-ref-bin           编译产物（本地构建）
  cases/               差分用例 flow 文档
  diffcheck.py         差分对账：每个用例双跑（Go vs Rust），结果 JSON 数值等价比对 / 错误核心文本比对
```

## 与 plaita-go 的刻意分歧（全部 fail-closed，不静默降级）

1. **javascript code 节点拒载**（加载期报错）：Rust 侧没有进程内 JS 解释器；Go 侧是 goja。
   要跑 JS 的 flow 请留在 plaita/go 运行时，或改写为 python。
2. **agentrun 与 plaita-nodes 节点族未移植**（gate/notify 族/parse_json/writefile/llm/decision/
   hitl/event…）：加载期逐类型给出 hint 报错（`KNOWN_UNSUPPORTED`）。Redis 节点同。
3. **dict 保持插入序**：serde_json `preserve_order` —— Python dict 序语义直接成立，
   闭合了 Go 版「键排序」的文档化分歧（`$F.keys`/`items`/`json_dumps`/模板渲染都受益）。
4. **`$F.index` 对 str 返回字符下标**（Python 语义）；Go 版返回字节下标（已知偏差，未跟随）。
5. **python sidecar 的代码错误按确定性分类**（不烧重试预算）：按 plaita 的 ValueError 语义；
   Go 版因错误码 10000 落入「可重试」（retryTimes=0 时无差别）。
6. 表达式嵌套字符串**不二次求值**、无 `switch:eagerMode` 全局、无 harness/调试 generator ——
   与 Go 版一致的简化面。
7. **Python 权威三对齐（B4 r7，规格以 plaita Python 引擎为准）**：
   ①无 timeout 的 flow 补执行级缺省预算 1800s（`PLAITA_EXECUTION_DEADLINE_SECONDS`
   可调、非法值回默认并告警；Python `deadline.py`/`strategies.py:144`；Go 依赖 worker 层
   deadline，本引擎自带）；②**无流级 errorHandler**——顶层该键=未知键 warning+忽略，
   流恒单次执行，Go 的整流重投 ×(1+n) 不实现（Python Flow 模型无该字段）；③parallel
   分支并发上限=Python 后台池口径（默认 8、同名 env 可调、嵌套池内降级串行内联；
   Go 分支 goroutine 无上限）。
8. **Python 权威二对齐（B4 r8，P3 双单）**：
   ④http 响应帽可调——`PLAITA_HTTP_MAX_RESPONSE_BYTES`（默认 10MiB、strip 后严格整数 ≥1、
   非法值警告+回默认；超限流式帽读具名错误 code 1003 可重试，消息点名 maxResponseBytes
   与 env 提升路径；Go/ureq 为 `into_string` 固定 10MiB）；⑤执行级取消面——`Host::cancel()`
   （幂等置位粘滞 `cancel_requested` + 节点作用域 `cancel_event`：节点边界具名拒绝
   `FlowError::Cancelled` code:-4 且先于重试/errorHandler、code 沙箱等待循环 killpg 整树、
   退避 0.5s 分片可打断、默认步界取消、Host 克隆共享=父子链传播；Go 无此面，Python
   runner/executor 2026-10 波次③语义，出处 plaita#267）。
9. **$PARENT 未知根 → null**（表达式层，#80 对齐）：`$PARENT.<名字>` 命名父作用域的
   一个根，父上不存在的根求值为 null（快照对象缺省字段读；golden `ref_parent_isolation`
   的 `$PARENT.n` → null 钉）——不再 KeyError。无父执行时行为不变（仍报错）。

## 节点对齐：plaita-go#80 parity 的 Rust 侧（2026-10-23）

**已转正 8 类**（golden 67 例逐例对拍 plaita Python 真跑 oracle，
`tests/node_parity_golden.rs`；golden 文件自 plaita-go `pkg/ir/testdata/` 逐字复用）：

- **集合五类 map/filter/find/reduce/while**：loop.py BaseCollectionNode 家族逐语义
  （字符串集合按 JSON 数组解析/普通串单元素；reduce 的 initial 缺省取首元素、空集合
  无 initial = 确定性 IndexError；while 先判断后执行、condition 缺省单轮、
  max_iterations 缺省 1000 触顶仅告警；map 的 concurrent（历史别名 async）并发走
  进程级许可池（缺省 8）+ maxConcurrent gate、池内线程降级串行、错误取最小失败下标）。
- **case** = decide.py SwitchLegacy 的装载期脱糖为 switch（每条 case → Branch(eq
  target, value, next=case.id) + 恒置 isDefault 兜底；default 缺省=字面 "default" 节点
  id；target 缺省以 `$NULL` 承载；缺 id/value 装载期拒）；metadata.caseDialect 留痕。
- **mock** = value 原样透传不求值（pin 语义），output 字段忽略。
- **reference** = `ir::load_with_flow_provider` + `FlowProvider` 装载期注入 childFlow
  （plaita 调度器角色的引擎侧对应物）；inline childFlow 在 reference 上具名拒绝
  （Python 运行时把它丢掉、执行期才炸——不给跨运行时假象）；嵌套解析深度 32、环
  具名拒、菱形不误伤；子流程走 `RuntimeCtx::child_isolated`——`$GLOBAL`/`$ENV`/执行
  身份继承、`$PARENT` 指向空 ambient 执行（`$PARENT.<x>` → null，隔离实测口径）；
  input 缺省继承调用方 $INPUT。

**capability 契约面**（plaita#300 五键形态）：`ir::capability_manifest()` +
`ir::CONTRACT_VERSION`（2026-10-23.1）；`features`/`node_dependencies` 恒空（无
checkpoint/hitl/suspend 面）。

**防漂移门**：`make node-parity`（CI 同名 job）——`scripts/extract_python_node_types.py`
从 plaita NodeRegistry（core 内建 + `plaita.nodes` entry points）现场提取权威清单，
`tests/node_type_parity.rs` 对拍 `ir::SUPPORTED_NODE_TYPES`：**双向差集必须恰等于显式
例外表**，单侧新增/例外过期即红；例外条目必须同时在 `KNOWN_UNSUPPORTED` 有具名 hint。
例外 28+1 条：挂起族 7（event/hitl_await/delay/approval/redis_queue/kafka_queue/
http_callback——等待/恢复全在引擎外，实装前置=先建 suspend/resume 执行架构）+
外部宿主族 21（agentrun + plaita-nodes，含 sql_query=plaita-go#84 同源评估）+
Rust-only `code`（Python 刻意 opt-in 注册）。负控双实证：权威清单缩水→红、
未设 env→skip（普通 cargo test 不受影响）。

## 挂起/恢复架构（B4 r12，2026-10-23）

**范围（Go checkpoint v1.5 同款，fail-closed）**：只有**根流程**可以挂起——嵌套
child/loop/parallel 里的挂起点具名拒绝（其恢复位置无法用执行状态表达）。
**Python 权威侧实弹核实（2026-10-23 探针）**：plaita 自己也**没有**嵌套挂起——
`_prepare_strategy` 对子执行（run_compatible）恒选 NORMAL/GENERATOR、永不选
DISTRIBUTED，`is_suspending` 只在分布式逐步边界的当前节点上生效；child 内的
event 节点在 NORMAL 与 DISTRIBUTED 两模式下都把 pending 载荷当**普通数据**
返回、内层流程照常走完——事件永不消费、订阅永不注册、零报错（fail-open 的
静默误执行）。本引擎的具名拒绝是把该缺陷 fail-closed 化，不是功能缺失；真要
支持嵌套挂起需三家共同立项（挂起传播 + checkpoint 表达内层路径 + 沿路径恢复）。

**引擎面**：挂起节点 execute 产出 `status:"pending"` 契约（extended 家族 =
{event_type, event_filter, is_async, event_id, service_config, node_subtype}；
hitl_await = {status, session_id, replies, timeout_secs}），挂起边界校验 pending、
经宿主订阅注册表登记唤醒订阅（`Host::with_subscriptions`——**无注册表 = 无唤醒
源，具名拒绝**；Python 订阅失败禁止挂起同款）、把 subscription_id/
subscription_tenant 写进挂起态并落 `$NODE`，返回 `FlowError::Suspended(
SuspendedExecution)`（载 flow+上下文现场）。`SuspendedExecution::resume(kind,
payload)` 恢复+续跑一步到位（Go `Workflow.Resume` 语义）：准入（节点挂起型/
status==pending/**订阅绑定**〔plaita#221：event/timeout 载体必须绑定挂起期登记
的订阅，`__plaita_subscription_id` 准入后剥离，event_type 还须一致〕）→ 节点
多态恢复（挂起态全量保留仅翻 status+event_data；event 节点恢复后保持 execute
解析的 event_type〔Python dict.update 覆盖序〕）→ 报 NodeEnd、注销订阅 → 从
后继继续。**恢复会话的执行预算新鲜发放**（flow 自有 timeout 或缺省 1800s）——
否则 24h 审批的恢复会落在早已过期的原 deadline 上。

**宿主面（刻意不在引擎内）**：订阅注册表实现、订阅超时的到期投递
（resume_type=timeout；Python SubscriptionTimeoutChecker 同位）、五节点的
service 层（delay 排程/approval 记录/队列消费/回调 ingress——deployment 设施，
manifest 的 `node_dependencies` 键逐节点标注）。capability `CONTRACT_VERSION`
bump **2026-10-23.2**。

**验收**：server_nodes golden **30 例逐例对拍 Python 真跑 oracle**（挂起态对
`$NODE` 对拍、恢复走 golden 预置载荷+订阅绑定注入、剔键+hex32/ms13 打码两侧
同款；拒绝用例钉字段名）先红后绿 + 引擎面单测 12 例（作用域闸/无注册表拒/
绑定准入/状态机/订阅注销/恢复记账/hitl dry_run+真实发送〔本地 HTTP stub〕/
approval 缺省 24h 订阅超时·显式 null=无限等待）。

## 嵌套挂起 v1（B4 第十三轮，2026-10-23，rs#16）

**语义（三平台同一契约，rs=NS-M1 参考实现；章程 docs/PROGRAM.md 程式一）**：
挂起节点位于 child/reference 调用链（任意深度）内部时，挂起穿透到根流程、恢复
沿调用栈精确续跑。

- **作用域闸（双层，验证报告修正后定案）**：`RuntimeCtx.FrameKind`
  （Root/ChildCall/ReferenceCall/LoopIteration/ParallelBranch，构造期标注）。
  **主闸 = 传播转换点的调用方帧 kind 检查**（execute_child/execute_reference
  的 `suspension_carrier_or_reject`：迭代帧/分支帧边界具名拒绝**并注销已注册
  订阅**——调用方 rc 永远真实，无 ambient 眼镜）；`frame_chain_blocker()`
  降级为注册前 advisory 快路径（**reference 的 ambient 父链会遮蔽链行走**，
  不能作为正确性保证——验证席实测坐实）。parallel 分支另有**派发前静态闸**
  （`flow_contains_suspending`：分支图含挂起型节点即同步具名拒——后台分支的
  动态拒绝无法同步上抛）。拒绝走 `FlowError::SuspendRejected` 专属类型：
  execute_node 专属拦截臂保证不被 errorHandler continue 吞、不被 join 哨兵
  吞。迭代状态快照（剩余集合/累积结果）是 v2 候选，独立评审。
- **传播**：内层 `execute_on` 的 `Err(Suspended)` 经 child/reference 节点体转
  `NodeError::suspension` 载体上抛；`execute_node` 策略链先于预算归属/超时/
  重试/errorHandler 原样转发（与取消门同待遇）；每层 `advance_from` 把本帧
  （flow+rc+子节点 id）压入 `inner.call_stack`（压入序=传播序，最深父帧在前）。
- **恢复**（`SuspendedExecution::resume` 续跑循环）：决策步（最内层 pending
  节点，准入/状态翻转/订阅注销不变；入口有取消门——cancel 后恢复被拒，载体
  未消费）→ 逐帧走 `advance_and_finalize`（= execute_on 的收尾链抽取：内层
  失败与首跑同链——FlowTimeout 终态/本帧 timeoutHandler/FlowEnd——不直抛
  宿主，验证报告 Q8.1 修正）→ 内层完成即「调用返回步」（父帧 `$NODE[child]`=
  返回值 + outputType 校验 + NodeEnd + 每帧预算新鲜发放 + 续跑 child.next）
  → 直到根完成；续跑中再次挂起 = 新现场调用栈（更深，在前）+ 剩余栈拼接，
  重入挂起态。**声明**：父帧子节点的 errorHandler/retry 不作用于恢复续跑
  失败（该链只覆盖首次执行）；子节点 node.timeout 只覆盖首次执行。
- **观测**：`checkpoint_json()` 附 `call_stack` 路径（flow_id/node_id 链）。
- **测试**：suspend.rs 3 例改造（child 链真挂起+栈路径/loop 帧拒/parallel 帧
  拒）+ nested_suspend.rs 4 例（孙代双帧栈/reference 隔离保持/恢复中再挂起栈
  合并/深位绑定准入与 timeout 载体）。
- **go/plaita 跟进**：plaita-go#88（NS-M2）、plaita#311（NS-M3，顺带修
  fail-open）；规格即本文档+docs/PROGRAM.md。

## 审计记录（2026-10-21，四路子智能体对抗审计）

表达式对齐 / ir 契约 / 执行语义与并发 / 红队四路审计，全部 findings 双侧实测。已修复：
- **P0** 表达式深嵌套调用栈溢出 abort（主线程 ~4500 层）→ parser 深度闸 256（Python 是 RecursionError；Go goroutine 栈可增长无此患，Rust 特有）；
- **P0** `%Nf` 精度 >65535 使 std format! panic → 钳位；
- **高** 节点退避写成 `1^attempt` 恒 1s → `2^n` cap 30s；
- **高** 子流自有 timeout 不与父预算取 min（父 1s+子 1h 实测逃逸）→ `RuntimeCtx::child` 预算合成，deadline 贯穿 loop/parallel/child；
- **中** code 节点 blanket deterministic 抹掉 Timeout 类致 timeoutHandler 失哑 → 保留 Timeout 类（1001 码保不重试）；
- **中** 后台并行分支无 join 面被进程退出杀死 → `Host::track_background/join_background`，CLI 退出前 drain；
- **P1** 模板 O(n²)（每个 `{%` 尝试全量克隆字符流，10000 段 51s）→ 借用 parser，**0.08s**；
- **P1** `sort` 混排静默不排序 / `mul` 超限静默截断 → 均改具名错误（对齐 Python；Go 同点已提 #44/#45）；
- **P1** `getListItem` dict 字符串键死分支 → 前置 dict 分支（Go 同点 #42）；
- P2 群：`_`/dunder 属性闸覆盖非映射基底、`$NULL/$EMPTYSTRING` 后继路径、`\`+换行拒绝、`%g` 负指数、零转换符多余实参报错、sidecar 真进程组杀（process_group(0)+kill -9 -PID）；
- ir 审计 6 单：end resultType 类型漏拒 / `$F` 扫描 lookbehind+`\s` / globalContext 显式 context 胜出 / http addressing·delegate fail-closed / unknownFields 落 metadata / retryTimes 严格化 + Property 双拼写残留清理 + trpc/mdb 族 hint + version 长度检查。

**文档化残余（有意/存档，不算 bug）**：无全图环保护——switch 自环合法装载，防死循环=配 flow timeout，缺省另有 1800s 执行预算兜底（B4 r7，不再无限自旋）；大整数域 >i64 回落 f64/报 OverflowError；None→零值桥镜像 Go（与 Python TypeError 面不同，crate 头已声明）；`join` 不收 str 可迭代、`json_loads` 不收 NaN/Infinity、`%b` 超集、`isDigit` 用 is_numeric（比 Python isdigit 宽）、`keys()/items()` 物化 list、strftime `%z/%Z` 空串（naive 语义，已对齐 Python）；流级重试已整体撤出（B4 r7 对齐 Python 单次执行——先前「每轮重建上下文」的分歧面随之消失）；子流预算沿父 remaining（min 合成）vs Python「子执行新鲜独立预算（缺省 1800s，父 remaining 不穿透，B4 r8 实读更正 r7 台账注记）」——形状分歧 P3 待裁决；`$ERROR` 根与 `maxRedirects` 静默忽略两枚 Go 残面已盘点入台账（B4 r8）待裁决。

## 运行期字符串契约：配置求值、运行期数据（2026-10-21，jeffkit 终裁）

表达式只活在 **flow 配置**里（节点 output/input/condition 字段），引擎在节点执行时就地求值一次；**节点结果/输入里长得像表达式的字符串（`"$F.add(2,3)"`、`"{% ... %}"`）是数据**，`$NODE` 路径读原样返回，绝不再解析。两个层次严格区分：

- **情况一（支持）**：`$NODE.a.b.c` 多层递进取值——一次求值、逐段走路径，与值层级无关；
- **情况二（不做）**：值本身是表达式形状的字符串时对该字符串再求值（plaita #104 Plan A 的「嵌套表达式字符串」语义）。裁決依据：无不可替代场景（表达式工厂有 `$F.ifelse`/code 节点；动态模板物化与跨层引用传递被 #104 根闸实际封死）；plaita 自己的 #104/#130 就是朝「节点输出就是数据」方向修的半程；LLM 输出中 `$F...` 形状文本被静默执行是真实危害；且同值不同读法结果不同（`$NODE.x` 求值 vs `$F.get($NODE,'x')` 原样）是实现残留指纹。

边界说明：①配置字面量在节点求值时就地算掉（三家一致）——所以「运行期不求值」⇔ Go/Rust 原行为，Python 是需要摘层的那个；②`$NODE.raw.<node>` 保留为兼容修饰符（本引擎路径读本就原样，含「真有节点叫 raw」碰撞规则，与 plaita-go 相同）；③R5 部署扫描不豁免 raw（三方一致）。Python 侧摘层已提单（jeffkit/plaita），plaita-go 对齐单 #46 已关闭（wontfix）。跨引擎契约用例已入差分对账（`runtime/string-is-data`，16/16）。

## 对账发现已提报 plaita-go 的单（移植差分坐实，2026-10-21）

- #40 `$F.index` 对 str 返回字节下标（Python 按字符）——本仓按字符实现；
- #41 capitalize/title 首字符须用 titlecase 映射（'ß'→"Ss" 而非 "SS"）——本仓已按 titlecase 实现；
- #42 getListItem 对 dict 的字符串键分支不可达（pyIndexArg 先拒 str）——本仓 dict 走 KeyError 语义、
  同样先拒非整数下标（与 Python 一致的报错面，但 `$F.get` 是正规取键路径）；
- #43 python sidecar 代码错误与 end FlowResultError 被判可重试（重试 6/4 次实证）——本仓两者均按
  确定性分类（Deterministic，不烧重试预算）；
- #44 `$F.sort` 混排静默输出（Python TypeError）——本仓比较错误上抛；
- #45 `$F.mul` 序列重复超上限静默返回空串——本仓具名错误。

## 差分对账（改行为前必跑）

```bash
# 参照侧：Go runner（依赖本机 plaita-go checkout；首次需
#   cd tools/go-ref && GOPROXY=https://goproxy.cn,direct GOSUMDB=off go mod tidy && go build -o ../go-ref-bin .）
python3 tools/diffcheck.py     # 23/23 用例与 plaita-go 双跑一致（2026-10-23；known-red 两案随 G-1 修口翻绿转正）
```

用例覆盖：plaita codeflow 编译器真产物（loop/parallel/child）、console 真实导出
（assignment_flow）、if/switch/条件算子、loop 条件早退、表达式混搭、错误策略 continue、
双端错误路径。**改 IR 契约、条件算子、节点语义前先跑它**；新增语义对齐时往 MANIFEST 加用例。

## 测试与门禁

```bash
cargo test                                   # 全仓：单测 + golden + 端到端 flow 测试
cargo test -p plaita-expression              # 29 单测 + 43 条 plaita golden（tests/golden.rs）
cargo test -p plaita-engine --test flows     # 真实 testdata flow 端到端（断言与 Go 侧相同输出）
cargo clippy --all-targets                   # 应保持零告警
```

- **golden 纪律**：`crates/plaita-expression/testdata/expression_golden.json` 是从 plaita
  测试导出的逐字副本，禁止手改；Python 侧语义升级后用 plaita-go 的
  `scripts/export_golden.py` 重新导出并同步到本仓。
- **testdata 纪律**：`crates/plaita-engine/testdata/*.json` 来自 plaita-go `pkg/ir/testdata/`
  （真实 console 导出 + codeflow 编译产物），同样禁止手改。

## 环境备注（本机）

- cargo 在 `~/.cargo/bin`（不在默认 PATH）。
- 本机不可达 `static.crates.io`；本项目用 **rsproxy.cn** 镜像，配置在
  `.cargo/config.toml`（**已 gitignore**，换机器需重建一份）。
- Go 参照 runner 构建需 `GOPROXY=https://goproxy.cn,direct GOSUMDB=off`（同 plaita-go 的本机约定）。

## 约定

- 引擎逻辑活在 flow 图里（jeffkit 红线）；不做「宿主代码 + flow 皮」。
- 所有降级（错误/超时策略、重试）必须可观测：事件回调 + `$ERROR` 记账，禁止静默。
- IR 契约只认一种表示（plaita 部署 camelCase JSON）；schema 变更必须同步
  normalize/validate 与 golden（吸取 plaita「DSL 三表示往返无不变量」的教训）。
