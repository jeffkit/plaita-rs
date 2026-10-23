# 程式章程：嵌套挂起（三平台）+ plaita-rs worker 运行时

> 立项人：jeffkit（2026-10-23 令「嵌套挂起要支持，三种平台都要」「rs 的 worker 也同样立项」）。
> 挂帅：主控席（B4 线，本仓 + 跨仓协同）。本文是两件事的共同章程：目标、语义契约、里程碑、验收。
> 子文档：`docs/specs/nested-suspend-validation.md`（设计验证席发现）、`docs/specs/rs-worker-design.md`（worker 设计规格）。

## 程式一：嵌套挂起（plaita / plaita-go / plaita-rs 三平台）

### 目标（一句话）

挂起节点位于任意深度的 child/reference 调用链内部时，三个引擎都能把流程停在该内层
位置、持久化恢复位置、并在决策载体到达时沿调用栈精确续跑；且三家行为一致（同一份
语义契约，rs 先行试点实现，go/plaita 跟进移植）。

### 背景事实（2026-10-23 实弹核实）

- 三家现状都**只支持根流程挂起**。Python 更糟：子执行恒走 NORMAL/GENERATOR 策略
  （`_prepare_strategy` 永不选 DISTRIBUTED），嵌套挂起节点把 pending 载荷当**普通
  数据**返回、流程照常走完——事件永不消费、订阅永不注册、零报错（fail-open 静默
  误执行，探针证据见 AGENTS.md r12 节）。
- Go/rs 在挂起边界对非根执行具名拒绝（fail-closed），但「不能」同样是功能缺口。

### 语义契约（v1，rs 为参考实现）

1. **作用域**：v1 支持 child/reference 调用链（任意深度）内的挂起穿透到根；
   任何 loop/map/filter/find/reduce/while 迭代帧或 parallel 分支帧上的挂起仍具名
   拒绝（消息点名阻断帧类型）——v2 候选，需要迭代状态快照（剩余集合/累积结果），
   正确性风险独立评审。
2. **传播**：内层挂起沿调用链逐层上抛，**不进任何节点 errorHandler/retry/timeout
   策略链**（与取消同待遇）；每层把「待返回帧」（flow + 上下文 + 子节点 id）压入
   挂起现场的调用栈。
3. **恢复**：决策载体（event/timeout/cancel + plaita#221 订阅绑定）只作用于最内层
   pending 节点；其完成后内层流程续跑，每层 flow 完成时作为「调用返回」把值交给
   父帧（$NODE[child]=返回值 + outputType 校验 + 续跑 child.next），直到根流程
   完成；续跑途中再次挂起则合并调用栈重新入挂起态。
4. **预算**：恢复会话每帧新鲜发放（flow 自有 timeout 或执行级缺省）。
5. **可观测**：挂起/恢复/返回步全部走事件回调；checkpoint 载调用栈路径
   （flow_id+node_id 链）。
6. **fail-closed 保留**：无法表达恢复位置的挂起（v1 的迭代/分支帧）具名拒绝，
   绝不静默——这是对 Python 现状 fail-open 的修复方向。

### 里程碑与验收

| 里程碑 | 内容 | 验收 |
|---|---|---|
| NS-M1（rs 试点） | 本章程+验证报告钉语义；rs 实现（call_frame 标记/传播通道/恢复栈/作用域闸收窄） | 单测：child 链、孙代链、reference 链（隔离保持）、迭代帧/分支帧具名拒、恢复中再挂起的栈合并、决策步绑定准入；全仓门禁绿 |
| NS-M2（go 移植） | suspend.go 作用域闸+传播+Resume 循环按契约改造 | 同语义测试集移植；与 rs 差分对拍（同 flow 同载体同结果） |
| NS-M3（plaita 移植） | strategies/state/_handle_resume 改造：子执行挂起传播（顺带修 fail-open）+ checkpoint 内层路径 + 沿路径恢复 | 同语义测试集；分布式模式实弹（探针场景从「假完成」变「真挂起+真恢复」） |
| NS-M4（对拍收官） | 三引擎同场景对拍 | 一份跨引擎差分用例集（挂起/恢复/返回/再挂起）三侧一致 |

## 程式二：plaita-rs worker 运行时

### 目标（一句话）

plaita-rs 获得与 plaita-go pkg/worker 同位的 worker 运行时（队列消费/租约/
checkpoint 持久化/订阅存储/超时投递/恢复重放），Redis 协议与 plaita-go 二进制
兼容（混合部署直接互操作），使挂起恢复从进程内 API 升级为分布式能力。

### 设计约束

- 同步线程模型（对齐引擎，不引 tokio）；Redis 协议与 plaita-go 逐字段兼容；
  fail-closed；每个降级必须可观测（仓规）。
- 序列化边界：worker 持久化的是「flow 文档引用 + 上下文快照 + 挂起路径 + 订阅
  信息」，恢复时经 ir::load 重建——不序列化 Rust 对象。引擎需要的最小接口增量
  （快照重建上下文等）在实现轮落地，规格先行（`docs/specs/rs-worker-design.md`）。

### 里程碑

| 里程碑 | 内容 | 验收 |
|---|---|---|
| WK-M1 骨架 | crate plaita-worker：队列消费+checkpoint 持久化+挂起/恢复闭环（单 worker、无租约） | 端到端：投递 flow → 挂起落库 → 注入事件 → 恢复续跑 → 完成落结果 |
| WK-M2 生产面 | 租约/fence/可见性超时/背压/订阅超时检查器 | 与 plaita-go 同 Redis 协议互操作测试；崩溃恢复测试 |
| WK-M3 运维面 | registry metadata（capability 清单）/观测钩子/drain | 与 keeper 管线对接判据齐备 |

## 推进机制

- 挂帅席（主控）按轮次推进，台账 `plaita/duo-b4-ledger.md` 逐轮记录；子智能体
  分工：设计验证席、worker 规格席、（后续）实现席/评审席。
- 三仓 issue 互链本章程；语义契约变更必须先改本文与 specs 再动代码。
