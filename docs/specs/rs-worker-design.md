# plaita-rs Worker 运行时设计规格

> 状态：设计稿（实现轮任务书）｜负责人：jeffkit｜起草：2026-10-23
> 参照实现：plaita-go `pkg/worker/`（结构参照）；语义权威：plaita `plaita/server/flow_worker.py` 及其协议族
> （`task_queue.py` / `execution_lease.py` / `event_filter.py` / `core/strategies.py`）。
> 本文件只做设计，不改任何代码；引擎改动仅列接口需求（§7.4），实现轮另开任务。

---

## 0. 文档定位与阅读方式

plaita-rs 现状是纯引擎（crates/plaita-engine：IR 装载 + 执行 + suspend/resume；`SuspendedExecution`
是进程内内存句柄），刻意无 worker（`AGENTS.md`「不在范围内（刻意不做）：pkg/worker（Redis Streams
消费、租约 Lua、fence、DLQ、checkpoint 恢复、drain）」）。本规格立项补齐 worker 运行时，使 rs 引擎
获得**分布式执行与挂起恢复**能力，定位对标 plaita-go `pkg/worker`。

- 每节末尾给「参照实现依据」，格式 `文件:行号`，全部真实可查（plaita-go 相对
  `/Users/kong/projects/infra4agent/plaita-go/`，plaita 相对 `/Users/kong/projects/infra4agent/plaita/`，
  rs 相对本仓根）。
- 三条设计铁律（承本仓 AGENTS.md「与 plaita-go 的刻意分歧（全部 fail-closed，不静默降级）」）：
  1. **同步线程模型**——对齐引擎（std::thread + Arc/RwLock），不引 tokio；
  2. **Redis 协议与 plaita-go 二进制兼容**——键名/信封/Lua/载荷字段逐一照抄，混合部署同一 Redis
     直接互操作（硬约束）；
  3. **fail-closed + 每个降级必须可观测**——任何降级路径必须具名（日志 marker + 指标名），禁止
     静默吞咽。

---

## 1. 目标与非目标

### 1.1 目标

| # | 能力 | 对标 |
|---|------|------|
| G1 | Redis Streams 队列消费（XREADGROUP + reclaim + DLQ + ack） | queue.go |
| G2 | 执行租约（fence 世代 + CAS 续租/释放） | lease.go |
| G3 | checkpoint 持久化 + 从快照重建挂起执行并 resume | checkpointstore.go + 引擎接口 |
| G4 | 订阅存储（引擎 `SubscriptionRegistry` 的 Redis 实现）+ 超时检查器 + Pub/Sub 事件桥 | eventbus.go |
| G5 | 执行状态行（fenced CAS 写 + 取消标志 + 终态 TTL） | state.go |
| G6 | worker registry 心跳 + capability manifest 元数据 | registry.go |
| G7 | 节点重试预算/退避/载体转换、死信守卫、优雅停机 drain | retry.go / drain.go / queue.go |
| G8 | 挂起服务派发（`{prefix}:{subtype}:queue`）与 dispatch_pending 凭据补偿 | worker.go / flow_worker.py |

### 1.2 非目标（本规格明确不做，留待后续立项）

- per-step checkpoint 与 mid-flow 续跑入口（Go `Workflow.ResumeFrom` / `CheckpointKindPerStep`）——
  rs 引擎无 mid-flow 入口（`advance_from` 私有且无按位恢复语义，exec.rs:379），M1–M3 的崩溃恢复
  退化为「从头重放 + 可观测降级事件」（§9.6）；
- 机器亲和 handover / dispatch ledger / DLQ replay 列入 M3（协议面小、依赖 registry）；tenant
  quota 列入 M2；
- console BFF、影子对账 harness、agentproc（仓外）；
- 嵌套挂起（引擎已具名拒绝，rs `AGENTS.md`「挂起/恢复架构」节：只有根流程可挂起）。

---

## 2. crate 布局与依赖选型

### 2.1 新 crate：`crates/plaita-worker`（workspace 成员）

**决策：独立 crate，不进 engine。** 依据：

1. rs 仓 AGENTS.md 已把 worker 划为「刻意不做」的独立层——engine 保持纯引擎边界是既定架构裁决，
   worker 依赖 Redis/网络/运维面，与 engine 的零外部依赖取向相斥；
2. plaita-go 同构：`pkg/workflow`（引擎）与 `pkg/worker`（运行时）分包，worker 依赖 workflow
   而非反向（worker.go:17-21 import 方向）；
3. worker 自带 bin（长驻进程）+ lib（宿主嵌入），engine 的 CLI 形态（plaita-cli）不受影响。

布局（模块与 go 一一对应，便于逐文件对拍移植）：

```
crates/plaita-worker/
  Cargo.toml            # workspace 成员；bin 目标 plaita-rs-worker
  src/
    lib.rs              # crate 级文档：协议权威声明（仿 config.go:1-8 的包注释）
    config.rs           # Config + 环境变量解析 + normalize + Namespace/ApplyShadow   ← config.go
    errors.rs           # 错误分类族（transient / deterministic / poison / dlq）      ← errors.go
    queue.rs            # TaskQueue：EnsureGroup/Read/Ack/Enqueue/DeadLetter/Sweep   ← queue.go
    lease.rs            # ExecutionLease：三段 Lua 逐字移植                          ← lease.go
    state.rs            # ExecutionStore + ExecutionState + fenced CAS               ← state.go
    checkpoint.rs       # CheckpointStore trait + RedisCheckpointStore + CheckpointRecord ← checkpointstore.go
    subscriptions.rs    # RedisSubscriptions（引擎 trait 实现）+ HandleEvent + Sweep  ← eventbus.go
    eventbus.rs         # EventPublisher（业务事件双写 + PUBLISH）                    ← events.go:25-75
    flowsource.rs       # FlowSource：flow IR 装载 + 指纹 + TTL 缓存                 ← flowsource.go
    registry.rs         # Registry：ServiceInfo/心跳/capability manifest              ← registry.go
    retry.rs            # RetryBudgets + 退避 + 载体转换                              ← retry.go
    quota.rs            # TenantQuota（M2）                                           ← quota.go
    agegate.rs          # 载体年龄闸（M2）                                            ← agegate.go
    orphan.rs           # 失租孤儿观察（M2）                                          ← orphan.go
    drain.rs            # D1/D2/D3 三态停机                                          ← drain.go
    metrics.rs          # Metrics trait + NoopMetrics                                 ← metrics.go
    worker.rs           # Worker：Run/ProcessOne/handleStart/handleResume/executeAndFinalize ← worker.go
    bin/worker.rs       # 信号处理（SIGTERM→RequestDrain）+ 组装 + Run
  tests/                # 行为测试（§12 验收清单），miniredis 对应物用 testcontainers 或内嵌 redis
```

### 2.2 Redis 客户端：`redis-rs`（blocking），理由

**决策：`redis = { version = "0.27", features = ["script"] }`（同步 API），不用 fred / 不引 tokio。**

1. **线程模型对齐**：引擎是同步线程模型（exec.rs 全部 `std::thread` + `std::sync` 原语；Host 的
   并行分支/后台 join 亦然，exec.rs:49-66）。plaita-go 的结构是「阻塞调用 + goroutine 各司其职」
   （worker.go:279-299 五个后台 goroutine），映射到 rs 就是「阻塞调用 + std::thread」，redis-rs
   的同步 API 与 go-redis 调用形状 1:1（`XReadGroupArgs`/`XPendingExtArgs`/`XClaimArgs`/
   `Script::Run` 全部齐备），移植时逐行可对拍；fred 是 tokio-first，其 blocking 面是二等公民。
2. **为单进程消费循环引入 async 运行时是净负债**：drain 的任务边界退出（worker.go:303-309）、
   分片退避可打断（drain.go:155-178）、看门狗线程（worker.go:819-901）在 std::thread + AtomicBool
   下已经是最小实现；async 化只会把「同步引擎包在 spawn_blocking 里」的丑陋传遍调用链。
3. **连接模型**：redis-rs `Client` 是连接池句柄（线程各自取连接），足够覆盖「1 条消费循环 +
   5 条后台线程 + 每执行 2 条看门狗线程」的并发度；Pub/Sub 用专用连接（`client.get_pubsub()`），
   与 Go `PSubscribe` 独立连接同构（eventbus.go:368）。
4. **协议兼容无风险**：Lua 脚本走 `EVALSHA`（redis::Script 自动加载），streams 命令是纯 RESP，
   与客户端实现无关——二进制兼容约束（§3）由键名/载荷/Lua 文本决定，不受客户端选型影响。

风险与对策：redis-rs 无 miniredis 等价物 → 行为测试用 testcontainers-rs（redis 镜像）；
BLOCK 命令在同步连接上不可被信号打断 → 消费循环的 BLOCK 窗口按 Python 的 ≤1s 分片口径
（flow_worker.py:7126-7131「切成 ≤1s 的窗口，停机延迟上限 ≈1s」）。

### 2.3 其他依赖

`serde/serde_json`（workspace 已有，preserve_order 保持插入序——挂起态 `service_config` 等字段
顺序语义与 Python dict 对齐）、`chrono`（naive ISO 时间戳，state.go:62-64 口径）、`uuid`（v4 hex
执行 id，worker.go:2476-2482 口径）、`sha2`（flow 指纹，§4.6）。日志面用 `log`/`tracing` 任一
（实现轮定），但**必须**保留具名 marker 字符串（§11.4）。

---

## 3. Redis 协议面：键空间总表（互操作硬约束）

以下键名、字段名、Lua 脚本、载荷形状**逐字节**对齐 plaita-go；`{ns}` = `cfg.Namespace(tenant)`
（default/空租户 = `KeyPrefix`，否则 `KeyPrefix:{tenant}`，config.go:424-429；同款规则见
worker.go:1272 注释「空租户归一 default」与 Python `tenant_id or DEFAULT_TENANT_ID`）。

| 键 / 通道 | 类型 | 用途 | 依据 |
|---|---|---|---|
| `{prefix}:flow:queue` | Stream | 任务主队列（信封字段 `payload`/`schema_version`） | config.go:313, queue.go:466-476 |
| `{prefix}:flow:queue:dlq` | Stream | 死信（XTRIM maxlen，默认 1000） | config.go:331-333, queue.go:422 |
| 消费组 `plaita-workers`（shadow 时 `{prefix}-workers`） | group | 唯一组 | config.go:316-322 |
| consumer = instance id（默认 `worker-<pid>`） | — | 领取者 | config.go:325-329 |
| `{ns}:execution:{id}` | String(JSON) | 执行状态行 | state.go:87-89 |
| `{ns}:execution:index` | ZSET | 执行列表（score=start_time epoch） | state.go:91-93, 207 |
| `{ns}:execution:lease:{id}` | String | 租约（值 `{holder}:{gen}`，TTL 120s） | lease.go:82-84 |
| `{ns}:execution:fence:{id}` | String | 当前 fence 世代（TTL 7d） | lease.go:86-88, config.go:57 |
| `{ns}:execution:fencegen:{id}` | String | 世代计数器（**永不删除、无 TTL**） | lease.go:90-92 |
| `{ns}:execution:cancel:{id}` | String | 取消意图标志（值=ISO 时间，TTL 7d，worker 只读） | state.go:105-110 |
| `{ns}:execution:checkpoint:{id}` | String(JSON) | 挂起检查点（挂起期**无 TTL**） | checkpointstore.go:37-39 |
| `{ns}:execution:noderetry:{id}[:{node}]` / `noderefetch:{id}` / `nofail:{id}` | String 计数 | 节点重试三计数（滑 TTL 7d） | retry.go:15-27 |
| `{ns}:execution:affinityhandover:{id}` | String 计数 | 亲和让渡界（M3，7d TTL） | worker.go:2280-2295 |
| `{ns}:event:subscriptions:{sub_id}` | String(JSON) | 订阅记录（TTL = event TTL，默认 7d） | eventbus.go:22, 113-118 |
| `{ns}:event:subscriptions:type:{event_type}` | SET | 按类型索引 | eventbus.go:23, 117 |
| `{prefix}:event:subscriptions:timeouts` | ZSET | 到期索引（member=订阅键，score=到期 epoch） | eventbus.go:24, 119-121 |
| `{prefix}:event_filter:dedup:{event_id}:{sub_id}` | String | 事件去重（NX，1h） | eventbus.go:25, 231-233 |
| `{prefix}:event_filter:timeout:{sub_id}` | String | 超时去重（NX，1h） | eventbus.go:26, 315-318 |
| `{ns}:event:events:{event_id}` / `{ns}:event:types:{type}` | String/ZSET | 业务事件存储（M3 完整面） | events.go:47-75 |
| `{ns}:events:{event_type}` | Pub/Sub | 业务事件通知（事件桥订阅 `{prefix}*:events:*`） | eventbus.go:55-57, 368 |
| `{prefix}:execution:events:{execution_id}` | Pub/Sub | 执行时间线直播 | events.go:405-419 |
| `{prefix}:registry:flow_worker:{instance_id}` | String(JSON) | worker 注册（TTL 30s） | registry.go:64-66, 232 |
| `{prefix}:dispatch:ledger` | Stream | 派发台账（M3） | config.go:178-184 |
| `{prefix}:tenant_quota` / `{prefix}:tenant_running:{t}` | HASH/ZSET | 租户并发闸（M2） | quota.go:17-22, 80-88 |
| `{ns}:flow:{flow_id}:{version}` / `{ns}:flow_list` / `{ns}:flow_versions:{flow_id}` | String/SET | flow IR 存储形状（console engine_sync 是生产写方） | flowsource.go:21-30 |
| `{prefix}:{subtype}:queue` | List | 挂起服务派发（RPUSH；delay/approval/…） | worker.go:1254-1281 |

**已知跨实现分歧（记录，不强行抹平）**：Python 服务派发队列名硬编码 `plaita:{subtype}:queue`
（flow_worker.py:4860），Go 按 KeyPrefix 派生（worker.go:1272 `keyPrefix + ":" + subtype + ":queue"`）。
rs 跟 Go（KeyPrefix 派生）：默认部署两者等值（prefix=plaita），shadow 部署下 rs 与 go 同进影子
命名空间、与 Python 生产队列天然隔离，符合「shadow 永不触生产键」约束（config.go:53-55）。

---

## 4. 队列消费协议

### 4.1 信封与任务体（wire format）

- Stream 消息恰两个字段：`payload`（任务体 JSON 串）+ `schema_version`（十进制串）。缺失 =
  v1；不可解析 = 回退 v1 并告警；**高于本 build 写版本 → DLQ**（§2.2 两处同判：首次投递与
  reclaim 走同一条 `checkSchema`）。依据 queue.go:197-248。
- 任务体字段表（serde struct 字段名逐一对齐，`#[serde(rename_all)]` 不用——直接蛇形命名）：
  `type`(start|resume) / `flow_id` / `execution_id` / `version` / `params` / `tenant_id` /
  `timestamp` / `dedup_key` / `resume_type` / `data` / `target_instance_id` / `quota_defers` /
  `quota_bypass`（queue.go:27-47）。`payload` 非 JSON object → `ErrInvalidPayload` → DLQ+ack
  （queue.go:220-230）。
- **决议载体**：`resume_type ∈ {event, timeout, cancel}` 置 `suspended_resume_chain` 标志——
  它们是挂起执行的唯一唤醒源，**租约冲突窗口内永不 ack**（queue.go:49-61；Python
  `_DECISION_RESUME_TYPES`，flow_worker.py:200-203；A47-F2 语义）。
- worker 作为生产者（重投/让渡/恢复克隆）入队时**继承源信封 schema_version**，绝不写本 build
  版本（queue.go:450-476，A44-02：副本不得冒充更新的 schema）。

### 4.2 读取循环（reclaim-before-read）

每轮 `ProcessOne`（Go worker.go:363-427 的同步单轮，rs 同构）：

1. `EnsureGroup`：`XGROUP CREATE … id=0 MKSTREAM`，BUSYGROUP 容忍；**每轮都做**——整流被 XDEL
   后组随键消失必须重建（queue.go:118-137）。
2. **reclaim 先行**：`XPENDING EXT` 从旋转游标扫 `reclaim_window=8` 条（queue.go:63-64；Python
   task_queue.py:534 同值），对 idle ≥ `claim_min_idle_ms`（默认 60000，config.go:263）的条目：
   - **活跃租约守卫**：XRANGE 无副作用取体，解析出 execution_id 后 `GetHolder` 探租约；
     仍被持有 → 跳过（不 XCLAIM、不计数）；探针失败 → 保守跳过（queue.go:317-351；Python
     task_queue.py:607-611 同语义）；
   - `XCLAIM min-idle-time` 单条；delivery_count = **claim 前**的 times_delivered；
   - 投递天花板只在 reclaim 路径生效：`delivery_count >= max_deliveries`（默认 5）→ 走守卫
     死信（queue.go:355-361；Python「首次投递 delivery_count 恒 1，不套预算判据」task_queue.py:810-824）。
   - 窗口旋转防卡尾：满窗 → 游标推进到末条；短窗 → 游标归零（queue.go:279-289，#196）。
3. `XREADGROUP … COUNT 1 BLOCK read_block_ms`（默认 1000、地板 100，config.go:143-144, 348-350）；
   redis.Nil/瞬时错误 → 告警 + `reconnect_backoff=1s` 后返回 None，**绝不炸穿循环**
   （queue.go:148-160；Python 瞬时故障族注记 task_queue.py:741-756）。
4. 解析/闸门失败在 queue 层就地 DLQ+ack（§4.4），不返回给上层。

### 4.3 Ack 与残留清扫

- Ack = `XACK` 后 best-effort `XDEL`（本部署每流恰一组；XDEL 失败不破坏 ack 语义），
  ack 匹配 0 条要告警（queue.go:366-376）。
- 周期清扫（默认 300s，config.go:276）：`XINFO GROUPS` 取 last-delivered-id，分页取 PEL
  （单页 1000，超页跳过本轮），XRANGE `- last-delivered` 内不在 PEL 的条目 XDEL
  （queue.go:511-560；Python sweep_acked_residue task_queue.py:980+）。

### 4.4 死信（DLQ）

- 信封字段：`reason` / `source_stream` / `source_id` / `delivery_count` / `dead_lettered_at` /
  `schema_version`（**源**信封版本）+ `payload`（任务体 dict 或原始串）；畸形消息另带
  `raw_fields`（各字段截 500 字符）+ `truncated`（queue.go:382-438, 494-507）。
- 原因常量：`invalid_task_payload` / `unsupported_schema_version` / `max_deliveries_exceeded` /
  `dead_letter_guard_fired`（queue.go:254-259）。
- 写序：XADD → XTRIM(exact) → ack；**DLQ 写成功后 ack 失败是具名大响**——消息留 pending 会再死信
  一次而不是消失（queue.go:425-430）。
- **守卫死信**（M2，queue 层回调接口 M1 先留 trait 钩子）：`max_deliveries` 命中时先问守卫
  （worker.go:2054-2131 的分支表：schema 拒绝/无 execution_id/已终态 → 放行；探针失败/租约活跃 →
  跳过留 pending；墓碑 → 放行不克隆；running 且重试预算尽 → 先终态化再放行；非终态无租约 →
  先入队恢复克隆再放行，克隆继承源信封版本）。

### 4.5 背压与 poison/park

- **背压 = 单在飞执行**：M1–M3 消费并发恒 1（Go MVP `concurrency: 1`，registry.go:76；Python
  默认亦串行，flow_worker.py:7000-7002）。`ProcessOne` 串行保证单条消息在飞上限天然成立；
  `queue_depth` gauge 每轮采样（XLEN O(1)，worker.go:317-319）。多线程并发消费（Python
  flow_worker.py:7004-7012 的 N 线程共享队列先例）列为 M3 后扩展，默认不开。
- **poison ack**（不执行、不重投、直接 ack）：未知 task type（worker.go:422-425）；resume 缺
  execution_id（worker.go:1770-1773）；start 命中他属 flow 的 execution id（worker.go:510-516）；
  墓碑命中的重投（retry.go:247-251，A51-01：再入队会复活已删执行）。
- **park（留 pending）**：一切 transient——Redis 读失败、租约探针失败、状态行读失败、入队失败
  （enqueue→ack 序中 enqueue 失败原条留 pending 等 reclaim）、租约冲突的非 ack 分支、drain 强停
  弃步（worker.go:988-998）。park 必须 Warn 日志 + 对应指标，禁止静默。
- **DLQ+ack**：payload/schema/超投递三族（§4.4）。

### 4.6 flow 定义源与指纹

- 读取形状：`{ns}:flow:{flow_id}:{version}` + versions set，空版本先取字面 `latest` 成员、否则
  数值最大版本（flowsource.go:21-30, 135-153；Python storage/redis.py:764-782 同规则）。
- 装载：`plaita_engine::ir::load`（ir/mod.rs:45）——IR loader fail-closed，不可装载 = 确定性
  `ErrFlowNotFound` 族终态（flowsource.go:115-120 同款注释「拒载清单」）。
- **指纹必须与 Python/Go 字节一致**：`sha256(canonical_json(raw))`，canonical = 对 json.loads
  后的对象做 `json.dumps(sort_keys=True, ensure_ascii=False, separators=(",",":"))`
  （flowsource.go:173-192；canonicaljson.go 全文件——Go 特别注明 json.Marshal 的 HTML 转义/
  U+2028 转义/NaN 处理会翻车，rs 用 serde_json 时须：递归排序键（serde_json PreserveOrder 下
  手动 BTree 化）、禁 HTML 转义（serde_json 天然不转义 `<` `>` `&`——满足）、非 ASCII 不转义
  （天然满足）、浮点格式以 golden 钉死）。**验收 = 复用 Go 的 `testdata/flow_hash_golden.json`
  fixture 跑 golden 测试**（flow_hash_golden_test.go）。
- 算法标签：Go 用 `flow-definition-json-sortkeys-v2-go`（config.go:28-48，#60 的教训是「共享标签
  + 独立实现 = 同名不同法的盲区」）。rs 刻 `flow-definition-json-sortkeys-v2-rs`：恢复闸只比哈希
  不比标签（config.go:38-40「hash equality passes the gate regardless of label」），标签差异只进
  `upgrade_suspected` 诊断明细——若 rs 指纹算错，会落进可解释的 upgrade_suspected 分支而不是
  假装没变。
- TTL 缓存 100 条/300s（config.go:278-279, 372-377）；console read-through（#299）非目标。

**参照实现依据**：queue.go 全文件；flowsource.go:21-236；canonicaljson.go；agegate.go:9-110
（M2）；flow_worker.py:354-360（Stream ID 兜底年龄源）、task_queue.py:739-826。

---

## 5. 执行租约（lease + fence）

### 5.1 三段 Lua：逐字移植，禁止「改进」

Python 权威（execution_lease.py:33-81）↔ Go 转录（lease.go:23-55）↔ rs 转录，三文本必须语义
等价（rs 侧照抄 Go 文本即可）：

- **acquire**：GET 判空 + fencegen `INCR` 产出世代（低于 fence 键地板则抬到 floor+1）+ SET fence
  （TTL 7d）+ SET lease（`{holder}:{gen}`，TTL=lease_ttl）；返回 0 = 已被持有 → `ErrExecutionLeaseConflict`
  （lease.go:23-35, 95-113；Python `_FENCED_ACQUIRE_LUA` execution_lease.py:68-81，含 A2R13 ABA
  防御注释：fencegen 永续单调）。
- **release**：全值相等才 DEL（lease.go:39-45）。
- **renew**：全值相等才 EXPIRE；返回 0 = 失租信号（lease.go:49-55, 124-131；§5.3 看门狗）。

### 5.2 holder token 与持有者反解

- 生成：`{prefix}:{instance_id}:{uuid4hex[:16]}`，prefix ∈ {start, resume, worker, dispatch,
  handover}；**末段恰 16 hex 是 wire format**（lease.go:146-162；Python `new_holder_token`
  execution_lease.py:252 + `_HOLDER_UUID_HEX_LEN=16` :99，#155 定长防误判）。
- 反解 `holder_instance_id`：3 段（fencing 值剥尾段）+ 首段白名单 + 末段 16 hex；不合规 = 不可
  解析（存活未知，保守留 pending）（lease.go:168-192）。

### 5.3 运行期纪律

- **先取租约再写 running 行**（§3.4 顺序，worker.go:477-486）；resume 路径取租约后**必须重读**
  状态行（TOCTOU，worker.go:1836-1855，plaita#241）。
- **看门狗**：每 `ttl/3`（地板 1s）renew；renew 错误（Redis 瞬断）不判死只告警；renew 返回 false
  = 失租 → **先发一次孤儿进度心跳**（防 keeper 在「租约已丢但仍在跑」窗口误杀，orphan.go:9-19）、
  标记 lease_lost、请求协作中止、移交孤儿观察（worker.go:851-901）。
- **挂起即释放**：suspended 行写完即 release——resume 载体要能拿到租约（worker.go:1010-1024 注释、
  测试断言 phase3_checkpoint_test.go:118-120）。
- **冲突处置（§3.5）**：决议载体（event/timeout/cancel）冲突 → 永不 ack 留 pending；
  其余载体 → 反解 holder → 查注册表状态 = running 才 ack（存活持有者证明），否则留 pending；
  探针失败保守留 pending（worker.go:1715-1753）。M1 无注册表时全量保守留 pending（fail-closed），
  M2 registry 落地后启用 ack 分支。
- 租约 TTL 默认 120s（config.go:264, 339-341）；fence 键 TTL 7d（config.go:57）。

**参照实现依据**：lease.go 全文件；worker.go:477-486, 851-901, 1715-1753；execution_lease.py:25-46,
63-140；flow_worker.py:3520-3530（租约对齐 run_distributed 窗口的 A′ 注记）。

---

## 6. 执行状态行（ExecutionStore）

### 6.1 状态机与字段

- 状态集：`queued`(console 占位行，worker 永不写)/`running`/`suspended`/`completed`/`error`/
  `cancelled`；终态集 {completed, error, cancelled}（state.go:21-35；Python event_filter.py:38）。
- 行字段（蛇形 JSON，字段名与 Go `ExecutionState` 逐一相同，state.go:40-60）：`execution_id` /
  `flow_id` / `flow_name` / `flow_version` / `flow_hash` / `flow_hash_algo` / `engine_version` /
  `tenant_id` / `context` / `status` / `start_time` / `last_update_time` / `end_time` / `error` /
  `invoker` / `node_timings` / `timeline` / `usage` / `dispatch_pending`。
  - `engine_version`：`"plaita-rs <版本>"`（worker.go:2401 同构）。
  - `context`：rs 侧写引擎快照形状（§7.2 的 `snapshot_json` 对象）。**形状分歧记录**：Go 写
    `$INPUT/$NODE/$LAST_NODE/…` dict（worker.go:2417-2442）；rs 写 `{input,nodes,errors,env,
    global,last_node,last_branch,flow_id,execution_id}`。影子对账按形状 diff（state.go:37-39
    注释口径），不假装同形。
- 时间戳：**naive 本地 ISO**（无时区后缀），格式 `%Y-%m-%dT%H:%M:%S%.6f`（state.go:62-64 的
  Python `datetime.now().isoformat()` 对齐口径）。
- 执行 id：缺省时现场铸 uuid4 hex（worker.go:2476-2482）。

### 6.2 写路径：fenced CAS

- `SaveExecutionState(state, fence_gen)`：`fence_gen > 0` 走 Lua CAS——fence 键值 ≠ gen → 0
  （fence mismatch → `ErrLeaseLost` 族「不 ack」）；fencegen 计数器 ≠ gen → -1（部分恢复疑似，
  具名 `ErrFenceGenerationInconsistent`）；通过则 SET（终态附加 TTL，默认 30 天）+ ZADD 索引
  同脚本原子（state.go:140-213；#54/A2R25-05 fencegen 双闸）。
- `fence_gen = 0` 仅限「无租约的确定性终态」（入口取消 worker.go:703-714）——**禁止**用于可能有
  活持有者的终态覆写（worker.go:2297-2313 #53 的整段教训：fenceGen=0 裸写会 clobber 活 run 并
  误放配额槽）。
- 读侧：键缺失 = (None, None)；**存在但损坏/读失败 = `ErrStateLoad`，绝不吞成"缺失"**
  （state.go:239-255）。
- 取消标志：worker 只读不删（自然过期），入口检查点 + 步界轮询 + 终态复核三处消费
  （state.go:105-110；worker.go:567-576, 819-844, 958-963；Python 波次①§3.1 选型 B
  flow_worker.py:4455-4470）。

### 6.3 时间线与节点计时（keeper 判据的载体）

- `timeline = {events[], dropped, max_events:500, enqueued_at, queue_wait_ms}`，容量 500 丢最旧
  计 dropped（events.go:77-78, 255-285）；事件形 `{seq, type, at, at_ms, ...}`。
- `node_timings[node] = {started_at, started_ms, ended_at, ended_ms, duration_ms,
  total_duration_ms, attempts, failed}`；**在跑节点 `ended_at=""` 是 keeper 判活判据①**
  （events.go:308-340, :321-322 注释）。
- resume 会话必须**继承**上一段的 node_timings/timeline（seq 保持单调，终态行读作一次执行）——
  `seedRunFromState`（worker.go:1104-1139；测试断言 phase3_checkpoint_test.go:165-171）。
- 直播通道 `{prefix}:execution:events:{id}`：worker 是唯一发布方（events.go:405-419）。

**参照实现依据**：state.go 全文件；events.go:77-419；worker.go:703-714, 958-963, 1104-1139,
2391-2442。

---

## 7. checkpoint 持久化与序列化边界

### 7.1 rs 引擎现状（消费面盘点）

- `Flow::execute(&input, &Host)` 一步跑完，挂起以 `FlowError::Suspended(Box<SuspendedExecution>)`
  返回（exec.rs:212, 400-402, 494-499；error.rs:127）。
- `SuspendedExecution { suspended_node, flow, rc, host }` 全内存（exec.rs:753-759）；对外只有
  `node_state()`（$NODE 表项：status pending + service_config + subscription_id/…，exec.rs:763-770）、
  `checkpoint_json()`（全量上下文快照，exec.rs:773-775）、`subscription_id()`（exec.rs:778-783）、
  `resume(kind, data)`（进程内恢复+续跑一步到位，exec.rs:795-891）。
- 快照形状（`RuntimeCtx::snapshot_json`，context.rs:466-481）：`{input, nodes, errors, env,
  global, last_node, last_branch, flow_id, execution_id}` 九根齐全。
- 挂起边界已内置：根执行闸（exec.rs:429-435）、pending 校验（:437-443）、**无订阅注册表 = 具名
  拒绝挂起**（:444-451）、订阅 id/租户回写 $NODE（:481-490）、恢复准入（挂起型/pending/订阅绑定
  `RESUME_BINDING_KEY` 剥离/event_type 一致，:795-822, 897-938）、恢复会话**新鲜预算**
  （:824-829）、恢复后注销订阅（:850-862）。
- Host 宿主面已齐：`with_execution_id`(:104)/`with_tenant_id`(:119)/`with_subscriptions`(:113)/
  `listen`(:161)/`cancel`(:140，幂等粘滞取消)。

### 7.2 序列化边界设计（worker 持久化什么）

**原则：worker 持久化的不是 Rust 对象，而是「flow 文档引用 + 上下文快照 + 挂起位置 + 订阅信息」
四元组；恢复 = ir::load 重建 Flow + 快照重建 RuntimeCtx + 重建 Host + 复用引擎 resume 入口。**

Go 的接线是把 `SuspendOptions{Store, Subscriptions}` 注入引擎上下文、由**引擎**在挂起边界写
checkpoint（worker.go:790-797；checkpointstore.go:14-26）。rs 引擎没有 Store 注入点——**这是
刻意的更简边界**：引擎返回 `Suspended` 后由 worker 调 `checkpoint_json()` 自行落盘。语义无损：
Go 的 `persistSuspended` 反正会把引擎写的 checkpoint 经 worker store 重存一遍（worker.go:1141-1187
注释「re-saved through the worker store so the key layout is owned by the worker」），rs 只是把
两次写合并为一次。

**CheckpointRecord（Redis 值 JSON）**——信封字段与 Go `workflow.Checkpoint`（checkpoint.go:72-86）
逐一同名，`context` 载 rs 原生快照 + 形状标记：

```json
{
  "schema_version": 1,
  "kind": "suspend",
  "execution_id": "…", "tenant_id": "…", "flow_id": "…",
  "suspended_node": "wait",
  "next_node": "",
  "resume_type": "event",
  "context_flavor": "plaita-rs-snapshot-v1",
  "context": { "input": …, "nodes": …, "errors": …, "env": …,
               "global": …, "last_node": …, "last_branch": …,
               "flow_id": …, "execution_id": … },
  "created_at": "2026-10-23T12:00:00.000000"
}
```

- `kind`：M1–M3 恒 `suspend`；`per-step` 是 Go #32 的保留字（checkpoint.go:62-70），rs 不写。
- `resume_type`：挂起边界固定 `"event"`（Go legacy 快照同款形状，worker.go:1494-1503）。
- **`context_flavor` 是 fail-closed 闸**：载入时必须等于本仓标记——Go 写的 checkpoint（context 为
  `$INPUT/$NODE` 形状、无 flavor）被 rs 读到时**具名拒绝**（`checkpoint_context_flavor_unknown`，
  ErrStateLoad 族），绝不静默重建出空上下文「从错误位置恢复」。同理 Go 侧读到 rs checkpoint 会因
  schema 校验失败报错（checkpointstore.go:59-76 的 fail-closed 读取契约），跨引擎互恢复被双向
  关死——这是特性不是缺陷：flow 指纹闸（§9.5）本就拒绝跨引擎 resume。

### 7.3 CheckpointStore trait + Redis 实现

```rust
pub trait CheckpointStore: Send + Sync {
    fn save(&self, cp: &CheckpointRecord) -> Result<(), WorkerError>;
    fn load(&self, tenant_id: &str, execution_id: &str) -> Result<Option<CheckpointRecord>, WorkerError>;
    fn delete(&self, tenant_id: &str, execution_id: &str) -> Result<(), WorkerError>;
}
```

Redis 实现（checkpointstore.go 的直译）：键 `{ns}:execution:checkpoint:{id}`；save = SET 无 TTL
（「人类审批可以等任意久，终态删除才是生命期边界」，checkpointstore.go:14-26 注释）；load：
缺失 = Ok(None)，损坏/超版本 = **Err 不是 None**（「never silently missing——否则 resume 会从
错误的地点开始」checkpointstore.go:56-76）；delete 幂等（:79-84）。终态写成功后删除，失败仅告警
（行是权威，worker.go:1283-1290）。engine 侧同时要求：**新 execution id 的 start 必须先清残留
checkpoint**（不继承前一世代的现场，worker.go:551-557）。

挂起落盘序（全部在租约内、消息 ack 之前）：
1. `checkpoints.save(cp)`（含订阅信息——订阅已在引擎挂起边界注册进 registry 并回写 $NODE）；
2. 状态行写 suspended（含 dispatch_pending 凭据，同一次写，§9.4）；
3. RPUSH 服务任务 → 成功后清凭据再落一次盘；RPUSH 失败**消息不 ack**（凭据在，补派路径负责
   重派；at-least-once，重复由服务侧幂等吸收）（worker.go:1153-1248；Python
   flow_worker.py:4437-4454「先落盘+凭据，后派发」的两段非原子论证）。

### 7.4 引擎需要新增的最小接口（只列需求，实现轮落地）

| # | 接口需求 | 形状建议 | 语义约束 |
|---|---|---|---|
| E1 | **RuntimeCtx 快照重建构造器** | `RuntimeCtx::from_snapshot_json(&Value) -> Result<RuntimeCtx, String>`（配 `shared()` 入 Arc） | 恢复 `snapshot_json`（context.rs:466）全部九根（input/nodes/errors/env/global/last_node/last_branch/flow_id/execution_id）；`parent = None`（挂起只在根执行，exec.rs:429-435 的不变量使父链不需要序列化）；缺键 = 具名错误，静默缺省禁止 |
| E2 | **SuspendedExecution 重建入口** | 首选 `SuspendedExecution::from_parts(flow: Flow, rc: SharedCtx, host: Host, suspended_node: String) -> SuspendedExecution`（现字段 pub(crate)，exec.rs:753-759，加 pub 构造器即可）；或等价的 `Flow::resume_from_snapshot(snapshot, suspended_node, kind, data, host)` | **必须复用** `SuspendedExecution::resume`（exec.rs:795-891）的准入/绑定剥离/新鲜预算/注销订阅/NodeEnd/advance 全链——严禁 worker 侧fork 一套恢复逻辑；重建后 `flow_deadline` 为 None 由 resume 自己发新预算（exec.rs:824-829 已保证），无需持久化 Instant |
| E3 | **快照往返性质** | 属性测试（proptest 或固定用例） | `snapshot_json → from_snapshot_json → snapshot_json` 恒等；重建后 `node_state()`/`subscription_id()` 与挂起时一致；`resume` 准入行为（含 `RESUME_BINDING_KEY` 校验，exec.rs:897-938）与进程内挂起完全一致 |
| E4 | （无需新增）取消/预算/事件/注册表面 | — | `Host::cancel`（exec.rs:140）+ 本地 flag 已足够实现 worker 级 deadline watchdog（worker 置 flag 后调 `host.cancel()`，分类时先查 flag——Go `run.markDeadlineHit()` 同款，worker.go:768-779, 1025-1062）；事件回调 `host.listen` 覆盖 node_timings/timeline 采集所需（exec.rs:18-39 的四事件） |

**不改**：`Flow::execute` 语义（单次执行契约）、`advance_from` 保持私有、`ResumeKind` 三态不变
（continue/retry 不进引擎——它们是 worker 层载体语义，§9.6）。

---

## 8. 订阅存储 + 超时检查器

### 8.1 引擎 trait 的 Redis 实现

引擎面是 `SubscriptionRegistry { register(&mut SubscriptionRequest), unregister(id, tenant) }`
（exec.rs:84-87；请求字段与 Go `SubscriptionRequest` 同形，exec.rs:71-81 ↔ suspend.go:67-81）。
rs `RedisSubscriptions` 实现之，记录 JSON 字段与 Go `EventSubscription` 逐一相同（schema_version /
subscription_id / event_type / filter / correlation_id / flow_id / node_id / tenant_id /
created_at / timeout / timeout_at，eventbus.go:41-53）——混合部署下任一引擎写的订阅对另一引擎的
事件桥**完全可读**。

- `register`（eventbus.go:83-126 的直译）：id 为空时现场铸（uuid4 hex）；`TxPipeline` = SET 记录
  (TTL=event_ttl 默认 7d) + SADD 类型索引 + EXPIRE + 超时>0 时 ZADD 到期索引（member=记录键，
  score=now+timeout）。engine 在注册失败时拒绝挂起（exec.rs:468-474 已有），符合「无唤醒源不挂起」。
- `unregister`（:130-150）：DEL 记录 + ZREM 到期索引 + 按 lush 记录里 event_type SREM 类型索引；
  幂等。
- 辅助读：`get`（损坏 = 错误不是缺失，:153-166）、`subscription_ids(type)`（SMEMBERS，:169-171）。

### 8.2 事件 → resume 载体（HandleEvent，EventFilter 语义）

eventbus.go:176-267 的直译 + Python 权威校验（event_filter.py:112-273）：

1. event 必须有 `event_id`+`event_type`；`correlation_id` 缺失 = 无执行可唤醒，跳过；
2. **租户只取受信顶层 `event.tenant_id`**（发布端绑定），**绝不读 `event.data["tenant_id"]`**
   （eventbus.go:32-35 注释；Python event_filter.py:126-135「把能给谁注入 resume 的开关交给事件
   内容」的历史缺陷修口）；
3. 取类型索引 → 逐订阅：correlation_id 匹配 + 点路径 filter 匹配（空 filter 恒匹配，
   eventbus.go:426-448）；
4. 载入执行状态行：缺失跳过；**状态租户 ≠ 事件租户 → 丢弃并告警**（纵深强校验，
   eventbus.go:220-224；Python event_filter.py:150-160）；终态 → 就地注销订阅（GC 残留，
   eventbus.go:225-229；Python event_filter.py:197-214）；
5. `(event_id, subscription_id)` NX 去重 1h；**入队失败必须释放去重键**（「键已占、载体未入队」
   会在 TTL 内永久吞掉该事件，eventbus.go:256-260；Python event_filter.py:258-270 #87）；
6. 入队载体：`{type:"resume", flow_id, execution_id, resume_type:"event", timestamp, tenant_id,
   data:{event_id, event_type, event_data, subscription_id}}`——`data.subscription_id` 正好喂给
   引擎绑定校验（exec.rs:905-917），零转换。

**Pub/Sub 桥**（eventbus.go:351-405）：PSUBSCRIBE `{prefix}*:events:*`；**先等订阅确认再消费**
（否则窗口内事件全丢且本 build 无回扫器，:369-375）；跳过 `:execution:events:` 通道（形状撞车，
:389-394）；不可解码 payload 告警跳过；断线 1s 重连（Redis 重启不得永久禁用唤醒）。
M3 补事件回扫器（event_reconcile.py 对应物）前，此窗口风险记录在案。

### 8.3 超时检查器（SubscriptionTimeoutChecker 对应物）

eventbus.go:272-346 的直译：10s tick（:407-422）；`ZRANGEBYSCORE timeouts 0 now COUNT 100` →
逐条：记录键已消失（TTL 到期）→ ZREM；损坏 → ZREM+告警；无 correlation_id → ZREM；状态行读
失败 → **留待下轮**（Python 同款「订阅 TTL 兜底」eventbus.go:305-310 注释）；终态 → 注销；否则
NX 去重 1h 后入队 `resume_type=timeout` 载体（data={subscription_id, event_type, node_id}），
入队失败释放去重键。

**参照实现依据**：eventbus.go 全文件；exec.rs:71-93, 422-500, 897-938；event_filter.py:112-390；
flow_worker.py:4694-4740（凭据与订阅的超时/补偿互不相扰边界）。

---

## 9. 恢复投递协议

### 9.1 载体处置总表（handleResume 分支表）

handleResume（worker.go:1767-1903）的语义全表，rs 逐行对齐：

| 前置态（盘上行） | resume_type | 处置 | 依据 |
|---|---|---|---|
| 终态 | 任意 | 幂等 ack（终态行永不被 resume 覆写） | worker.go:1787-1793 |
| 任意 | cancel（或取消标志已置） | 取租约 → 终态化 cancelled + 删 checkpoint（幂等） | worker.go:1795-1800, 1908-1955 |
| suspended | continue/retry | **无推进语义**：不取租约幂等 ack；仅当有 dispatch_pending 凭据时短期租约补派（plaita#233/#243） | worker.go:1802-1813, 2002-2040 |
| running | event/timeout | 错过挂起点 → 归一化为 continue 载体重投入队，成功后 ack 原条（#258，不 ack 掉唯一恢复路径） | worker.go:1868-1873, 1961-1983 |
| suspended | event/timeout | **真唤醒**：§9.2 | worker.go:1875-1880 |
| running | continue/retry | 中流续跑（M1 退化为 fresh-run 重放 + 可观测事件，§9.6） | worker.go:1882-1889 |
| 其他组合 | 任意 | `resume_not_supported` 具名错误终态（fail-closed，不静默 ack） | worker.go:1891-1902 |

租约内**必须重读**状态与取消标志（入口快照可能已陈旧，worker.go:1836-1867 plaita#241）。
租约冲突按 §5.3 处置（决议载体永不 ack）。

### 9.2 真唤醒路径（suspended + event/timeout/cancel）

1. **流定义闸**（loadContinuationDefinition，worker.go:1440-1482）：
   - 载体 flow_id ≠ 行 flow_id → poison ack（行不动）；
   - 定义不可装载（确定性）→ `flow_not_found` 错误终态 + 删 checkpoint；
   - **指纹硬闸**：`state.flow_hash ≠ def.hash` 且载体未带 `data.allow_flow_hash_change=true`
     → `flow_hash_mismatch` 错误终态（明细含 stored/current 哈希与算法标签、upgrade_suspected
     判定，worker.go:1462-1480）。
2. **checkpoint 装载**（resumeSuspended，worker.go:1295-1343）：专用键为权威；缺失/`context_flavor`
   不符 → `resume_checkpoint_missing` 错误终态（挂起执行无 checkpoint 没有安全的默认动作）。
3. **重建**：`ir::load(raw)` 得 Flow → `from_snapshot_json(context)` 得 rc → 重建 Host
   （execution_id/tenant/registry/listeners 与 start 路径同一套装配）→ `from_parts` 得
   SuspendedExecution（§7.4 E1/E2）。
4. **施加载体**：`resume(ResumeKind::parse(carrier.resume_type), data)`：
   - data 直接透传（`data.subscription_id` 进绑定校验、`RESUME_BINDING_KEY` 由引擎剥离）；
   - 引擎判定 `ResumeGuard`（陈旧载体/绑定不符/non-pending）→ **执行保持挂起，载体 ack**
     （永久拒绝，留着只喂 reclaim→DLQ→克隆的自馈循环——#68 的结构性论证，worker.go:436-463,
     999-1009；Python「ResumeProtocolError … 否则原样 ack」）+ `resume_carrier_rejected_total`
     计数；
   - 引擎判定 `ResumeError`（节点恢复处理器失败）→ 视为 transient，**消息留 pending**（可能是
     瞬时处理器故障，worker.go:999-1009 的双类区分）。
5. **续跑分类**：resume 返回后与 start 同一套 `executeAndFinalize` 分类（可能完成/再挂起/失败/
   被取消/deadline）——再挂起时按 §7.3 序重新落盘（Go：resume 后再次 SuspendError 走
   persistSuspended 分支，worker.go:1010-1024）。

### 9.3 start 路径闸序（handleStart）

handleStart（worker.go:469-625）闸序逐项照抄：

```
租约 acquire（冲突→§3.5 处置）
→ 幂等门：行已存在时按状态分支（queued=console 占位，放行正常 start；
   running=崩溃恢复载体，转 continue 载体入队→ack【§9.6】；
   suspended=幂等 ack + 补派欠账；终态=幂等 ack 不覆写；
   行 flow_id ≠ 载体 flow_id=poison ack）
→ 全新 id：清三重试计数 + 删残留 checkpoint
→ 入口取消检查点（命中→cancelled 终态，fence_gen=0 写安全——行尚不存在）
→ [M2] 租户配额 admit
→ flow 定义装载（确定性缺失→错误终态；瞬态读失败→留 pending）
→ 写 running 行（fenced）
→ executeAndFinalize
```

### 9.4 挂起服务派发（dispatch_pending 凭据）

- 凭据与 suspended 行**同一次落盘**：`{pending:true, service_type, service_config, node_id,
  recorded_at}`，service_config 取自挂起态 `$NODE.{id}.service_config`（rs 引擎七类挂起节点的
  extended 挂起态已含该键，nodes/suspend.rs:233-251）；非服务挂起（event/hitl_await）无凭据
  （worker.go:1153-1210；Python flow_worker.py:4694-4716）。
- 派发：RPUSH `{prefix}:{subtype}:queue`，体 = service_config + `execution_id` + `tenant_id`
  （空租户归一 default）；成功后清凭据再落盘；清凭据失败 = 上抛不 ack（重投可能**重复派发一次**，
  服务侧幂等吸收——代价远小于永久 suspended，flow_worker.py:4717-4740）。
- 补偿触发面（三条载体共用）：start 幂等门的 suspended 分支、suspended+continue 短路、租约内
  复核——补派持短期租约 + 持锁重读（防旧快照倒灌 + 防并发双派），被活持有者占租约则跳过
  （worker.go:2002-2040；Python flow_worker.py:4745-4863 的 lease_held 论证）。

### 9.5 执行期分类（executeAndFinalize 的判定序）

worker.go:965-1096 的判定序（顺序即优先级，rs 严格保持）：

```
lease_lost → ErrLeaseLost（不 ack，先孤儿心跳再中止）
→ 取消（标志为准，终态后复核一次）→ cancelled 终态（含 checkpoint 快照）+ 删 checkpoint
→ drain 强停弃步 → 不写终态、消息留 pending（ErrDrainAborted）
→ ResumeGuard/ResumeError 载体拒绝 → §9.2 双类处置
→ FlowError::Suspended → suspended 行（§7.3 序）
→ [M2] deadline flag → execution_deadline_exceeded 错误终态（非重试、无 DLQ）
→ FlowError::Node 且可重试 → 节点重试路径（[M2] 预算/退避/载体转换；M1 直接入错误终态并告警）
→ 其余 Err → 归一化错误终态（§11.2）
→ Ok → completed 终态（含 checkpoint 快照 + run_end(final=true)）+ 删 checkpoint
```

rs 错误归一化（errors.go:100-186 对应物）：`FlowError::Handler{code}` → code 取 handler 码、
error_type="Handler"；`FlowError::Node{node_id, error}` → code 取 NodeError.code（error.rs:43-51），
`failed_node` 记 node_id；`FlowTimeout`/`Other` → code=-500（`flowErrorCode`，errors.go:96），
error_type 取变体名。worker 自判终态固定 error_type + `DefaultNodeAbortCode=-520`
（node_retry_budget_exhausted）或 -500（flow_not_found / flow_hash_mismatch /
resume_checkpoint_missing / resume_not_supported / execution_deadline_exceeded）。取消是引擎具名
变体（`FlowError::Cancelled`，code=-4，error.rs:30,121）+ worker 侧 deadline/cancel flag 双证据，
取消永不落 error 行（worker.go:955-963「标志是事实源」）。

### 9.6 崩溃恢复与 fresh-run 重放（rs 的已知降级，必须可观测）

Go 把「running 行 + PEL 残留消息」收敛为 continue 载体中流续跑（#32/#47/#91）。rs 引擎无 mid-flow
入口 → **M1–M3 行为**：沿用载体转换机器（retryCarrierBody，retry.go:234-307）保持协议面一致
（混合部署下 go worker 与 rs worker 看到同样的载体形状），但 continue 载体在 rs 上退化为
flow-start 重入——执行从头重放（已完成节点副作用重放）。降级必须可观测：timeline 记
`continue_fallback_start {reason:"no mid-flow checkpoint; re-entering at flow start"}` 并直播
（worker.go:903-913 同款）。该降级随引擎 E-2 后续（mid-flow 入口 + per-step checkpoint）立项
消除，届时本节收缩为纯转发。

**参照实现依据**：worker.go:436-625, 718-1102, 1295-1343, 1440-1482, 1767-2040；retry.go:204-326；
flow_worker.py:3860-4348（resume_flow）、4348-4560（结果分类）、strategies.py:225-470
（分布式单步 + `_handle_resume` 准入）；phase2_resume_test.go / phase3_checkpoint_test.go
（行为断言来源）。

---

## 10. worker registry metadata 与观测

### 10.1 注册键与生命周期

- 键 `{prefix}:registry:flow_worker:{instance_id}`，SETEX 30s；心跳 10s（registry.go:64-66,
  232-250；config.go:273-274）。状态机 `starting → running → (drain) draining → stopping →
  stopped`（registry.go:40-46）；**状态迁移立即 flush（3 次短重试）**——draining 必须当轮对派发
  方可见（registry.go:165-197）。注销 = stopped 墓碑 SETEX 5s → DEL（registry.go:269-277）。
- `LookupStatus` 是 §5.3 冲突处置的存活证据源（registry.go:254-267）。

### 10.2 ServiceInfo 与能力清单（plaita#300 capability 门）

字段与 registry.go:28-37 一致（instance_id/service_type="flow_worker"/host/status/start_time/
metadata/active_tasks/last_heartbeat）。metadata（registry.go:70-107）：queue_name / cache_size /
cache_ttl / concurrency / redis_url（密码掩码）/ host / port / db / worker_capacity（schema v1，
free_disk_gib=null 允许）/ **engine** / **capability_manifest**：

- `engine = "plaita-rs"`；manifest 直接嵌入 `plaita_engine::ir::capability_manifest()`
  （ir/mod.rs:98，五键形态；`CONTRACT_VERSION` ir/mod.rs:58 随引擎漂移）。console 能力门按池
  收集 manifest，无 manifest 的池 fail-open（registry.go:121-137 注释）。
- **降级可观测**：manifest 序列化失败（实际不可能）→ 注册项**整个省略 capability_manifest 键**
  （让门 fail-open 并可解释）并打 WARNING marker `PLAITA_CAPABILITY_MANIFEST_UNAVAILABLE`——
  **禁止**写入「存在但为空」的 manifest（那个形状会挂五键门还看起来健康，registry.go:131-137）。
- active_tasks 变化时刷新 capacity 块并 flush（registry.go:199-219）。

### 10.3 指标面

`Metrics` trait（IncCounter/SetGauge/ObserveDuration）+ NoopMetrics 默认、宿主注入
（metrics.go:16-31）。指标名稳定性契约（metrics.go:35-57）：`queue_depth` / `lease_acquired` /
`lease_lost` / `exec_duration` / `exec_result`(status 标签) / `retry_total` / `dlq_total` /
`event_wake_latency` / `task_expired_total` / `task_dedup_hit_total` /
`resume_carrier_rejected_total` / `exec_deadline_total`。**基数契约：标签只允许低基数维
（tenant/flow/status/reason/node/resume_type/type/queue），execution_id 永远不作标签**
（metrics.go:5-15）。M1 先埋点 queue_depth / exec_result / dlq_total，其余随 M2/M3 功能落地。

---

## 11. 停机 / drain、错误分类与运维判据

### 11.1 三态 drain（§9.3）

drain.go:9-21 的语义：

- **D1 `RequestDrain`**：停止领新任务（消费循环在**任务边界**退出，worker.go:303-309）；注册表
  状态立即刷 draining（3 次短重试）；起 DrainTimeout（默认 30s）计时器，到点自动升级 D2；
  幂等（二次调用不重置计时）。
- **post-read gate**：drain 开始后才送达的消息**不派发也不 ack**，留 PEL 给其他 worker
  （worker.go:369-374；Python drain-read-gate flow_worker.py:7106-7112）。
- **D2 `ForceStop`**：无条件停消费 + 尽力协作取消全部在飞 run（`host.cancel()`；无硬中断——
  「abandoning the step」语义）；被弃步**不写终态**、消息留 pending（可恢复性优先，
  worker.go:988-998）。退避睡眠 200ms 分片可打断（drain.go:150-178），保证 D2 后退出延迟有界。
- **D3 信号**：SIGTERM/SIGINT 只调 `RequestDrain`；退出序 = 消费循环退出 → 后台线程 join →
  注册表 stopping → 注销（worker.go:262-268 的 defer 链，**注册必须先于一切可失败步骤**防
  半初始化残留，worker.go:256-262 注释 #75）。

### 11.2 错误分类（transient vs park vs fail vs poison）

错误族表（errors.go:19-67 + 处置表 worker.go:23-31）：

| 族 | 成员 | 处置 |
|---|---|---|
| transient（park 留 pending） | Redis 读/写失败、`ErrFlowLoad`、`ErrStateLoad/Persist`（重投可愈）、租约冲突非 ack 分支、enqueue 失败、`ErrDrainAborted`、`ResumeError` | Warn + 计数，消息留 PEL 等 reclaim |
| deterministic terminal（写错误行 + ack） | `flow_not_found` / `flow_hash_mismatch` / `resume_checkpoint_missing` / `resume_not_supported` / `node_retry_budget_exhausted`(-520) / `execution_deadline_exceeded` / 引擎归一化节点失败 | §5.4 归一错误结构 `{message, code, error_type, response?, details?, node_retries?, failed_node?}`（errors.go:100-186） |
| poison ack（不执行直接 ack） | 未知 type、resume 缺 execution_id、execution id 他属、墓碑重投 | Warn + `task_dedup_hit_total` 族计数 |
| DLQ+ack | `invalid_task_payload` / `unsupported_schema_version` / `max_deliveries`（守卫后） | §4.4 |
| carrier-rejected ack | `FlowError::ResumeGuard`（永久拒绝，执行保持挂起） | §9.2 第 4 步 |

fail-closed 总则：**探针失败一律按最保守分支**（守卫跳过、冲突留 pending、指纹闸拒绝），
绝不 fail-open（retry.go:219-225, 253-261「绝不 fail-open 重排」）。

### 11.3 执行预算（deadline）

- worker 级预算默认 1800s（config.go:96-112）；env `PLAITA_EXECUTION_DEADLINE_SECONDS` 覆盖，
  **非法/≤0 回默认 + WARNING，绝不静默无界**（config.go:483-498）；显式负值仅 Config 钩子可表达
  （关闸是部署决策，env 永远关不掉）。与 flow 自有 timeout 取更严者（config.go:214-217；引擎侧
  `DEFAULT_EXECUTION_DEADLINE=1800s` budget.rs:41 已同值，双层合成天然一致）。
- 到点：置本地 flag → `host.cancel()` 协作取消（无 kill -9）→ 分类时挂起/取消/drain 判定之后、
  引擎错误之前判 flag → `execution_deadline_exceeded` 终态（挂起判定先于 deadline：完成 park 的
  执行已离开 worker、resume 载体拿新预算，worker.go:1025-1040 注释）。迟到 tick 不把边界完成误判
  （deadlineTimer.Stop 先于分类，worker.go:939-945）。

### 11.4 可观测性与 keeper 运维判据（对账钩子）

- **每个降级具名**：日志必带稳定 marker（`continue_fallback_start`、
  `PLAITA_CAPABILITY_MANIFEST_UNAVAILABLE`、`resume carrier acked while a live holder owns the
  lease — this resume had no effect`（worker.go:1744-1749，plaita#53）等）；指标面 §10.3。
- keeper 判据所需的行内证据全部由本设计产出：`node_timings.*.ended_at=""` 在跑标记、
  `last_update_time` 心跳保鲜（ttl/3 合并 node_timings 进 running 行，worker.go:846-901,
  1699-1710）、失租孤儿只发心跳不续租不终态（orphan.go:9-19, 111-147）、suspended 行无 end_time
  且租约已释放（phase3_checkpoint_test.go:88-120 断言面）。
- 僵尸判定输入：running + 最新节点计时停滞 + 租约为空 → reaper 域（仓外）；本 worker 保证不产生
  「行停跳但进程还活」的假象（orphan watch 的存在理由，orphan.go:11-19）。

---

## 12. 里程碑切分与验收清单

每个里程碑独立可验收（行为测试优先，命名对齐 Go 测试便于对拍评审）。

### M1 骨架闭环（单 worker：队列 + checkpoint + 挂起恢复）

范围：workspace 接入 + config/keys + queue（§4.1–4.4，守卫回调留 trait 空）+ lease（§5.1–5.2）+
state（§6）+ checkpoint store（§7.3）+ subscriptions store + HandleEvent + timeout sweep + Pub/Sub
桥（§8）+ 引擎接口 E1/E2/E3（§7.4，另提 engine 任务）+ worker 循环：start/resume/cancel 全路径 +
挂起派发凭据（§9.4）+ ResumeGuard-ack + drain D1/D2 + post-read gate + `queue_depth`/
`exec_result`/`dlq_total` 埋点。

验收行为测试（miniredis/testcontainers）：
1. 挂起→唤醒→恢复全链：start carrier → suspended 行（无 end_time、$NODE pending、checkpoint 键
   在、订阅键在、租约已释放、原条已 ack）→ 注入 event → 恰一条 event 载体（重复事件去重）→
   resume → completed、checkpoint/订阅键删除、node_timings/timeline 继承
   （对拍 phase3_checkpoint_test.go:77-172）。
2. 订阅超时：timeout 字段到期 → sweep 恰一条 timeout 载体 → 恢复后 $NODE.status=timeout
   （phase3_checkpoint_test.go:174-206）。
3. 陈旧 event 载体（订阅绑定不符）→ 执行保持 suspended、载体 ack、计数具名
   （phase3_checkpoint_test.go:208-247；exec.rs:897-938）。
4. cancel 载体 / 取消标志 → cancelled 终态 + checkpoint 删除（phase3_checkpoint_test.go:249-278）。
5. flow 指纹不匹配 → flow_hash_mismatch 终态、allow_flow_hash_change 放行
   （phase3_checkpoint_test.go:279-323）。
6. 畸形 payload / 超 schema / 超投递 → DLQ 信封字段断言（含 raw_fields 截断）
   （queue_test.go:14-171）。
7. Pub/Sub 桥投递 event 载体 + `:execution:events:` 通道不误吃（phase3_checkpoint_test.go:324-363）。
8. 挂起服务派发：suspended 行带 dispatch_pending → RPUSH 到 `{prefix}:{subtype}:queue` → 凭据清；
   RPUSH 失败 → 消息不 ack、凭据在；suspended+continue 短路补派一次
   （server_nodes_dispatch_test.go:28-110；phase2_resume_test.go:15-117）。
9. 引擎快照往返 + 重建恢复与进程内 resume 行为一致（E3 性质测试）。
10. 指纹 golden：rs FlowDefinitionHash 与 `testdata/flow_hash_golden.json` 全量一致。

### M2 租约纪律 / 重试 / 预算 / 超时面

范围：start 幂等门全分支表（§9.3）+ retry budgets 三计数 + 指数退避（可打断分片）+ 载体转换
（retryCarrierBody 含墓碑/flow 冲突分支）+ dead-letter 守卫（§4.4 分支表）+ registry 心跳与
`LookupStatus`（启用租约冲突的 live-holder ack）+ 孤儿观察 + 进度心跳（running 行保鲜）+
deadline watchdog + 取消轮询器 + tenant quota（Lua admit/defer/bypass）+ 载体年龄闸（双源取大 +
unknown fail-closed）+ 残留清扫循环 + 全量指标埋点。

验收行为测试：
1. 重复 start（running/suspended/终态/他属 flow 四分支）逐分支断言（idempotent_gate_routing_test.go）。
2. 可重试节点失败：计数 INCR、退避节奏（桩时钟）、新载体入队后原条 ack、预算尽 → -520 终态；
   成功推进清计数（phase2_retry_test.go:452 行族）。
3. running 行 + PEL 消息（模拟崩溃）→ reclaim → 转换 continue 载体 → fresh-run 重放 +
   `continue_fallback_start` 事件在 timeline（§9.6）。
4. 失租：renew 假 → 孤儿心跳发布、步中止、消息留 pending、无终态；新持有者 fence 世代使旧写 CAS
   失败（lease_test.go:279 行族）。
5. 死信守卫：租约活跃跳过、终态放行、非终态无租约先克隆再放行、墓碑不克隆
   （worker.go:2054-2131 分支逐条）。
6. 取消标志轮询中止在途执行（软中断）且终态为 cancelled 非 error。
7. deadline：短预算执行被终态化为 execution_deadline_exceeded；挂起判定优先（park 不吃预算）。
8. 租约冲突：决议载体永不 ack；活持有者时普通载体 ack + 具名 Warn（phase2_resume_test.go:191-213）。
9. 年龄闸：超龄载体 ack 不执行 + task_expired_total；双源取大；unknown fail-closed
   （agegate.go:9-110）。
10. quota 满时 defer 至队尾、预算尽 quota_bypass 放行、终态收口释放槽（phase3_quota_test.go）。

### M3 registry metadata / 台账 / 回放 / 亲和 / 完整 drain

范围：ServiceInfo 全量（engine + capability_manifest + worker_capacity）+ 台账（handover 决策
记录）+ DLQ replay（one-shot marker + 确定性 id + 源信封版本继承）+ 亲和让渡（target_instance_id
+ 有界计数 + 广播回退 + 耗尽终态）+ ApplyShadow 影子命名空间 + Metrics 宿主注入文档化 + 多线程
消费扩展评估（默认仍 1）。

验收行为测试：
1. 注册键 SETEX/心跳/状态即时 flush/注销墓碑序列（manifest_registry_test.go:186 行族）。
2. 外来 target：转投 → 计数键 7d TTL；目标消失 → 剥 target 广播；超 3 次 → 有界终态
   （写前 Acquire 取 fence，禁止 fenceGen=0 裸写，worker.go:2297-2380）。
3. DLQ replay：单次性 marker、源 schema 继承、拒绝原因全族（dlq_replay_age_test.go:308 行族）。
4. drain 全序：D1 后 post-read gate 消息留 PEL；D1 超时自动 D2；D2 弃步无终态、退出延迟有界
   （phase2_drain_test.go:179 行族）。
5. 影子运行全键（队列/组/状态/订阅/台账）落在 shadow 前缀，生产键零接触。
6. /metrics 注入后各计数在关键路径可见（phase3_metrics_test.go:281 行族）。

---

## 13. 开放问题（实现轮裁决）

1. **per-step checkpoint / mid-flow 入口**：§9.6 降级的根治需引擎侧「按位恢复」立项（引擎接口
   E-2 扩展：`advance_from` 公开 + 位置语义 + per-step 落盘时机）。在此之前混合部署中 go worker
   有中流续跑、rs worker 重放，运维须知（建议在 keeper 侧按 engine 标签区分告警文案）。
2. **事件回扫器**：M3 前事件桥的「订阅确认前丢事件」窗口无对账（eventbus.go:369-375 注释明示
   本 build 无 reconciler）；是否引入 event_reconcile 对应物独立立项。
3. **测试 Redis 依赖**：testcontainers（需 Docker）vs 内嵌 redis 进程（`redis-server` 依赖）vs
   协议假件；倾向 testcontainers + 可选 env 跳过，与「未设 env→skip」的既有门禁口径一致。
4. **`usage` 字段**：Go 行字段预留但 M1–M3 不产数据（state.go:58）；是否接预算消耗上报待定。
5. **消费并发 >1**：Python 有 N 线程先例（flow_worker.py:7004-7012），Go 恒 1；rs M3 后若开，
   consumer 命名与 active_tasks 口径需与注册表对齐后再定。
