# AGENTS.md — plaita-rs

> plaita 的 Rust 版引擎 + IR 解析。参考 [plaita-go](../plaita-go)（同仓群的 Golang 版）移植：
> **只做「解析 Plaita 的 Flow 并运行」的运行时**，不做 Flow Worker（队列/租约/checkpoint/事件桥
> 全部不在范围内，属于 worker 运行时层）。负责人：jeffkit | 创建：2026-10-21

## 定位与范围

- **在范围内**：部署形态 IR 装载（归一化 + 校验 + fail-closed 拒载）、plaita 表达式语言、
  执行引擎（变量根 / 条件 / 循环 / 并行 / 子流程 / 错误·超时·重试策略 / 事件回调）、
  内置节点（start / end / assignment / switch / if / loop / parallel / child /
  code〔python sidecar〕/ http）。
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
    src/nodes/         节点体：condition（15 算子 + None 短路）、loop（末次子结果语义）、
                       parallel（joinBranches 空=全后台；分支失败=错误哨兵）、
                       code（python3 sidecar，镜像 plaita _RUNNER_TEMPLATE + env 白名单 + 进程组超时）、
                       http（{data,status,statusText,headers} 契约；非 2xx 不是错误）
  plaita-cli/          bin `plaita-rs`：run / validate / eval
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

## 差分对账（改行为前必跑）

```bash
# 参照侧：Go runner（依赖本机 plaita-go checkout；首次需
#   cd tools/go-ref && GOPROXY=https://goproxy.cn,direct GOSUMDB=off go mod tidy && go build -o ../go-ref-bin .）
python3 tools/diffcheck.py     # 15/15 用例与 plaita-go 双跑一致（2026-10-21 基线）
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
