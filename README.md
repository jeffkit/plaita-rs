# plaita-rs

plaita 的 Rust 版 **Flow 引擎 + IR 解析 + Worker 运行时**（参考同仓群的 [plaita-go](../plaita-go) 移植）。
只做运行时：解析 plaita 部署形态的 flow JSON 并执行；`plaita-worker` 提供 worker 运行时层
（队列/租约/checkpoint/事件桥，WK-M1 起入 main，消费 `plaita:flow:queue:rust`）。

```bash
# 跑一个 flow（plaita codeflow 编译器产物）
plaita-rs run testdata/parallel_flow.json --input '{"x": 5}'
# → {"a":10,"b":15}

# 校验（归一化 + 结构规则 + fail-closed 拒载）
plaita-rs validate testdata/assignment_flow.json

# 调试一个表达式（Python 值语义：$ 根 / $F 函数 / {% %} 模板）
plaita-rs eval 'hi {%$INPUT.name%}, n={%$F.add($INPUT.n, 1)%}' --context '{"INPUT": {"name": "rs", "n": 1}}'

# 带 NDJSON 事件流执行
plaita-rs run testdata/loop_flow.json --input '{"items":[1,2,3]}' --events
```

## Crate 布局

| crate | 内容 |
|---|---|
| `plaita-expression` | plaita 表达式语言：文法解析、`$` 变量根、`$F` 函数库（~70）、`{% %}` 模板、Python 真值/缺键语义、PyStr/PyRepr |
| `plaita-engine` | IR 装载（normalize + validate + fail-closed）、运行时上下文、执行循环（重试/超时/错误策略/事件）、内置节点（start/end/assignment/switch/if/loop/parallel/child/code·python sidecar/http + #80 对齐：map/filter/find/reduce/while/case/mock/reference） |
| `plaita-cli` | `plaita-rs` 命令行 |
| `plaita-worker` | worker 运行时：队列消费、五键能力自报注册、租约/checkpoint、事件桥与兜底回扫、亲和让渡闸（WK-M1..M3） |

## 差分对账

`tools/diffcheck.py` 用本机 plaita-go（`tools/go-ref` 直接 import 其 `pkg/ir`）双跑同一组
flow 并比对结果 —— 当前 **23/23 一致**（KNOWN_RED 空）：

```bash
python3 tools/diffcheck.py
```

## 构建

```bash
cargo build --release -p plaita-cli   # 产物 target/release/plaita-rs
cargo test                            # 单测 + 43 条 plaita 表达式 golden + 端到端 flow 测试
```

环境备注（本机）：cargo 在 `~/.cargo/bin`；crates 走 rsproxy 镜像
（`.cargo/config.toml`，本地文件已 gitignore）。详见 [AGENTS.md](AGENTS.md)。
