#!/usr/bin/env python3
"""Extract the authoritative Python-side node-type list for the CI
Python<->Rust parity gate (make node-parity / tests/node_type_parity.rs).

Vendored from plaita-go (scripts/extract_python_node_types.py, #80) — the
authority is the same plaita NodeRegistry; only the consumer differs.

权威数据源 = plaita 的 NodeRegistry：core 内建（plaita/node/__init__.py
_BUILTIN_NODES）+ entry_points 组 "plaita.nodes" 的插件（plaita 自带 server
五类 + plaita-nodes 发行版 22 类）。提取器只读：对目标 checkout 不写不装，
registry 经 auto_discover 现场构建，节点类型清单取 list_types()。

前提：运行解释器已装 plaita 与 plaita-nodes（本机 monorepo 布局是 editable
安装；CI 里先 pip install 两个 checkout 再跑本脚本）。脚本能把 checkout 路径
插到 sys.path 解决**代码**定位，但 entry points 来自**已安装发行版**的
dist-info，sys.path 替代不了 pip install。

环境不健全必须硬失败（exit 2）而不是输出缩水清单——缩水的"权威清单"会把
parity 门做成假绿。两类硬失败：
  - 一个 "plaita.nodes" entry point 都看不见（plaita-nodes 没装）；
  - 有插件 entry point 加载失败（registry._load_errors 非空，#98 字段）。

用法：
    python3 scripts/extract_python_node_types.py \
        --plaita ../plaita [--plaita-nodes ../plaita-nodes] [--out FILE]

输出 JSON（stdout 或 --out）：
    {"sources": {...各仓 git 短 SHA...},
     "entry_points": [...发现的插件名...],
     "node_types": [排序后的权威清单]}
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import subprocess
import sys
from pathlib import Path

ENTRY_POINT_GROUP = "plaita.nodes"


def _git_short_head(repo: Path) -> str:
    """Best-effort provenance stamp; a missing git dir must not kill extraction."""
    try:
        return subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 - provenance is informational
        return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--plaita", required=True, type=Path,
                        help="path to the plaita checkout (read-only)")
    parser.add_argument("--plaita-nodes", type=Path, default=None,
                        help="path to the plaita-nodes checkout (provenance stamp only)")
    parser.add_argument("--out", type=Path, default=None,
                        help="write JSON here instead of stdout")
    args = parser.parse_args()

    for label, path in (("--plaita", args.plaita), ("--plaita-nodes", args.plaita_nodes)):
        if path is not None and not (path / "pyproject.toml").is_file():
            print(f"extract: {label} {path} is not a plaita checkout (no pyproject.toml)",
                  file=sys.stderr)
            return 2

    # 代码定位：checkout 优先于环境里可能陈旧的安装副本。
    sys.path.insert(0, str(args.plaita.resolve()))
    if args.plaita_nodes is not None:
        sys.path.insert(0, str(args.plaita_nodes.resolve()))

    ep_names = sorted(ep.name for ep in
                      importlib.metadata.entry_points(group=ENTRY_POINT_GROUP))
    if not ep_names:
        print(
            f"extract: no '{ENTRY_POINT_GROUP}' entry points visible to this interpreter; "
            "plaita-nodes is not installed here — pip install both checkouts first "
            "(a shrunken list would fake parity green)",
            file=sys.stderr)
        return 2

    from plaita.node import NodeRegistry

    registry = NodeRegistry(auto_discover=True)
    # #98 字段：插件加载失败会被 registry 吞进 _load_errors 并从节点清单里缺席。
    # 带着失败现场输出清单 = 用残缺集做权威，parity 门假绿——硬失败。
    load_errors = getattr(registry, "_load_errors", {})
    if load_errors:
        detail = "; ".join(f"{name}: {type(exc).__name__}: {exc}"
                           for name, exc in sorted(load_errors.items()))
        print(f"extract: {len(load_errors)} node plugin(s) failed to load — "
              f"refusing to emit a shrunken authority list: {detail}", file=sys.stderr)
        return 2

    doc = {
        "sources": {
            "plaita": _git_short_head(args.plaita),
            **({"plaita-nodes": _git_short_head(args.plaita_nodes)}
               if args.plaita_nodes is not None else {}),
        },
        "entry_points": ep_names,
        "node_types": sorted(registry.list_types()),
    }
    text = json.dumps(doc, indent=2, sort_keys=True) + "\n"
    if args.out is not None:
        args.out.write_text(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
