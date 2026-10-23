#!/usr/bin/env python3
"""NS-M4 差分 runner（rs 腿）：逐例驱动 plaita-rs 的隐藏子命令 `ns`。

`ns` 子命令（crates/plaita-cli/src/ns.rs）在单进程内完成
execute → suspend → resume(…) 并打印统一结果 JSON；本脚本只做进程编排：
为每份用例调一次 CLI、原样转发其 stdout（一行一个结果 JSON）。

用法：
    python3 run_rs.py [CASE.json ...]      # 缺省 = cases/ 目录全部用例
    python3 run_rs.py --cli PATH           # 显式指定 CLI 二进制

引擎不可用（二进制缺失/落后于 crate 源码）时以 exit 3 退出并在 stderr 说明——
diffcheck 据此把该腿记为 SKIP。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CASES = HERE / "cases"
REPO = HERE.parent.parent
DEFAULT_CLI = REPO / "target" / "debug" / "plaita-rs"
UNAVAILABLE_EXIT = 3


def unavailable(message: str) -> None:
    print(f"run_rs: {message}", file=sys.stderr)
    sys.exit(UNAVAILABLE_EXIT)


def ensure_cli_current(cli: Path) -> None:
    """拒绝缺失或落后于 crate 源码的二进制（tools/diffcheck.py 同款纪律）：
    陈旧二进制参与对拍会让「一致」失真——fail-closed，不回退旧件。"""
    if not cli.is_file():
        unavailable(
            f"CLI under test not found: {cli}\n"
            "  build it first: cargo build -p plaita-cli (or just: make ns-m4)"
        )
    newest_src = max(
        (p.stat().st_mtime for p in (REPO / "crates").glob("*/src/**/*.rs")),
        default=0.0,
    )
    if cli.stat().st_mtime < newest_src:
        unavailable(
            "CLI binary is older than crate sources — refusing to "
            "diffcheck a stale binary\n"
            f"  binary: {cli}\n"
            "  rebuild: cargo build -p plaita-cli (or just: make ns-m4)"
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cases", nargs="*", help="case file paths (default: all in cases/)")
    ap.add_argument("--cli", default=str(DEFAULT_CLI))
    args = ap.parse_args()

    cli = Path(args.cli)
    ensure_cli_current(cli)

    case_files = [Path(p) for p in args.cases] or sorted(CASES.glob("*.json"))
    for case in case_files:
        proc = subprocess.run(
            [str(cli), "ns", str(case)], capture_output=True, text=True, timeout=60
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout).strip()
            print(json.dumps({
                "name": case.stem, "outcome": "engine_error", "detail": detail,
            }))
            continue
        print(proc.stdout.strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
