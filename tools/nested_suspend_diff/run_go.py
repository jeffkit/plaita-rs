#!/usr/bin/env python3
"""NS-M4 差分 runner（go 腿）：构建并逐例驱动 gorunner（单进程
Execute → Suspend → ResumeSuspension）。

用法：
    python3 run_go.py [CASE.json ...]      # 缺省 = cases/ 目录全部用例
    python3 run_go.py --bin PATH           # 显式指定已构建的 gorunner 二进制

gorunner 源码或 go.mod 比 缓存二进制 新时自动重建（缓存放 target/nsm4/，
不入 git）；go 工具链缺失时以 exit 3 退出并在 stderr 说明——diffcheck 据此
把该腿记为 SKIP。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CASES = HERE / "cases"
GORUNNER = HERE / "gorunner"
REPO = HERE.parent.parent
DEFAULT_BIN = REPO / "target" / "nsm4" / "gorunner-bin"
UNAVAILABLE_EXIT = 3
# 与仓内 Makefile go-ref 同款环境（GOPROXY 注记见 AGENTS.md）
GO_ENV = {**os.environ, "GOPROXY": "https://goproxy.cn,direct", "GOSUMDB": "off"}


def unavailable(message: str) -> None:
    print(f"run_go: {message}", file=sys.stderr)
    sys.exit(UNAVAILABLE_EXIT)


def ensure_bin(bin_path: Path) -> None:
    """缺失或落后于源码时重建（go build 自带增量缓存，代价一次编译）。"""
    sources = sorted(GORUNNER.glob("*.go")) + [GORUNNER / "go.mod", GORUNNER / "go.sum"]
    newest_src = max(p.stat().st_mtime for p in sources if p.is_file())
    if bin_path.is_file() and bin_path.stat().st_mtime >= newest_src:
        return
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            ["go", "build", "-o", str(bin_path), "."],
            cwd=GORUNNER, env=GO_ENV, check=True, capture_output=True, text=True,
        )
    except FileNotFoundError:
        unavailable("go toolchain not found on PATH — go leg unavailable")
    except subprocess.CalledProcessError as e:
        unavailable(f"go build failed:\n{e.stderr or e.stdout}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cases", nargs="*", help="case file paths (default: all in cases/)")
    ap.add_argument("--bin", default=str(DEFAULT_BIN))
    args = ap.parse_args()

    bin_path = Path(args.bin)
    ensure_bin(bin_path)

    case_files = [Path(p) for p in args.cases] or sorted(CASES.glob("*.json"))
    for case in case_files:
        proc = subprocess.run(
            [str(bin_path), str(case)], capture_output=True, text=True, timeout=60
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
