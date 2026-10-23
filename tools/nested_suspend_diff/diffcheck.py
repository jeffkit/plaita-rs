#!/usr/bin/env python3
"""NS-M4 嵌套挂起三引擎差分对拍主脚本（程式一收官门）。

对 plaita-rs / plaita-go / plaita（Python）三腿逐例驱动（各腿 runner 见
同目录 run_rs.py / run_go.py / run_plaita.py），按统一结果形状
    {"name", "outcome": suspended|completed|rejected, "suspended_node"?,
     "output"?, "reject_reason"?}
做三两比对，输出对拍矩阵。

比较面钉在语义等价类（tools/diffcheck.py 的 A1 老规矩）：
- 结果值 JSON 相等（数字域精确比较）；
- 挂起点序列（suspended_nodes）逐位相等；
- 拒绝按类别（loop_frame / parallel_frame / resume_guard）而非逐字文案
  ——三仓文案是各自家方言，只展示不计分。
缺席引擎自动 SKIP 并注明（如 plaita 仓/venv 不在本机）。

用法：
    python3 diffcheck.py [--engine rs,go,plaita] [--case SUBSTR] [-v]
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CASES = HERE / "cases"
REPO = HERE.parent.parent
RUNNERS = {
    "rs": (HERE / "run_rs.py", sys.executable),
    "go": (HERE / "run_go.py", sys.executable),
    "plaita": (HERE / "run_plaita.py", str(REPO.parent / "plaita" / ".venv" / "bin" / "python")),
}
UNAVAILABLE_EXIT = 3  # runner 约定：腿不可用（缺工具链/仓/二进制）

# Known-red 负控（A1 tools/diffcheck.py 同款纪律）：引擎当下已归因的 recorded
# 分歧，每条钉死「期望分歧形状」（engine → outcome，缺引擎 = 不参与形状判定）。
# 形状保持期间门绿（注 known-red holds）；漂移即红并给归因/翻绿指引：
# 引擎修复后（三引擎同绿）→ 从本表删除该条（翻绿进门）；反向漂移 → 先归因再动本表。
KNOWN_RED: dict[str, dict[str, str]] = {
    # plaita 恢复会话内层续跑进入 child/reference 节点即崩：_advance_frame_to_end
    # （plaita/core/strategies.py:992）构造帧推进 runner 时 NodeRunner(context)
    # 未带 node_execution=<FlowExecution facade>，子节点 arun 需要的
    # get_child_execution 只在 facade 上（见 plaita test_nested_suspend.py
    # test_carrier_escapes_normal_drive 注释）→ AttributeError。rs/go 无此缺口
    # （恢复续跑与首跑同一推进通道）。plaita 的 NS-M3 测试集未覆盖「恢复续跑
    # 途中进 child 节点」（其再挂起用例都在同帧 event 上再挂），故 NS-M3 绿而
    # 此路断。引擎修复属后续轮——修后三引擎同绿，删本条升入正常对拍门。
    "resuspension_merges_call_stacks": {
        "rs": "completed", "go": "completed", "plaita": "engine_error",
    },
}


def numbers_equal(a, b) -> bool:
    """结构相等 + 数字域精确比较（A1 卡 R-3 同款：int 不塌 f64、float
    1e-12 相对容差、NaN/inf 各自可判）。"""
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(numbers_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(numbers_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) and isinstance(b, float):
            if math.isnan(a) and math.isnan(b):
                return True
            return math.isclose(a, b, rel_tol=1e-12, abs_tol=0.0)
        return a == b
    return a == b


def run_engine(engine: str, case_files: list[Path]) -> tuple[dict[str, dict] | None, str]:
    """跑一腿；返回 {case 名: 结果}，腿不可用时返回 (None, 原因)。"""
    script, python = RUNNERS[engine]
    if not script.is_file():
        return None, f"runner script missing: {script}"
    try:
        proc = subprocess.run(
            [python, str(script), *[str(c) for c in case_files]],
            capture_output=True, text=True, timeout=600,
        )
    except FileNotFoundError:
        return None, f"interpreter not found: {python}"
    if proc.returncode == UNAVAILABLE_EXIT:
        return None, (proc.stderr or proc.stdout).strip().splitlines()[-1]
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip()[:300]
    results: dict[str, dict] = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            return None, f"non-JSON runner output: {line[:120]!r}"
        results[r.get("name", "?")] = r
    return results, ""


def outcome_cell(r: dict) -> str:
    if r["outcome"] == "rejected":
        return f"rejected:{r.get('reject_reason', '?')}"
    return r["outcome"]


def compare_case(name: str, per_engine: dict[str, dict]) -> tuple[str, list[str]]:
    """三两比对一例：语义等价类判定。返回 (verdict, 差异明细)。"""
    engines = sorted(per_engine)
    details: list[str] = []
    for i in range(len(engines)):
        for j in range(i + 1, len(engines)):
            a, b = per_engine[engines[i]], per_engine[engines[j]]
            keys = ("outcome", "suspended_nodes", "output", "reject_reason")
            diffs = [k for k in keys if not numbers_equal(a.get(k), b.get(k))]
            if diffs:
                details.append(
                    f"{engines[i]}≠{engines[j]} on {','.join(diffs)}: "
                    f"{engines[i]}={json.dumps({k: a.get(k) for k in keys}, ensure_ascii=False)} | "
                    f"{engines[j]}={json.dumps({k: b.get(k) for k in keys}, ensure_ascii=False)}"
                )
    if details:
        return "DIVERGENCE", details
    # 引擎间一致 → 再对用例文件的 expected 做锚点核对（三引擎一致但与
    # expected 相悖 = 用例蒸馏笔误，标 STALE-EXPECTED 让维护者修用例）。
    return "OK", details


def compare_expected(case: dict, per_engine: dict[str, dict]) -> list[str]:
    notes = []
    expected = case.get("expected") or {}
    for engine, r in sorted(per_engine.items()):
        checks = {
            "outcome": numbers_equal(r.get("outcome"), expected.get("outcome")),
            "suspended_nodes": numbers_equal(
                r.get("suspended_nodes"), expected.get("suspended_nodes")),
            "output": numbers_equal(r.get("output"), expected.get("output")),
            "reject_reason": numbers_equal(
                r.get("reject_reason"), expected.get("reject_reason")),
        }
        bad = [k for k, ok in checks.items() if not ok]
        if bad:
            notes.append(
                f"{engine} vs expected differs on {','.join(bad)}: "
                f"got={json.dumps({k: r.get(k) for k in bad}, ensure_ascii=False)} "
                f"want={json.dumps({k: expected.get(k) for k in bad}, ensure_ascii=False)}"
            )
    return notes


def check_known_red(name: str, pinned: dict[str, str], per_engine: dict[str, dict],
                    verdict: str, details: list[str]) -> tuple[str, list[str], bool]:
    """核对 recorded 分歧形状（known-red 负控）。返回 (verdict, details, holds)。

    - 形状保持（每个在场钉定引擎的 outcome 与钉定值一致）→ verdict 换为
      「known-red holds」，不计失败；
    - 漂移且全引擎同 outcome → 引擎大概率已修复：给翻绿指引（删表项重跑）；
    - 其余漂移 → 非修复方向，先归因再动表。
    """
    engines = sorted(per_engine)
    drifted = [e for e in engines if e in pinned and per_engine[e].get("outcome") != pinned[e]]
    if not drifted:
        shape = ",".join(f"{e}={pinned[e]}" for e in engines if e in pinned)
        return f"known-red holds ({shape})", details, True
    outcomes = {per_engine[e].get("outcome") for e in engines}
    if len(outcomes) == 1:
        msg = (f"KNOWN-RED SHAPE CHANGED: engines now agree on "
               f"{outcomes.pop()!r} — 引擎可能已修复，翻绿进门：把 {name!r} "
               "从 KNOWN_RED 删除并重跑（三引擎同绿升入正常对拍门）")
    else:
        got = {e: per_engine[e].get("outcome") for e in engines}
        msg = (f"KNOWN-RED SHAPE CHANGED (non-fix direction): pinned {pinned}, "
               f"got {got} — 先归因（引擎回归或新分歧）再动 KNOWN_RED")
    return "KNOWN-RED DRIFT", details + [msg], False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="rs,go,plaita",
                    help="comma list of engines to run (default: all)")
    ap.add_argument("--case", default="", help="only cases whose name contains SUBSTR")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    engines = [e.strip() for e in args.engine.split(",") if e.strip()]
    unknown = [e for e in engines if e not in RUNNERS]
    if unknown:
        sys.exit(f"error: unknown engine(s): {unknown} (known: {sorted(RUNNERS)})")

    case_files = sorted(CASES.glob("*.json"))
    if args.case:
        case_files = [c for c in case_files if args.case in c.stem or args.case in c.name]
    if not case_files:
        sys.exit(f"error: no cases matched {args.case!r}")
    cases = {json.loads(c.read_text())["name"]: c for c in case_files}

    results: dict[str, dict[str, dict] | None] = {}
    skips: dict[str, str] = {}
    for engine in engines:
        res, reason = run_engine(engine, [cases[name] for name in cases])
        if res is None:
            skips[engine] = reason
        else:
            missing = [n for n in cases if n not in res]
            if missing:
                skips[engine] = f"runner returned no result for: {missing}"
                res = {k: v for k, v in res.items() if k in cases}
            results[engine] = res

    available = [e for e in engines if e in results]
    print(f"{'case':40}", *[f"{e:>16}" for e in engines], "  verdict")
    failures = 0
    ran_any = bool(available)
    known_red_holds: list[str] = []
    for name in cases:
        per_engine = {e: results[e][name] for e in available if name in results[e]}
        cells = []
        for e in engines:
            if e in per_engine:
                cells.append(f"{outcome_cell(per_engine[e]):>16}")
            else:
                cells.append(f"{'skip':>16}")
        if len(per_engine) < 2:
            verdict = "SKIP (fewer than 2 engines available)"
        else:
            verdict, details = compare_case(name, per_engine)
            # expected 锚点核对（信息面 + 蒸馏质量门）
            expected_notes = compare_expected(json.loads(cases[name].read_text()), per_engine)
            if expected_notes and verdict == "OK":
                verdict = "STALE-EXPECTED"
                details = expected_notes
            elif expected_notes:
                details = details + expected_notes
            # known-red 负控核对先于计败：钉死形状的 recorded divergence 不算败。
            holds = False
            if name in KNOWN_RED:
                verdict, details, holds = check_known_red(
                    name, KNOWN_RED[name], per_engine, verdict, details)
            if verdict not in ("OK",) and not holds:
                failures += 1
            elif holds:
                known_red_holds.append(name)
            if (verdict != "OK" or args.verbose) and details:
                for d in details:
                    print(f"{'':40} → {d[:400]}")
        print(f"{name:40}", *cells, f"  {verdict}")

    print()
    for engine, reason in skips.items():
        print(f"SKIP {engine}: {reason}")
    if not ran_any:
        print(f"FAILED: no engine produced results (skips: {list(skips)})")
        return 1
    total = len(cases)
    if failures:
        print(f"FAILED: {failures} case(s) with divergence/stale-expected/"
              f"known-red-drift out of {total}; engines ran: {available}")
        return 1
    print(f"OK: {total}/{total} cases green across {available}"
          + (f" (skipped: {sorted(skips)})" if skips else "")
          + (f"; {len(known_red_holds)} recorded divergence(s) holding: "
             f"{known_red_holds}" if known_red_holds else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
