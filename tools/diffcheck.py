#!/usr/bin/env python3
"""Differential harness: run each case through BOTH engines and compare.

- reference side: tools/go-ref (imports plaita-go's pkg/ir directly)
- candidate side: the plaita-rs CLI

A case passes when both engines agree: same result (JSON-equal — integers
compared exactly even beyond 2^53, int 6 == float 6.0, floats within a
1e-12 relative tolerance) or both failing. `--selftest` pins the predicate.
Error agreement compares only the failure CLASS — both engines refusing the
definition IS agreement; message texts are engine dialects (go may fail in
typed unmarshal where rust fails in a type gate) and are reported, never
compared (A1 r132 card R-3).

Usage: python3 tools/diffcheck.py [--go-ref PATH] [--cli PATH]

The candidate CLI must exist and be no older than the crate sources
(`make diffcheck` builds it first); a stale or missing binary is refused
fail-closed instead of being silently used as the comparison side.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GO_IR_TESTDATA = ROOT.parent / "plaita-go" / "pkg" / "ir" / "testdata"
GO_EXAMPLES = ROOT.parent / "plaita-go" / "examples"
CASES = ROOT / "tools" / "cases"

# (name, flow_path, input) — input None means {}
MANIFEST: list[tuple[str, Path, dict | None]] = [
    ("hello_world", GO_IR_TESTDATA / "hello_world.json", None),
    ("assignment/condition-hit", GO_IR_TESTDATA / "assignment_flow.json", {"bb": "123"}),
    ("assignment/default", GO_IR_TESTDATA / "assignment_flow.json", {"bb": "zzz"}),
    ("loop/for-in", GO_IR_TESTDATA / "loop_flow.json", {"items": [1, 2, 3]}),
    ("parallel/join", GO_IR_TESTDATA / "parallel_flow.json", {"x": 5}),
    ("child/inline", GO_IR_TESTDATA / "child_flow.json", {"name": "plaita"}),
    ("example/add", GO_EXAMPLES / "add.json", {"a": 2, "b": 3}),
    ("example/hello", GO_EXAMPLES / "hello_world.json", None),
    ("if/true", CASES / "if_flow.json", {"v": 11}),
    ("if/false", CASES / "if_flow.json", {"v": 5}),
    ("switch/hit", CASES / "switch_nodefault.json", {"v": "1"}),
    ("switch/other", CASES / "switch_nodefault.json", {"v": "42"}),
    ("loop/condition-early-exit", CASES / "loop_condition.json", None),
    ("expr/mash", CASES / "expr_mash.json", {"first": "Ada", "last": "L", "n": 4}),
    ("error/continue-policy", CASES / "error_continue.json", {"v": "42"}),
    # 契约用例（2026-10-21）：运行期字符串是数据——两侧都必须原样返回
    ("runtime/string-is-data", CASES / "runtime_data.json", None),
    # 装载面扩例（A1 r132 卡 R-3，2026-10-23；探针源 /tmp/a1r130/flows，
    # plaita-go 侧同源钉入=卡 G-3）：go/rs 对 Python 宽松形状的一致前置。
    # 等值门只比失败类——branches-str 两引擎文案全不同（go typed-unmarshal /
    # rs 类型闸）但同属装载拒，即一致。
    ("load/neg-infinity", CASES / "load_neg_infinity.json", None),
    ("load/depth-10000", CASES / "load_depth_10000.json", None),
    ("load/depth-100", CASES / "load_depth_100.json", None),
    ("load/branches-str", CASES / "load_branches_str.json", None),
    ("load/next-int", CASES / "load_next_int.json", None),
    # 孤立代理转义（卡 R-3 known-red 转正，2026-10-23）：G-1 修口已合
    # plaita-go main（RejectLoneSurrogateEscapes 双执法点）、go-ref 重建后
    # 形状由 ok/error 翻为双侧同拒——按门的翻绿指引升入本表。
    ("load/lone-surrogate-high", CASES / "load_lone_surrogate_high.json", None),
    ("load/lone-surrogate-low", CASES / "load_lone_surrogate_low.json", None),
]

# Known-red 负控：引擎当下的 recorded 分歧，每条钉死「期望分歧形状」；
# 形状保持期间门绿，漂移即红并给归因/翻绿指引（双侧同拒→升 MANIFEST，
# 反向漂移→先归因再动本表）。当前为空——lone-surrogate 两案已随 G-1
# 修口合并（2026-10-23）翻绿转正升入 MANIFEST。
#
# 深度 150（go 过/rs 128 拒）按卡**永不进门**：永久刻意分歧，只入
# AGENTS.md + cargo test 钉 128（卡 R-4），不进等值门也不进本表。
KNOWN_RED: list[tuple[str, Path, dict | None, str, str]] = []


def run_go(go_ref: Path, flow: Path, inp: dict | None) -> tuple[str | None, str]:
    cmd = [str(go_ref), "-flow", str(flow)]
    input_file = None
    try:
        if inp is not None:
            import tempfile

            tf = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
            json.dump(inp, tf)
            tf.close()
            input_file = tf.name
            cmd += ["-input", input_file]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if proc.returncode == 0:
            return proc.stdout.strip(), ""
        return None, (proc.stderr or proc.stdout).strip()
    finally:
        if input_file:
            Path(input_file).unlink(missing_ok=True)


def run_rs(cli: Path, flow: Path, inp: dict | None) -> tuple[str | None, str]:
    cmd = [str(cli), "run", str(flow)]
    if inp is not None:
        cmd += ["--input", json.dumps(inp)]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode == 0:
        return proc.stdout.strip(), ""
    return None, (proc.stderr or proc.stdout).strip().replace("error: ", "", 1)


def numbers_equal(a, b) -> bool:
    """Structural equality with domain-appropriate numeric leaves (issue #12).

    int/int and mixed int/float compare exactly via Python `==` — never
    coerce to f64 first, or every pair of integers beyond 2^53 collapses
    onto the same float and real drift reads as agreement. Only two floats
    get tolerance: math.isclose(rel_tol=1e-12, abs_tol=0.0). NaN matches
    NaN and inf matches only itself — the old `abs(a-b) < 1e-9` flipped
    inf==inf into a false mismatch (inf-inf=nan).
    """
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


SELFTEST_CASES: list[tuple[object, object, bool, str]] = [
    (2**63 - 1, 2**63 - 2, False, "adjacent i64 max must differ"),
    (9007199254740993, 9007199254740992, False, "2^53+1 vs 2^53 must differ"),
    (10**19, 10**19 + 1, False, "huge adjacent ints must differ"),
    (6, 6.0, True, "int 6 == float 6.0"),
    (9007199254740993, 9007199254740992.0, False, "mixed int/float stays exact"),
    (0.1 + 0.2, 0.3, True, "float tolerance (rel 1e-12)"),
    (1.0, 1.0 + 1e-15, True, "float tolerance at 1.0 scale"),
    (1.0, 1.0 + 1e-9, False, "1e-9 apart at 1.0 scale must differ"),
    (float("inf"), float("inf"), True, "inf equals itself"),
    (float("-inf"), float("-inf"), True, "-inf equals itself"),
    (float("inf"), float("-inf"), False, "+inf != -inf"),
    (float("nan"), float("nan"), True, "NaN == NaN (engines agree)"),
    (float("nan"), 1.0, False, "NaN != number"),
    (True, 1, False, "bool never equals int"),
    ({"a": 6}, {"a": 6.0}, True, "recursion keeps numeric leaves"),
    ({"a": 2**53 + 1}, {"a": 2**53 + 2}, False, "recursion keeps int exactness"),
    ([1, [2**53 + 1]], [1, [2**53 + 2]], False, "nested lists keep int exactness"),
]


def selftest() -> int:
    bad = 0
    for a, b, want, label in SELFTEST_CASES:
        got = numbers_equal(a, b)
        mark = "ok" if got == want else "SELFTEST-FAIL"
        if got != want:
            bad += 1
        print(f"{mark:>13} numbers_equal({a!r}, {b!r}) = {got}  # {label}")
    print()
    if bad:
        print(f"FAILED: {bad}/{len(SELFTEST_CASES)} selftest assertions")
        return 1
    print(f"OK: all {len(SELFTEST_CASES)} equality assertions hold")
    return 0


def ensure_cli_current(cli: Path) -> None:
    """Refuse to run against a missing or stale CLI binary (issue #11).

    The reference side is rebuilt from the plaita-go worktree on every run;
    if the candidate side stayed stale, the gate would report agreement for
    code that never participated. Fail closed with the build command — no
    fallback to the old binary. Only `crates/*/src/**` counts: changes to
    tests/benches don't invalidate the bin, and cargo build would not
    refresh it, leaving the gate stuck.
    """
    if not cli.is_file():
        sys.exit(
            f"error: CLI under test not found: {cli}\n"
            "build it first: cargo build -p plaita-cli (or just: make diffcheck)"
        )
    newest_src = max(
        (p.stat().st_mtime for p in (ROOT / "crates").glob("*/src/**/*.rs")),
        default=0.0,
    )
    if cli.stat().st_mtime < newest_src:
        sys.exit(
            "error: CLI binary is older than crate sources — refusing to "
            "diffcheck a stale binary\n"
            f"  binary: {cli}\n"
            "  rebuild: cargo build -p plaita-cli (or just: make diffcheck)"
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--go-ref", default=str(ROOT / "tools" / "go-ref-bin"))
    ap.add_argument("--cli", default=str(ROOT / "target" / "debug" / "plaita-rs"))
    args = ap.parse_args()
    go_ref, cli = Path(args.go_ref), Path(args.cli)
    ensure_cli_current(cli)

    failures = 0
    print(f"{'case':34} {'go':>10} {'rust':>10} verdict")
    for name, flow, inp in MANIFEST:
        go_out, go_err = run_go(go_ref, flow, inp)
        rs_out, rs_err = run_rs(cli, flow, inp)
        if go_out is None and rs_out is None:
            # 等值门只比失败类（卡 R-3）：双侧都拒该定义即一致——go 可能
            # 败在 typed unmarshal、rs 败在类型闸，文案是引擎方言，只展示
            # 不计分。子串命中时仍标注「same core message」留观察面。
            go_core = go_err.split("execute: ", 1)[-1]
            go_core = go_core.split("load: ", 1)[-1]
            if go_core in rs_err:
                verdict = "both-error (same core message)"
            else:
                verdict = "both-error (dialect texts differ)"
            detail = f"go: {go_err[:80]!r} | rust: {rs_err[:80]!r}"
        elif go_out is None or rs_out is None:
            failures += 1
            verdict = "MISMATCH (one side failed)"
            detail = f"go: {go_out or go_err[:80]!r} | rust: {rs_out or rs_err[:80]!r}"
        else:
            go_json, rs_json = json.loads(go_out), json.loads(rs_out)
            if numbers_equal(go_json, rs_json):
                verdict = "OK"
                detail = f"{json.dumps(go_json)[:72]}"
            else:
                failures += 1
                verdict = "MISMATCH"
                detail = f"go={go_out[:60]!r} rust={rs_out[:60]!r}"
        print(f"{name:34} {'ok' if go_out is not None else 'ERR':>10} "
              f"{'ok' if rs_out is not None else 'ERR':>10} {verdict}")
        if verdict != "OK":
            print(f"{'':34} → {detail}")

    for name, flow, inp, go_want, rs_want in KNOWN_RED:
        go_out, go_err = run_go(go_ref, flow, inp)
        rs_out, rs_err = run_rs(cli, flow, inp)
        go_got = "ok" if go_out is not None else "error"
        rs_got = "ok" if rs_out is not None else "error"
        shape = f"{go_got}/{rs_got}"
        if (go_got, rs_got) == (go_want, rs_want):
            print(f"{name:34} {'ok' if go_out is not None else 'ERR':>10} "
                  f"{'ok' if rs_out is not None else 'ERR':>10} "
                  f"known-red holds ({shape})")
            continue
        failures += 1
        print(f"{name:34} {'ok' if go_out is not None else 'ERR':>10} "
              f"{'ok' if rs_out is not None else 'ERR':>10} "
              f"KNOWN-RED SHAPE CHANGED ({shape}, was {go_want}/{rs_want})")
        if go_got == "error" and rs_got == "error":
            print(f"{'':34} → 双侧同拒已成一致——"
                  f"把本案升入 MANIFEST 并从 KNOWN_RED 删除(翻绿进门)")
        else:
            print(f"{'':34} → 形状漂移非 G-1 方向:先归因(rs 开始收=回归,查拒载面;"
                  f"go 开始拒=另有修口,核对 plaita-go 日志)再动本表")

    print()
    if failures:
        print(f"FAILED: {failures} mismatch(es) out of "
              f"{len(MANIFEST)} manifest + {len(KNOWN_RED)} known-red cases")
        return 1
    print(f"OK: {len(MANIFEST)}/{len(MANIFEST)} manifest cases agree; "
          f"{len(KNOWN_RED)}/{len(KNOWN_RED)} known-red negatives hold"
          + (" (recorded divergence still holding)" if KNOWN_RED else ""))
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
