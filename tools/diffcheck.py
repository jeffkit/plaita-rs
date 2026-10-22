#!/usr/bin/env python3
"""Differential harness: run each case through BOTH engines and compare.

- reference side: tools/go-ref (imports plaita-go's pkg/ir directly)
- candidate side: the plaita-rs CLI

A case passes when both engines agree: same result (JSON-equal, with
numbers compared numerically so int 6 == float 6.0) or both failing.
Error agreement compares only the failure CLASS prefix (both error) — the
message texts are engine dialects and are reported on mismatch.

Usage: python3 tools/diffcheck.py [--go-ref PATH] [--cli PATH]

The candidate CLI must exist and be no older than the crate sources
(`make diffcheck` builds it first); a stale or missing binary is refused
fail-closed instead of being silently used as the comparison side.
"""

from __future__ import annotations

import argparse
import json
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
]


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
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < 1e-9
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(numbers_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(numbers_equal(x, y) for x, y in zip(a, b))
    return a == b


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
            # the error CLASS must agree: the Rust message must contain the
            # Go core message (Go wraps with "execute: ", Rust with its own
            # node prefix — the plaita message text itself is shared)
            go_core = go_err.split("execute: ", 1)[-1]
            go_core = go_core.split("load: ", 1)[-1]
            if go_core in rs_err:
                verdict = "both-error (same core message)"
            else:
                failures += 1
                verdict = "MISMATCH (error text diverged)"
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

    print()
    if failures:
        print(f"FAILED: {failures} mismatch(es) out of {len(MANIFEST)} cases")
        return 1
    print(f"OK: {len(MANIFEST)}/{len(MANIFEST)} cases agree between plaita-go and plaita-rs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
