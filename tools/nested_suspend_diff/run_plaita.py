#!/usr/bin/env python3
"""NS-M4 差分 runner（plaita 腿）：用 plaita 分布式模式逐例驱动——
FlowExecution(mode=DISTRIBUTED) 逐步执行 + 事件注入恢复（参考
plaita tests/unit/test_nested_suspend.py 的驱动方式），输出统一结果 JSON。

与 rs/go 两腿的差异：plaita 分布式是**逐步**契约——恢复步可能只完成 child
返回（未到根 end），runner 用 resume_type="continue" 继续推进到 is_end 或
再挂起；最终 output 取根 end 的 result，与 rs/go 的「resume 一路到根」对齐。

用法：
    <plaita venv python> run_plaita.py [CASE.json ...]

plaita（或其依赖）不可导入时以 exit 3 退出——diffcheck 据此把该腿记为 SKIP。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CASES = HERE / "cases"
REPO = HERE.parent.parent
PLAITA = REPO.parent / "plaita"
UNAVAILABLE_EXIT = 3
MAX_STEPS = 32  # continue 推进护栏（正常几步内到 end）


def load_plaita():
    """venv 优先；system python 兜底把兄弟仓挂上 sys.path。不可导入 = 腿缺席。"""
    try:
        import plaita  # noqa: F401
        return
    except ImportError:
        pass
    if PLAITA.is_dir():
        sys.path.insert(0, str(PLAITA))
    try:
        import plaita  # noqa: F401
    except Exception as e:  # 依赖缺失（pydantic 等）同样算缺席
        print(
            f"run_plaita: plaita not importable ({e}); "
            f"run with the plaita venv python: {PLAITA}/.venv/bin/python",
            file=sys.stderr,
        )
        sys.exit(UNAVAILABLE_EXIT)


load_plaita()

from plaita.core.errors import ResumeGuardError  # noqa: E402
from plaita.core.executor import FlowExecution  # noqa: E402
from plaita.core.flow import Flow  # noqa: E402
from plaita.core.strategies import RESUME_BINDING_KEY  # noqa: E402


class RecordingBus:
    """带注册/注销台账的简化 event bus 替身（test_nested_suspend.py 同款）。"""

    def __init__(self):
        self.subscriptions = {}
        self.unregistered = []
        self._seq = 0

    async def register_subscription(self, event_type=None, filter_condition=None,
                                    correlation_id=None, flow_id=None, node_id=None, **kw):
        self._seq += 1
        sid = f"sub-{self._seq}"
        self.subscriptions[sid] = {
            "event_type": event_type,
            "correlation_id": correlation_id,
            "flow_id": flow_id,
            "node_id": node_id,
        }
        return sid

    def unregister_subscription(self, subscription_id):
        self.unregistered.append(subscription_id)
        return self.subscriptions.pop(subscription_id, None)


def reject_category(text: str) -> str:
    """拒绝文案 → 具名类别（plaita 方言：迭代帧闸点名 frame boundary +
    帧类型、parallel 静态闸点名 parallel frames、绑定守卫是 Stale/不匹配）。
    """
    if "parallel frames" in text or ("frame boundary" in text and "'parallel'" in text):
        return "parallel_frame"
    if "frame boundary" in text:
        return "loop_frame"
    if ("Stale" in text or "subscription_id" in text or "订阅" in text
            or "不匹配" in text):
        return "resume_guard"
    return "error"


def inject_references(flow, flows: dict):
    """调度器职责：reference 节点按 flowID 从用例注册表注入 child_flow
    （test_nested_suspend.py::_make 同款），递归穿 child_flow。"""
    for node in getattr(flow, "nodes", None) or []:
        if getattr(node, "node_type", "") == "reference":
            fid = getattr(node, "flow_id", None)
            if fid and fid in flows and getattr(node, "child_flow", None) is None:
                node.child_flow = Flow.model_validate(flows[fid])
        child = getattr(node, "child_flow", None)
        if child is not None:
            inject_references(child, flows)


def _state_of(context: dict, node_id: str) -> dict:
    nodes = (context or {}).get("$NODE") or {}
    state = nodes.get(node_id)
    return state if isinstance(state, dict) else {}


def innermost_suspension(r: dict) -> tuple[str, str | None]:
    """(挂起节点 id, 载体订阅 id)：嵌套挂起读帧路径最内帧；根直挂读 r['id']
    （向后兼容路径，本用例集不覆盖但 runner 不炸）。"""
    context = r.get("context") or {}
    frames = context.get("$SUSPEND_FRAMES") or context.get("SUSPEND_FRAMES")
    if isinstance(frames, list) and frames:
        f0 = frames[0]
        node = f0.get("node_id")
        return node, _state_of(f0.get("context") or {}, node).get("subscription_id")
    node = r.get("id")
    return node, _state_of(context, node).get("subscription_id")


def drive(case: dict) -> dict:
    name = case.get("name", "?")
    flow = Flow.model_validate(case["flow"])
    inject_references(flow, case.get("flows") or {})
    bus = RecordingBus()
    ex = FlowExecution(event_bus=bus)
    ex.mode = "distributed"

    resumes = case.get("resume") or []
    if isinstance(resumes, dict):
        resumes = [resumes]
    idx = 0
    suspended_nodes: list[str] = []

    def result(outcome: str, **extra) -> dict:
        base = {
            "name": name,
            "outcome": outcome,
            "suspended_node": suspended_nodes[0] if suspended_nodes else None,
            "suspended_nodes": suspended_nodes,
        }
        base.update(extra)
        return base

    try:
        r = ex.run_distributed(flow, case.get("input") or {})
        steps = 0
        while True:
            steps += 1
            if steps > MAX_STEPS:
                return result("engine_error", detail="step guard tripped")
            if r.get("is_suspend"):
                node, sub = innermost_suspension(r)
                suspended_nodes.append(node)
                if idx >= len(resumes):
                    return result("suspended")
                step = resumes[idx]
                idx += 1
                data = dict(step.get("data") or {})
                data[RESUME_BINDING_KEY] = (
                    "sub-wrong" if step.get("bind") == "wrong" else (sub or ""))
                r = ex.run_distributed(
                    flow, saved_context=r["context"],
                    resume_type=step.get("type", "event"), resume_data=data)
            elif r.get("is_end"):
                return result("completed", output=r.get("result"))
            else:
                # 中间步（child 完成回填父帧、根未到 end）：continue 推进。
                r = ex.run_distributed(
                    flow, saved_context=r["context"], resume_type="continue")
    except ResumeGuardError as e:  # 策略层直抛的守卫（不走 run_distributed 归一时）
        return result("rejected", reject_reason=reject_category(str(e)),
                      detail=str(e))
    except Exception as e:  # 分布式契约把异常归一为 FlowErrorException 链
        category = reject_category(str(e))
        if category == "error":
            # 未具名的引擎侧异常（如恢复会话推进崩溃）不是语义拒绝——
            # 以 engine_error 上报，让矩阵亮 DIVERGENCE 而非误判拒绝类。
            return result("engine_error", detail=str(e))
        return result("rejected", reject_reason=category, detail=str(e))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cases", nargs="*", help="case file paths (default: all in cases/)")
    args = ap.parse_args()
    case_files = [Path(p) for p in args.cases] or sorted(CASES.glob("*.json"))
    for case_path in case_files:
        case = json.loads(case_path.read_text())
        print(json.dumps(drive(case), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
