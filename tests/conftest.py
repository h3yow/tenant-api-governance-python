"""测试公共配置。

统一打开治理日志，确保单测输出中包含 **幂等键、租户标识与判定依据**
（basis 字段），便于人工核对每个判定是如何做出的。

同时在测试结束后按“业务场景 -> 用例”汇总通过情况：每个业务的覆盖
范围取自测试模块的 docstring，每个用例的意图取自测试函数的 docstring。
"""

from __future__ import annotations

import logging
import sys

import pytest

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("[%(levelname)s] %(name)s %(message)s")
)
_root = logging.getLogger("governance")
_root.handlers.clear()
_root.addHandler(handler)
_root.setLevel(logging.INFO)
_root.propagate = False


# ---- 业务覆盖与用例通过情况汇总 ----

_results: dict[str, list[tuple[str, str]]] = {}


def _first_line(text: str | None, fallback: str) -> str:
    if not text:
        return fallback
    return text.strip().splitlines()[0] if text.strip() else fallback


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    if report.when != "call":
        return report
    module_doc = _first_line(getattr(item.module, "__doc__", None),
                             item.module.__name__)
    case_doc = _first_line(getattr(getattr(item, "obj", None), "__doc__", None),
                           item.name)
    _results.setdefault(module_doc, []).append((case_doc, report.outcome))
    return report


def pytest_terminal_summary(terminalreporter) -> None:
    terminalreporter.write_sep("=", "治理业务覆盖与用例通过情况")
    marks = {"passed": "PASS", "failed": "FAIL", "skipped": "SKIP"}
    for module_doc, cases in _results.items():
        terminalreporter.write_line(f"[业务] {module_doc}")
        for case_doc, outcome in cases:
            terminalreporter.write_line(
                f"    {marks.get(outcome, outcome)}  {case_doc}"
            )
