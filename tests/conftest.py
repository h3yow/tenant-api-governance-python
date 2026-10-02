"""测试公共配置。

1. 统一打开治理日志，确保单测输出中包含 **幂等键、租户标识与判定依据**
   （basis 字段），便于人工核对每个判定是如何做出的。
2. 按“业务域 -> 用例 -> 通过情况”输出测试覆盖：收集阶段打印每个业务域
   的用例数（-s 可见），运行结束在终端汇总每个业务域下各用例的结果。
"""

from __future__ import annotations

import logging
import sys

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("[%(levelname)s] %(name)s %(message)s")
)
_root = logging.getLogger("governance")
_root.handlers.clear()
_root.addHandler(handler)
_root.setLevel(logging.INFO)
_root.propagate = False

_test_log = logging.getLogger("governance.tests")

# 测试模块 -> 业务域标签（用于覆盖情况汇总）
_AREA_BY_MODULE = {
    "test_identity": "身份解析与权限边界",
    "test_quota": "配额计数与时间窗口",
    "test_idempotency": "幂等结算（并发重复提交）",
    "test_middleware": "治理判定链编排",
    "test_api": "HTTP 接口治理",
    "test_api_retry": "HTTP 重试与结论查询",
    "test_retry_settlement": "重试结算与策略热更新",
    "test_retention_query": "结论保留/淘汰与只读查询",
    "test_retry_governance": "重试治理增强（拒绝回放/保留热调/边界并发）",
}

# 业务域 -> [(用例名, 结果)]，按执行顺序记录
_results: dict[str, list[tuple[str, str]]] = {}


def _area_of(nodeid: str) -> str:
    module = nodeid.split("::")[0].rsplit("/", 1)[-1].removesuffix(".py")
    return _AREA_BY_MODULE.get(module, module)


def pytest_collection_finish(session) -> None:
    """收集结束：打印每个业务域覆盖的用例数（-s 可见）。"""
    by_area: dict[str, int] = {}
    for item in session.items:
        area = _area_of(item.nodeid)
        by_area[area] = by_area.get(area, 0) + 1
    for area, count in by_area.items():
        _test_log.info("collected area=%s cases=%d", area, count)


def pytest_runtest_logreport(report) -> None:
    """每个用例结束：记录结果并打印 业务域/用例/通过情况（-s 可见）。"""
    if report.when != "call":
        return
    area = _area_of(report.nodeid)
    case = report.nodeid.split("::")[-1]
    outcome = report.outcome.upper()
    _results.setdefault(area, []).append((case, outcome))
    _test_log.info("area=%s case=%s outcome=%s", area, case, outcome)


def pytest_terminal_summary(terminalreporter) -> None:
    """运行结束：在终端按业务域汇总各用例通过情况（无需 -s 也可见）。"""
    tr = terminalreporter
    tr.write_sep("=", "业务覆盖与用例通过情况")
    mark = {"PASSED": "✓", "FAILED": "✗", "SKIPPED": "○"}
    for area, cases in _results.items():
        passed = sum(1 for _, outcome in cases if outcome == "PASSED")
        tr.write_line(f"[{area}] {passed}/{len(cases)} 通过")
        for case, outcome in cases:
            tr.write_line(f"  {mark.get(outcome, '?')} {case} {outcome}")
