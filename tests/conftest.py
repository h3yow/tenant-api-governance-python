"""共享夹具：内存存储 + 注入时钟的治理引擎，测试判定日志输出到 stderr。"""

from __future__ import annotations

import logging
from typing import Callable

import pytest

from app.governance import GovernanceEngine
from app.identity import StaticTokenResolver
from app.idempotency import (
    IdemFailMode,
    IdempotencyPolicy,
    IdempotencyService,
    InMemoryIdempotencyStore,
)
from app.logging_setup import configure_logging
from app.models import Principal
from app.quota import (
    InMemoryQuotaStore,
    QuotaFailMode,
    QuotaPolicy,
    QuotaService,
)

DIRECTORY = {
    "token-a-writer": Principal("tenant-a", "caller-a-1", frozenset({"order_writer"})),
    "token-b-writer": Principal("tenant-b", "caller-b-1", frozenset({"order_writer"})),
    "token-a-reader": Principal("tenant-a", "caller-a-2", frozenset({"order_reader"})),
}
ROLE_PERMISSIONS = {
    "order_writer": frozenset({"orders.write", "orders.read"}),
    "order_reader": frozenset({"orders.read"}),
}


class FakeClock:
    """可手动推进的时钟，配额窗口与幂等超时共用同一时间源。"""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(autouse=True)
def _decision_logs(caplog):
    """保证每个测试都能看到幂等键、租户标识与判定依据（caplog + stderr 双通道）。"""
    logger = configure_logging()
    previous_propagate = logger.propagate
    logger.propagate = True  # 让 pytest caplog 也能捕获判定日志
    caplog.set_level(logging.INFO, logger="governance")
    yield
    logger.propagate = previous_propagate


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def build_engine(
    clock: Callable[[], float],
    *,
    quota_limit: int = 3,
    window_seconds: int = 60,
    quota_fail_mode: QuotaFailMode = QuotaFailMode.CLOSED,
    idem_fail_mode: IdemFailMode = IdemFailMode.CLOSED,
):
    resolver = StaticTokenResolver(DIRECTORY, ROLE_PERMISSIONS)

    quota_store = InMemoryQuotaStore()
    quota_policy = QuotaPolicy(
        limits={"orders.write": quota_limit},
        default_limit=quota_limit,
        window_seconds=window_seconds,
        fail_mode=quota_fail_mode,
    )
    quota_service = QuotaService(quota_store, quota_policy, clock=clock)

    idem_store = InMemoryIdempotencyStore()
    idem_policy = IdempotencyPolicy(
        in_flight_timeout_seconds=2.0,
        poll_interval_seconds=0.002,
        fail_mode=idem_fail_mode,
    )
    idem_service = IdempotencyService(idem_store, idem_policy, clock=clock)

    engine = GovernanceEngine(resolver, quota_service, idem_service)
    return {
        "engine": engine,
        "quota_store": quota_store,
        "idem_store": idem_store,
        "quota_service": quota_service,
        "idem_service": idem_service,
    }


@pytest.fixture
def harness(clock):
    return build_engine(clock)
