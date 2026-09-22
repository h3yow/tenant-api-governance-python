"""in-flight 超时：等待方拿到可区分的 409；陈旧记录可被接管，崩溃方迟到的完成被 CAS 丢弃。"""

from __future__ import annotations

import threading
import time

import pytest

from app.errors import ErrorCode, GovernanceError
from app.idempotency import (
    AcquireOutcome,
    IdempotencyPolicy,
    IdempotencyService,
    InMemoryIdempotencyStore,
    RecordStatus,
)
from app.models import ApiScope, Principal
from app.governance import GovernanceEngine
from app.identity import StaticTokenResolver
from app.quota import InMemoryQuotaStore, QuotaPolicy, QuotaService
from tests.conftest import DIRECTORY, ROLE_PERMISSIONS

SCOPE = ApiScope("orders.write", True)


def _engine(timeout: float) -> tuple[GovernanceEngine, IdempotencyService]:
    resolver = StaticTokenResolver(DIRECTORY, ROLE_PERMISSIONS)
    quota_store = InMemoryQuotaStore()
    idem_store = InMemoryIdempotencyStore()
    idem = IdempotencyService(
        idem_store,
        IdempotencyPolicy(in_flight_timeout_seconds=timeout,
                          poll_interval_seconds=0.005),
    )
    engine = GovernanceEngine(
        resolver,
        QuotaService(quota_store, QuotaPolicy(default_limit=10)),
        idem,
    )
    return engine, idem


def test_waiter_gets_inflight_409_when_owner_never_completes():
    engine, idem = _engine(timeout=0.1)
    p = Principal("tenant-a", "c", frozenset())
    first = idem.acquire("K-hang", p, {"x": 1})
    assert first.outcome is AcquireOutcome.FIRST

    box: dict[str, object] = {}

    def waiter():
        try:
            engine.handle(
                "token-a-writer", SCOPE, {"x": 1}, "K-hang",
                lambda: pytest.fail("waiter must not execute handler"),
            )
        except GovernanceError as exc:
            box["error"] = exc

    t = threading.Thread(target=waiter)
    t.start()
    t.join(timeout=3)
    assert not t.is_alive()
    assert isinstance(box.get("error"), GovernanceError)
    assert box["error"].code is ErrorCode.IDEMPOTENCY_INFLIGHT  # type: ignore[index]


def test_stale_inflight_can_be_taken_over_and_late_complete_is_discarded():
    store = InMemoryIdempotencyStore()
    svc = IdempotencyService(
        store,
        IdempotencyPolicy(in_flight_timeout_seconds=0.1, poll_interval_seconds=0.005),
    )
    p = Principal("tenant-a", "c", frozenset())
    r1 = svc.acquire("K-takeover", p, {"x": 1})
    assert r1.outcome is AcquireOutcome.FIRST

    # 第一轮等待：记录刚到超时窗口但未过宽限 -> 返回 IN_FLIGHT，不接管、不执行
    start = time.monotonic()
    r2 = svc.acquire("K-takeover", p, {"x": 1})
    assert time.monotonic() - start >= 0.09
    assert r2.outcome is AcquireOutcome.IN_FLIGHT

    # 客户端重试：记录已超过“超时+宽限”，确认陈旧后接管成为新 owner
    r3 = svc.acquire("K-takeover", p, {"x": 1})
    assert r3.outcome is AcquireOutcome.FIRST
    assert r3.record.owner_token != r1.owner_token

    # “崩溃”的首请求迟到完成：CAS 因 owner 不匹配而失败，记录保持接管者 in_flight
    assert svc.complete("K-takeover", r1.owner_token, {"stale": True}) is False
    current = store.get("K-takeover")
    assert current is not None
    assert current.status is RecordStatus.IN_FLIGHT
    assert current.owner_token == r3.owner_token

    # 接管者正常完成，之后重复提交回放到的是接管者结论
    assert svc.complete("K-takeover", r3.owner_token, {"final": True}) is True
    replay = svc.acquire("K-takeover", p, {"x": 1})
    assert replay.outcome is AcquireOutcome.REPLAY
    assert replay.record.result == {"final": True}
