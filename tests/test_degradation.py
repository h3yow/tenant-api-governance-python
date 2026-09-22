"""依赖异常与窗口切换下的确定性行为：降级策略可配置、并发判定一致、不透出底层异常。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import ErrorCode, GovernanceError
from app.idempotency import AcquireOutcome, IdemFailMode, IdempotencyService
from app.models import ApiScope, Principal
from app.quota import (
    QuotaDecision,
    QuotaFailMode,
    QuotaService,
    InMemoryQuotaStore,
    QuotaPolicy,
)
from tests.conftest import build_engine

SCOPE = ApiScope("orders.write", True)
P = Principal("tenant-a", "c-a", frozenset())


# ---------- 配额降级 ----------

def test_quota_fail_closed_rejects_with_masked_cause(clock):
    store = InMemoryQuotaStore()
    svc = QuotaService(
        store, QuotaPolicy(default_limit=5, fail_mode=QuotaFailMode.CLOSED), clock=clock
    )
    store.set_failing(True)
    with pytest.raises(GovernanceError) as exc:
        svc.check_and_consume(P, SCOPE)
    assert exc.value.code is ErrorCode.DEPENDENCY_UNAVAILABLE
    assert "fail-closed" in exc.value.message
    # 底层 DependencyError 不出现在响应中
    assert "injected failure" not in exc.value.reason


def test_quota_fail_open_allows_and_flags_degraded(clock):
    store = InMemoryQuotaStore()
    svc = QuotaService(
        store, QuotaPolicy(default_limit=5, fail_mode=QuotaFailMode.OPEN), clock=clock
    )
    store.set_failing(True)
    r = svc.check_and_consume(P, SCOPE)
    assert r.decision is QuotaDecision.DEGRADED_ALLOW and r.degraded
    assert "fail_mode=open" in r.reason


def test_quota_last_known_same_window_full_then_rollover_allows(clock):
    store = InMemoryQuotaStore()
    svc = QuotaService(
        store, QuotaPolicy(default_limit=2, window_seconds=60,
                           fail_mode=QuotaFailMode.LAST_KNOWN),
        clock=clock,
    )
    svc.check_and_consume(P, SCOPE)
    svc.check_and_consume(P, SCOPE)  # 同窗口用满
    store.set_failing(True)

    r_full = svc.check_and_consume(P, SCOPE)
    assert r_full.decision is QuotaDecision.DEGRADED_REJECT
    assert r_full.reason.startswith("last_known used=2/2")

    # 窗口切换后，last_known 缓存属于旧窗口 -> 新窗口确定放行
    clock.advance(60)
    r_rollover = svc.check_and_consume(P, SCOPE)
    assert r_rollover.decision is QuotaDecision.DEGRADED_ALLOW
    assert r_rollover.window_start == 1020  # 1000 -> 960 旧窗；+60 -> 1020 新窗
    assert "rolled over" in r_rollover.reason


def test_quota_last_known_with_slack_allows_mid_window(clock):
    store = InMemoryQuotaStore()
    svc = QuotaService(
        store, QuotaPolicy(default_limit=5, fail_mode=QuotaFailMode.LAST_KNOWN), clock=clock
    )
    svc.check_and_consume(P, SCOPE)  # used=1/5
    store.set_failing(True)
    r = svc.check_and_consume(P, SCOPE)
    assert r.decision is QuotaDecision.DEGRADED_ALLOW and r.used == 1


def test_degraded_decision_is_consistent_under_concurrency(clock):
    """故障 + last_known 同窗口已满：所有并发请求必须一致拒绝，不出现放行。"""
    store = InMemoryQuotaStore()
    svc = QuotaService(
        store, QuotaPolicy(default_limit=1, fail_mode=QuotaFailMode.LAST_KNOWN), clock=clock
    )
    svc.check_and_consume(P, SCOPE)
    store.set_failing(True)
    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = list(pool.map(lambda _: svc.check_and_consume(P, SCOPE).decision,
                                 range(64)))
    assert set(outcomes) == {QuotaDecision.DEGRADED_REJECT}


# ---------- 幂等降级 ----------

def test_idempotency_fail_closed_rejects(clock):
    from app.idempotency import InMemoryIdempotencyStore, IdempotencyPolicy

    store = InMemoryIdempotencyStore()
    svc = IdempotencyService(
        store, IdempotencyPolicy(fail_mode=IdemFailMode.CLOSED), clock=clock
    )
    store.set_failing(True)
    with pytest.raises(GovernanceError) as exc:
        svc.acquire("K-deg", P, {"x": 1})
    assert exc.value.code is ErrorCode.DEPENDENCY_UNAVAILABLE
    assert "DependencyError" not in exc.value.message  # 底层异常不透出


def test_idempotency_local_dedupe_single_side_effect(clock):
    from app.idempotency import InMemoryIdempotencyStore, IdempotencyPolicy

    store = InMemoryIdempotencyStore()
    svc = IdempotencyService(
        store, IdempotencyPolicy(fail_mode=IdemFailMode.LOCAL_DEDUPE), clock=clock
    )
    store.set_failing(True)
    r1 = svc.acquire("K-local", P, {"x": 1})
    assert r1.outcome is AcquireOutcome.DEGRADED_LOCAL_FIRST
    svc.complete("K-local", r1.owner_token, {"v": 1})
    r2 = svc.acquire("K-local", P, {"x": 1})
    assert r2.outcome is AcquireOutcome.DEGRADED_LOCAL_REPLAY
    assert r2.record.result == {"v": 1}
    r3 = svc.acquire("K-local", P, {"x": 2})
    assert r3.outcome is AcquireOutcome.CONFLICT


def test_engine_http_surface_masks_dependency_errors(clock):
    """通过引擎全链路：依赖故障时调用方只看到 503 分类错误，看不到 DependencyError 文本。"""
    h = build_engine(clock, quota_fail_mode=QuotaFailMode.CLOSED,
                     idem_fail_mode=IdemFailMode.CLOSED)
    h["quota_store"].set_failing(True)
    with pytest.raises(GovernanceError) as exc:
        h["engine"].handle(
            "token-a-writer", SCOPE, {"x": 1}, "K-mask", lambda: "ok"
        )
    assert exc.value.code is ErrorCode.DEPENDENCY_UNAVAILABLE
    assert "injected" not in str(exc.value.to_dict())


def test_engine_quota_open_degradation_marks_response(clock):
    h = build_engine(clock, quota_fail_mode=QuotaFailMode.OPEN,
                     idem_fail_mode=IdemFailMode.CLOSED)
    h["quota_store"].set_failing(True)
    resp = h["engine"].handle(
        "token-a-writer", SCOPE, {"x": 1}, "K-open", lambda: "served"
    )
    assert resp.result == "served" and resp.degraded is True
    assert "fail_mode=open" in resp.reason


def test_idempotency_store_outage_with_local_dedupe_through_engine(clock):
    h = build_engine(clock, quota_fail_mode=QuotaFailMode.OPEN,
                     idem_fail_mode=IdemFailMode.LOCAL_DEDUPE)
    h["idem_store"].set_failing(True)
    import threading
    import time

    calls = {"n": 0}
    lock = threading.Lock()

    def slow():
        with lock:
            calls["n"] += 1
        time.sleep(0.05)
        return {"order_id": "ord-deg"}

    def submit(_):
        return h["engine"].handle(
            "token-a-writer", SCOPE, {"x": 1}, "K-deg-conc", slow
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        resps = list(pool.map(submit, range(8)))
    assert calls["n"] == 1
    first = [r for r in resps if not r.idempotent_replay]
    replays = [r for r in resps if r.idempotent_replay]
    assert len(first) == 1 and len(replays) == 7
    assert all(r.degraded for r in resps)
