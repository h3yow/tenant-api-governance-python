"""配额：租户+接口双维度、跨窗口边界、并发计数与失败原因分类。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import ErrorCode, GovernanceError
from app.models import ApiScope, Principal
from app.quota import (
    QuotaDecision,
    InMemoryQuotaStore,
    QuotaPolicy,
    QuotaService,
)

TENANT_A = Principal("tenant-a", "c-a", frozenset())
TENANT_B = Principal("tenant-b", "c-b", frozenset())
SCOPE = ApiScope("orders.write", True)
OTHER_SCOPE = ApiScope("billing.write", True)


def test_quota_exhausted_is_classified(harness):
    engine = harness["engine"]
    for i in range(3):
        engine.handle("token-a-writer", SCOPE, {"i": i}, f"K-{i}", lambda: "ok")
    with pytest.raises(GovernanceError) as exc:
        engine.handle("token-a-writer", SCOPE, {"i": 9}, "K-over", lambda: "ok")
    assert exc.value.code is ErrorCode.QUOTA_EXHAUSTED
    assert "counter=4/3" in exc.value.reason


def test_quotas_are_isolated_per_tenant(harness):
    engine = harness["engine"]
    # 租户 A 用满 3 次
    for i in range(3):
        engine.handle("token-a-writer", SCOPE, {"i": i}, f"KA-{i}", lambda: "a")
    # 租户 B 独立配额，不受影响
    for i in range(3):
        resp = engine.handle("token-b-writer", SCOPE, {"i": i}, f"KB-{i}", lambda: "b")
        assert resp.tenant_id == "tenant-b"
    with pytest.raises(GovernanceError) as exc:
        engine.handle("token-b-writer", SCOPE, {"i": 9}, "KB-over", lambda: "b")
    assert exc.value.code is ErrorCode.QUOTA_EXHAUSTED


def test_quotas_are_isolated_per_api(clock):
    store = InMemoryQuotaStore()
    svc = QuotaService(
        store,
        QuotaPolicy(limits={"orders.write": 1, "billing.write": 5},
                    default_limit=5, window_seconds=60),
        clock=clock,
    )
    svc.check_and_consume(TENANT_A, SCOPE)
    # orders 已满不影响 billing
    r = svc.check_and_consume(TENANT_A, OTHER_SCOPE)
    assert r.decision is QuotaDecision.ALLOWED


def test_window_rollover_resets_counter(harness, clock):
    engine = harness["engine"]
    for i in range(3):
        engine.handle("token-a-writer", SCOPE, {"i": i}, f"K-{i}", lambda: "ok")
    with pytest.raises(GovernanceError):
        engine.handle("token-a-writer", SCOPE, {"i": 9}, "K-over", lambda: "ok")
    # 恰好跨过窗口边界（window_seconds=60）
    clock.advance(60)
    resp = engine.handle("token-a-writer", SCOPE, {"i": 0}, "K-new-window", lambda: "ok")
    assert resp.idempotent_replay is False
    assert "counter=1/3" in resp.reason


def test_boundary_request_starts_new_window(harness, clock):
    # 窗口起点为 60 的整数倍：59s 时刻属于旧窗口，60s 时刻属于新窗口
    clock.now = 59.0
    svc = QuotaService(
        InMemoryQuotaStore(),
        QuotaPolicy(default_limit=1, window_seconds=60),
        clock=clock,
    )
    r1 = svc.check_and_consume(TENANT_A, SCOPE)
    assert r1.window_start == 0
    clock.advance(1)
    r2 = svc.check_and_consume(TENANT_A, SCOPE)
    assert r2.window_start == 60
    assert r2.decision is QuotaDecision.ALLOWED and r2.used == 1


def test_concurrent_consumers_never_overcount_but_are_ordered(harness):
    svc = QuotaService(
        InMemoryQuotaStore(),
        QuotaPolicy(default_limit=50, window_seconds=60),
    )
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: svc.check_and_consume(TENANT_A, SCOPE), range(100)))
    allowed = [r for r in results if r.decision is QuotaDecision.ALLOWED]
    rejected = [r for r in results if r.decision is QuotaDecision.REJECTED]
    assert len(allowed) == 50 and len(rejected) == 50
    # 计数严格 1..100 各出现一次，无错乱
    assert sorted(r.used for r in results) == list(range(1, 101))


def test_quota_exhaustion_releases_idempotency_slot_for_retry(harness, clock):
    engine = harness["engine"]
    for i in range(3):
        engine.handle("token-a-writer", SCOPE, {"i": i}, f"K-{i}", lambda: "ok")
    # 配额耗尽时占用了幂等键；窗口切换后同一键应可重试成功
    with pytest.raises(GovernanceError) as exc:
        engine.handle("token-a-writer", SCOPE, {"i": 9}, "K-retry", lambda: "ok")
    assert exc.value.code is ErrorCode.QUOTA_EXHAUSTED
    clock.advance(60)
    resp = engine.handle("token-a-writer", SCOPE, {"i": 9}, "K-retry", lambda: "ok-retry")
    assert resp.result == "ok-retry"
