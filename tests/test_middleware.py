"""治理编排链测试：拒绝原因可区分、依赖故障不透出、并发端到端幂等。"""

from __future__ import annotations

import threading

import pytest

from governance.config import DegradeMode, GovernanceConfig, QuotaRule
from governance.errors import DenyReason, GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import IdempotencyService, InMemoryIdempotencyStore
from governance.middleware import GovernanceMiddleware
from governance.quota import FailingQuotaStore, InMemoryQuotaStore, QuotaEnforcer


def _identity() -> IdentityResolver:
    return IdentityResolver(
        {
            "token-t1": ("t1", "alice", frozenset({"/orders"})),
            "token-t2": ("t2", "bob", frozenset({"/read"})),
        }
    )


def _chain(degrade: DegradeMode = DegradeMode.FAIL_CLOSED,
           quota_store=None) -> tuple[GovernanceMiddleware, list[int]]:
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=1, window_seconds=60),),
        degrade_mode=degrade,
    )
    clock = lambda: 1000.0
    quota = QuotaEnforcer(
        cfg, quota_store or InMemoryQuotaStore(clock), clock=clock
    )
    side_effects: list[int] = []
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(InMemoryIdempotencyStore())
    )
    return chain, side_effects


def test_missing_identity_reason() -> None:
    chain, _ = _chain()
    with pytest.raises(GovernanceError) as exc:
        chain.handle(None, "/orders", "k", {}, lambda: None)
    assert exc.value.reason is DenyReason.MISSING_IDENTITY


def test_access_denied_reason() -> None:
    chain, _ = _chain()
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t2", "/orders", "k", {}, lambda: None)
    assert exc.value.reason is DenyReason.ACCESS_DENIED


def test_quota_exhausted_reason_and_no_side_effect() -> None:
    chain, effects = _chain()
    chain.handle("token-t1", "/orders", "k1", {}, lambda: effects.append(1))
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", "k2", {}, lambda: effects.append(1))
    assert exc.value.reason is DenyReason.QUOTA_EXHAUSTED
    assert effects == [1], "被配额拒绝的请求不得产生副作用"


def test_idempotency_conflict_reason() -> None:
    chain, effects = _chain()
    # t1 只有 1 个配额；第二次提交改用 fail-open 风格的不受限场景：
    # 这里直接同键不同载荷在配额消耗前由幂等层拦截，故先放额度内冲突。
    chain.handle("token-t1", "/orders", "dup", {"v": 1},
                 lambda: effects.append(1))
    # 配额已耗尽（1/1），冲突无法到达幂等层——为独立验证冲突分类，
    # 重新构造一条无配额限制的链。
    cfg = GovernanceConfig(quota_rules=())
    quota = QuotaEnforcer(cfg, InMemoryQuotaStore(lambda: 1000.0),
                          clock=lambda: 1000.0)
    chain2 = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(InMemoryIdempotencyStore())
    )
    fx: list[int] = []
    chain2.handle("token-t1", "/orders", "dup", {"v": 1}, lambda: fx.append(1))
    with pytest.raises(GovernanceError) as exc:
        chain2.handle("token-t1", "/orders", "dup", {"v": 2},
                      lambda: fx.append(1))
    assert exc.value.reason is DenyReason.IDEMPOTENCY_CONFLICT
    assert fx == [1]


def test_quota_backend_failure_fail_closed_is_dependency_reason() -> None:
    chain, effects = _chain(degrade=DegradeMode.FAIL_CLOSED,
                            quota_store=FailingQuotaStore())
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", "k", {}, lambda: effects.append(1))
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE
    assert effects == []
    assert "refused" not in exc.value.message


def test_quota_backend_failure_fail_open_still_idempotent() -> None:
    """fail-open 只放开配额闸门；幂等保证仍然成立，副作用仍只一次。"""
    chain, effects = _chain(degrade=DegradeMode.FAIL_OPEN,
                            quota_store=FailingQuotaStore())
    release = threading.Event()
    started = threading.Event()

    def action() -> str:
        effects.append(1)
        started.set()
        release.wait(timeout=2.0)
        return "DONE"

    results: list[object] = []

    def worker() -> None:
        results.append(
            chain.handle("token-t1", "/orders", "open-key", {}, action)
        )

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    assert started.wait(timeout=2.0)
    release.set()
    for t in threads:
        t.join()
    assert effects == [1]
    assert results == ["DONE"] * 6


def test_concurrent_requests_across_chain_one_side_effect() -> None:
    """端到端并发同键：身份/权限通过后，配额+幂等共同保证一次副作用。"""
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),)
    )
    quota = QuotaEnforcer(cfg, InMemoryQuotaStore(lambda: 1000.0),
                          clock=lambda: 1000.0)
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(InMemoryIdempotencyStore())
    )
    effects: list[int] = []
    release = threading.Event()
    started = threading.Event()

    def action() -> dict:
        effects.append(1)
        started.set()
        release.wait(timeout=2.0)
        return {"order_id": "X"}

    results: list[object] = []

    def worker() -> None:
        results.append(
            chain.handle("token-t1", "/orders", "e2e-key", {"v": 1}, action)
        )

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    assert started.wait(timeout=2.0)
    release.set()
    for t in threads:
        t.join()
    assert len(effects) == 1
    assert all(r == {"order_id": "X"} for r in results)
