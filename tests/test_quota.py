"""配额模型测试：双维度约束、跨窗口边界、并发不超卖、降级策略。"""

from __future__ import annotations

import threading

import pytest

from governance.config import DegradeMode, GovernanceConfig, QuotaRule
from governance.errors import DenyReason, GovernanceError
from governance.quota import (
    FailingQuotaStore,
    InMemoryQuotaStore,
    QuotaEnforcer,
)


def _config() -> GovernanceConfig:
    return GovernanceConfig(
        quota_rules=(
            QuotaRule("t1", "/orders", limit=2, window_seconds=60),
            QuotaRule("t2", "/orders", limit=1, window_seconds=60),
        ),
        degrade_mode=DegradeMode.FAIL_CLOSED,
    )


def test_quota_exhausted_is_distinct_reason() -> None:
    env = QuotaEnforcer(_config(), InMemoryQuotaStore(lambda: 1000.0), clock=lambda: 1000.0)
    env.check_and_consume("t1", "/orders")
    env.check_and_consume("t1", "/orders")
    with pytest.raises(GovernanceError) as exc:
        env.check_and_consume("t1", "/orders")
    assert exc.value.reason is DenyReason.QUOTA_EXHAUSTED


def test_quota_is_per_tenant_and_per_api_independent() -> None:
    """租户维度与接口维度互不串账。"""
    clock = iter([1000.0])
    env = QuotaEnforcer(_config(), InMemoryQuotaStore(lambda: next(clock)))

    def tick() -> float:
        # 所有调用固定在同一窗口
        return 1000.0

    env2 = QuotaEnforcer(_config(), InMemoryQuotaStore(tick))
    # t1 耗尽不影响 t2（t2 自己只有 1 个额度）
    env2.check_and_consume("t1", "/orders")
    env2.check_and_consume("t1", "/orders")
    env2.check_and_consume("t2", "/orders")
    with pytest.raises(GovernanceError) as exc:
        env2.check_and_consume("t2", "/orders")
    assert exc.value.reason is DenyReason.QUOTA_EXHAUSTED
    # 未配置配额的接口不受限
    env2.check_and_consume("t1", "/anything-else")


def test_quota_resets_at_window_boundary() -> None:
    """跨过窗口切换点后，新窗口从 0 开始，旧窗口的耗尽不带入。"""
    ticks = iter([59.0, 118.0, 119.0, 120.0])
    env = QuotaEnforcer(
        _config(), InMemoryQuotaStore(lambda: next(ticks))
    )
    env.check_and_consume("t1", "/orders")          # 窗口 0，used=1
    env.check_and_consume("t1", "/orders")          # 窗口 1（118//60），used=1
    env.check_and_consume("t1", "/orders")          # 窗口 1，used=2
    env.check_and_consume("t1", "/orders")          # 120 -> 窗口 2，新窗口 used=1


def test_concurrent_consumers_never_oversell() -> None:
    """并发占用：放行数严格等于额度，绝不超卖、计数不错乱。"""
    limit = 50
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=limit, window_seconds=60),)
    )
    store = InMemoryQuotaStore(lambda: 1000.0)
    env = QuotaEnforcer(cfg, store)
    granted: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            env.check_and_consume("t1", "/orders")
            ok = True
        except GovernanceError as exc:
            ok = exc.reason is not DenyReason.QUOTA_EXHAUSTED
        with lock:
            granted.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(200)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(granted) == limit
    assert store.current("t1", "/orders", 60) == limit


@pytest.mark.parametrize(
    "mode,expected_reason_or_none",
    [
        (DegradeMode.FAIL_CLOSED, DenyReason.DEPENDENCY_UNAVAILABLE),
        (DegradeMode.FAIL_OPEN, None),
        (DegradeMode.USE_CACHE, DenyReason.DEPENDENCY_UNAVAILABLE),  # 冷缓存
    ],
)
def test_degrade_modes_on_store_failure(mode, expected_reason_or_none) -> None:
    env = QuotaEnforcer(_config(), FailingQuotaStore(), mode)
    if expected_reason_or_none is None:
        assert env.check_and_consume("t1", "/orders") is None
    else:
        with pytest.raises(GovernanceError) as exc:
            env.check_and_consume("t1", "/orders")
        assert exc.value.reason is expected_reason_or_none


def test_use_cache_allows_when_last_known_below_limit() -> None:
    """故障前缓存显示未达上限 -> USE_CACHE 放行；达到上限 -> 拒绝。"""
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=2, window_seconds=60),)
    )
    ticks = iter([1000.0])
    store = InMemoryQuotaStore(lambda: next(ticks))
    env = QuotaEnforcer(
        cfg, store, DegradeMode.USE_CACHE, clock=lambda: 1000.0
    )
    env.check_and_consume("t1", "/orders")  # 预热缓存 used=1
    env._store = FailingQuotaStore()
    env.check_and_consume("t1", "/orders")  # 1 < 2 放行
    # 手动把缓存推到上限，再次故障必须拒绝
    env._last_known[("t1", "/orders", 16)] = 2
    with pytest.raises(GovernanceError) as exc:
        env.check_and_consume("t1", "/orders")
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE


def test_backend_exception_is_never_leaked() -> None:
    """底层 StoreUnavailable 不得作为其它异常类型透出。"""
    env = QuotaEnforcer(_config(), FailingQuotaStore(), DegradeMode.FAIL_CLOSED)
    with pytest.raises(GovernanceError) as exc:
        env.check_and_consume("t1", "/orders")
    assert "connection refused" not in str(exc.value)
