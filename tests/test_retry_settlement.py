"""重试结算语义：同标识重试永远回放首次结论，且只有首个真实写请求占额度。"""

from __future__ import annotations

import threading

import pytest

from governance.config import GovernanceConfig, QuotaRule, RetentionConfig
from governance.errors import DenyReason, GovernanceError
from governance.idempotency import (
    InMemoryIdempotencyStore,
    IdempotencyService,
)
from governance.identity import IdentityResolver
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer


def _identity() -> IdentityResolver:
    return IdentityResolver(
        {
            "token-t1": ("t1", "alice", frozenset({"/orders"})),
            "token-t2": ("t2", "bob", frozenset({"/orders"})),
        }
    )


def _chain(limit: int = 1, window: int = 60,
           times: list[float] | None = None,
           retention: RetentionConfig | None = None):
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=limit,
                               window_seconds=window),),
        retention=retention or RetentionConfig(),
    )
    clock_values = times if times is not None else [1000.0]

    def clock() -> float:
        return clock_values[0]

    store = InMemoryQuotaStore(clock)
    quota = QuotaEnforcer(cfg, store, clock=clock)
    idem_store = InMemoryIdempotencyStore(retention=cfg.retention, clock=clock)
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(idem_store), cfg
    )
    return chain, store, quota, idem_store, clock_values


def test_duplicate_after_quota_exhausted_replays_first_result() -> None:
    """额度耗尽后同标识重试仍拿首次结论，不新增占用、不产生第二个副作用。"""
    chain, store, _, _, _ = _chain(limit=1, window=60)
    effects: list[int] = []

    first = chain.handle("token-t1", "/orders", "retry-1",
                         {"item": "a"}, lambda: (effects.append(1), "R1")[1])
    # 新的提交标识：额度已用完，照常被配额拒绝。
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", "brand-new",
                     {"item": "a"}, lambda: (effects.append(1), "X")[1])
    assert exc.value.reason is DenyReason.QUOTA_EXHAUSTED
    assert store.current("t1", "/orders", 60) == 1

    # 旧标识重试（断线重连场景）：回放首次结论，不占额度、不再产生副作用。
    replay = chain.handle("token-t1", "/orders", "retry-1",
                          {"item": "a"}, lambda: (effects.append(1), "R2")[1])
    assert replay == "R1"
    assert effects == [1], "重复请求不得再次产生业务副作用"
    assert store.current("t1", "/orders", 60) == 1, "重复请求不得新增配额占用"


def test_cross_window_replay_does_not_consume_new_window_quota() -> None:
    """首次提交与重试落在不同窗口：回放首次结论，不占新窗口额度。"""
    times = [1000.0]
    chain, store, _, _, _ = _chain(limit=1, window=60, times=times)
    effects: list[int] = []
    first = chain.handle("token-t1", "/orders", "cross-w",
                         {"v": 1}, lambda: (effects.append(1), "W1")[1])
    assert first == "W1"
    assert store.current("t1", "/orders", 60) == 1  # 窗口 16

    # 跨到下一个窗口（1000 -> 1060 -> 窗口 17）。
    times[0] = 1060.0
    replay = chain.handle("token-t1", "/orders", "cross-w",
                          {"v": 1}, lambda: (effects.append(1), "W2")[1])
    assert replay == "W1", "跨窗口重试必须回放首次结论"
    assert effects == [1]
    assert store.current("t1", "/orders", 60) == 0, "回放不得占用新窗口配额"

    # 新窗口里新的提交标识仍有完整额度（1 个），证明回放没占新窗口。
    fresh = chain.handle("token-t1", "/orders", "fresh-w",
                         {"v": 1}, lambda: (effects.append(1), "NEW")[1])
    assert fresh == "NEW"
    assert store.current("t1", "/orders", 60) == 1


# ---------------- 需求点 2：写入策略与降级一致性 ----------------

from governance.config import (  # noqa: E402
    DegradeMode,
    PolicyRegistry,
    WritePolicyRule,
)
from governance.quota import FailingQuotaStore  # noqa: E402


def _policy_chain(cfg: GovernanceConfig, times: list[float] | None = None):
    clock_values = times if times is not None else [1000.0]

    def clock() -> float:
        return clock_values[0]

    registry = PolicyRegistry(cfg)
    store = InMemoryQuotaStore(clock)
    quota = QuotaEnforcer(registry, store, clock=clock)
    idem_store = InMemoryIdempotencyStore(retention=cfg.retention, clock=clock)
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(idem_store), registry
    )
    return chain, registry, store, quota, idem_store, clock_values


def test_required_key_endpoint_missing_key_is_distinct_denial() -> None:
    """要求标识的接口缺标识 -> MISSING_IDEMPOTENCY_KEY，与其它失败明确区分。"""
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=5, window_seconds=60),),
        write_policy_rules=(WritePolicyRule(
            "t1", "/orders", require_idempotency_key=True),),
    )
    chain, _, store, _, _, _ = _policy_chain(cfg)
    effects: list[int] = []
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", None, {"v": 1},
                     lambda: effects.append(1))
    assert exc.value.reason is DenyReason.MISSING_IDEMPOTENCY_KEY
    assert effects == [], "缺标识拒绝不得产生副作用"
    assert store.current("t1", "/orders", 60) == 0, "缺标识拒绝不得占额度"

    # 携带标识后正常放行。
    result = chain.handle("token-t1", "/orders", "has-key", {"v": 1},
                          lambda: "ok")
    assert result == "ok"


def test_policy_hot_reload_takes_effect_consistently_after_switch() -> None:
    """不重启调整策略：切换后所有新请求统一按新策略；切换前的请求按旧策略。"""
    cfg_relaxed = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),),
    )
    chain, registry, _, _, _, _ = _policy_chain(cfg_relaxed)
    # 切换前：缺标识允许执行。
    assert chain.handle("token-t1", "/orders", None, {}, lambda: "old") == "old"

    cfg_required = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),),
        write_policy_rules=(WritePolicyRule(
            "t1", "/orders", require_idempotency_key=True),),
    )
    registry.update(cfg_required)
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", None, {}, lambda: "x")
    assert exc.value.reason is DenyReason.MISSING_IDEMPOTENCY_KEY


def test_concurrent_requests_all_see_same_policy_version() -> None:
    """策略在一批并发中途切换：每个请求只按一份快照裁决，不会出现半旧半新。"""
    cfg_relaxed = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=10_000, window_seconds=60),),
    )
    chain, registry, _, _, _, _ = _policy_chain(cfg_relaxed)
    enter = threading.Event()
    decisions: list[DenyReason | None] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        enter.wait(timeout=2.0)
        try:
            chain.handle("token-t1", "/orders", None, {"i": i}, lambda: None)
            decision = None
        except GovernanceError as exc:
            decision = exc.reason
        with lock:
            decisions.append(decision)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
    for t in threads:
        t.start()
    # 在所有工作线程被同一个闸门挡住时切换策略。
    registry.update(GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=10_000, window_seconds=60),),
        write_policy_rules=(WritePolicyRule(
            "t1", "/orders", require_idempotency_key=True),),
    ))
    enter.set()
    for t in threads:
        t.join()
    # 切换先于任何线程取快照：所有请求必须一致地看到新策略（全部缺标识拒绝）。
    assert decisions, "必须有并发判定结果"
    assert all(d is DenyReason.MISSING_IDEMPOTENCY_KEY for d in decisions), decisions


def test_fail_open_degrade_never_allows_duplicate_side_effect() -> None:
    """配额依赖故障 + FAIL_OPEN：只放开配额闸门；同键重复仍只一次副作用。"""
    cfg = GovernanceConfig(
        degrade_mode=DegradeMode.FAIL_OPEN,
        write_policy_rules=(WritePolicyRule(
            "t1", "/orders", degrade_mode=DegradeMode.FAIL_OPEN),),
    )
    registry = PolicyRegistry(cfg)
    quota = QuotaEnforcer(registry, FailingQuotaStore())
    idem_store = InMemoryIdempotencyStore(retention=cfg.retention)
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(idem_store), registry
    )
    effects: list[int] = []
    r1 = chain.handle("token-t1", "/orders", "open-1", {"v": 1},
                      lambda: (effects.append(1), "DONE")[1])
    r2 = chain.handle("token-t1", "/orders", "open-1", {"v": 1},
                      lambda: (effects.append(1), "DONE2")[1])
    assert r1 == r2 == "DONE"
    assert effects == [1], "fail-open 不得放开结算闸门而产生重复副作用"


def test_fail_closed_degrade_on_quota_failure_denies() -> None:
    """默认保守拒绝：配额存储故障 -> DEPENDENCY_UNAVAILABLE，不产生副作用。"""
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=10, window_seconds=60),),
    )  # 默认 FAIL_CLOSED
    registry = PolicyRegistry(cfg)
    quota = QuotaEnforcer(registry, FailingQuotaStore())
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(InMemoryIdempotencyStore()), registry
    )
    effects: list[int] = []
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", "closed-1", {},
                     lambda: effects.append(1))
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE
    assert effects == []


def test_concurrent_degrade_decisions_are_consistent_under_switch() -> None:
    """配额依赖持续故障期间热切换降级模式：每批并发结论在同一模式下一致。"""
    cfg_open = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),),
        degrade_mode=DegradeMode.FAIL_OPEN,
    )
    registry = PolicyRegistry(cfg_open)
    quota = QuotaEnforcer(registry, FailingQuotaStore())
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(InMemoryIdempotencyStore()), registry
    )
    gate = threading.Event()
    results: list[bool] = []
    lock = threading.Lock()

    def worker(key: str) -> None:
        gate.wait(timeout=2.0)
        try:
            chain.handle("token-t1", "/orders", key, {}, lambda: None)
            allowed = True
        except GovernanceError:
            allowed = False
        with lock:
            results.append(allowed)

    threads = [threading.Thread(target=worker, args=(f"k{i}",))
               for i in range(20)]
    for t in threads:
        t.start()
    registry.update(GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),),
        degrade_mode=DegradeMode.FAIL_CLOSED,
    ))
    gate.set()
    for t in threads:
        t.join()
    # 切换先于闸门放行：全部按 FAIL_CLOSED 拒绝（不会一半放行一半拒绝）。
    assert results and all(allowed is False for allowed in results), results
