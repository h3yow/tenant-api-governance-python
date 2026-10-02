"""过期/淘汰边界的并发重试：结论要么回放、要么明确过期，绝不二次执行。

业务背景：结论快到期、或刚好被容量淘汰的那一刻，往往正是调用方断线
重连、集中重试的时刻。此时必须保证：
- 保留期内同一标识始终回放同一条结论；
- 已超期/被淘汰的标识明确返回 RESULT_EXPIRED（与其它失败可区分，
  提示调用方换新标识重发）；
- 任何交错下业务副作用最多一次、额度不被重复占用。
"""

from __future__ import annotations

import threading

from governance.config import GovernanceConfig, QuotaRule, RetentionConfig
from governance.errors import DenyReason, GovernanceError
from governance.idempotency import InMemoryIdempotencyStore, IdempotencyService
from governance.identity import IdentityResolver
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer


def _service(retention: RetentionConfig, now: list[float]):
    clock = lambda: now[0]
    store = InMemoryIdempotencyStore(retention=retention, clock=clock)
    return IdempotencyService(store), store


def test_concurrent_retries_around_ttl_expiry_never_reexecute() -> None:
    """结论到期前后一批批并发重试：批内结论一致，副作用最多一次。

    保留期按最近访问起算：到期前一刻的并发批全部回放（并顺延保留期）；
    顺延后的过期点之后，并发批全部拿到明确的 RESULT_EXPIRED。任何交错
    下都不会出现第二次业务副作用。
    """
    now = [1000.0]
    svc, _ = _service(RetentionConfig(result_ttl_seconds=60.0,
                                      max_completed_records=None,
                                      tombstone_ttl_seconds=600.0), now)
    effects: list[int] = []
    svc.execute("k", "t1", {"v": 1}, lambda: (effects.append(1), "R")[1])

    def batch(size: int) -> list[str]:
        gate = threading.Event()
        outcomes: list[str] = []
        lock = threading.Lock()

        def worker() -> None:
            gate.wait(timeout=2.0)
            try:
                r = svc.execute("k", "t1", {"v": 1}, lambda: effects.append(1))
                outcome = f"replayed:{r}"
            except GovernanceError as exc:
                outcome = exc.reason.value
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=worker) for _ in range(size)]
        for t in threads:
            t.start()
        gate.set()
        for t in threads:
            t.join()
        return outcomes

    # 到期前最后一次访问：过期点确定为 1059 + 60 = 1119。
    now[0] = 1059.0
    assert svc.execute("k", "t1", {"v": 1}, lambda: effects.append(1)) == "R"

    # 到期前一刻（1118 < 1119）：整批统一回放，
    # 且回放把保留期顺延到 1118 + 60 = 1178。
    now[0] = 1118.0
    before = batch(20)
    assert before == ["replayed:R"] * 20, "保留期内的并发批必须统一回放"

    # 顺延后仍空闲超期：整批统一拿到明确的 RESULT_EXPIRED。
    now[0] = 1179.0
    after = batch(20)
    assert after == [DenyReason.RESULT_EXPIRED.value] * 20, (
        "超期后的并发批必须统一明确过期"
    )
    assert effects == [1], "过期边界并发重试不得造成第二次业务副作用"


def test_concurrent_retries_around_capacity_eviction_never_reexecute() -> None:
    """容量淘汰瞬间并发重试被淘汰标识：只回放或明确过期，绝不二次执行。"""
    now = [1000.0]
    svc, store = _service(RetentionConfig(result_ttl_seconds=None,
                                          max_completed_records=1,
                                          tombstone_ttl_seconds=600.0), now)
    effects: list[int] = []
    svc.execute("victim", "t1", {}, lambda: (effects.append(1), "V")[1])

    gate = threading.Event()
    outcomes: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        gate.wait(timeout=2.0)
        try:
            r = svc.execute("victim", "t1", {}, lambda: effects.append(1))
            outcome = f"replayed:{r}"
        except GovernanceError as exc:
            outcome = exc.reason.value
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    # 与并发重试同一时刻，新标识落定触发容量淘汰（cap=1）。
    gate.set()
    svc.execute("newcomer", "t1", {}, lambda: (effects.append(1), "N")[1])
    for t in threads:
        t.join()

    assert effects == [1, 1], "两个标识各自最多一次副作用"
    allowed = {"replayed:V", DenyReason.RESULT_EXPIRED.value}
    assert set(outcomes) <= allowed, (
        f"淘汰边界并发只允许回放或明确过期，实际: {set(outcomes)}"
    )
    # 淘汰已发生：之后的重试必须稳定地拿到 RESULT_EXPIRED。
    for _ in range(3):
        try:
            svc.execute("victim", "t1", {}, lambda: effects.append(1))
            raise AssertionError("被淘汰标识不得再回放或执行")
        except GovernanceError as exc:
            assert exc.reason is DenyReason.RESULT_EXPIRED
    assert effects == [1, 1]


def test_expired_result_is_distinct_and_prompts_new_key() -> None:
    """RESULT_EXPIRED 与额度/依赖等失败明确区分，且提示换新提交标识。"""
    now = [1000.0]
    svc, _ = _service(RetentionConfig(result_ttl_seconds=10.0,
                                      max_completed_records=None,
                                      tombstone_ttl_seconds=10.0), now)
    svc.execute("k", "t1", {}, lambda: "R")
    now[0] = 1011.0
    try:
        svc.execute("k", "t1", {}, lambda: "R2")
        raise AssertionError("超期标识必须被拒绝")
    except GovernanceError as exc:
        assert exc.reason is DenyReason.RESULT_EXPIRED
        assert exc.reason not in (DenyReason.QUOTA_EXHAUSTED,
                                  DenyReason.DEPENDENCY_UNAVAILABLE,
                                  DenyReason.IDEMPOTENCY_CONFLICT)
        assert "新的提交标识" in exc.message, "应提示调用方换新标识重发"


def test_expired_boundary_retry_does_not_consume_quota() -> None:
    """整条链路：过期边界上的重试不执行、不占额度；新标识才正常占额度。"""
    now = [1000.0]
    retention = RetentionConfig(result_ttl_seconds=30.0,
                                max_completed_records=None,
                                tombstone_ttl_seconds=300.0)
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=2, window_seconds=60),),
        retention=retention,
    )

    def clock() -> float:
        return now[0]

    quota_store = InMemoryQuotaStore(clock)
    quota = QuotaEnforcer(cfg, quota_store, clock=clock)
    idem_store = InMemoryIdempotencyStore(retention=retention, clock=clock)
    chain = GovernanceMiddleware(
        IdentityResolver({"token-t1": ("t1", "alice", frozenset({"/orders"}))}),
        quota, IdempotencyService(idem_store), cfg,
    )
    effects: list[int] = []
    chain.handle("token-t1", "/orders", "k1", {"v": 1},
                 lambda: (effects.append(1), "R1")[1])
    assert quota_store.current("t1", "/orders", 60) == 1

    # 结论超期后同标识重试：RESULT_EXPIRED，不执行、不占额度。
    # （1031s 已落在下一个配额窗口，当前窗口计数从 0 起算。）
    now[0] = 1031.0
    assert quota_store.current("t1", "/orders", 60) == 0
    try:
        chain.handle("token-t1", "/orders", "k1", {"v": 1},
                     lambda: effects.append(1))
        raise AssertionError("超期标识必须被拒绝")
    except GovernanceError as exc:
        assert exc.reason is DenyReason.RESULT_EXPIRED
    assert effects == [1]
    assert quota_store.current("t1", "/orders", 60) == 0, "过期重试不得占额度"

    # 调用方换新标识重发：按全新请求正常占额度、正常执行。
    r = chain.handle("token-t1", "/orders", "k2", {"v": 1},
                     lambda: (effects.append(1), "R2")[1])
    assert r == "R2"
    assert effects == [1, 1]
    assert quota_store.current("t1", "/orders", 60) == 1
