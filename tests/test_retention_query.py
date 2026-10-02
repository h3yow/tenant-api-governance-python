"""结论保留/淘汰与只读查询：超期或被淘汰的旧标识不静默二次执行；查询按租户隔离。"""

from __future__ import annotations

import threading

import pytest

from governance.config import GovernanceConfig, QuotaRule, RetentionConfig
from governance.errors import DenyReason, GovernanceError
from governance.idempotency import (
    CompleteFailingIdempotencyStore,
    InMemoryIdempotencyStore,
    IdempotencyService,
    QueryStatus,
    stable_payload_hash,
)
from governance.identity import IdentityResolver
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer


def _service(retention: RetentionConfig, now: list[float]):
    clock = lambda: now[0]
    store = InMemoryIdempotencyStore(retention=retention, clock=clock)
    return IdempotencyService(store), store, clock


# ---------------- TTL 超期 ----------------

def test_expired_key_is_rejected_not_silently_executed_again() -> None:
    now = [100.0]
    ret = RetentionConfig(result_ttl_seconds=60.0,
                          max_completed_records=None,
                          tombstone_ttl_seconds=60.0)
    svc, store, _ = _service(ret, now)
    effects: list[int] = []
    svc.execute("k", "t1", {"v": 1}, lambda: effects.append(1))
    assert effects == [1]

    # 保留期内：正常回放，不再产生副作用；回放本身刷新空闲过期起点，
    # 因此“刚被用过的结论”不会在 161 秒被误清。
    now[0] = 159.0
    svc.execute("k", "t1", {"v": 1}, lambda: effects.append(1))
    assert effects == [1]
    now[0] = 161.0
    svc.execute("k", "t1", {"v": 1}, lambda: effects.append(1))
    assert effects == [1]

    # 自最近一次访问（161 秒的回放）起空闲超期后：明确的 RESULT_EXPIRED，
    # 绝不静默当新请求再执行一遍。
    now[0] = 161.0 + 61.0
    with pytest.raises(GovernanceError) as exc:
        svc.execute("k", "t1", {"v": 1}, lambda: effects.append(1))
    assert exc.value.reason is DenyReason.RESULT_EXPIRED
    assert effects == [1], "超期标识不得触发第二次业务副作用"
    assert store.query("t1", "k").status is QueryStatus.EXPIRED_OR_NOT_FOUND


def test_tombstone_expiry_eventually_allows_clean_state() -> None:
    """墓碑保护期也过去后，标识按不存在处理（调用方此时本应已换新标识）。"""
    now = [0.0]
    ret = RetentionConfig(result_ttl_seconds=10.0,
                          max_completed_records=None,
                          tombstone_ttl_seconds=10.0)
    svc, _, _ = _service(ret, now)
    svc.execute("k", "t1", {}, lambda: "first")
    now[0] = 11.0  # 结论超期 -> 墓碑
    with pytest.raises(GovernanceError) as exc:
        svc.execute("k", "t1", {}, lambda: "second")
    assert exc.value.reason is DenyReason.RESULT_EXPIRED
    now[0] = 22.0  # 墓碑也过期 -> 彻底不存在
    outcome = svc.query("t1", "k")
    assert outcome.status is QueryStatus.EXPIRED_OR_NOT_FOUND


# ---------------- 容量 LRU 淘汰 ----------------

def test_capacity_eviction_rejects_old_key_instead_of_replaying_or_executing() -> None:
    now = [0.0]
    ret = RetentionConfig(result_ttl_seconds=None,
                          max_completed_records=2,
                          tombstone_ttl_seconds=3600.0)
    svc, _, _ = _service(ret, now)
    effects: list[int] = []
    svc.execute("k1", "t1", {}, lambda: (effects.append("k1"), "r1")[1])
    now[0] = 1.0
    svc.execute("k2", "t1", {}, lambda: (effects.append("k2"), "r2")[1])
    now[0] = 2.0
    # 访问 k1，使其成为最近使用；k2 成为 LRU 最久未访问。
    assert svc.execute("k1", "t1", {},
                       lambda: (effects.append("k1-again"), "x")[1]) == "r1"
    now[0] = 3.0
    # 第 3 个标识触发容量淘汰：k2 被转为墓碑。
    svc.execute("k3", "t1", {}, lambda: (effects.append("k3"), "r3")[1])

    with pytest.raises(GovernanceError) as exc:
        svc.execute("k2", "t1", {}, lambda: effects.append("k2-again"))
    assert exc.value.reason is DenyReason.RESULT_EXPIRED
    assert "k2-again" not in effects, "被淘汰标识不得二次执行"
    # k1 最近刚被访问，未被淘汰，仍可回放。
    assert svc.execute("k1", "t1", {}, lambda: effects.append("x")) == "r1"


def test_inflight_record_is_not_evicted_under_capacity_pressure() -> None:
    """容量淘汰不得清掉正在处理中的结论（否则并发重试会产生双份结果）。"""
    now = [0.0]
    ret = RetentionConfig(result_ttl_seconds=None,
                          max_completed_records=1,
                          tombstone_ttl_seconds=3600.0)
    clock = lambda: now[0]
    store = InMemoryIdempotencyStore(retention=ret, clock=clock)
    svc = IdempotencyService(store)
    release = threading.Event()
    started = threading.Event()

    def slow_action() -> str:
        started.set()
        release.wait(timeout=2.0)
        return "HOT"

    outcome_box: list[object] = []

    def leader() -> None:
        outcome_box.append(svc.execute("inflight-k", "t1", {}, slow_action))

    t = threading.Thread(target=leader)
    t.start()
    assert started.wait(timeout=2.0)
    # inflight 记录存在；此时强行做容量清扫也不能把它转成墓碑。
    store.sweep()
    record, outcome = store.acquire("t1", "inflight-k",
                                    stable_payload_hash({}))
    assert outcome.value == "inflight", "处理中的记录必须可被并发跟随者找到"
    release.set()
    t.join()
    settled = store.wait_until_settled("t1", "inflight-k", timeout=2.0)
    assert settled.result == "HOT"
    assert outcome_box == ["HOT"]


def test_concurrent_replay_during_eviction_has_single_consistent_result() -> None:
    """淘汰与并发重放同时发生：同一标识要么可回放，要么明确过期，不能执行两次。"""
    now = [0.0]
    ret = RetentionConfig(result_ttl_seconds=10.0,
                          max_completed_records=100,
                          tombstone_ttl_seconds=3600.0)
    svc, store, _ = _service(ret, now)
    effects: list[int] = []
    svc.execute("hot", "t1", {}, lambda: effects.append(1))
    decisions: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            svc.execute("hot", "t1", {}, lambda: effects.append(1))
            verdict = "replayed"
        except GovernanceError as exc:
            verdict = exc.reason.value
        with lock:
            decisions.append(verdict)

    # 一半线程在结论快超期的边界重放，主线程推进时钟触发清扫。
    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    now[0] = 11.0
    store.sweep()
    for t in threads:
        t.join()
    assert effects == [1], "任何情况下副作用只能一次"
    assert set(decisions) <= {"replayed", "result_expired"}, decisions


# ---------------- 只读查询：状态区分与租户隔离 ----------------

def _query_chain():
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),),
    )
    clock = lambda: 1000.0
    quota = QuotaEnforcer(cfg, InMemoryQuotaStore(clock), clock=clock)
    store = InMemoryIdempotencyStore(retention=cfg.retention, clock=clock)
    identity = IdentityResolver({
        "token-t1": ("t1", "alice", frozenset({"/orders"})),
        "token-t2": ("t2", "bob", frozenset({"/orders"})),
    })
    chain = GovernanceMiddleware(
        identity, quota, IdempotencyService(store), cfg
    )
    return chain, identity


def test_query_statuses_distinguished() -> None:
    chain, _ = _query_chain()
    # 不存在（或已过期）。
    out = chain.query_settlement("token-t1", "/orders", "missing")
    assert out.status is QueryStatus.EXPIRED_OR_NOT_FOUND
    assert out.result is None

    # 处理中：首请求 action 内查询自身键。
    seen: list[object] = []

    def action() -> str:
        seen.append(
            chain.query_settlement("token-t1", "/orders", "q-live").status
        )
        return "DONE"

    chain.handle("token-t1", "/orders", "q-live", {}, action)
    assert seen == [QueryStatus.INFLIGHT]

    # 已完成：只读，带首次结论。
    out = chain.query_settlement("token-t1", "/orders", "q-live")
    assert out.status is QueryStatus.COMPLETED
    assert out.result == "DONE"


def test_query_is_tenant_isolated() -> None:
    """一个租户的凭据查不到另一个租户同标识的结论。"""
    chain, _ = _query_chain()
    chain.handle("token-t1", "/orders", "shared-key", {"v": 1},
                 lambda: "t1-result")
    # t2 使用同一标识：对 t2 而言不存在。
    out_t2 = chain.query_settlement("token-t2", "/orders", "shared-key")
    assert out_t2.status is QueryStatus.EXPIRED_OR_NOT_FOUND
    # t1 自己能查到。
    out_t1 = chain.query_settlement("token-t1", "/orders", "shared-key")
    assert out_t1.status is QueryStatus.COMPLETED
    assert out_t1.result == "t1-result"
    # 无身份不能查询。
    with pytest.raises(GovernanceError) as exc:
        chain.query_settlement(None, "/orders", "shared-key")
    assert exc.value.reason is DenyReason.MISSING_IDENTITY


def test_query_does_not_consume_quota_or_side_effect() -> None:
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=1, window_seconds=60),),
    )
    clock = lambda: 1000.0
    qstore = InMemoryQuotaStore(clock)
    quota = QuotaEnforcer(cfg, qstore, clock=clock)
    istore = InMemoryIdempotencyStore(retention=cfg.retention, clock=clock)
    identity = IdentityResolver({
        "token-t1": ("t1", "alice", frozenset({"/orders"})),
    })
    chain = GovernanceMiddleware(
        identity, quota, IdempotencyService(istore), cfg
    )
    chain.handle("token-t1", "/orders", "qk", {}, lambda: "R")  # 用掉唯一额度
    for _ in range(3):
        out = chain.query_settlement("token-t1", "/orders", "qk")
        assert out.status is QueryStatus.COMPLETED
    assert qstore.current("t1", "/orders", 60) == 1, "查询不得占额度"


def test_uncertain_status_when_side_effect_happened_but_complete_fails() -> None:
    """副作用已发生但结论无法落定：查询返回 uncertain，且拒绝重复提交。"""
    store = CompleteFailingIdempotencyStore()
    svc = IdempotencyService(store)
    effects: list[int] = []
    with pytest.raises(GovernanceError) as exc:
        svc.execute("unc-k", "t1", {}, lambda: effects.append(1))
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE
    assert effects == [1]  # 副作用确实发生了一次

    out = svc.query("t1", "unc-k")
    assert out.status is QueryStatus.UNCERTAIN

    # 再次提交同标识：不能重试（否则重复副作用），返回不可确认而非执行。
    with pytest.raises(GovernanceError) as exc2:
        svc.execute("unc-k", "t1", {}, lambda: effects.append(1))
    assert exc2.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE
    assert effects == [1], "UNCERTAIN 下重复提交不得再产生副作用"
