"""首次被额度拒绝后的重试结算：拒绝也是落定结论，同标识重试原样回放。

业务背景：某提交标识首次提交时因该租户在该接口的额度用完被拒
（QUOTA_EXHAUSTED）。此后无论是一批并发一起重试、断线重连再试、跨到
下一个时间窗口、还是额度后来又有空余，都必须拿到与第一次完全一样的
拒绝结论——不重新占额度、不产生业务副作用、不得变成其它失败（如
DEPENDENCY_UNAVAILABLE）。只有真正没见过的全新标识才照常走配额闸门。
"""

from __future__ import annotations

import threading

import pytest

from governance.config import GovernanceConfig, QuotaRule, RetentionConfig
from governance.errors import DenyReason, GovernanceError
from governance.idempotency import (
    InMemoryIdempotencyStore,
    IdempotencyService,
    QueryStatus,
)
from governance.identity import IdentityResolver
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer


def _identity() -> IdentityResolver:
    return IdentityResolver(
        {"token-t1": ("t1", "alice", frozenset({"/orders"}))}
    )


def _chain(limit: int = 1, window: int = 60,
           times: list[float] | None = None,
           retention: RetentionConfig | None = None,
           quota_store=None):
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=limit,
                               window_seconds=window),),
        retention=retention or RetentionConfig(),
    )
    clock_values = times if times is not None else [1000.0]

    def clock() -> float:
        return clock_values[0]

    store = quota_store if quota_store is not None else InMemoryQuotaStore(clock)
    quota = QuotaEnforcer(cfg, store, clock=clock)
    idem_store = InMemoryIdempotencyStore(retention=cfg.retention, clock=clock)
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(idem_store), cfg
    )
    return chain, store, idem_store, clock_values


def _deny_reason(chain, key: str, effects: list, payload=None) -> DenyReason:
    """提交一次并断言被治理拒绝，返回拒绝原因。"""
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", key, payload or {"item": "a"},
                     lambda: effects.append(1))
    return exc.value.reason


def test_concurrent_retries_after_quota_denial_all_get_same_conclusion() -> None:
    """首次额度拒绝后，一批并发重试必须全部拿到同一个 QUOTA_EXHAUSTED。"""
    chain, store, _, _ = _chain(limit=1, window=60)
    effects: list[int] = []

    # 第一个标识占满额度；第二个标识首次提交被额度拒绝（结论落定）。
    assert chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                        lambda: (effects.append(1), "OK")[1]) == "OK"
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.QUOTA_EXHAUSTED
    assert store.current("t1", "/orders", 60) == 1

    # 一批并发重试同一个被拒标识：结论必须完全一致。
    reasons: list[DenyReason] = []
    lock = threading.Lock()
    gate = threading.Event()

    def worker() -> None:
        gate.wait(timeout=2.0)
        reason = _deny_reason(chain, "k-denied", effects)
        with lock:
            reasons.append(reason)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    assert len(reasons) == 20
    assert set(reasons) == {DenyReason.QUOTA_EXHAUSTED}, (
        "同标识并发重试不得出现与首次不同的结论（如 dependency_unavailable）"
    )
    assert effects == [1], "被拒提交的重试不得产生业务副作用"
    assert store.current("t1", "/orders", 60) == 1, "被拒提交的重试不得再占额度"


def test_cross_window_retry_of_denied_key_replays_denial() -> None:
    """跨到下一个窗口、额度已有空余：同标识重试仍回放额度拒绝，不重新执行。"""
    times = [1000.0]
    chain, store, _, _ = _chain(limit=1, window=60, times=times)
    effects: list[int] = []

    chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                 lambda: (effects.append(1), "OK")[1])
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.QUOTA_EXHAUSTED

    # 跨窗口：新窗口额度完全空闲。
    times[0] = 1060.0
    assert store.current("t1", "/orders", 60) == 0
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.QUOTA_EXHAUSTED
    assert effects == [1], "跨窗口重试不得把被拒提交当新请求再执行一遍"
    assert store.current("t1", "/orders", 60) == 0, "回放拒绝结论不得占新窗口额度"

    # 同一新窗口里，真正全新的标识照常获得额度——证明上面不是被额度挡住。
    assert chain.handle("token-t1", "/orders", "k-fresh", {"item": "a"},
                        lambda: (effects.append(1), "NEW")[1]) == "NEW"
    assert store.current("t1", "/orders", 60) == 1
    assert effects == [1, 1]


def test_followers_waiting_during_denial_replay_same_conclusion() -> None:
    """首请求正在配额判定时同标识并发到达：跟随者等待后也回放同一拒绝结论。"""

    class SlowDenyQuotaStore:
        """第一次额度拒绝前卡住，让并发跟随者先排到等待队列里。"""

        def __init__(self, inner):
            self._inner = inner
            self.entered = threading.Event()
            self.release = threading.Event()
            self._slowed = False

        def try_acquire(self, tenant_id, api, window_seconds, limit):
            granted, used = self._inner.try_acquire(
                tenant_id, api, window_seconds, limit
            )
            if not granted and not self._slowed:
                self._slowed = True
                self.entered.set()
                self.release.wait(timeout=5.0)
            return granted, used

        def current(self, tenant_id, api, window_seconds):
            return self._inner.current(tenant_id, api, window_seconds)

    times = [1000.0]

    def clock() -> float:
        return times[0]

    inner = InMemoryQuotaStore(clock)
    slow = SlowDenyQuotaStore(inner)
    chain, store, _, _ = _chain(limit=1, window=60, times=times,
                                quota_store=slow)
    effects: list[int] = []
    chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                 lambda: (effects.append(1), "OK")[1])

    leader_done: list[DenyReason] = []

    def leader() -> None:
        leader_done.append(_deny_reason(chain, "k-denied", effects))

    lt = threading.Thread(target=leader)
    lt.start()
    assert slow.entered.wait(timeout=2.0), "leader 应卡在配额判定上"

    # leader 卡住期间，一批同标识并发请求到达并成为跟随者。
    follower_reasons: list[DenyReason] = []
    lock = threading.Lock()

    def follower() -> None:
        reason = _deny_reason(chain, "k-denied", effects)
        with lock:
            follower_reasons.append(reason)

    fts = [threading.Thread(target=follower) for _ in range(10)]
    for t in fts:
        t.start()
    threading.Event().wait(0.2)  # 让跟随者进入等待队列
    slow.release.set()
    lt.join(timeout=5.0)
    for t in fts:
        t.join(timeout=5.0)

    assert leader_done == [DenyReason.QUOTA_EXHAUSTED]
    assert len(follower_reasons) == 10
    assert set(follower_reasons) == {DenyReason.QUOTA_EXHAUSTED}, (
        "等待首请求落定的跟随者必须回放同一拒绝结论"
    )
    assert effects == [1]
    assert store.current("t1", "/orders", 60) == 1


def test_brand_new_key_still_rejected_normally_when_quota_exhausted() -> None:
    """真正没见过的全新标识在额度用完时照常拒绝，且其结论同样可回放。"""
    chain, store, _, _ = _chain(limit=1, window=60)
    effects: list[int] = []
    chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                 lambda: (effects.append(1), "OK")[1])

    assert _deny_reason(chain, "k-new", effects) is DenyReason.QUOTA_EXHAUSTED
    # 全新标识的拒绝同样落定：断线重连再试结论一致。
    assert _deny_reason(chain, "k-new", effects) is DenyReason.QUOTA_EXHAUSTED
    assert effects == [1]
    assert store.current("t1", "/orders", 60) == 1


def test_denied_conclusion_is_queryable() -> None:
    """被拒结论可凭提交标识只读查询：状态 denied 且带原始拒绝原因。"""
    chain, _, _, _ = _chain(limit=1, window=60)
    effects: list[int] = []
    chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                 lambda: (effects.append(1), "OK")[1])
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.QUOTA_EXHAUSTED

    outcome = chain.query_settlement("token-t1", "/orders", "k-denied")
    assert outcome.status is QueryStatus.DENIED
    assert outcome.result["reason"] == DenyReason.QUOTA_EXHAUSTED.value
    assert effects == [1], "查询不得产生副作用"


def test_denied_conclusion_expires_to_result_expired() -> None:
    """被拒结论超期后：明确的 RESULT_EXPIRED（提示换新标识），绝不重新执行。"""
    times = [1000.0]
    ret = RetentionConfig(result_ttl_seconds=30.0,
                          max_completed_records=None,
                          tombstone_ttl_seconds=30.0)
    chain, store, _, _ = _chain(limit=1, window=60, times=times, retention=ret)
    effects: list[int] = []
    chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                 lambda: (effects.append(1), "OK")[1])
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.QUOTA_EXHAUSTED

    # 保留期内（最近一次访问后 30s 内）重试：仍回放拒绝结论。
    times[0] = 1020.0
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.QUOTA_EXHAUSTED

    # 空闲超期后：RESULT_EXPIRED，与额度拒绝明确区分，且不再执行。
    times[0] = 1020.0 + 31.0
    assert _deny_reason(chain, "k-denied", effects) is DenyReason.RESULT_EXPIRED
    assert effects == [1]
    # 此时已在新窗口，过期重试不得触碰新窗口额度。
    assert store.current("t1", "/orders", 60) == 0


def test_payload_conflict_on_denied_key_is_rejected() -> None:
    """被拒标识携带不同载荷重试：IDEMPOTENCY_CONFLICT，而非回放或执行。"""
    chain, _, _, _ = _chain(limit=1, window=60)
    effects: list[int] = []
    chain.handle("token-t1", "/orders", "k-ok", {"item": "a"},
                 lambda: (effects.append(1), "OK")[1])
    assert _deny_reason(chain, "k-denied", effects,
                        payload={"item": "a"}) is DenyReason.QUOTA_EXHAUSTED
    assert _deny_reason(chain, "k-denied", effects,
                        payload={"item": "b"}) is DenyReason.IDEMPOTENCY_CONFLICT
    assert effects == [1]
