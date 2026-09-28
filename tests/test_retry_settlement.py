"""重试结算新语义：

- 已落定结论的重复/并发/断线重连回放首结论，不占配额（含额度耗尽、跨窗口）；
- 按租户×接口热更新的写入策略（强制标识、降级模式），并发下结论一致；
- TTL/LRU 淘汰后旧标识得到明确的"已过期"失败，绝不静默二次执行；
- 按提交标识只读查询：状态可区分、按租户隔离、不占配额。
"""

from __future__ import annotations

import threading

import pytest

from governance.config import DegradeMode, GovernanceConfig, QuotaRule
from governance.errors import DenyReason, GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import (
    CompleteFailingStore,
    IdemStatus,
    InMemoryIdempotencyStore,
    IdempotencyService,
    SubmissionStatus,
)
from governance.middleware import GovernanceMiddleware
from governance.policy import PolicyRegistry, RetentionConfig, WritePolicy
from governance.quota import FailingQuotaStore, InMemoryQuotaStore, QuotaEnforcer


def _identity() -> IdentityResolver:
    return IdentityResolver(
        {
            "token-t1": ("t1", "alice", frozenset({"/orders"})),
            "token-t2": ("t2", "bob", frozenset({"/orders"})),
            "token-t3": ("t3", "carol", frozenset({"/health"})),
        }
    )


class MutableClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds


def _build_chain(
    limit: int = 1,
    window_seconds: int = 60,
    clock: MutableClock | None = None,
    quota_store=None,
    retention: RetentionConfig | None = None,
    policies: PolicyRegistry | None = None,
    idem_store=None,
):
    clock = clock or MutableClock()
    cfg = GovernanceConfig(
        quota_rules=(
            QuotaRule("t1", "/orders", limit=limit, window_seconds=window_seconds),
            QuotaRule("t2", "/orders", limit=limit, window_seconds=window_seconds),
        ),
        degrade_mode=DegradeMode.FAIL_CLOSED,
    )
    if policies is None:
        policies = PolicyRegistry(cfg, retention=retention or RetentionConfig())
    quota = QuotaEnforcer(
        cfg, quota_store or InMemoryQuotaStore(clock), clock=clock
    )
    store = idem_store or InMemoryIdempotencyStore(
        clock=clock, retention=retention
    )
    idem = IdempotencyService(store, retention_provider=policies.retention)
    chain = GovernanceMiddleware(_identity(), quota, idem, policies=policies)
    return chain, quota, store, clock


# ---------- 1. 额度耗尽后的同标识重试 ----------

def test_retry_after_quota_exhausted_replays_without_extra_charge() -> None:
    chain, quota, _, clock = _build_chain(limit=1)
    effects: list[int] = []

    first = chain.handle(
        "token-t1", "/orders", "charge-k", {"item": "book"},
        lambda: effects.append(1) or {"order_id": "ORD-1"},
    )
    assert first == {"order_id": "ORD-1"}
    assert quota.check_and_consume  # sanity

    # 此时新窗口额度已耗尽：全新标识必须照常拒绝
    with pytest.raises(GovernanceError) as exc:
        chain.handle(
            "token-t1", "/orders", "brand-new-k", {"item": "book"},
            lambda: effects.append(1),
        )
    assert exc.value.reason is DenyReason.QUOTA_EXHAUSTED

    # 但同一标识的断线重连/重复提交仍回放首结论，且不新增占用
    replay = chain.handle(
        "token-t1", "/orders", "charge-k", {"item": "book"},
        lambda: effects.append(1) or {"order_id": "SHOULD-NOT-HAPPEN"},
    )
    assert replay == {"order_id": "ORD-1"}
    assert effects == [1], "重试不得产生第二次业务副作用"
    assert quota._store.current("t1", "/orders", 60) == 1


def test_replay_across_window_boundary_does_not_charge_new_window() -> None:
    clock = MutableClock(start=59.0)
    chain, quota, _, clock = _build_chain(limit=1, window_seconds=60,
                                          clock=clock)
    effects: list[int] = []
    first = chain.handle(
        "token-t1", "/orders", "window-k", {"v": 1},
        lambda: effects.append(1) or "W0",
    )
    assert first == "W0"
    clock.advance(61)  # 跨入再下一个窗口
    replay = chain.handle(
        "token-t1", "/orders", "window-k", {"v": 1},
        lambda: effects.append(1) or "W1",
    )
    assert replay == "W0"
    assert effects == [1]
    # 新窗口计数必须保持 0：重试没有占用新窗口额度
    assert quota._store.current("t1", "/orders", 60) == 0


def test_same_key_different_payload_conflicts_even_when_quota_exhausted() -> None:
    chain, _, _, _ = _build_chain(limit=1)
    chain.handle("token-t1", "/orders", "k", {"amount": 100}, lambda: "ok")
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", "k", {"amount": 200}, lambda: "ok")
    assert exc.value.reason is DenyReason.IDEMPOTENCY_CONFLICT


def test_keyless_request_still_charges_and_executes_every_time() -> None:
    """无标识写入保持兼容：每次执行、每次占额度（受配额约束）。"""
    chain, _, _, _ = _build_chain(limit=2)
    effects: list[int] = []
    chain.handle("token-t1", "/orders", None, {"v": 1},
                 lambda: effects.append(1))
    chain.handle("token-t1", "/orders", None, {"v": 1},
                 lambda: effects.append(1))
    assert effects == [1, 1]
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", None, {"v": 1},
                     lambda: effects.append(1))
    assert exc.value.reason is DenyReason.QUOTA_EXHAUSTED


# ---------- 2. 强制提交标识与热更新策略 ----------

def test_required_idempotency_key_missing_is_distinct_rejection() -> None:
    cfg = GovernanceConfig()
    policies = PolicyRegistry(cfg)
    policies.update_write_policy(
        "t1", "/orders", WritePolicy(require_idempotency_key=True)
    )
    chain, _, _, _ = _build_chain(policies=policies)
    effects: list[int] = []
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", None, {"v": 1},
                     lambda: effects.append(1))
    assert exc.value.reason is DenyReason.IDEMPOTENCY_KEY_REQUIRED
    assert effects == []


def test_required_key_policy_takes_effect_for_later_concurrent_batch() -> None:
    """策略调整后，后续一整批并发请求统一按新策略执行，不出现半旧半新。"""
    cfg = GovernanceConfig(quota_rules=())
    policies = PolicyRegistry(cfg)  # 默认不要求标识
    quota = QuotaEnforcer(cfg, InMemoryQuotaStore(lambda: 1000.0),
                          clock=lambda: 1000.0)
    idem = IdempotencyService(
        InMemoryIdempotencyStore(), retention_provider=policies.retention
    )
    chain = GovernanceMiddleware(_identity(), quota, idem, policies=policies)

    batch_start = threading.Barrier(9)
    outcomes: list[DenyReason | str] = []
    lock = threading.Lock()

    def worker() -> None:
        batch_start.wait()
        try:
            chain.handle("token-t1", "/orders", None, {"v": 1}, lambda: "done")
            with lock:
                outcomes.append("executed")
        except GovernanceError as exc:
            with lock:
                outcomes.append(exc.reason)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    # 所有 worker 已在策略读取前对齐，此时热更新为"必须携带标识"
    policies.update_write_policy(
        "t1", "/orders", WritePolicy(require_idempotency_key=True)
    )
    batch_start.wait()  # 主线程放闸
    for t in threads:
        t.join()
    assert outcomes == [DenyReason.IDEMPOTENCY_KEY_REQUIRED] * 8


def test_degrade_policy_hot_update_is_uniform_under_dependency_failure() -> None:
    """配额存储故障：热更新为 FAIL_OPEN 后整批并发全部放行，没有一半拒绝。"""
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=5, window_seconds=60),),
        degrade_mode=DegradeMode.FAIL_CLOSED,
    )
    policies = PolicyRegistry(cfg)
    quota = QuotaEnforcer(cfg, FailingQuotaStore())
    idem = IdempotencyService(
        InMemoryIdempotencyStore(), retention_provider=policies.retention
    )
    chain = GovernanceMiddleware(_identity(), quota, idem, policies=policies)

    # 更新前：默认保守拒绝
    with pytest.raises(GovernanceError) as exc:
        chain.handle("token-t1", "/orders", "open-0", {}, lambda: "x")
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE

    policies.update_write_policy(
        "t1", "/orders", WritePolicy(degrade_mode=DegradeMode.FAIL_OPEN)
    )
    gate = threading.Barrier(9)
    results: list[object] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        gate.wait()
        try:
            out = chain.handle(
                "token-t1", "/orders", f"open-{i}", {}, lambda: "OPEN"
            )
            with lock:
                results.append(out)
        except GovernanceError as exc:
            with lock:
                results.append(exc.reason)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    gate.wait()
    for t in threads:
        t.join()
    assert results == ["OPEN"] * 8


def test_fail_open_quota_gate_never_lets_duplicate_side_effect_through() -> None:
    """放行配置只放开配额闸门；同键并发副作用仍严格只一次。"""
    cfg = GovernanceConfig(degrade_mode=DegradeMode.FAIL_OPEN)
    policies = PolicyRegistry(cfg)
    quota = QuotaEnforcer(cfg, FailingQuotaStore(), DegradeMode.FAIL_OPEN)
    idem = IdempotencyService(
        InMemoryIdempotencyStore(), retention_provider=policies.retention
    )
    chain = GovernanceMiddleware(_identity(), quota, idem, policies=policies)
    effects: list[int] = []
    release = threading.Event()
    started = threading.Event()

    def action() -> str:
        effects.append(1)
        started.set()
        release.wait(timeout=2.0)
        return "ONCE"

    results: list[object] = []

    def worker() -> None:
        results.append(
            chain.handle("token-t1", "/orders", "dup-open", {}, action)
        )

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    assert started.wait(timeout=2.0)
    release.set()
    for t in threads:
        t.join()
    assert effects == [1]
    assert results == ["ONCE"] * 6


# ---------- 3. TTL / LRU 保留与淘汰 ----------

def _service(clock: MutableClock, retention: RetentionConfig,
             store=None) -> IdempotencyService:
    retention_holder = type("R", (), {"get": lambda self: retention})()
    return IdempotencyService(
        store or InMemoryIdempotencyStore(clock=clock, retention=retention),
        retention_provider=retention_holder.get,
    )


def test_expired_key_is_rejected_not_silently_reexecuted() -> None:
    clock = MutableClock()
    svc = _service(clock, RetentionConfig(ttl_seconds=10, max_completed=100))
    effects: list[int] = []
    first = svc.execute("old-k", "t1", {"v": 1},
                        lambda: effects.append(1) or "FIRST")
    assert first == "FIRST"
    clock.advance(11)

    # 同键重放：明确的"结果已过期"，且不再执行业务
    with pytest.raises(GovernanceError) as exc:
        svc.execute("old-k", "t1", {"v": 1},
                    lambda: effects.append(1) or "SECOND")
    assert exc.value.reason is DenyReason.RESULT_EXPIRED
    assert effects == [1]

    # 换成不同载荷也不能借过期之名变成新请求
    with pytest.raises(GovernanceError) as exc2:
        svc.execute("old-k", "t1", {"v": 2}, lambda: effects.append(1))
    assert exc2.value.reason is DenyReason.RESULT_EXPIRED
    assert effects == [1]

    # 全新标识不受影响
    assert svc.execute("fresh-k", "t1", {"v": 1},
                       lambda: effects.append(1) or "NEW") == "NEW"


def test_lru_evicted_key_is_rejected_not_silently_reexecuted() -> None:
    clock = MutableClock()
    svc = _service(clock, RetentionConfig(ttl_seconds=0, max_completed=1))
    effects: list[int] = []
    svc.execute("k1", "t1", {}, lambda: effects.append(1) or 1)
    svc.execute("k2", "t1", {}, lambda: effects.append(1) or 2)  # 淘汰 k1
    with pytest.raises(GovernanceError) as exc:
        svc.execute("k1", "t1", {}, lambda: effects.append(1) or 11)
    assert exc.value.reason is DenyReason.RESULT_EXPIRED
    assert effects == [1, 1]
    # 未被淘汰的 k2 仍可回放
    assert svc.execute("k2", "t1", {}, lambda: effects.append(1)) == 2
    assert effects == [1, 1]


def test_recently_accessed_record_is_not_lru_evicted() -> None:
    clock = MutableClock()
    svc = _service(clock, RetentionConfig(ttl_seconds=0, max_completed=2))
    svc.execute("k1", "t1", {}, lambda: "v1")
    clock.advance(1)
    svc.execute("k2", "t1", {}, lambda: "v2")
    clock.advance(1)
    assert svc.execute("k1", "t1", {}, lambda: "X") == "v1"  # 刷新 k1
    clock.advance(1)
    svc.execute("k3", "t1", {}, lambda: "v3")  # 应淘汰最久未访问的 k2
    assert svc.execute("k1", "t1", {}, lambda: "X") == "v1"
    with pytest.raises(GovernanceError) as exc:
        svc.execute("k2", "t1", {}, lambda: "X")
    assert exc.value.reason is DenyReason.RESULT_EXPIRED


def test_inflight_record_is_never_evicted_under_capacity_pressure() -> None:
    clock = MutableClock()
    retention = RetentionConfig(ttl_seconds=0, max_completed=1)
    store = InMemoryIdempotencyStore(clock=clock, retention=retention)
    svc = _service(clock, retention, store=store)
    effects: list[int] = []
    leader_started = threading.Event()
    release = threading.Event()

    def slow() -> str:
        effects.append(1)
        leader_started.set()
        release.wait(timeout=2.0)
        return "HELD"

    def leader() -> None:
        svc.execute("hot", "t1", {}, slow)

    t = threading.Thread(target=leader)
    t.start()
    assert leader_started.wait(timeout=2.0)
    # 处理中期间制造容量压力；hot 是 INFLIGHT，不能被淘汰
    clock.advance(1)
    svc.execute("other", "t1", {}, lambda: "OTHER")
    # 容量压力下直接核对：处理中的 hot 仍是 INFLIGHT，未被翻成墓碑
    held = store.lookup("t1", "hot")
    assert held is not None and held.status is IdemStatus.INFLIGHT
    clock.advance(1)
    release.set()
    t.join()
    assert effects == [1]
    assert svc.execute("hot", "t1", {}, lambda: effects.append(1)) == "HELD"
    assert effects == [1]


def test_concurrent_replays_of_expired_key_all_get_expired() -> None:
    clock = MutableClock()
    svc = _service(clock, RetentionConfig(ttl_seconds=10, max_completed=10))
    effects: list[int] = []
    svc.execute("race", "t1", {}, lambda: effects.append(1) or "DONE")
    clock.advance(11)
    errors: list[DenyReason] = []

    def worker() -> None:
        try:
            svc.execute("race", "t1", {}, lambda: effects.append(1))
        except GovernanceError as exc:
            errors.append(exc.reason)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [DenyReason.RESULT_EXPIRED] * 8
    assert effects == [1], "绝不能出现同标识两份结果/两次副作用"


def test_retention_hot_update_applies_to_later_access() -> None:
    clock = MutableClock()
    retention = RetentionConfig(ttl_seconds=100, max_completed=100)
    store = InMemoryIdempotencyStore(clock=clock, retention=retention)
    svc = IdempotencyService(store, retention_provider=lambda: retention)
    svc.execute("rk", "t1", {}, lambda: "V")
    clock.advance(50)
    assert svc.execute("rk", "t1", {}, lambda: "X") == "V"  # 旧 TTL 内
    retention = RetentionConfig(ttl_seconds=10, max_completed=100)  # 热缩短
    with pytest.raises(GovernanceError) as exc:
        svc.execute("rk", "t1", {}, lambda: "X")
    assert exc.value.reason is DenyReason.RESULT_EXPIRED


# ---------- 4. 按提交标识只读查询 ----------

def test_query_distinguishes_all_states() -> None:
    clock = MutableClock()
    store = InMemoryIdempotencyStore(
        clock=clock, retention=RetentionConfig(ttl_seconds=10)
    )
    cur_retention = lambda: store._retention
    svc = IdempotencyService(store, retention_provider=cur_retention)

    # 从未见过
    view = svc.query("nope", "t1")
    assert view.status is SubmissionStatus.EXPIRED_OR_UNKNOWN
    assert view.result is None

    # 处理中
    started = threading.Event()
    release = threading.Event()

    def slow() -> str:
        started.set()
        release.wait(timeout=2.0)
        return "SETTLED"

    def run() -> None:
        svc.execute("live", "t1", {}, slow)

    t = threading.Thread(target=run)
    t.start()
    assert started.wait(timeout=2.0)
    assert svc.query("live", "t1").status is SubmissionStatus.PROCESSING
    release.set()
    t.join()

    # 已完成（含结论）
    done = svc.query("live", "t1")
    assert done.status is SubmissionStatus.COMPLETED
    assert done.result == "SETTLED"

    # 已过期
    clock.advance(11)
    expired = svc.query("live", "t1")
    assert expired.status is SubmissionStatus.EXPIRED_OR_UNKNOWN
    assert expired.result is None


def test_query_failed_state_after_leader_business_failure() -> None:
    clock = MutableClock()
    svc = _service(clock, RetentionConfig(ttl_seconds=100))

    def boom() -> None:
        raise RuntimeError("biz down")

    with pytest.raises(RuntimeError):
        svc.execute("fk", "t1", {}, boom)
    view = svc.query("fk", "t1")
    assert view.status is SubmissionStatus.FAILED
    assert view.result is None


def test_query_unverifiable_state_after_side_effect_but_commit_lost() -> None:
    clock = MutableClock()
    store = CompleteFailingStore(
        clock=clock, retention=RetentionConfig(ttl_seconds=100)
    )
    svc = IdempotencyService(store, retention_provider=lambda: store._retention)
    effects: list[int] = []
    with pytest.raises(GovernanceError) as exc:
        svc.execute("uncertain", "t1", {},
                    lambda: effects.append(1) or "SIDE-EFFECT")
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE
    assert effects == [1], "副作用已发生一次"

    view = svc.query("uncertain", "t1")
    assert view.status is SubmissionStatus.UNVERIFIABLE

    # 同键再次写必须被挡住，不能重放成新请求再执行一次
    with pytest.raises(GovernanceError) as exc2:
        svc.execute("uncertain", "t1", {},
                    lambda: effects.append(1) or "AGAIN")
    assert exc2.value.reason is DenyReason.RESULT_UNVERIFIABLE
    assert effects == [1]


def test_query_is_tenant_isolated() -> None:
    clock = MutableClock()
    svc = _service(clock, RetentionConfig(ttl_seconds=100))
    svc.execute("shared-key", "t1", {}, lambda: "T1-RESULT")
    # 另一租户用相同标识：互不可见、互不回放
    assert svc.query("shared-key", "t2").status is \
        SubmissionStatus.EXPIRED_OR_UNKNOWN
    assert svc.execute("shared-key", "t2", {}, lambda: "T2-RESULT") == \
        "T2-RESULT"
    v1 = svc.query("shared-key", "t1")
    v2 = svc.query("shared-key", "t2")
    assert v1.status is SubmissionStatus.COMPLETED and v1.result == "T1-RESULT"
    assert v2.status is SubmissionStatus.COMPLETED and v2.result == "T2-RESULT"


def test_query_does_not_consume_quota_or_run_side_effects() -> None:
    chain, quota, _, _ = _build_chain(limit=1)
    chain.handle("token-t1", "/orders", "q-k", {"v": 1}, lambda: "OK")
    for _ in range(5):
        view = chain.query_submission("token-t1", "/orders", "q-k")
        assert view.status is SubmissionStatus.COMPLETED
    assert quota._store.current("t1", "/orders", 60) == 1


def test_query_requires_identity_and_permission() -> None:
    chain, _, _, _ = _build_chain()
    with pytest.raises(GovernanceError) as exc:
        chain.query_submission(None, "/orders", "k")
    assert exc.value.reason is DenyReason.MISSING_IDENTITY
    with pytest.raises(GovernanceError) as exc2:
        chain.query_submission("bogus-token", "/orders", "k")
    assert exc2.value.reason is DenyReason.MISSING_IDENTITY
    # t3 已识别身份但只有 /health 权限：越权查询 /orders 结论被拒
    with pytest.raises(GovernanceError) as exc3:
        chain.query_submission("token-t3", "/orders", "k")
    assert exc3.value.reason is DenyReason.ACCESS_DENIED


# ---------- 5. 首请求失败时并发跟随方结论一致、可安全重试（失败终态） ----------

class _SlowQuotaStore(InMemoryQuotaStore):
    """首笔占用阻塞在闸门上，便于并发跟随方先进入等待。"""

    def __init__(self, clock, gate: threading.Event) -> None:
        super().__init__(clock)
        self._gate = gate
        self._calls = 0

    def try_acquire(self, tenant_id, api, window_seconds, limit):
        self._calls += 1
        if self._calls == 1:
            self._gate.wait(timeout=2.0)
        return super().try_acquire(tenant_id, api, window_seconds, limit)


def test_followers_get_same_quota_denial_when_leader_rejected() -> None:
    """leader 在配额闸门被拒时，并发跟随方拿到与首次一致的 quota_exhausted，
    而不是笼统的 503；也不产生任何副作用。"""
    gate = threading.Event()
    clock = MutableClock()
    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=1, window_seconds=60),)
    )
    policies = PolicyRegistry(cfg)
    qstore = _SlowQuotaStore(clock, gate)
    qstore._counters[("t1", "/orders", int(clock() // 60))] = 1  # 预先耗尽
    quota = QuotaEnforcer(cfg, qstore, clock=clock)
    idem = IdempotencyService(
        InMemoryIdempotencyStore(clock=clock),
        retention_provider=policies.retention,
    )
    chain = GovernanceMiddleware(_identity(), quota, idem, policies=policies)

    effects: list[int] = []
    reasons: list[DenyReason] = []
    lock = threading.Lock()

    def run() -> None:
        try:
            chain.handle("token-t1", "/orders", "qdeny-k", {"v": 1},
                         lambda: effects.append(1))
        except GovernanceError as exc:
            with lock:
                reasons.append(exc.reason)

    leader = threading.Thread(target=run)
    followers = [threading.Thread(target=run) for _ in range(4)]
    leader.start()
    import time
    time.sleep(0.1)
    for f in followers:
        f.start()
    time.sleep(0.1)
    gate.set()
    leader.join()
    for f in followers:
        f.join()
    assert reasons == [DenyReason.QUOTA_EXHAUSTED] * 5
    assert effects == []


def test_business_failure_followers_uniform_and_same_key_retry_succeeds() -> None:
    svc = _service(MutableClock(), RetentionConfig())
    attempts = {"n": 0}
    started = threading.Event()
    release = threading.Event()

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            started.set()
            release.wait(timeout=2.0)
            raise RuntimeError("boom")
        return "RECOVERED"

    reasons: list[DenyReason] = []
    lock = threading.Lock()

    def run() -> None:
        try:
            svc.execute("bf-k", "t1", {}, flaky)
        except GovernanceError as exc:
            with lock:
                reasons.append(exc.reason)
        except RuntimeError:
            # 首请求线程会把业务异常原样向上抛（治理层不吞业务异常）
            pass

    leader = threading.Thread(target=run)
    followers = [threading.Thread(target=run) for _ in range(4)]
    leader.start()
    assert started.wait(timeout=2.0)
    import time
    for f in followers:
        f.start()
    time.sleep(0.1)
    release.set()
    leader.join()
    for f in followers:
        f.join()
    assert reasons == [DenyReason.DEPENDENCY_UNAVAILABLE] * 4
    # 同键同载荷安全重试：只有一次新的业务执行（总计 2 次）
    assert svc.execute("bf-k", "t1", {}, flaky) == "RECOVERED"
    assert attempts["n"] == 2


def test_retry_after_failure_recharges_quota_once_as_new_leader() -> None:
    """失败终态被原子回收后，重试作为新首请求再次占配额（受当前额度约束）。"""
    clock = MutableClock()
    chain, quota, _, clock = _build_chain(limit=1, clock=clock)
    # 首请求业务失败
    with pytest.raises(RuntimeError):
        chain.handle("token-t1", "/orders", "retry-k", {"v": 1},
                     _raise_once)
    assert quota._store.current("t1", "/orders", 60) == 1  # 首笔占过额度
    # 跨入下一窗口，额度恢复；同键重试作为新首请求占新窗口额度并成功
    clock.advance(61)
    out = chain.handle("token-t1", "/orders", "retry-k", {"v": 1},
                       lambda: "OK")
    assert out == "OK"
    assert quota._store.current("t1", "/orders", 60) == 1


def _raise_once() -> None:
    raise RuntimeError("temporary business failure")
