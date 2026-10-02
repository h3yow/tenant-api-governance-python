"""重试治理增强：首次被拒后的重试结算、保留配置运行中生效、边界并发稳定。

覆盖三类场景：
A. 首次被额度拒绝后，同标识并发重试 / 断线重连 / 跨窗口重试都回放同一份
   拒绝结论，不新增配额占用、不产生业务副作用；只有全新标识照常判定。
B. 保留时长与容量上限在运行中调整（PolicyRegistry.update）后立即生效，
   调整之后进入的一批并发请求统一按新配置判定；处理中、刚被提交或查询过
   的结论不被误清。
C. 临近过期 / 容量淘汰边界的并发重试行为稳定：保留期内回放同一条结论，
   超期/淘汰后明确返回 RESULT_EXPIRED，绝不二次执行、不重复占额。
"""

from __future__ import annotations

import threading

import pytest

from governance.config import (
    GovernanceConfig,
    PolicyRegistry,
    QuotaRule,
    RetentionConfig,
)
from governance.errors import DenyReason, GovernanceError
from governance.idempotency import (
    InMemoryIdempotencyStore,
    IdempotencyService,
    QueryStatus,
)
from governance.identity import IdentityResolver
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer, StoreUnavailable


def _identity() -> IdentityResolver:
    return IdentityResolver(
        {
            "token-t1": ("t1", "alice", frozenset({"/orders"})),
            "token-t2": ("t2", "bob", frozenset({"/orders"})),
        }
    )


class _ToggleQuotaStore:
    """可切换 正常/故障 的配额存储：验证拒绝结论在依赖故障时仍一致回放。"""

    def __init__(self, inner: InMemoryQuotaStore) -> None:
        self._inner = inner
        self.healthy = True

    def try_acquire(self, tenant_id, api, window_seconds, limit):
        if not self.healthy:
            raise StoreUnavailable("quota backend connection refused")
        return self._inner.try_acquire(tenant_id, api, window_seconds, limit)

    def current(self, tenant_id, api, window_seconds):
        return self._inner.current(tenant_id, api, window_seconds)


def _chain(
    limit: int = 1,
    window: int = 60,
    times: list[float] | None = None,
    retention: RetentionConfig | None = None,
    quota_store=None,
):
    """装配完整治理链；保留配置经 PolicyRegistry 供结算存储实时读取。"""
    times = times if times is not None else [1000.0]

    def clock() -> float:
        return times[0]

    cfg = GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=limit,
                               window_seconds=window),),
        retention=retention or RetentionConfig(),
    )
    registry = PolicyRegistry(cfg)
    qstore = quota_store or InMemoryQuotaStore(clock)
    quota = QuotaEnforcer(registry, qstore, clock=clock)
    idem_store = InMemoryIdempotencyStore(
        clock=clock,
        retention_provider=lambda: registry.get().retention,
    )
    chain = GovernanceMiddleware(
        _identity(), quota, IdempotencyService(idem_store), registry
    )
    return chain, registry, qstore, idem_store, times


def _run_concurrent(fn, n: int = 12) -> list:
    """同一闸门放行 n 个并发调用，返回各自结果（按完成顺序）。"""
    gate = threading.Event()
    results: list = []
    lock = threading.Lock()

    def worker() -> None:
        gate.wait(timeout=2.0)
        out = fn()
        with lock:
            results.append(out)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()
    return results


def _attempt(chain, key, payload, effects, tag):
    """发起一次写请求；返回 'executed' 或拒绝原因。"""
    try:
        chain.handle("token-t1", "/orders", key, payload,
                     lambda: (effects.append(tag), tag)[1])
        return "executed"
    except GovernanceError as exc:
        return exc.reason


# ---------------------------------------------------------------------------
# A. 首次被额度拒绝后的重试结算
# ---------------------------------------------------------------------------

def test_quota_denied_retry_replays_same_denial_across_windows() -> None:
    """首次额度拒绝后：并发重试、跨窗口重试都回放同一 429，不新增占用。"""
    chain, _, qstore, _, times = _chain(limit=1, window=60)
    effects: list[str] = []

    # 首次提交成功，用掉本窗口唯一额度。
    assert _attempt(chain, "k-ok", {"v": 1}, effects, "ok") == "executed"
    # 全新标识 + 额度耗尽 -> 照常 429；该拒绝被落定为 k-denied 的结论。
    assert _attempt(chain, "k-denied", {"v": 2}, effects, "denied") \
        is DenyReason.QUOTA_EXHAUSTED
    assert effects == ["ok"]
    assert qstore.current("t1", "/orders", 60) == 1

    # 一批并发重试同一被拒标识：全部拿到与首次完全一样的 429。
    decisions = _run_concurrent(
        lambda: _attempt(chain, "k-denied", {"v": 2}, effects, "again"), n=12
    )
    assert len(decisions) == 12
    assert all(d is DenyReason.QUOTA_EXHAUSTED for d in decisions), decisions
    assert effects == ["ok"], "被拒标识的重试不得产生业务副作用"
    assert qstore.current("t1", "/orders", 60) == 1, "重试不得新增配额占用"

    # 跨到下一个时间窗口：仍回放同一拒绝，而不是当新请求再执行、再扣额度。
    times[0] = 1060.0
    assert _attempt(chain, "k-denied", {"v": 2}, effects, "cross-window") \
        is DenyReason.QUOTA_EXHAUSTED
    assert effects == ["ok"]
    assert qstore.current("t1", "/orders", 60) == 0, "跨窗口重试不得占新窗口额度"

    # 新窗口里真正全新的标识照常判定：有额度则成功，额度耗尽则 429。
    assert _attempt(chain, "k-new", {"v": 3}, effects, "new") == "executed"
    assert qstore.current("t1", "/orders", 60) == 1
    assert _attempt(chain, "k-brand-new", {"v": 4}, effects, "brand-new") \
        is DenyReason.QUOTA_EXHAUSTED
    assert effects == ["ok", "new"]


def test_denial_replay_consistent_when_quota_backend_degrades() -> None:
    """首次 429 之后配额依赖故障：同标识重试仍一致回放 429，不混出 503。"""
    times = [1000.0]

    def clock() -> float:
        return times[0]

    toggle = _ToggleQuotaStore(InMemoryQuotaStore(clock))
    chain, _, _, _, _ = _chain(limit=1, window=60, times=times,
                               quota_store=toggle)
    effects: list[str] = []
    assert _attempt(chain, "k-ok", {"v": 1}, effects, "ok") == "executed"
    assert _attempt(chain, "k-denied", {"v": 2}, effects, "denied") \
        is DenyReason.QUOTA_EXHAUSTED

    # 配额依赖转为故障：此时同标识重试必须仍回放首次的 429 结论，
    # 绝不能同一批里有的 429、有的 503。
    toggle.healthy = False
    decisions = _run_concurrent(
        lambda: _attempt(chain, "k-denied", {"v": 2}, effects, "again"), n=12
    )
    assert all(d is DenyReason.QUOTA_EXHAUSTED for d in decisions), decisions
    assert DenyReason.DEPENDENCY_UNAVAILABLE not in decisions
    assert effects == ["ok"]


def test_concurrent_first_submissions_on_exhausted_quota_share_one_denial() -> None:
    """同一标识的首批并发提交撞上额度耗尽：全部得到同一份 429 结论。"""
    chain, _, qstore, idem_store, _ = _chain(limit=1, window=60)
    effects: list[str] = []
    assert _attempt(chain, "k-ok", {"v": 1}, effects, "ok") == "executed"

    # 12 个并发请求携带同一新标识同时到达：一个成为 leader 被拒并落定结论，
    # 其余作为跟随者/重放者拿到同一份拒绝。
    decisions = _run_concurrent(
        lambda: _attempt(chain, "k-race", {"v": 9}, effects, "race"), n=12
    )
    assert all(d is DenyReason.QUOTA_EXHAUSTED for d in decisions), decisions
    assert effects == ["ok"], "被拒请求不得产生任何业务副作用"
    assert qstore.current("t1", "/orders", 60) == 1
    # 同一标识在任何时刻最多对应一份结论。
    records = [k for (tenant, k) in idem_store._records if k == "k-race"]
    assert len(records) == 1


def test_rejected_conclusion_is_queryable_and_conflict_still_checked() -> None:
    """被拒结论可只读查询（rejected）；同键不同载荷仍按冲突拒绝。"""
    chain, _, _, _, _ = _chain(limit=1, window=60)
    effects: list[str] = []
    assert _attempt(chain, "k-ok", {"v": 1}, effects, "ok") == "executed"
    assert _attempt(chain, "k-denied", {"v": 2}, effects, "denied") \
        is DenyReason.QUOTA_EXHAUSTED

    out = chain.query_settlement("token-t1", "/orders", "k-denied")
    assert out.status is QueryStatus.REJECTED

    # 同键携带语义不同的载荷：仍是 409 冲突，而不是回放 429。
    assert _attempt(chain, "k-denied", {"v": 999}, effects, "conflict") \
        is DenyReason.IDEMPOTENCY_CONFLICT
    assert effects == ["ok"]


# ---------------------------------------------------------------------------
# B. 保留时长与容量上限：运行中调整立即生效、并发判定一致
# ---------------------------------------------------------------------------

def _retention_only_config(ttl, cap, tombstone=3600.0) -> GovernanceConfig:
    return GovernanceConfig(
        quota_rules=(QuotaRule("t1", "/orders", limit=100, window_seconds=60),),
        retention=RetentionConfig(result_ttl_seconds=ttl,
                                  max_completed_records=cap,
                                  tombstone_ttl_seconds=tombstone),
    )


def test_ttl_shrink_takes_effect_immediately_and_uniformly() -> None:
    """运行中调小保留时长：之后进入的一批并发重试统一按新 TTL 判定。"""
    retention = RetentionConfig(result_ttl_seconds=1000.0,
                                max_completed_records=None,
                                tombstone_ttl_seconds=3600.0)
    # 配额窗口放大到 600s，让全程处于同一窗口内便于断言占用数。
    chain, registry, qstore, _, times = _chain(limit=100, window=600,
                                               retention=retention)
    effects: list[str] = []
    assert _attempt(chain, "hk", {"v": 1}, effects, "R") == "executed"

    # 运行中把 TTL 从 1000s 调小为 30s。
    registry.update(_retention_only_config(ttl=30.0, cap=None))

    # 新配置立即生效但尚未超期：仍回放（同时刷新最近访问时间到 1001）。
    times[0] = 1001.0
    assert _attempt(chain, "hk", {"v": 1}, effects, "x") == "executed"
    assert effects == ["R"], "回放不得产生新副作用"

    # 越过新 TTL（1001 + 30）后：一批并发重试统一判定为已过期，
    # 不会一半按旧 TTL 回放、一半按新 TTL 过期。
    times[0] = 1032.0
    decisions = _run_concurrent(
        lambda: _attempt(chain, "hk", {"v": 1}, effects, "y"), n=16
    )
    assert len(decisions) == 16
    assert all(d is DenyReason.RESULT_EXPIRED for d in decisions), decisions
    assert effects == ["R"], "过期标识绝不二次执行"
    assert qstore.current("t1", "/orders", 600) == 1, "过期重试不得再占额度"


def test_capacity_shrink_evicts_immediately_but_protects_recent() -> None:
    """运行中调小容量上限：立即按 LRU 淘汰；刚被查询/提交的结论不被误清。"""
    retention = RetentionConfig(result_ttl_seconds=None,
                                max_completed_records=5,
                                tombstone_ttl_seconds=3600.0)
    chain, registry, _, _, times = _chain(limit=100, retention=retention)
    effects: list[str] = []
    for i in range(1, 6):
        assert _attempt(chain, f"k{i}", {"v": i}, effects, f"r{i}") == "executed"

    # 查询 k1，使其成为最近访问（LRU 最新）。
    out = chain.query_settlement("token-t1", "/orders", "k1")
    assert out.status is QueryStatus.COMPLETED

    # 运行中把容量上限从 5 调小为 2。
    registry.update(_retention_only_config(ttl=None, cap=2))

    # 下一个请求触发淘汰：最久未访问的 k2/k3/k4 被淘汰为墓碑；
    # 新键 k6 落定后再挤掉 k5；刚被查询的 k1 与新提交的 k6 保留。
    times[0] = 1001.0
    assert _attempt(chain, "k6", {"v": 6}, effects, "r6") == "executed"

    for evicted in ("k2", "k3", "k4", "k5"):
        assert _attempt(chain, evicted, {"v": int(evicted[1])},
                        effects, "again") is DenyReason.RESULT_EXPIRED, evicted
    assert _attempt(chain, "k1", {"v": 1}, effects, "again") == "executed", \
        "刚被查询过的结论不得被误清"
    assert _attempt(chain, "k6", {"v": 6}, effects, "again") == "executed"
    # 每个标识的副作用都恰好一次。
    assert sorted(effects) == ["r1", "r2", "r3", "r4", "r5", "r6"]


def test_inflight_submission_survives_retention_shrink() -> None:
    """调整保留配置时正在处理中的提交不被误清，并发跟随者仍拿到同一结论。"""
    retention = RetentionConfig(result_ttl_seconds=5.0,
                                max_completed_records=1,
                                tombstone_ttl_seconds=3600.0)
    chain, registry, _, _, times = _chain(limit=100, retention=retention)
    effects: list[str] = []
    started = threading.Event()
    release = threading.Event()

    def slow() -> str:
        started.set()
        release.wait(timeout=2.0)
        effects.append("slow")
        return "SLOW-DONE"

    outcomes: list[object] = []

    def leader() -> None:
        try:
            outcomes.append(chain.handle("token-t1", "/orders", "inflight-k",
                                         {"v": 1}, slow))
        except GovernanceError as exc:
            outcomes.append(exc.reason)

    t = threading.Thread(target=leader)
    t.start()
    assert started.wait(timeout=2.0)

    # 处理中期间：时钟越过 TTL、容量调到 0，都不能清掉处理中的记录。
    times[0] = 1100.0
    registry.update(_retention_only_config(ttl=5.0, cap=0))

    follower_outcomes = _run_concurrent(
        lambda: _attempt(chain, "inflight-k", {"v": 1}, effects, "dup"), n=6
    )
    release.set()
    t.join()

    assert outcomes == ["SLOW-DONE"]
    assert follower_outcomes == ["executed"] * 6, \
        "处理中的结论不得被保留配置调整误清"
    assert effects == ["slow"], "副作用必须且只能发生一次"


# ---------------------------------------------------------------------------
# C. 临近过期 / 淘汰边界的并发重试
# ---------------------------------------------------------------------------

def test_expiry_boundary_concurrent_retries_are_stable() -> None:
    """过期边界两侧并发重试：边界前统一回放，边界后统一 410，副作用一次。"""
    retention = RetentionConfig(result_ttl_seconds=50.0,
                                max_completed_records=None,
                                tombstone_ttl_seconds=3600.0)
    chain, _, qstore, _, times = _chain(limit=100, window=600,
                                        retention=retention)
    effects: list[str] = []
    assert _attempt(chain, "bk", {"v": 1}, effects, "R") == "executed"
    assert qstore.current("t1", "/orders", 600) == 1

    # 边界前（1000 + 50 之前）：一批并发重试统一回放首次结论。
    times[0] = 1049.0
    before = _run_concurrent(
        lambda: _attempt(chain, "bk", {"v": 1}, effects, "x"), n=8
    )
    assert before == ["executed"] * 8, before
    assert effects == ["R"]

    # 到达边界（最近访问 1049 + 50）：一批并发重试统一判定为已过期。
    times[0] = 1099.0
    after = _run_concurrent(
        lambda: _attempt(chain, "bk", {"v": 1}, effects, "y"), n=8
    )
    assert all(d is DenyReason.RESULT_EXPIRED for d in after), after
    assert effects == ["R"], "边界并发下副作用仍然最多一次"
    assert qstore.current("t1", "/orders", 600) == 1, "边界重试不得重复占额"


def test_eviction_race_with_concurrent_retry_never_double_executes() -> None:
    """容量淘汰与并发重试同时发生：要么回放同一结论，要么明确过期。"""
    retention = RetentionConfig(result_ttl_seconds=None,
                                max_completed_records=1,
                                tombstone_ttl_seconds=3600.0)
    chain, _, qstore, idem_store, times = _chain(limit=100, retention=retention)
    effects: list[str] = []
    assert _attempt(chain, "victim", {"v": 1}, effects, "R1") == "executed"

    gate = threading.Event()
    retry_decisions: list = []

    def retry_worker() -> None:
        gate.wait(timeout=2.0)
        retry_decisions.append(
            _attempt(chain, "victim", {"v": 1}, effects, "R1-again")
        )

    def evicting_worker() -> None:
        gate.wait(timeout=2.0)
        # 新键落定会把容量 1 中的 victim 挤成墓碑。
        _attempt(chain, "newcomer", {"v": 2}, effects, "R2")

    threads = ([threading.Thread(target=retry_worker) for _ in range(10)]
               + [threading.Thread(target=evicting_worker)])
    for t in threads:
        t.start()
    times[0] = 1001.0
    gate.set()
    for t in threads:
        t.join()

    assert len(retry_decisions) == 10
    assert set(retry_decisions) <= {"executed", DenyReason.RESULT_EXPIRED}, \
        retry_decisions
    assert effects.count("R1") == 1, "victim 的副作用不得发生第二次"
    assert effects.count("R2") == 1, "newcomer 的副作用必须恰好一次"
    assert effects.count("R1-again") == 0, "重试不得再次执行 victim 的业务"
    assert qstore.current("t1", "/orders", 60) == 2, "只有两次真实写占用额度"
    # 同一标识任何时刻最多一份结论（victim 要么在记录中，要么只剩墓碑）。
    records = [k for (tenant, k) in idem_store._records if k == "victim"]
    assert len(records) <= 1
