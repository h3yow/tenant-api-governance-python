"""保留与容量配置的运行中调整：改完立即生效、并发判定一致、热数据不误清。

业务背景：结论的保留时长（result_ttl_seconds）与每租户容量上限
（max_completed_records）需要支持不重启调整。调整之后：
- 后续请求立即按新值判定，不能还按旧的过期时间/旧的上限在跑；
- 调整后进入的一批并发请求统一按新配置，不能一半旧一半新；
- 正在处理中、刚被提交或查询过的结论不能被误清掉；
- 同一提交标识在任何时刻最多对应一份结论。
"""

from __future__ import annotations

import threading

import pytest

from governance.config import RetentionConfig
from governance.errors import DenyReason, GovernanceError
from governance.idempotency import (
    InMemoryIdempotencyStore,
    IdempotencyService,
    QueryStatus,
)


def _service(retention: RetentionConfig, now: list[float]):
    clock = lambda: now[0]
    store = InMemoryIdempotencyStore(retention=retention, clock=clock)
    return IdempotencyService(store), store


def _retention(ttl=None, cap=None, tomb=3600.0) -> RetentionConfig:
    return RetentionConfig(result_ttl_seconds=ttl,
                           max_completed_records=cap,
                           tombstone_ttl_seconds=tomb)


def test_ttl_shrink_takes_effect_immediately() -> None:
    """运行中缩短保留时长：之后的判定立即按新时长，不再按旧值。"""
    now = [1000.0]
    svc, store = _service(_retention(ttl=100.0), now)
    svc.execute("k", "t1", {"v": 1}, lambda: "R")

    # 缩短为 10s：15s 后按旧配置（100s）仍在保留期内，按新配置已超期。
    store.update_retention(_retention(ttl=10.0))
    now[0] = 1015.0
    with pytest.raises(GovernanceError) as exc:
        svc.execute("k", "t1", {"v": 1}, lambda: "R2")
    assert exc.value.reason is DenyReason.RESULT_EXPIRED, (
        "缩短保留时长后必须立即按新值判定，不能还按旧的 100s 在跑"
    )


def test_ttl_extend_takes_effect_immediately() -> None:
    """运行中延长保留时长：原本会过期的结论按新值继续可回放。"""
    now = [1000.0]
    svc, store = _service(_retention(ttl=10.0), now)
    svc.execute("k", "t1", {"v": 1}, lambda: "R")

    # 延长为 1000s：50s 后按旧配置（10s）早该过期，按新配置必须仍可回放。
    store.update_retention(_retention(ttl=1000.0))
    now[0] = 1050.0
    assert svc.execute("k", "t1", {"v": 1}, lambda: "R2") == "R", (
        "延长保留时长后必须立即按新值判定"
    )


def test_capacity_shrink_evicts_immediately_but_protects_hot_records() -> None:
    """运行中调小容量上限：立即淘汰到新区间；刚访问过的与处理中的不清。"""
    now = [1000.0]
    svc, store = _service(_retention(cap=10), now)
    for i in range(5):
        now[0] = 1000.0 + i
        svc.execute(f"k{i}", "t1", {}, lambda i=i: f"r{i}")
    # 把 k0 访问成最热，k4 是次新；k1 最久未访问。
    now[0] = 1010.0
    assert svc.execute("k0", "t1", {}, lambda: "x") == "r0"

    # 处理中的记录：另起一个键保持 INFLIGHT。
    release = threading.Event()
    started = threading.Event()

    def slow() -> str:
        started.set()
        release.wait(timeout=5.0)
        return "HOT"

    outcome_box: list[object] = []
    lt = threading.Thread(
        target=lambda: outcome_box.append(
            svc.execute("k-inflight", "t1", {}, slow)))
    lt.start()
    assert started.wait(timeout=2.0)

    # 容量从 10 调到 2：立即淘汰到只剩 2 条最热的已落定结论。
    store.update_retention(_retention(cap=2))
    assert svc.query("t1", "k0").status is QueryStatus.COMPLETED, "刚访问过的不清"
    assert svc.query("t1", "k4").status is QueryStatus.COMPLETED, "次新保留"
    for cold in ("k1", "k2", "k3"):
        assert svc.query("t1", cold).status is QueryStatus.EXPIRED_OR_NOT_FOUND
    # 处理中的记录不受容量淘汰影响。
    assert svc.query("t1", "k-inflight").status is QueryStatus.INFLIGHT

    release.set()
    lt.join(timeout=5.0)
    assert outcome_box == ["HOT"], "处理中的结论不得被容量调整误清"


def test_just_queried_conclusion_survives_ttl_shrink() -> None:
    """刚被查询过的结论，即使把保留时长调小也不会被误清（空闲过期）。"""
    now = [1000.0]
    svc, store = _service(_retention(ttl=100.0), now)
    svc.execute("k", "t1", {"v": 1}, lambda: "R")

    # 结论已完成 90s，但刚刚被查询/回放过。
    now[0] = 1090.0
    assert svc.query("t1", "k").status is QueryStatus.COMPLETED

    # 把保留时长调到 30s：若按完成时间算（90s > 30s）会被误清；
    # 按最近访问起算则必须保留。
    store.update_retention(_retention(ttl=30.0))
    assert svc.query("t1", "k").status is QueryStatus.COMPLETED, (
        "刚被查询过的结论不得因调小保留时长被误清"
    )
    # 之后再空闲 31s 才允许过期。
    now[0] = 1090.0 + 31.0
    assert svc.query("t1", "k").status is QueryStatus.EXPIRED_OR_NOT_FOUND


def test_concurrent_batch_after_update_uniformly_uses_new_config() -> None:
    """调整生效后进入的一批并发请求：统一按新配置判定，不半旧半新。"""
    now = [1000.0]
    svc, store = _service(_retention(ttl=100.0), now)
    svc.execute("k", "t1", {"v": 1}, lambda: "R")

    # 先把时间推进到旧配置仍有效、新配置已过期的点上，再调小保留时长。
    now[0] = 1050.0
    store.update_retention(_retention(ttl=10.0))

    gate = threading.Event()
    outcomes: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        gate.wait(timeout=2.0)
        try:
            svc.execute("k", "t1", {"v": 1}, lambda: "R2")
            outcome = "replayed"
        except GovernanceError as exc:
            outcome = exc.reason.value
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    assert len(outcomes) == 20
    assert set(outcomes) == {DenyReason.RESULT_EXPIRED.value}, (
        "调整后进入的一批并发请求必须统一按新配置判定"
    )


def test_concurrent_batch_after_extend_uniformly_replays() -> None:
    """反向场景：调长保留时长后，一批并发重试统一回放，无人收到过期。"""
    now = [1000.0]
    svc, store = _service(_retention(ttl=10.0), now)
    svc.execute("k", "t1", {"v": 1}, lambda: "R")

    now[0] = 1050.0  # 旧配置下早已过期
    store.update_retention(_retention(ttl=1000.0))

    gate = threading.Event()
    results: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        gate.wait(timeout=2.0)
        try:
            r = svc.execute("k", "t1", {"v": 1}, lambda: "R2")
            with lock:
                results.append(r)
        except GovernanceError as exc:
            with lock:
                results.append(exc.reason.value)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    assert results == ["R"] * 20, (
        "调长保留时长后，同批并发重试必须统一回放首次结论"
    )


def test_single_conclusion_per_key_under_update_and_replay() -> None:
    """配置调整与并发重试交错：同一标识任何时刻只有一份结论。"""
    now = [1000.0]
    svc, store = _service(_retention(ttl=100.0, cap=100), now)
    effects: list[int] = []
    svc.execute("k", "t1", {"v": 1}, lambda: (effects.append(1), "R")[1])

    stop = threading.Event()
    errors: list[Exception] = []

    def replayer() -> None:
        while not stop.is_set():
            try:
                assert svc.execute("k", "t1", {"v": 1},
                                   lambda: effects.append(1)) == "R"
            except GovernanceError as exc:  # 只允许明确的过期类结论
                if exc.reason is not DenyReason.RESULT_EXPIRED:
                    errors.append(exc)
                    return

    def updater() -> None:
        for ttl in (50.0, 200.0, 80.0, 500.0):
            store.update_retention(_retention(ttl=ttl, cap=100))

    threads = ([threading.Thread(target=replayer) for _ in range(4)]
               + [threading.Thread(target=updater)])
    for t in threads:
        t.start()
    for t in threads[4:]:
        t.join(timeout=10.0)
    threading.Event().wait(0.2)
    stop.set()
    for t in threads[:4]:
        t.join(timeout=10.0)

    assert not errors, f"出现非预期结论: {errors}"
    assert effects == [1], "任何时刻不得出现第二次业务副作用"
