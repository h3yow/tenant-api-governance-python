"""幂等治理测试：并发重复提交只产生一次副作用、结论一致、冲突明确拒绝。"""

from __future__ import annotations

import threading

import pytest

from governance.errors import DenyReason, GovernanceError
from governance.idempotency import (
    FailingIdempotencyStore,
    InMemoryIdempotencyStore,
    IdempotencyService,
    stable_payload_hash,
)


@pytest.fixture()
def service() -> IdempotencyService:
    return IdempotencyService(InMemoryIdempotencyStore())


def test_sequential_duplicate_replays_same_result_with_one_side_effect(
    service: IdempotencyService,
) -> None:
    calls: list[int] = []

    def action() -> dict:
        calls.append(1)
        return {"order_id": "ORD-1"}

    first = service.execute("key-1", "t1", {"item": "book"}, action)
    replay = service.execute("key-1", "t1", {"item": "book"}, action)
    assert first == replay == {"order_id": "ORD-1"}
    assert len(calls) == 1


def test_same_semantic_payload_different_key_order_is_not_conflict() -> None:
    """JSON 键顺序不同但语义相同 -> 视为同一载荷，回放而非冲突。"""
    svc = IdempotencyService(InMemoryIdempotencyStore())
    calls = []
    svc.execute("k", "t1", {"a": 1, "b": 2}, lambda: (calls.append(1), "v")[1])
    svc.execute("k", "t1", {"b": 2, "a": 1}, lambda: (calls.append(1), "v")[1])
    assert len(calls) == 1
    assert stable_payload_hash({"a": 1, "b": 2}) == stable_payload_hash(
        {"b": 2, "a": 1}
    )


def test_different_payload_same_key_is_conflict(service: IdempotencyService) -> None:
    service.execute("key-2", "t1", {"amount": 100}, lambda: "ok")
    with pytest.raises(GovernanceError) as exc:
        service.execute("key-2", "t1", {"amount": 200}, lambda: "ok")
    assert exc.value.reason is DenyReason.IDEMPOTENCY_CONFLICT


def test_concurrent_duplicates_have_exactly_one_side_effect() -> None:
    svc = IdempotencyService(InMemoryIdempotencyStore())
    side_effects = {"n": 0}
    leader_started = threading.Event()
    release = threading.Event()

    def slow_action() -> str:
        side_effects["n"] += 1
        leader_started.set()
        release.wait(timeout=2.0)
        return "COMMITTED"

    outcomes: list[object] = []
    outcome_lock = threading.Lock()

    def worker() -> None:
        out = svc.execute("hot-key", "t1", {"x": 1}, slow_action)
        with outcome_lock:
            outcomes.append(out)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    assert leader_started.wait(timeout=2.0)
    # 给跟随者留出进入等待的时间，再放行首请求
    release.set()
    for t in threads:
        t.join()

    assert side_effects["n"] == 1, "业务副作用必须且只能发生一次"
    assert outcomes == ["COMMITTED"] * 16, "所有并发提交必须得到一致结论"


def test_leader_business_failure_releases_for_retry(
    service: IdempotencyService,
) -> None:
    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("temporary business failure")
        return "RECOVERED"

    with pytest.raises(RuntimeError):
        service.execute("retry-key", "t1", {}, flaky)
    assert service.execute("retry-key", "t1", {}, flaky) == "RECOVERED"
    assert attempts["n"] == 2


def test_missing_idempotency_key_executes_every_time(
    service: IdempotencyService,
) -> None:
    calls = []
    for _ in range(3):
        service.execute(None, "t1", {"v": 1}, lambda: calls.append(1))
    assert len(calls) == 3


def test_store_failure_is_mapped_to_dependency_unavailable() -> None:
    svc = IdempotencyService(FailingIdempotencyStore())
    with pytest.raises(GovernanceError) as exc:
        svc.execute("any-key", "t1", {}, lambda: None)
    assert exc.value.reason is DenyReason.DEPENDENCY_UNAVAILABLE
    assert "unavailable" not in exc.value.message


def test_decision_logs_include_key_tenant_and_basis(
    service: IdempotencyService, caplog: pytest.LogCaptureFixture
) -> None:
    """单测日志必须能看到幂等键、租户标识与判定依据。"""
    service.execute("log-key", "t-log", {"v": 1}, lambda: "ok")
    service.execute("log-key", "t-log", {"v": 1}, lambda: "ok")
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "idempotency_key=log-key" in text
    assert "tenant=t-log" in text
    assert "basis=first_submission" in text
    assert "basis=duplicate_after_completion" in text
