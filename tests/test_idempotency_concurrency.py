"""并发重复提交：只产生一次副作用，所有调用方拿到一致结论。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import ErrorCode, GovernanceError
from app.models import ApiScope

SCOPE = ApiScope("orders.write", True)


class SlowHandler:
    """模拟有延迟、有计数的业务副作用。"""

    def __init__(self, delay: float = 0.05) -> None:
        self.delay = delay
        self.calls = 0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            self.calls += 1
        import time as _time
        _time.sleep(self.delay)
        return {"order_id": "ord-0001", "item": "book"}


def test_concurrent_same_key_executes_side_effect_once(harness):
    engine = harness["engine"]
    handler = SlowHandler()
    payload = {"item": "book", "quantity": 1}

    def submit(_):
        return engine.handle("token-a-writer", SCOPE, payload, "IDEM-CONC-1", handler)

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(submit, range(12)))

    assert handler.calls == 1, "业务副作用必须恰好一次"
    firsts = [r for r in responses if not r.idempotent_replay]
    replays = [r for r in responses if r.idempotent_replay]
    assert len(firsts) == 1 and len(replays) == 11
    assert all(r.result == {"order_id": "ord-0001", "item": "book"} for r in responses)
    assert all(r.tenant_id == "tenant-a" for r in responses)


def test_sequential_repeated_submission_returns_same_result(harness):
    engine = harness["engine"]
    calls = {"n": 0}

    def handler():
        calls["n"] += 1
        return {"n": calls["n"]}

    r1 = engine.handle("token-a-writer", SCOPE, {"x": 1}, "IDEM-SEQ", handler)
    r2 = engine.handle("token-a-writer", SCOPE, {"x": 1}, "IDEM-SEQ", handler)
    r3 = engine.handle("token-a-writer", SCOPE, {"x": 1}, "IDEM-SEQ", handler)
    assert calls["n"] == 1
    assert r1.result == r2.result == r3.result == {"n": 1}
    assert r1.idempotent_replay is False
    assert r2.idempotent_replay and r3.idempotent_replay


def test_same_key_different_payload_is_rejected_not_silently_first_wins(harness):
    engine = harness["engine"]
    engine.handle("token-a-writer", SCOPE, {"item": "book"}, "IDEM-DIFF",
                  lambda: {"order_id": "ord-1"})
    with pytest.raises(GovernanceError) as exc:
        engine.handle("token-a-writer", SCOPE, {"item": "phone"}, "IDEM-DIFF",
                      lambda: {"order_id": "should-not-happen"})
    assert exc.value.code is ErrorCode.IDEMPOTENCY_CONFLICT
    assert "fingerprint" in exc.value.reason or "different" in exc.value.reason
    # 首次结论仍然完好，可继续回放
    replay = engine.handle("token-a-writer", SCOPE, {"item": "book"}, "IDEM-DIFF",
                           lambda: pytest.fail("handler must not run again"))
    assert replay.result == {"order_id": "ord-1"} and replay.idempotent_replay


def test_replay_charges_no_quota(harness):
    """重复提交回放不再次占用配额：3 次额度下可用同键回放任意次，再用新键仍有 2 次额度。"""
    engine = harness["engine"]
    engine.handle("token-a-writer", SCOPE, {"i": 0}, "K-0", lambda: "ok")
    for _ in range(10):
        r = engine.handle("token-a-writer", SCOPE, {"i": 0}, "K-0",
                          lambda: pytest.fail("replay must not run handler"))
        assert r.idempotent_replay
    # 还剩 2 次新键额度
    engine.handle("token-a-writer", SCOPE, {"i": 1}, "K-1", lambda: "ok")
    engine.handle("token-a-writer", SCOPE, {"i": 2}, "K-2", lambda: "ok")
    with pytest.raises(GovernanceError) as exc:
        engine.handle("token-a-writer", SCOPE, {"i": 3}, "K-3", lambda: "ok")
    assert exc.value.code is ErrorCode.QUOTA_EXHAUSTED


def test_business_rejection_is_replayed_consistently(harness):
    """业务规则拒绝（GovernanceError）也是确定结论：重复提交回放同一错误。"""
    engine = harness["engine"]

    def validate():
        raise GovernanceError(
            ErrorCode.FORBIDDEN, "business rule denied", reason="item blacklisted"
        )

    with pytest.raises(GovernanceError) as e1:
        engine.handle("token-a-writer", SCOPE, {"item": "bad"}, "IDEM-BIZ", validate)
    with pytest.raises(GovernanceError) as e2:
        engine.handle("token-a-writer", SCOPE, {"item": "bad"}, "IDEM-BIZ",
                      lambda: pytest.fail("handler must not run again"))
    assert e1.value.code == e2.value.code is ErrorCode.FORBIDDEN
    assert e2.value.reason == "item blacklisted"


def test_concurrent_different_keys_isolated(harness):
    engine = harness["engine"]
    counter = {"n": 0}
    lock = threading.Lock()

    def handler():
        with lock:
            counter["n"] += 1
            v = counter["n"]
        return {"seq": v}

    def submit(i):
        return engine.handle(
            "token-a-writer", SCOPE, {"i": i}, f"IDEM-UNIQUE-{i}", handler
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(submit, range(3)))
    assert counter["n"] == 3
    seqs = sorted(r.result["seq"] for r in responses)
    assert seqs == [1, 2, 3]


def test_concurrent_decision_logs_include_key_tenant_basis(harness, caplog):
    engine = harness["engine"]
    handler = SlowHandler(delay=0.02)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(
            lambda _: engine.handle(
                "token-a-writer", SCOPE, {"item": "book"}, "IDEM-LOG-1", handler
            ),
            range(4),
        ))
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "IDEM-LOG-1" in text
    assert "tenant=tenant-a" in text
    assert "decision=first" in text
    assert "decision=replay" in text
