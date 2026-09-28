"""HTTP 层重试治理：428 缺标识、410 结论过期、查询端点租户隔离与状态。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import main as app_module


@pytest.fixture()
def client() -> TestClient:
    app_module.reset_governance_state()
    return TestClient(app_module.app)


def test_payment_without_key_is_428(client: TestClient) -> None:
    r = client.post(
        "/payments", json={"item": "x"},
        headers={"X-Auth-Token": "token-t1"},
    )
    assert r.status_code == 428
    assert r.json()["error"] == "missing_idempotency_key"
    assert app_module.orders.created == 0


def test_payment_with_key_succeeds(client: TestClient) -> None:
    r = client.post(
        "/payments", json={"item": "x"},
        headers={"X-Auth-Token": "token-t1", "X-Idempotency-Key": "pay-1"},
    )
    assert r.status_code == 200
    assert r.json()["data"]["order_id"]


def test_query_completed_status(client: TestClient) -> None:
    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "q-1"}
    client.post("/orders", json={"item": "book"}, headers=headers)
    r = client.get("/submissions/q-1", headers={"X-Auth-Token": "token-t1"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "completed"
    assert body["data"]["item"] == "book"


def test_query_not_found_status(client: TestClient) -> None:
    r = client.get("/submissions/no-such",
                   headers={"X-Auth-Token": "token-t1"})
    assert r.status_code == 200
    assert r.json()["status"] == "expired_or_not_found"


def test_query_tenant_isolation(client: TestClient) -> None:
    client.post("/orders", json={"item": "book"}, headers={
        "X-Auth-Token": "token-t1", "X-Idempotency-Key": "iso-1"})
    # t2 用同标识查询：查不到 t1 的结论。
    r = client.get("/submissions/iso-1",
                   headers={"X-Auth-Token": "token-t2"})
    assert r.json()["status"] == "expired_or_not_found"
    # 未认证不能查询。
    assert client.get("/submissions/iso-1").status_code == 401


def test_replay_after_quota_exhausted_still_200(client: TestClient) -> None:
    """t2 每窗口 2 个额度：首请求后用新键耗尽，再用旧键重试仍回放。"""
    h = {"X-Auth-Token": "token-t2", "X-Idempotency-Key": "t2-retry"}
    r1 = client.post("/orders", json={"item": "a"}, headers=h)
    assert r1.status_code == 200
    client.post("/orders", json={"item": "b"}, headers={
        "X-Auth-Token": "token-t2", "X-Idempotency-Key": "t2-other"})
    # 额度已耗尽；新键应被 429。
    r_new = client.post("/orders", json={"item": "c"}, headers={
        "X-Auth-Token": "token-t2", "X-Idempotency-Key": "t2-new"})
    assert r_new.status_code == 429
    # 旧键重试仍回放首次结论。
    r2 = client.post("/orders", json={"item": "a"}, headers=h)
    assert r2.status_code == 200
    assert r2.json()["data"] == r1.json()["data"]


def test_expired_submission_is_410(monkeypatch: pytest.MonkeyPatch) -> None:
    """结论超期后再提交同标识 -> 410 result_expired，而不是执行第二遍。"""
    from governance.config import RetentionConfig
    from governance.idempotency import InMemoryIdempotencyStore, IdempotencyService

    clock = {"t": 1000.0}
    ret = RetentionConfig(result_ttl_seconds=60.0,
                          max_completed_records=None,
                          tombstone_ttl_seconds=3600.0)
    store = InMemoryIdempotencyStore(retention=ret, clock=lambda: clock["t"])
    monkeypatch.setattr(app_module, "IDEMPOTENCY_STORE", store)
    monkeypatch.setattr(app_module, "IDEMPOTENCY", IdempotencyService(store))
    monkeypatch.setattr(app_module.GOVERNANCE, "_idempotency",
                        app_module.IDEMPOTENCY)
    client = TestClient(app_module.app)
    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "exp-1"}
    assert client.post("/orders", json={"item": "x"},
                       headers=headers).status_code == 200
    created_before = app_module.orders.created
    clock["t"] += 61
    r = client.post("/orders", json={"item": "x"}, headers=headers)
    assert r.status_code == 410
    assert r.json()["error"] == "result_expired"
    assert app_module.orders.created == created_before, "过期重放不得二次执行"
