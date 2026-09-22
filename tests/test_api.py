"""FastAPI HTTP 层测试：拒绝原因 -> 可区分的 HTTP 状态码与错误体。"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

import main as app_module


@pytest.fixture()
def client() -> TestClient:
    # 每个测试重置内存中的业务副作用计数与治理单例状态
    app_module.reset_governance_state()
    return TestClient(app_module.app)


def test_duplicate_submission_creates_one_order(client: TestClient) -> None:
    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "api-k1"}
    r1 = client.post("/orders", json={"item": "book"}, headers=headers)
    r2 = client.post("/orders", json={"item": "book"}, headers=headers)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["data"]["order_id"] == r2.json()["data"]["order_id"]
    assert app_module.orders.created == 1


def test_concurrent_duplicate_requests_create_one_order(
    client: TestClient,
) -> None:
    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "api-k2"}
    responses: list[int] = []
    order_ids: set[str] = set()
    lock = threading.Lock()

    def worker() -> None:
        r = client.post("/orders", json={"item": "pen"}, headers=headers)
        with lock:
            responses.append(r.status_code)
            order_ids.add(r.json()["data"]["order_id"])

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert responses == [200, 200, 200]
    assert len(order_ids) == 1
    assert app_module.orders.created == 1


def test_missing_identity_is_401(client: TestClient) -> None:
    r = client.post("/orders", json={"item": "x"},
                    headers={"X-Idempotency-Key": "k"})
    assert r.status_code == 401
    assert r.json()["error"] == "missing_identity"


def test_access_denied_is_403(client: TestClient) -> None:
    # t3 只有 /health 权限
    r = client.post(
        "/orders", json={"item": "x"},
        headers={"X-Auth-Token": "token-t3", "X-Idempotency-Key": "k"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "access_denied"


def test_idempotency_conflict_is_409(client: TestClient) -> None:
    base = {"X-Auth-Token": "token-t2"}  # t2 配额 2，足够两次提交
    r1 = client.post("/orders", json={"item": "a"},
                     headers={**base, "X-Idempotency-Key": "conflict-k"})
    assert r1.status_code == 200
    r2 = client.post("/orders", json={"item": "different"},
                     headers={**base, "X-Idempotency-Key": "conflict-k"})
    assert r2.status_code == 409
    assert r2.json()["error"] == "idempotency_conflict"
    assert app_module.orders.created == 1


def test_quota_exhausted_is_429(client: TestClient) -> None:
    # t2 每分钟仅 2 个配额，使用不同幂等键以真实消耗配额
    for i in range(2):
        r = client.post(
            "/orders", json={"item": "x"},
            headers={"X-Auth-Token": "token-t2",
                     "X-Idempotency-Key": f"t2-k{i}"},
        )
        assert r.status_code == 200
    r3 = client.post(
        "/orders", json={"item": "x"},
        headers={"X-Auth-Token": "token-t2", "X-Idempotency-Key": "t2-k2"},
    )
    assert r3.status_code == 429
    assert r3.json()["error"] == "quota_exhausted"


def test_dependency_unavailable_is_503_and_does_not_leak(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from governance.errors import GovernanceError  # noqa: F401
    from governance.quota import FailingQuotaStore

    monkeypatch.setattr(app_module.QUOTA, "_store", FailingQuotaStore())
    r = client.post(
        "/orders", json={"item": "x"},
        headers={"X-Auth-Token": "token-t1", "X-Idempotency-Key": "dep-k"},
    )
    assert r.status_code == 503
    body = r.json()
    assert body["error"] == "dependency_unavailable"
    assert "refused" not in body["message"]
