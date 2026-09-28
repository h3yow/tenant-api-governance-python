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


# ---------- 重试结算新语义的 HTTP 层覆盖 ----------

def test_retry_after_quota_exhausted_returns_first_order(client: TestClient) -> None:
    """t2 额度 2：耗尽后同标识重试仍拿首次结论，不新增订单。"""
    headers = {"X-Auth-Token": "token-t2", "X-Idempotency-Key": "retry-k"}
    r1 = client.post("/orders", json={"item": "book"}, headers=headers)
    assert r1.status_code == 200
    order_id = r1.json()["data"]["order_id"]
    # 用其他新标识耗尽剩余额度
    client.post(
        "/orders", json={"item": "pen"},
        headers={"X-Auth-Token": "token-t2", "X-Idempotency-Key": "other-k"},
    )
    r_deny = client.post(
        "/orders", json={"item": "pen"},
        headers={"X-Auth-Token": "token-t2", "X-Idempotency-Key": "new-k"},
    )
    assert r_deny.status_code == 429
    # 同标识重试：回放，不再占额度
    r2 = client.post("/orders", json={"item": "book"}, headers=headers)
    assert r2.status_code == 200
    assert r2.json()["data"]["order_id"] == order_id
    assert app_module.orders.created == 2


def test_required_key_policy_rejects_missing_key_with_428(
    client: TestClient,
) -> None:
    from governance.policy import WritePolicy

    app_module.POLICIES.update_write_policy(
        "t1", "/orders", WritePolicy(require_idempotency_key=True)
    )
    r = client.post("/orders", json={"item": "x"},
                    headers={"X-Auth-Token": "token-t1"})
    assert r.status_code == 428
    assert r.json()["error"] == "idempotency_key_required"
    assert app_module.orders.created == 0
    # 携带标识后正常
    r2 = client.post(
        "/orders", json={"item": "x"},
        headers={"X-Auth-Token": "token-t1", "X-Idempotency-Key": "ok-k"},
    )
    assert r2.status_code == 200


def test_query_completed_and_processing_states(client: TestClient) -> None:
    import threading

    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "query-k"}
    r = client.post("/orders", json={"item": "book"}, headers=headers)
    assert r.status_code == 200
    q = client.get("/submissions/query-k", headers={"X-Auth-Token": "token-t1"})
    assert q.status_code == 200
    body = q.json()
    assert body["status"] == "completed"
    assert body["data"]["order_id"] == r.json()["data"]["order_id"]

    # 从未见过 -> 404 expired_or_unknown
    q404 = client.get("/submissions/never-seen",
                      headers={"X-Auth-Token": "token-t1"})
    assert q404.status_code == 404
    assert q404.json()["status"] == "expired_or_unknown"

    # 租户隔离：t2 查 t1 的标识查不到
    q_iso = client.get("/submissions/query-k",
                       headers={"X-Auth-Token": "token-t2"})
    assert q_iso.status_code == 404


def test_query_requires_identity(client: TestClient) -> None:
    r = client.get("/submissions/x")
    assert r.status_code == 401
    # t3 越权访问 /orders 命名空间的查询
    r2 = client.get("/submissions/x", headers={"X-Auth-Token": "token-t3"})
    assert r2.status_code == 403


def test_expired_key_rejected_with_410(client: TestClient,
                                       monkeypatch: pytest.MonkeyPatch) -> None:
    """热缩短保留 TTL 并推进时钟后，旧标识重放得到 410 result_expired。"""
    from governance.policy import RetentionConfig

    # 给幂等存储注入可控时钟
    t = {"now": 1000.0}
    monkeypatch.setattr(app_module.IDEMPOTENCY_STORE, "_clock", lambda: t["now"])

    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "ttl-k"}
    r = client.post("/orders", json={"item": "book"}, headers=headers)
    assert r.status_code == 200
    created = app_module.orders.created

    app_module.POLICIES.update_retention(
        RetentionConfig(ttl_seconds=60, max_completed=10_000)
    )
    t["now"] += 61  # 超过保留时长
    r2 = client.post("/orders", json={"item": "book"}, headers=headers)
    assert r2.status_code == 410
    assert r2.json()["error"] == "result_expired"
    assert app_module.orders.created == created, "过期重放不得再次下单"

    # 查询同样落在 expired_or_unknown
    q = client.get("/submissions/ttl-k", headers={"X-Auth-Token": "token-t1"})
    assert q.status_code == 404


def test_same_key_different_payload_still_409(client: TestClient) -> None:
    headers = {"X-Auth-Token": "token-t1", "X-Idempotency-Key": "conf-new"}
    assert client.post("/orders", json={"item": "a"},
                       headers=headers).status_code == 200
    r = client.post("/orders", json={"item": "b"}, headers=headers)
    assert r.status_code == 409
    assert r.json()["error"] == "idempotency_conflict"


def test_hot_policy_takes_effect_without_restart(client: TestClient) -> None:
    # 热更新前：无标识请求受配额约束但不要求标识，第一笔应成功
    r0 = client.post("/orders", json={"item": "x"},
                     headers={"X-Auth-Token": "token-t1"})
    assert r0.status_code == 200
    r = client.post(
        "/admin/policies/write",
        params={"tenant_id": "t1", "api": "/orders",
                "require_idempotency_key": "true"},
    )
    assert r.status_code == 200
    r2 = client.post("/orders", json={"item": "x"},
                     headers={"X-Auth-Token": "token-t1"})
    assert r2.status_code == 428
    assert r2.json()["error"] == "idempotency_key_required"
