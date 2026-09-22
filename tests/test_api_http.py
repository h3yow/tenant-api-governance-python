"""HTTP 层：失败原因 -> 状态码映射、回放状态码、管理端点故障注入与降级切换。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.dependencies import build_state
from app.main import create_app


@pytest.fixture
def client():
    return TestClient(create_app(build_state()))


WRITER = {"Authorization": "Bearer token-tenant-a-writer"}
READER = {"Authorization": "Bearer token-tenant-a-reader"}


def test_missing_identity_401(client):
    r = client.post("/api/orders", json={"item": "a"})
    assert r.status_code == 401
    assert r.json()["error"] == "unauthenticated"


def test_missing_idempotency_key_400(client):
    r = client.post("/api/orders", json={"item": "a"}, headers=WRITER)
    assert r.status_code == 400
    assert r.json()["error"] == "idempotency_key_required"


def test_forbidden_403(client):
    r = client.post("/api/orders", json={"item": "a"},
                    headers={**READER, "Idempotency-Key": "K1"})
    assert r.status_code == 403 and r.json()["error"] == "forbidden"


def test_first_201_replay_200_consistent_body(client):
    r1 = client.post("/api/orders", json={"item": "book", "quantity": 1},
                     headers={**WRITER, "Idempotency-Key": "HTTP-1"})
    assert r1.status_code == 201 and r1.json()["meta"]["idempotent_replay"] is False
    r2 = client.post("/api/orders", json={"quantity": 1, "item": "book"},
                     headers={**WRITER, "Idempotency-Key": "HTTP-1"})
    assert r2.status_code == 200 and r2.json()["meta"]["idempotent_replay"] is True
    assert r1.json()["data"] == r2.json()["data"]
    assert client.get("/admin/state").json()["side_effect_count"] == 1


def test_payload_conflict_422(client):
    client.post("/api/orders", json={"item": "book"},
                headers={**WRITER, "Idempotency-Key": "HTTP-C"})
    r = client.post("/api/orders", json={"item": "phone"},
                    headers={**WRITER, "Idempotency-Key": "HTTP-C"})
    assert r.status_code == 422 and r.json()["error"] == "idempotency_conflict"


def test_quota_exhaustion_429(client):
    for i in range(3):
        assert client.post("/api/orders", json={"item": "x"},
                           headers={**WRITER, "Idempotency-Key": f"Q-{i}"}).status_code == 201
    r = client.post("/api/orders", json={"item": "x"},
                    headers={**WRITER, "Idempotency-Key": "Q-over"})
    assert r.status_code == 429 and r.json()["error"] == "quota_exhausted"
    assert "counter=4/3" in r.json()["reason"]


def test_quota_outage_closed_returns_503_with_masked_cause(client):
    client.post("/admin/faults", json={"quota_store": True})
    r = client.post("/api/orders", json={"item": "x"},
                    headers={**WRITER, "Idempotency-Key": "F-1"})
    assert r.status_code == 503 and r.json()["error"] == "dependency_unavailable"
    assert "injected failure" not in r.text


def test_quota_outage_open_serves_degraded(client):
    client.post("/admin/policy", json={"quota_fail_mode": "open"})
    client.post("/admin/faults", json={"quota_store": True})
    r = client.post("/api/orders", json={"item": "x"},
                    headers={**WRITER, "Idempotency-Key": "F-2"})
    assert r.status_code == 201 and r.json()["meta"]["degraded"] is True
    assert "fail_mode=open" in r.json()["meta"]["reason"]


def test_idem_outage_local_dedupe_serves_degraded(client):
    client.post("/admin/policy", json={"quota_fail_mode": "open",
                                       "idem_fail_mode": "local_dedupe"})
    client.post("/admin/faults", json={"idem_store": True})
    r1 = client.post("/api/orders", json={"item": "x"},
                     headers={**WRITER, "Idempotency-Key": "F-3"})
    r2 = client.post("/api/orders", json={"item": "x"},
                     headers={**WRITER, "Idempotency-Key": "F-3"})
    assert r1.status_code == 201 and r1.json()["meta"]["degraded"] is True
    assert r2.json()["meta"]["idempotent_replay"] is True
    assert client.get("/admin/state").json()["side_effect_count"] == 1


def test_admin_reset_clears_state(client):
    client.post("/api/orders", json={"item": "x"},
                headers={**WRITER, "Idempotency-Key": "R-1"})
    client.post("/admin/reset")
    state = client.get("/admin/state").json()
    assert state["side_effect_count"] == 0 and state["orders"] == []


def test_reader_can_list_orders(client):
    assert client.get("/api/orders", headers=READER).status_code == 200
