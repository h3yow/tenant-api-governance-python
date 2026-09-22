"""身份缺失与越权必须被拒绝，且失败原因可区分。"""

from __future__ import annotations

import pytest

from app.errors import ErrorCode, GovernanceError
from app.models import ApiScope

SCOPE_WRITE = ApiScope("orders.write", True)
SCOPE_READ = ApiScope("orders.read", False)
SCOPE_SECRET = ApiScope("billing.write", True)


def _noop():
    raise AssertionError("handler must not run when authz fails")


def test_missing_token_is_unauthenticated(harness):
    with pytest.raises(GovernanceError) as exc:
        harness["engine"].handle(None, SCOPE_WRITE, {"x": 1}, "K-1", _noop)
    assert exc.value.code is ErrorCode.UNAUTHENTICATED
    assert "token" in exc.value.reason


def test_unknown_token_is_unauthenticated(harness):
    with pytest.raises(GovernanceError) as exc:
        harness["engine"].handle("Bearer nope", SCOPE_WRITE, {}, "K-1", _noop)
    assert exc.value.code is ErrorCode.UNAUTHENTICATED


def test_reader_cannot_write_is_forbidden(harness):
    with pytest.raises(GovernanceError) as exc:
        harness["engine"].handle("token-a-reader", SCOPE_WRITE, {}, "K-1", _noop)
    assert exc.value.code is ErrorCode.FORBIDDEN
    assert "orders.write" in exc.value.reason


def test_unpermitted_api_is_forbidden(harness):
    with pytest.raises(GovernanceError) as exc:
        harness["engine"].handle("token-a-writer", SCOPE_SECRET, {}, "K-1", _noop)
    assert exc.value.code is ErrorCode.FORBIDDEN


def test_writer_can_write_and_carries_tenant(harness):
    resp = harness["engine"].handle(
        "token-a-writer", SCOPE_WRITE, {"item": "book"}, "K-1",
        lambda: {"ok": True},
    )
    assert resp.tenant_id == "tenant-a"
    assert resp.idempotent_replay is False


def test_reader_can_read(harness):
    resp = harness["engine"].handle(
        "token-a-reader", SCOPE_READ, None, None, lambda: []
    )
    assert resp.tenant_id == "tenant-a"


def test_decision_logs_contain_tenant_and_reason(harness, caplog):
    with pytest.raises(GovernanceError):
        harness["engine"].handle("token-a-reader", SCOPE_WRITE, {}, "K-9", _noop)
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "tenant=tenant-a" in text
    assert "decision=forbidden" in text
    assert "order_reader" in text  # 判定依据：角色缺少该接口权限
