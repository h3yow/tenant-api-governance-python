"""HTTP 入口：多租户订单写/读接口 + 本地验证用管理端点。

写接口：POST /api/orders
  - Authorization: Bearer <token>      身份
  - Idempotency-Key: <key>             幂等键（写请求必填）
  - 配额按 (tenant, api) 双维度计数。
读接口：GET /api/orders （演示只读，不计配额）

管理端点（仅限本地验证，生产中不应暴露）：
  POST /admin/faults   {"quota_store": bool, "idem_store": bool}
  POST /admin/policy   {"quota_fail_mode": "open|closed|last_known",
                        "idem_fail_mode": "closed|local_dedupe"}
  POST /admin/reset
  GET  /admin/state
"""

from __future__ import annotations

import threading
from typing import Any

from fastapi import FastAPI, Header
from pydantic import BaseModel
from fastapi.responses import JSONResponse

from app.dependencies import AppState, build_state
from app.errors import ErrorCode, GovernanceError
from app.idempotency import IdemFailMode
from app.logging_setup import configure_logging, log_decision
from app.models import ApiScope
from app.quota import QuotaFailMode

# 错误码 -> HTTP 状态：调用方可据此区分失败原因
_ERROR_STATUS: dict[ErrorCode, int] = {
    ErrorCode.UNAUTHENTICATED: 401,
    ErrorCode.FORBIDDEN: 403,
    ErrorCode.QUOTA_EXHAUSTED: 429,
    ErrorCode.IDEMPOTENCY_CONFLICT: 422,
    ErrorCode.IDEMPOTENCY_INFLIGHT: 409,
    ErrorCode.IDEMPOTENCY_KEY_MISSING: 400,
    ErrorCode.DEPENDENCY_UNAVAILABLE: 503,
    ErrorCode.INTERNAL_ERROR: 500,
}

ORDER_SCOPE_WRITE = ApiScope("orders.write", write=True)
ORDER_SCOPE_READ = ApiScope("orders.read", write=False)


class CreateOrderRequest(BaseModel):
    item: str
    quantity: int = 1


class FaultsRequest(BaseModel):
    quota_store: bool | None = None
    idem_store: bool | None = None


class PolicyRequest(BaseModel):
    quota_fail_mode: QuotaFailMode | None = None
    idem_fail_mode: IdemFailMode | None = None


def create_app(state: AppState | None = None) -> FastAPI:
    configure_logging()
    state = state or build_state()
    application = FastAPI(title="tenant-api-governance", version="0.1.0")

    # 演示用业务副作用：进程内订单表 + 计数，便于测试断言“副作用一次”
    orders_lock = threading.Lock()
    orders: dict[str, dict[str, Any]] = {}
    side_effect_count = {"n": 0}

    def create_order_side_effect(payload: dict[str, Any]) -> dict[str, Any]:
        with orders_lock:
            side_effect_count["n"] += 1
            order_id = f"ord-{len(orders) + 1:04d}"
            order = {
                "order_id": order_id,
                "item": payload["item"],
                "quantity": payload["quantity"],
            }
            orders[order_id] = order
            return order

    @application.exception_handler(GovernanceError)
    async def governance_error_handler(request, exc: GovernanceError):  # noqa: ANN001
        log_decision(
            "http_reject",
            api=request.url.path,
            decision=exc.code.value,
            reason=exc.reason,
        )
        return JSONResponse(
            status_code=_ERROR_STATUS[exc.code],
            content=exc.to_dict(),
        )

    @application.post("/api/orders")
    def create_order(
        body: CreateOrderRequest,
        authorization: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        token = _bearer_token(authorization)
        payload = body.model_dump()
        governed = state.engine.handle(
            token=token,
            scope=ORDER_SCOPE_WRITE,
            payload=payload,
            idempotency_key=idempotency_key,
            handler=lambda: create_order_side_effect(payload),
        )
        http_status = 200 if governed.idempotent_replay else 201
        return JSONResponse(
            status_code=http_status,
            content={
                "data": governed.result,
                "meta": {
                    "tenant_id": governed.tenant_id,
                    "idempotent_replay": governed.idempotent_replay,
                    "degraded": governed.degraded,
                    "reason": governed.reason,
                    "idempotency_key": idempotency_key,
                },
            },
        )

    @application.get("/api/orders")
    def list_orders(authorization: str | None = Header(default=None)):
        token = _bearer_token(authorization)
        governed = state.engine.handle(
            token=token,
            scope=ORDER_SCOPE_READ,
            payload=None,
            idempotency_key=None,
            handler=lambda: list(orders.values()),
        )
        return {
            "data": governed.result,
            "meta": {"tenant_id": governed.tenant_id, "reason": governed.reason},
        }

    # ---- 本地验证用管理端点 ----
    @application.post("/admin/faults")
    def set_faults(req: FaultsRequest):
        if req.quota_store is not None:
            state.quota_store.set_failing(req.quota_store)
        if req.idem_store is not None:
            state.idem_store.set_failing(req.idem_store)
        return {"quota_store_failing": state.quota_store.failing,
                "idem_store_failing": state.idem_store.failing}

    @application.post("/admin/policy")
    def set_policy(req: PolicyRequest):
        if req.quota_fail_mode is not None:
            state.quota_service._policy.fail_mode = req.quota_fail_mode
        if req.idem_fail_mode is not None:
            state.idem_service.fail_mode = req.idem_fail_mode
        return {
            "quota_fail_mode": state.quota_service._policy.fail_mode.value,
            "idem_fail_mode": state.idem_service.fail_mode.value,
        }

    @application.post("/admin/reset")
    def reset_all():
        state.quota_store.reset()
        state.idem_store.reset()
        with orders_lock:
            orders.clear()
            side_effect_count["n"] = 0
        return {"reset": True}

    @application.get("/admin/state")
    def get_state():
        return {
            "side_effect_count": side_effect_count["n"],
            "orders": list(orders.values()),
            "quota_store_failing": state.quota_store.failing,
            "idem_store_failing": state.idem_store.failing,
            "quota_fail_mode": state.quota_service._policy.fail_mode.value,
            "idem_fail_mode": state.idem_service.fail_mode.value,
        }

    return application


def _bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


app = create_app()
