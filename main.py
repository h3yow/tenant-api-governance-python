"""示例多租户订单服务：在 FastAPI 上挂载治理链。

运行：``uv run fastapi dev main.py``
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from governance.config import DegradeMode, GovernanceConfig, QuotaRule
from governance.errors import DenyReason, GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import IdempotencyService, InMemoryIdempotencyStore
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
)

# ---- 治理组件装配（演示用内存实现，可替换为 Redis 等远端存储） ----

CONFIG = GovernanceConfig(
    quota_rules=(
        # 每个租户对每个写接口独立计额：t1 每分钟 3 次，t2 每分钟 2 次
        QuotaRule(tenant_id="t1", api="/orders", limit=3, window_seconds=60),
        QuotaRule(tenant_id="t2", api="/orders", limit=2, window_seconds=60),
    ),
    degrade_mode=DegradeMode.FAIL_CLOSED,
)

IDENTITY = IdentityResolver(
    {
        "token-t1": ("t1", "alice", frozenset({"/orders", "/health"})),
        "token-t2": ("t2", "bob", frozenset({"/orders", "/health"})),
        "token-t3": ("t3", "carol", frozenset({"/health"})),  # 无 /orders 权限
    }
)

IDEMPOTENCY = IdempotencyService(InMemoryIdempotencyStore())
QUOTA = QuotaEnforcer(CONFIG, InMemoryQuotaStore())
GOVERNANCE = GovernanceMiddleware(IDENTITY, QUOTA, IDEMPOTENCY)

app = FastAPI(title="Tenant API Governance Demo")

# 拒绝原因 -> HTTP 状态码（可区分、可解释，不透出底层异常）
_STATUS = {
    DenyReason.MISSING_IDENTITY: 401,
    DenyReason.ACCESS_DENIED: 403,
    DenyReason.QUOTA_EXHAUSTED: 429,
    DenyReason.IDEMPOTENCY_CONFLICT: 409,
    DenyReason.DEPENDENCY_UNAVAILABLE: 503,
}


@app.exception_handler(GovernanceError)
async def governance_error_handler(_request, exc: GovernanceError) -> JSONResponse:
    return JSONResponse(
        status_code=_STATUS[exc.reason],
        content={"error": exc.reason.value, "message": exc.message},
    )


class OrderRequest(BaseModel):
    item: str
    quantity: int = 1


@dataclass
class _OrderStore:
    """模拟真实业务副作用：创建订单。"""

    created: int = 0

    def create(self, item: str, quantity: int) -> dict:
        self.created += 1
        return {"order_id": f"ORD-{self.created:06d}", "item": item,
                "quantity": quantity}


orders = _OrderStore()


def reset_governance_state() -> None:
    """重置内存态（业务副作用计数、配额计数、幂等记录），主要供测试使用。"""
    orders.created = 0
    QUOTA._last_known.clear()
    QUOTA._store._counters.clear()
    IDEMPOTENCY._store._records.clear()


@app.post("/orders")
def create_order(
    body: OrderRequest,
    x_auth_token: str | None = Header(default=None),
    x_idempotency_key: str | None = Header(default=None),
):
    payload = body.model_dump()

    def action() -> dict:
        return orders.create(payload["item"], payload["quantity"])

    result = GOVERNANCE.handle(
        credentials=x_auth_token,
        api="/orders",
        idempotency_key=x_idempotency_key,
        payload=payload,
        action=action,
    )
    return {"status": "ok", "data": result}


@app.get("/health")
def health(x_auth_token: str | None = Header(default=None)):
    # 只读接口同样过身份/权限，但不要求幂等键，也不受写配额约束。
    identity = IDENTITY.resolve(x_auth_token)
    IDENTITY.authorize(identity, "/health")
    return {"status": "ok", "tenant": identity.tenant_id}
