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
from governance.idempotency import (
    IdempotencyService,
    InMemoryIdempotencyStore,
    SubmissionStatus,
)
from governance.middleware import GovernanceMiddleware
from governance.policy import PolicyRegistry, RetentionConfig, WritePolicy
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

# 已完成结论默认保留 24 小时、最多 10000 条；可通过 POLICIES 热更新。
RETENTION = RetentionConfig(ttl_seconds=24 * 3600, max_completed=10_000)
POLICIES = PolicyRegistry(CONFIG, retention=RETENTION)
# 示例：/orders 不允许重复副作用，可在这里（或运行时通过 update_write_policy）
# 要求必须携带提交标识。默认放开以保持兼容，需要时取消下行注释：
# POLICIES.update_write_policy("t1", "/orders", WritePolicy(require_idempotency_key=True))

IDEMPOTENCY_STORE = InMemoryIdempotencyStore(retention=RETENTION)
IDEMPOTENCY = IdempotencyService(
    IDEMPOTENCY_STORE,
    retention_provider=POLICIES.retention,
)
QUOTA = QuotaEnforcer(CONFIG, InMemoryQuotaStore())
GOVERNANCE = GovernanceMiddleware(IDENTITY, QUOTA, IDEMPOTENCY, policies=POLICIES)

app = FastAPI(title="Tenant API Governance Demo")

# 拒绝原因 -> HTTP 状态码（可区分、可解释，不透出底层异常）
_STATUS = {
    DenyReason.MISSING_IDENTITY: 401,
    DenyReason.ACCESS_DENIED: 403,
    DenyReason.QUOTA_EXHAUSTED: 429,
    DenyReason.IDEMPOTENCY_CONFLICT: 409,
    DenyReason.DEPENDENCY_UNAVAILABLE: 503,
    DenyReason.IDEMPOTENCY_KEY_REQUIRED: 428,
    DenyReason.RESULT_EXPIRED: 410,
    DenyReason.RESULT_UNVERIFIABLE: 409,
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
    """重置内存态（业务副作用计数、配额计数、幂等记录、策略覆盖），供测试使用。"""
    orders.created = 0
    QUOTA._last_known.clear()
    QUOTA._store._counters.clear()
    IDEMPOTENCY_STORE._records.clear()
    IDEMPOTENCY_STORE._conditions.clear()
    POLICIES._write_policies.clear()
    POLICIES.update_retention(RETENTION)


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


@app.get("/submissions/{idempotency_key}")
def query_submission(
    idempotency_key: str,
    x_auth_token: str | None = Header(default=None),
):
    """只读查询提交结论：按调用方租户隔离，不产生副作用、不占配额。

    返回体 status 取值：processing / completed / failed /
    expired_or_unknown / unverifiable。
    """
    view = GOVERNANCE.query_submission(
        credentials=x_auth_token,
        api="/orders",
        idempotency_key=idempotency_key,
    )
    # 已过期/被淘汰/从未见过 -> 404，与"有结论"的 200 明确区分
    if view.status is SubmissionStatus.EXPIRED_OR_UNKNOWN:
        return JSONResponse(
            status_code=404,
            content={
                "status": view.status.value,
                "message": "未找到该提交标识的结论（已过期、被淘汰或从未提交）",
            },
        )
    body: dict = {"status": view.status.value}
    if view.status is SubmissionStatus.COMPLETED:
        body["data"] = view.result
    return body


@app.post("/admin/policies/write")
def demo_update_write_policy(
    tenant_id: str,
    api: str,
    require_idempotency_key: bool = False,
    degrade: DegradeMode | None = None,
):
    """【演示用】运行时热更新某租户×接口的写入策略，无需重启服务。

    真实接入应由受控的配置中心/管理面调用 PolicyRegistry，此处仅为便于
    手工验证热更新语义而暴露。
    """
    POLICIES.update_write_policy(
        tenant_id, api,
        WritePolicy(
            require_idempotency_key=require_idempotency_key,
            degrade_mode=degrade,
        ),
    )
    return {
        "status": "updated",
        "tenant_id": tenant_id,
        "api": api,
        "require_idempotency_key": require_idempotency_key,
        "degrade": degrade.value if degrade else None,
    }


@app.get("/health")
def health(x_auth_token: str | None = Header(default=None)):
    # 只读接口同样过身份/权限，但不要求幂等键，也不受写配额约束。
    identity = IDENTITY.resolve(x_auth_token)
    IDENTITY.authorize(identity, "/health")
    return {"status": "ok", "tenant": identity.tenant_id}
