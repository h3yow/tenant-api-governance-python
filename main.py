"""示例多租户订单服务：在 FastAPI 上挂载治理链。

运行：``uv run fastapi dev main.py``
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from governance.config import (
    DegradeMode,
    GovernanceConfig,
    PolicyRegistry,
    QuotaRule,
    RetentionConfig,
    WritePolicyRule,
)
from governance.errors import DenyReason, GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import (
    IdempotencyService,
    InMemoryIdempotencyStore,
)
from governance.middleware import GovernanceMiddleware
from governance.quota import InMemoryQuotaStore, QuotaEnforcer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
)
logger = logging.getLogger("governance.api")

# ---- 治理组件装配（演示用内存实现，可替换为 Redis 等远端存储） ----

CONFIG = GovernanceConfig(
    quota_rules=(
        # 每个租户对每个写接口独立计额：t1 每分钟 3 次，t2 每分钟 2 次
        QuotaRule(tenant_id="t1", api="/orders", limit=3, window_seconds=60),
        QuotaRule(tenant_id="t2", api="/orders", limit=2, window_seconds=60),
    ),
    # 全局降级默认：依赖故障时保守拒绝；可按租户×接口覆盖。
    degrade_mode=DegradeMode.FAIL_CLOSED,
    write_policy_rules=(
        # /payments 不允许重复副作用：必须携带提交标识，缺标识会被明确拒绝。
        WritePolicyRule(tenant_id="t1", api="/payments",
                        require_idempotency_key=True),
        WritePolicyRule(tenant_id="t2", api="/payments",
                        require_idempotency_key=True),
    ),
    retention=RetentionConfig(
        result_ttl_seconds=86400.0,      # 已完成结论保留 24h
        max_completed_records=10_000,   # 每租户最多 1 万条，超出 LRU 淘汰
        tombstone_ttl_seconds=86400.0,  # 过期/淘汰标识再挡 24h 静默重放
    ),
)

# 策略注册表：update() 后无需重启，后续请求统一按新策略快照裁决。
POLICIES = PolicyRegistry(CONFIG)

IDENTITY = IdentityResolver(
    {
        "token-t1": ("t1", "alice",
                     frozenset({"/orders", "/payments", "/health"})),
        "token-t2": ("t2", "bob",
                     frozenset({"/orders", "/payments", "/health"})),
        "token-t3": ("t3", "carol", frozenset({"/health"})),  # 无写权限
    }
)

IDEMPOTENCY_STORE = InMemoryIdempotencyStore(retention=CONFIG.retention)
IDEMPOTENCY = IdempotencyService(IDEMPOTENCY_STORE)
QUOTA = QuotaEnforcer(POLICIES, InMemoryQuotaStore())
GOVERNANCE = GovernanceMiddleware(IDENTITY, QUOTA, IDEMPOTENCY, POLICIES)

app = FastAPI(title="Tenant API Governance Demo")

# 拒绝原因 -> HTTP 状态码（可区分、可解释，不透出底层异常）
_STATUS = {
    DenyReason.MISSING_IDENTITY: 401,
    DenyReason.ACCESS_DENIED: 403,
    DenyReason.QUOTA_EXHAUSTED: 429,
    DenyReason.MISSING_IDEMPOTENCY_KEY: 428,
    DenyReason.IDEMPOTENCY_CONFLICT: 409,
    DenyReason.RESULT_EXPIRED: 410,
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
    """重置内存态（业务副作用计数、配额计数、结算记录），主要供测试使用。"""
    orders.created = 0
    QUOTA.reset_cache()
    QUOTA._store._counters.clear()
    IDEMPOTENCY_STORE.reset()
    # 保留配置恢复为初始值，避免测试间互相影响。
    IDEMPOTENCY_STORE.update_retention(CONFIG.retention)
    POLICIES.update(CONFIG)


class RetentionUpdate(BaseModel):
    """保留与容量配置的运行中调整（未提供的字段沿用当前值）。

    - ``result_ttl_seconds``：结论保留时长（按最近一次访问起算的空闲过期），
      显式传 null 表示不按时间过期；
    - ``max_completed_records``：每租户已落定结论容量上限，显式传 null
      表示不限；
    - ``tombstone_ttl_seconds``：过期/淘汰标识的墓碑保护时长。
    """

    result_ttl_seconds: float | None = None
    max_completed_records: int | None = None
    tombstone_ttl_seconds: float | None = None


@app.put("/governance/retention")
def update_retention_config(
    body: RetentionUpdate,
    x_auth_token: str | None = Header(default=None),
):
    """运行中调整结论保留/容量配置：立即生效，无需重启。

    新配置在结算存储的同一把锁内原子替换并立即按新值清扫/淘汰；之后进入
    的任意一批并发请求统一按新配置判定。正在处理中、刚被提交或查询过的
    结论按既有保护规则保留，不会被误清。
    """
    caller = IDENTITY.resolve(x_auth_token)
    IDENTITY.authorize(caller, "/orders")  # 演示：要求具备写权限的租户身份
    provided = body.model_fields_set
    current = IDEMPOTENCY_STORE.retention
    new_retention = RetentionConfig(
        result_ttl_seconds=(body.result_ttl_seconds
                            if "result_ttl_seconds" in provided
                            else current.result_ttl_seconds),
        max_completed_records=(body.max_completed_records
                               if "max_completed_records" in provided
                               else current.max_completed_records),
        tombstone_ttl_seconds=(body.tombstone_ttl_seconds
                               if "tombstone_ttl_seconds" in provided
                               else current.tombstone_ttl_seconds),
    )
    IDEMPOTENCY_STORE.update_retention(new_retention)
    # 策略注册表同步替换，保证配置视图与结算存储同源。
    POLICIES.update(replace(POLICIES.get(), retention=new_retention))
    logger.info(
        "tenant=%s decision=retention_updated basis=admin_api "
        "result_ttl_seconds=%s max_completed_records=%s tombstone_ttl_seconds=%s",
        caller.tenant_id, new_retention.result_ttl_seconds,
        new_retention.max_completed_records, new_retention.tombstone_ttl_seconds,
    )
    return {"status": "ok", "retention": {
        "result_ttl_seconds": new_retention.result_ttl_seconds,
        "max_completed_records": new_retention.max_completed_records,
        "tombstone_ttl_seconds": new_retention.tombstone_ttl_seconds,
    }}


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


@app.post("/payments")
def create_payment(
    body: OrderRequest,
    x_auth_token: str | None = Header(default=None),
    x_idempotency_key: str | None = Header(default=None),
):
    """强制要求提交标识的写接口（缺失 -> 428 missing_idempotency_key）。"""
    payload = body.model_dump()

    def action() -> dict:
        return orders.create(payload["item"], payload["quantity"])

    result = GOVERNANCE.handle(
        credentials=x_auth_token,
        api="/payments",
        idempotency_key=x_idempotency_key,
        payload=payload,
        action=action,
    )
    return {"status": "ok", "data": result}


@app.get("/submissions/{key}")
def query_submission(
    key: str,
    api: str = "/orders",
    x_auth_token: str | None = Header(default=None),
):
    """凭提交标识只读查询处理结论（按租户隔离，不占额度、不产生副作用）。

    返回体 status 取值：inflight / completed / denied / expired_or_not_found /
    uncertain；completed 时附带首次结论 data，denied 时附带原始拒绝原因。
    """
    outcome = GOVERNANCE.query_settlement(x_auth_token, api, key)
    return {"status": outcome.status.value, "data": outcome.result}


@app.get("/health")
def health(x_auth_token: str | None = Header(default=None)):
    # 只读接口同样过身份/权限，但不要求幂等键，也不受写配额约束。
    identity = IDENTITY.resolve(x_auth_token)
    IDENTITY.authorize(identity, "/health")
    return {"status": "ok", "tenant": identity.tenant_id}
