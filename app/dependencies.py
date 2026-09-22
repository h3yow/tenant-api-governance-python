"""依赖装配与演示数据：令牌目录、接口权限、配额限额、故障注入开关。"""

from __future__ import annotations

from dataclasses import dataclass

from app.governance import GovernanceEngine
from app.identity import StaticTokenResolver
from app.idempotency import (
    IdemFailMode,
    IdempotencyPolicy,
    IdempotencyService,
    InMemoryIdempotencyStore,
)
from app.models import Principal
from app.quota import (
    InMemoryQuotaStore,
    QuotaFailMode,
    QuotaPolicy,
    QuotaService,
)

# 演示令牌目录：token -> (租户, 调用方, 角色)
DEMO_DIRECTORY: dict[str, Principal] = {
    "token-tenant-a-writer": Principal(
        tenant_id="tenant-a", caller_id="caller-a-1", roles=frozenset({"order_writer"})
    ),
    "token-tenant-b-writer": Principal(
        tenant_id="tenant-b", caller_id="caller-b-1", roles=frozenset({"order_writer"})
    ),
    "token-tenant-a-reader": Principal(
        tenant_id="tenant-a", caller_id="caller-a-2", roles=frozenset({"order_reader"})
    ),
}

# 角色 -> 可访问接口集合（权限边界）
DEMO_ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "order_writer": frozenset({"orders.write", "orders.read"}),
    "order_reader": frozenset({"orders.read"}),
}


@dataclass
class AppState:
    engine: GovernanceEngine
    resolver: StaticTokenResolver
    quota_service: QuotaService
    quota_store: InMemoryQuotaStore
    idem_service: IdempotencyService
    idem_store: InMemoryIdempotencyStore


def build_state(
    *,
    quota_fail_mode: QuotaFailMode = QuotaFailMode.CLOSED,
    idem_fail_mode: IdemFailMode = IdemFailMode.CLOSED,
    quota_default_limit: int = 3,
    window_seconds: int = 60,
) -> AppState:
    resolver = StaticTokenResolver(DEMO_DIRECTORY, DEMO_ROLE_PERMISSIONS)

    quota_store = InMemoryQuotaStore()
    quota_policy = QuotaPolicy(
        limits={"orders.write": quota_default_limit},
        default_limit=quota_default_limit,
        window_seconds=window_seconds,
        fail_mode=quota_fail_mode,
        read_quota=False,
    )
    quota_service = QuotaService(quota_store, quota_policy)

    idem_store = InMemoryIdempotencyStore()
    idem_policy = IdempotencyPolicy(
        in_flight_timeout_seconds=2.0,
        poll_interval_seconds=0.005,
        fail_mode=idem_fail_mode,
    )
    idem_service = IdempotencyService(idem_store, idem_policy)

    engine = GovernanceEngine(resolver, quota_service, idem_service)
    return AppState(
        engine=engine,
        resolver=resolver,
        quota_service=quota_service,
        quota_store=quota_store,
        idem_service=idem_service,
        idem_store=idem_store,
    )
