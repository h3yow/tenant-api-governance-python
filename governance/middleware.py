"""治理编排：身份 -> 权限 -> 结算幂等（内含配额闸门与写入策略）。

判定顺序是确定的：
1. 解析身份（缺失/无效凭据 -> MISSING_IDENTITY）；
2. 校验接口权限（越权 -> ACCESS_DENIED）；
3. 取一次配置快照，后续所有判定（写入策略、配额额度、降级模式）都基于
   同一份不可变配置，保证并发请求不会各走新旧策略；
4. 结算执行（IdempotencyService）：
   - 要求提交标识的接口缺标识 -> MISSING_IDEMPOTENCY_KEY；
   - 同标识重试/并发/断线重连 -> 回放首次结论，*不占额度、不产生副作用*，
     即使额度已耗尽或已跨窗口；
   - 仅真正第一次出现的写请求（leader）才占用配额：额度耗尽 ->
     QUOTA_EXHAUSTED，且该拒绝落定为可回放结论（同标识重试原样回放，
     跨窗口/额度恢复也不会重新执行）；配额依赖故障 -> 按写入策略降级裁决；
   - 载荷冲突 -> IDEMPOTENCY_CONFLICT；结论过期/淘汰 -> RESULT_EXPIRED；
   - 结算存储故障 -> DEPENDENCY_UNAVAILABLE（结算闸门永远保守，即使降级
     配置为 FAIL_OPEN 也不会放过重复副作用）。

任何底层依赖异常都在治理层被转换为可解释的 GovernanceError，
不会直接透出给调用方。
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from governance.config import GovernanceConfig, PolicyRegistry
from governance.errors import GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import IdempotencyService, QueryOutcome
from governance.quota import QuotaEnforcer

logger = logging.getLogger("governance.chain")


class GovernanceMiddleware:
    """把身份解析、写入策略、配额、结算幂等串成一条确定的判定链。"""

    def __init__(
        self,
        identity: IdentityResolver,
        quota: QuotaEnforcer,
        idempotency: IdempotencyService,
        policies: GovernanceConfig | PolicyRegistry | None = None,
    ) -> None:
        self._identity = identity
        self._quota = quota
        self._idempotency = idempotency
        if policies is None:
            # 未显式提供时，复用配额判定器的配置来源，保证两边同源。
            provider = quota._provider
            policies = provider if isinstance(provider, PolicyRegistry) else (
                provider if isinstance(provider, GovernanceConfig)
                else GovernanceConfig()
            )
        if isinstance(policies, GovernanceConfig):
            policies = PolicyRegistry(policies)
        self._policies = policies

    @property
    def policies(self) -> PolicyRegistry:
        return self._policies

    def handle(
        self,
        credentials: str | None,
        api: str,
        idempotency_key: str | None,
        payload: Any,
        action: Callable[[], Any],
    ) -> Any:
        # 1) 身份
        caller = self._identity.resolve(credentials)
        logger.info("credentials_present=%s tenant=%s caller=%s decision=resolved "
                    "basis=credentials", bool(credentials), caller.tenant_id,
                    caller.caller)
        # 2) 权限边界
        self._identity.authorize(caller, api)
        logger.info("tenant=%s api=%s decision=allow basis=authorized",
                    caller.tenant_id, api)
        # 3) 单次请求只用一份策略快照：并发下策略切换一致、请求内不混用
        snapshot = self._policies.get()
        write_policy = snapshot.write_policy(caller.tenant_id, api)

        # 4) 结算执行；配额消耗仅由首个真实写请求触发（见 idempotency.py）
        def reserve_quota() -> None:
            self._quota.check_and_consume(
                caller.tenant_id, api, snapshot=snapshot
            )

        return self._idempotency.execute(
            idempotency_key,
            caller.tenant_id,
            payload,
            action,
            policy=write_policy,
            reserve_quota=reserve_quota,
        )

    def query_settlement(
        self, credentials: str | None, api: str, idempotency_key: str
    ) -> QueryOutcome:
        """只读查询提交结论：仍需身份与接口权限，且严格按租户隔离。

        不产生业务副作用、不占配额；查询不会改变既有结算结论。
        """
        caller = self._identity.resolve(credentials)
        self._identity.authorize(caller, api)
        return self._idempotency.query(caller.tenant_id, idempotency_key)
