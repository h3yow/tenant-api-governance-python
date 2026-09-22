"""治理编排：身份 -> 权限 -> 配额 -> 幂等执行业务。

判定顺序是确定的：
1. 解析身份（缺失/无效凭据 -> MISSING_IDENTITY）；
2. 校验接口权限（越权 -> ACCESS_DENIED）；
3. 消耗租户×接口配额（耗尽 -> QUOTA_EXHAUSTED；依赖故障 -> 降级裁决）；
4. 写请求在幂等保护下执行业务（重复 -> 回放一致结论；载荷冲突 -> 拒绝）。

任何底层依赖异常都在治理层被转换为可解释的 GovernanceError，
不会直接透出给调用方。
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from governance.errors import GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import IdempotencyService
from governance.quota import QuotaEnforcer

logger = logging.getLogger("governance.chain")


class GovernanceMiddleware:
    """把身份解析、配额、幂等与降级串成一条确定的判定链。"""

    def __init__(
        self,
        identity: IdentityResolver,
        quota: QuotaEnforcer,
        idempotency: IdempotencyService,
    ) -> None:
        self._identity = identity
        self._quota = quota
        self._idempotency = idempotency

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
        # 3) 租户 × 接口配额（内含依赖异常降级）
        self._quota.check_and_consume(caller.tenant_id, api)
        # 4) 幂等执行业务
        try:
            return self._idempotency.execute(
                idempotency_key, caller.tenant_id, payload, action
            )
        except GovernanceError:
            raise
