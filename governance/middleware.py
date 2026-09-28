"""治理编排：身份 -> 权限 -> 写入策略 -> 幂等执行（仅首请求占配额）。

判定顺序是确定的：
1. 解析身份（缺失/无效凭据 -> MISSING_IDENTITY）；
2. 校验接口权限（越权 -> ACCESS_DENIED）；
3. 读取写入策略快照（要求提交标识但缺失 -> IDEMPOTENCY_KEY_REQUIRED）；
4. 幂等占位：重复/并发/断线重连直接等待或回放首结论，不占配额；
   仅真正的首请求在执行业务前消耗配额（耗尽 -> QUOTA_EXHAUSTED，
   依赖故障 -> 按策略快照里的降级模式裁决）。

任何底层依赖异常都在治理层被转换为可解释的 GovernanceError，
不会直接透出给调用方。
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from governance.errors import DenyReason, GovernanceError
from governance.identity import IdentityResolver
from governance.idempotency import IdempotencyService, SubmissionView
from governance.policy import PolicyRegistry
from governance.quota import QuotaEnforcer

logger = logging.getLogger("governance.chain")


class GovernanceMiddleware:
    """把身份解析、写入策略、配额、幂等串成一条确定的判定链。"""

    def __init__(
        self,
        identity: IdentityResolver,
        quota: QuotaEnforcer,
        idempotency: IdempotencyService,
        policies: PolicyRegistry | None = None,
    ) -> None:
        self._identity = identity
        self._quota = quota
        self._idempotency = idempotency
        self._policies = policies

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

        # 3) 写入策略快照：一次请求只读一次，并发调整不会半新半旧
        policy = None
        if self._policies is not None:
            policy = self._policies.write_policy(caller.tenant_id, api)
            if policy.require_idempotency_key and not idempotency_key:
                logger.info(
                    "idempotency_key=<none> tenant=%s api=%s decision=deny "
                    "basis=idempotency_key_required",
                    caller.tenant_id, api,
                )
                raise GovernanceError(
                    DenyReason.IDEMPOTENCY_KEY_REQUIRED,
                    f"接口 {api} 要求写请求必须携带提交标识（X-Idempotency-Key）",
                )
        degrade_override = None
        if policy is not None and policy.degrade_mode is not None:
            degrade_override = policy.degrade_mode

        # 4) 幂等执行；配额消耗回调只由首请求触发
        def consume_quota() -> None:
            self._quota.check_and_consume(
                caller.tenant_id, api, degrade_override=degrade_override
            )

        return self._idempotency.execute(
            idempotency_key,
            caller.tenant_id,
            payload,
            action,
            consume_quota=consume_quota,
        )

    def query_submission(
        self,
        credentials: str | None,
        api: str,
        idempotency_key: str,
    ) -> SubmissionView:
        """只读查询提交结论：需要身份与权限，按租户隔离，不占配额。"""
        caller = self._identity.resolve(credentials)
        self._identity.authorize(caller, api)
        return self._idempotency.query(idempotency_key, caller.tenant_id)
