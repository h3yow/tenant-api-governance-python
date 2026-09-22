"""治理门面：身份/权限 → 幂等 → 配额 → 业务处理器。

顺序约定（写请求）：
1. 身份解析（缺失 -> unauthenticated）与接口授权（越权 -> forbidden）；
2. 写请求必须携带幂等键（缺失 -> idempotency_key_required）；
3. 幂等占坑：FIRST 才继续；REPLAY 直接回放首次结论（不再次占配额、
   不再次产生副作用）；CONFLICT 明确拒绝；IN_FLIGHT 等待超时返回 409；
4. FIRST 路径预占配额，配额耗尽 -> quota_exhausted，并回滚幂等占坑，
   使客户端纠正后可用同一键重试；
5. 执行业务处理器：成功则以 compare-and-swap 完成幂等记录；
   业务代码抛出的 GovernanceError（如领域校验失败）也被记录为“确定结论”，
   后续同键同载荷请求回放该错误，保证重复提交结论一致；
   未预期异常则回滚占坑允许重试，并转译为 internal_error，不透出细节。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.errors import ErrorCode, GovernanceError
from app.identity import IdentityResolver
from app.idempotency import (
    AcquireOutcome,
    AcquireResult,
    IdempotencyService,
)
from app.logging_setup import log_decision
from app.models import ApiScope, AuthzDecision
from app.quota import QuotaDecision, QuotaResult, QuotaService


@dataclass(frozen=True)
class GovernedResponse:
    result: Any
    idempotent_replay: bool
    degraded: bool
    reason: str
    tenant_id: str


BusinessHandler = Callable[[], Any]


class GovernanceEngine:
    def __init__(
        self,
        identity: IdentityResolver,
        quota: QuotaService,
        idempotency: IdempotencyService,
    ) -> None:
        self._identity = identity
        self._quota = quota
        self._idempotency = idempotency

    # ---- 步骤 1：身份与权限 ----
    def resolve_and_authorize(
        self, token: str | None, scope: ApiScope
    ) -> AuthzDecision:
        principal = self._identity.resolve(token)
        if principal is None:
            log_decision(
                "authz", api=scope.api, decision="unauthenticated",
                reason="missing or unknown token",
            )
            raise GovernanceError(
                ErrorCode.UNAUTHENTICATED,
                "authentication required",
                reason="missing or unknown bearer token",
            )
        decision = self._identity.authorize(principal, scope)
        if not decision.allowed:
            log_decision(
                "authz", tenant_id=principal.tenant_id, api=scope.api,
                decision="forbidden", reason=decision.reason,
            )
            raise GovernanceError(
                ErrorCode.FORBIDDEN,
                f"caller is not permitted to invoke api={scope.api}",
                reason=decision.reason,
            )
        log_decision(
            "authz", tenant_id=principal.tenant_id, api=scope.api,
            decision="allowed", reason=decision.reason,
        )
        return decision

    # ---- 统一入口 ----
    def handle(
        self,
        token: str | None,
        scope: ApiScope,
        payload: Any,
        idempotency_key: str | None,
        handler: BusinessHandler,
    ) -> GovernedResponse:
        decision = self.resolve_and_authorize(token, scope)
        principal = decision.principal

        if not scope.write:
            return self._handle_read(principal, scope, handler)
        return self._handle_write(principal, scope, payload, idempotency_key, handler)

    # ---- 只读路径 ----
    def _handle_read(
        self, principal, scope: ApiScope, handler: BusinessHandler
    ) -> GovernedResponse:
        quota = self._quota.check_and_consume(principal, scope)
        if quota.decision is QuotaDecision.REJECTED:
            raise self._quota_exhausted(principal, scope, quota)
        result = handler()
        return GovernedResponse(
            result=result,
            idempotent_replay=False,
            degraded=quota.degraded,
            reason=f"read served; quota: {quota.reason}",
            tenant_id=principal.tenant_id,
        )

    # ---- 写路径 ----
    def _handle_write(
        self,
        principal,
        scope: ApiScope,
        payload: Any,
        idempotency_key: str | None,
        handler: BusinessHandler,
    ) -> GovernedResponse:
        if not idempotency_key:
            log_decision(
                "idem_acquire", tenant_id=principal.tenant_id, api=scope.api,
                decision="rejected", reason="write request without Idempotency-Key",
            )
            raise GovernanceError(
                ErrorCode.IDEMPOTENCY_KEY_MISSING,
                "write requests require an Idempotency-Key header",
                reason="missing Idempotency-Key",
            )

        acquired = self._idempotency.acquire(idempotency_key, principal, payload)
        replay, response = self._replay_if_present(principal, scope, idempotency_key, acquired)
        if replay:
            return response  # type: ignore[return-value]

        if acquired.outcome is AcquireOutcome.IN_FLIGHT:
            raise GovernanceError(
                ErrorCode.IDEMPOTENCY_INFLIGHT,
                "a request with the same Idempotency-Key is still in progress",
                reason="timed out waiting for owner after in-flight window",
            )

        owner_token = acquired.owner_token
        degraded = acquired.outcome in (
            AcquireOutcome.DEGRADED_LOCAL_FIRST,
            AcquireOutcome.DEGRADED_LOCAL_REPLAY,
        )

        # FIRST / DEGRADED_LOCAL_FIRST：预占配额
        quota = self._quota.check_and_consume(principal, scope)
        if quota.decision is QuotaDecision.REJECTED:
            # 未产生副作用：回滚幂等占坑，允许客户端补额后以同键重试
            self._idempotency.abandon(idempotency_key, owner_token)
            raise self._quota_exhausted(principal, scope, quota)

        # 执行唯一一次业务副作用
        try:
            result = handler()
        except GovernanceError as biz_error:
            # 领域错误同样是“确定结论”：记录并让重复提交回放同一错误
            self._idempotency.complete(idempotency_key, owner_token, None, biz_error)
            log_decision(
                "write_outcome", tenant_id=principal.tenant_id,
                idempotency_key=idempotency_key, api=scope.api,
                decision="business_rejected", reason=biz_error.reason,
            )
            raise
        except Exception as exc:  # noqa: BLE001
            # 未预期错误：回滚占坑允许重试，转译为内部错误，不透出细节
            self._idempotency.abandon(idempotency_key, owner_token)
            log_decision(
                "write_outcome", tenant_id=principal.tenant_id,
                idempotency_key=idempotency_key, api=scope.api,
                decision="internal_error", reason=f"exception_type={type(exc).__name__}",
            )
            raise GovernanceError(
                ErrorCode.INTERNAL_ERROR,
                "internal error; the request may be safely retried with the same key",
                reason="unexpected handler exception; idempotency slot released",
            ) from None

        self._idempotency.complete(idempotency_key, owner_token, result, None)
        log_decision(
            "write_outcome", tenant_id=principal.tenant_id,
            idempotency_key=idempotency_key, api=scope.api,
            decision="completed",
            reason=f"side effect executed once; quota: {quota.reason}",
        )
        return GovernedResponse(
            result=result,
            idempotent_replay=False,
            degraded=degraded or quota.degraded,
            reason=(
                f"first execution; quota: {quota.reason}"
                + ("; idempotency degraded to local dedupe" if degraded else "")
            ),
            tenant_id=principal.tenant_id,
        )

    def _replay_if_present(
        self, principal, scope: ApiScope, key: str, acquired: AcquireResult
    ) -> tuple[bool, GovernedResponse | None]:
        """处理 REPLAY / CONFLICT / DEGRADED_LOCAL_REPLAY，返回 (是否已决, 响应)。"""
        outcome = acquired.outcome
        record = acquired.record

        if outcome is AcquireOutcome.CONFLICT and record is not None:
            log_decision(
                "idem_acquire", tenant_id=principal.tenant_id,
                idempotency_key=key, api=scope.api, decision="conflict",
                reason=(
                    f"same key reused with different payload: "
                    f"{fingerprint_short(record.fingerprint)} != incoming"
                ),
            )
            raise GovernanceError(
                ErrorCode.IDEMPOTENCY_CONFLICT,
                "Idempotency-Key was already used with a different request payload",
                reason="payload fingerprint mismatch; first request is not silently overwritten",
            )

        if outcome in (AcquireOutcome.REPLAY, AcquireOutcome.DEGRADED_LOCAL_REPLAY):
            assert record is not None
            local = outcome is AcquireOutcome.DEGRADED_LOCAL_REPLAY
            log_decision(
                "write_outcome", tenant_id=principal.tenant_id,
                idempotency_key=key, api=scope.api,
                decision="replay",
                reason=("replaying stored conclusion"
                        + (" (local degraded dedupe)" if local else "")),
            )
            if record.error is not None:
                # 回放首次的业务拒绝结论，保证重复提交得到一致结论
                raise record.error
            return True, GovernedResponse(
                result=record.result,
                idempotent_replay=True,
                degraded=local or record.degraded,
                reason="replayed first request's stored result; no side effect or quota charge",
                tenant_id=principal.tenant_id,
            )
        return False, None

    @staticmethod
    def _quota_exhausted(principal, scope: ApiScope, quota: QuotaResult) -> GovernanceError:
        log_decision(
            "quota_check", tenant_id=principal.tenant_id, api=scope.api,
            decision=quota.decision.value, reason=quota.reason,
        )
        if quota.decision is QuotaDecision.DEGRADED_REJECT:
            return GovernanceError(
                ErrorCode.QUOTA_EXHAUSTED,
                "request rejected based on last-known quota state during outage",
                reason=quota.reason,
            )
        return GovernanceError(
            ErrorCode.QUOTA_EXHAUSTED,
            f"quota exhausted for tenant={principal.tenant_id} api={scope.api}",
            reason=quota.reason,
        )


def fingerprint_short(fp: str) -> str:
    return fp[:8]
