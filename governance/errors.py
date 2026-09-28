"""治理域统一错误类型。

所有被治理层拒绝的请求都抛出 GovernanceError，并携带可区分的
``reason``（机器可读）与 ``message``（人可读），上层据此映射为
明确的 HTTP 响应，绝不把底层依赖异常直接透出给调用方。
"""

from __future__ import annotations

from enum import Enum


class DenyReason(str, Enum):
    """可区分的拒绝原因。"""

    MISSING_IDENTITY = "missing_identity"
    ACCESS_DENIED = "access_denied"
    QUOTA_EXHAUSTED = "quota_exhausted"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    IDEMPOTENCY_KEY_REQUIRED = "idempotency_key_required"
    RESULT_EXPIRED = "result_expired"
    RESULT_UNVERIFIABLE = "result_unverifiable"


class GovernanceError(Exception):
    """治理拒绝的统一异常。"""

    def __init__(self, reason: DenyReason, message: str) -> None:
        self.reason = reason
        self.message = message
        super().__init__(message)
