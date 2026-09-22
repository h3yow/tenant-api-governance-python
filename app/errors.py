"""错误分类：调用方必须能区分身份缺失、越权、配额耗尽等失败原因。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    UNAUTHENTICATED = "unauthenticated"
    FORBIDDEN = "forbidden"
    QUOTA_EXHAUSTED = "quota_exhausted"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    IDEMPOTENCY_INFLIGHT = "idempotency_inflight"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    IDEMPOTENCY_KEY_MISSING = "idempotency_key_required"
    INTERNAL_ERROR = "internal_error"


class DependencyError(RuntimeError):
    """底层依赖（配额/幂等存储）异常，不得直接透传给调用方。"""


class GovernanceError(Exception):
    """治理层拒绝请求时抛出，携带可区分的错误码与判定依据。"""

    def __init__(self, code: ErrorCode, message: str, *, reason: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.reason = reason

    def to_dict(self) -> dict[str, str]:
        return {"error": self.code.value, "message": self.message, "reason": self.reason}
