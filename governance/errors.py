"""治理域统一错误类型。

所有被治理层拒绝的请求都抛出 GovernanceError，并携带可区分的
``reason``（机器可读）与 ``message``（人可读），上层据此映射为
明确的 HTTP 响应，绝不把底层依赖异常直接透出给调用方。

拒绝原因中与“重试结算”相关的两类需要和普通失败明确区分：

- ``MISSING_IDEMPOTENCY_KEY``：接口策略要求写请求必须携带提交标识，
  调用方未携带（区别于额度、冲突等其它失败）。
- ``RESULT_EXPIRED``：该提交标识的结论已超期或已被容量淘汰，
  治理层无法再安全地确认/回放，调用方必须换新标识重新提交。
"""

from __future__ import annotations

from enum import Enum


class DenyReason(str, Enum):
    """可区分的拒绝原因。"""

    MISSING_IDENTITY = "missing_identity"
    ACCESS_DENIED = "access_denied"
    QUOTA_EXHAUSTED = "quota_exhausted"
    MISSING_IDEMPOTENCY_KEY = "missing_idempotency_key"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    RESULT_EXPIRED = "result_expired"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"


class GovernanceError(Exception):
    """治理拒绝的统一异常。"""

    def __init__(self, reason: DenyReason, message: str) -> None:
        self.reason = reason
        self.message = message
        super().__init__(message)
