"""配额治理：租户 + 接口双维度、固定时间窗口、可配置降级。

关键约定：
- 配额键为 (tenant_id, api, window_start)，自增在存储侧原子完成，
  并发请求看到的 used 严格递增，不会出现计数错乱。
- 依赖异常时按 QuotaFailMode 决定：
    open       -> 降级放行（DEGRADED_ALLOW，响应中明确标注降级依据）
    closed     -> 拒绝（DEPENDENCY_UNAVAILABLE，不把底层异常透传给调用方）
    last_known -> 依据本地缓存的“最近已知窗口计数”决定；
                  缓存窗口与当前窗口一致：未超限放行、已超限拒绝；
                  缓存属于更早窗口（刚发生窗口切换）：新窗口视为全新，放行。
                  同一时刻并发请求读到同一份缓存，判定一致。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Protocol

from app.errors import DependencyError
from app.logging_setup import log_decision
from app.models import ApiScope, Principal


class QuotaDecision(str, Enum):
    ALLOWED = "allowed"
    REJECTED = "rejected"
    DEGRADED_ALLOW = "degraded_allow"
    DEGRADED_REJECT = "degraded_reject"


class QuotaFailMode(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    LAST_KNOWN = "last_known"


@dataclass(frozen=True)
class QuotaResult:
    decision: QuotaDecision
    limit: int
    used: int
    window_start: int
    reason: str

    @property
    def degraded(self) -> bool:
        return self.decision in (QuotaDecision.DEGRADED_ALLOW, QuotaDecision.DEGRADED_REJECT)


class QuotaStore(Protocol):
    def incr_and_get(
        self, tenant_id: str, api: str, window_start: int, limit: int
    ) -> tuple[int, bool]:
        """原子自增，返回 (自增后计数, 是否本窗口首次计数)。依赖异常抛 DependencyError。"""
        ...

    def get_last_known(self, tenant_id: str, api: str) -> tuple[int, int] | None:
        """本地缓存的最近一次成功计数 (window_start, used)，存储故障时仍可读。"""
        ...

    def refund(self, tenant_id: str, api: str, window_start: int) -> None: ...


@dataclass
class QuotaPolicy:
    limits: dict[str, int] = field(default_factory=dict)
    default_limit: int = 100
    window_seconds: int = 60
    fail_mode: QuotaFailMode = QuotaFailMode.CLOSED
    read_quota: bool = False


class InMemoryQuotaStore:
    """线程安全的内存配额存储，支持故障注入用于本地验证。"""

    def __init__(self) -> None:
        self._counts: dict[tuple[str, str, int], int] = {}
        self._last_known: dict[tuple[str, str], tuple[int, int]] = {}
        self._lock = threading.RLock()
        self._failing = False

    def set_failing(self, failing: bool) -> None:
        with self._lock:
            self._failing = failing

    @property
    def failing(self) -> bool:
        return self._failing

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()
            self._last_known.clear()
            self._failing = False

    def incr_and_get(
        self, tenant_id: str, api: str, window_start: int, limit: int
    ) -> tuple[int, bool]:
        with self._lock:
            if self._failing:
                raise DependencyError("quota store injected failure")
            key = (tenant_id, api, window_start)
            first = key not in self._counts
            value = self._counts.get(key, 0) + 1
            self._counts[key] = value
            self._last_known[(tenant_id, api)] = (window_start, value)
            # 清理同维度历史窗口，避免无限增长
            for w in list(self._counts):
                if w[0] == tenant_id and w[1] == api and w[2] < window_start:
                    del self._counts[w]
            return value, first

    def get_last_known(self, tenant_id: str, api: str) -> tuple[int, int] | None:
        # 本地镜像：存储整体故障时仍然可读，作为 last_known 降级依据
        with self._lock:
            return self._last_known.get((tenant_id, api))

    def refund(self, tenant_id: str, api: str, window_start: int) -> None:
        with self._lock:
            if self._failing:
                raise DependencyError("quota store injected failure")
            key = (tenant_id, api, window_start)
            if self._counts.get(key, 0) > 0:
                value = self._counts[key] - 1
                self._counts[key] = value
                self._last_known[(tenant_id, api)] = (window_start, value)


class QuotaService:
    def __init__(
        self,
        store: QuotaStore,
        policy: QuotaPolicy,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._store = store
        self._policy = policy
        self._clock = clock or time.time

    def set_clock(self, clock: Callable[[], float]) -> None:
        self._clock = clock


    def _window_start(self) -> int:
        now = int(self._clock())
        return now - (now % self._policy.window_seconds)

    def _limit_for(self, api: str) -> int:
        return self._policy.limits.get(api, self._policy.default_limit)

    def check_and_consume(self, principal: Principal, scope: ApiScope) -> QuotaResult:
        limit = self._limit_for(scope.api)
        window_start = self._window_start()

        if not scope.write and not self._policy.read_quota:
            return QuotaResult(QuotaDecision.ALLOWED, limit, 0, window_start,
                               "read requests are not metered by policy")

        try:
            used, _first = self._store.incr_and_get(
                principal.tenant_id, scope.api, window_start, limit
            )
        except DependencyError as exc:
            return self._degrade(principal, scope, limit, window_start, str(exc))

        if used <= limit:
            result = QuotaResult(QuotaDecision.ALLOWED, limit, used, window_start,
                                 f"counter={used}/{limit} in window@{window_start}")
        else:
            result = QuotaResult(QuotaDecision.REJECTED, limit, used, window_start,
                                 f"counter={used}/{limit} exceeded in window@{window_start}")
        log_decision(
            "quota_check",
            tenant_id=principal.tenant_id,
            api=scope.api,
            decision=result.decision.value,
            reason=result.reason,
        )
        return result

    def _degrade(
        self,
        principal: Principal,
        scope: ApiScope,
        limit: int,
        window_start: int,
        cause: str,
    ) -> QuotaResult:
        mode = self._policy.fail_mode
        if mode is QuotaFailMode.OPEN:
            result = QuotaResult(
                QuotaDecision.DEGRADED_ALLOW, limit, -1, window_start,
                f"dependency failed ({cause}); fail_mode=open -> allow",
            )
        elif mode is QuotaFailMode.CLOSED:
            log_decision(
                "quota_check",
                tenant_id=principal.tenant_id, api=scope.api,
                decision="degraded_reject",
                reason="dependency failed; fail_mode=closed -> reject",
            )
            from app.errors import ErrorCode, GovernanceError

            raise GovernanceError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "quota subsystem unavailable; request rejected by fail-closed policy",
                reason=f"fail_mode=closed; cause_masked={type(DependencyError()).__name__}",
            )
        else:  # LAST_KNOWN
            known = self._store.get_last_known(principal.tenant_id, scope.api)
            if known is None:
                from app.errors import ErrorCode, GovernanceError

                raise GovernanceError(
                    ErrorCode.DEPENDENCY_UNAVAILABLE,
                    "quota subsystem unavailable and no last-known state; rejected",
                    reason="fail_mode=last_known but cache empty",
                )
            known_window, known_used = known
            if known_window < window_start:
                result = QuotaResult(
                    QuotaDecision.DEGRADED_ALLOW, limit, 0, window_start,
                    f"window rolled over ({known_window}->{window_start}); "
                    "new window presumed fresh under last_known policy",
                )
            elif known_used < limit:
                result = QuotaResult(
                    QuotaDecision.DEGRADED_ALLOW, limit, known_used, window_start,
                    f"last_known used={known_used}/{limit} -> allow",
                )
            else:
                result = QuotaResult(
                    QuotaDecision.DEGRADED_REJECT, limit, known_used, known_window,
                    f"last_known used={known_used}/{limit} -> reject",
                )
        log_decision(
            "quota_check",
            tenant_id=principal.tenant_id, api=scope.api,
            decision=result.decision.value, reason=result.reason,
        )
        return result

    def refund(self, principal: Principal, scope: ApiScope) -> None:
        """归还已预占的配额（用于幂等冲突等未产生副作用的路径），尽力而为。"""
        try:
            self._store.refund(
                principal.tenant_id, scope.api, self._window_start()
            )
        except DependencyError:
            log_decision(
                "quota_refund_failed",
                tenant_id=principal.tenant_id, api=scope.api,
                decision="ignored", reason="refund best-effort during outage",
            )
