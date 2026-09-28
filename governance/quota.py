"""配额计数：租户 × 接口 × 时间窗口。

设计要点
--------
1. 配额同时受 *租户* 与 *接口* 两个维度约束（见 ``config.QuotaRule``）。
2. 计数器以固定窗口编号 ``epoch // window_seconds`` 为键，窗口切换后
   自动从 0 开始，行为确定、不依赖惰性过期。
3. ``try_acquire`` 在存储内部以锁保证原子：并发请求不会出现
   "读到相同旧值后双双放行" 的超卖，也不会因被拒绝的请求而虚增计数。
4. 存储故障由 Enforcer 按可配置降级策略统一裁决，绝不把底层异常透出。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Protocol

from governance.config import (
    DegradeMode,
    GovernanceConfig,
    PolicyRegistry,
)
from governance.errors import DenyReason, GovernanceError

logger = logging.getLogger("governance.quota")


class StoreUnavailable(RuntimeError):
    """底层配额存储不可用（连接失败、超时等），仅在治理层内部流转。"""


class QuotaStore(Protocol):
    """配额状态存储协议（可替换为 Redis 等实现）。"""

    def try_acquire(
        self, tenant_id: str, api: str, window_seconds: int, limit: int
    ) -> tuple[bool, int]:
        """原子地判定并占用一个配额。

        返回 ``(是否放行, 占用后窗口内计数)``；计数达到上限时不放行、
        也不递增，保证被拒请求不虚耗配额。
        """
        ...

    def current(self, tenant_id: str, api: str, window_seconds: int) -> int:
        """返回当前窗口已用计数（不递增）。"""
        ...


class InMemoryQuotaStore:
    """线程安全的固定窗口内存存储，时钟可注入（便于跨窗口测试）。"""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, str, int], int] = {}

    def _window(self, window_seconds: int) -> int:
        return int(self._clock()) // window_seconds

    def try_acquire(
        self, tenant_id: str, api: str, window_seconds: int, limit: int
    ) -> tuple[bool, int]:
        key = (tenant_id, api, self._window(window_seconds))
        with self._lock:
            used = self._counters.get(key, 0)
            if used >= limit:
                return False, used
            used += 1
            self._counters[key] = used
            return True, used

    def current(self, tenant_id: str, api: str, window_seconds: int) -> int:
        key = (tenant_id, api, self._window(window_seconds))
        with self._lock:
            return self._counters.get(key, 0)


class FailingQuotaStore:
    """测试用：始终抛出底层依赖故障的存储。"""

    def try_acquire(
        self, tenant_id: str, api: str, window_seconds: int, limit: int
    ) -> tuple[bool, int]:
        raise StoreUnavailable("quota backend connection refused")

    def current(self, tenant_id: str, api: str, window_seconds: int) -> int:
        raise StoreUnavailable("quota backend connection refused")


class QuotaEnforcer:
    """配额判定器：封装配额查询与依赖异常时的降级裁决。

    ``provider`` 既可传入静态 ``GovernanceConfig``，也可传入可热更新的
    ``PolicyRegistry``；请求可显式传入本次判定专用的不可变 ``snapshot``，
    以保证同一次请求 / 同一批并发请求按同一策略版本裁决。
    """

    def __init__(
        self,
        provider: GovernanceConfig | PolicyRegistry,
        store: QuotaStore,
        degrade_mode: DegradeMode | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._provider = provider
        self._store = store
        # 构造期显式覆盖（测试或临时固定模式用）；None 表示按配置逐请求解析。
        self._override_degrade = degrade_mode
        self._clock = clock
        self._cache_lock = threading.Lock()
        # 最近一次已知窗口计数：键为 (租户, 接口, 窗口编号)
        self._last_known: dict[tuple[str, str, int], int] = {}

    def _snapshot(self, snapshot: GovernanceConfig | None) -> GovernanceConfig:
        if snapshot is not None:
            return snapshot
        provider = self._provider
        if isinstance(provider, PolicyRegistry):
            return provider.get()
        return provider

    def reset_cache(self) -> None:
        with self._cache_lock:
            self._last_known.clear()

    def check_and_consume(
        self,
        tenant_id: str,
        api: str,
        snapshot: GovernanceConfig | None = None,
    ) -> None:
        """消耗一个配额；耗尽 -> QUOTA_EXHAUSTED；依赖异常 -> 按降级策略。"""
        config = self._snapshot(snapshot)
        rule = config.quota_limit(tenant_id, api)
        if rule is None:
            logger.info(
                "tenant=%s api=%s quota=unlimited decision=allow basis=no_quota_rule",
                tenant_id, api,
            )
            return
        limit, window_seconds = rule
        window = int(self._clock()) // window_seconds
        try:
            granted, used = self._store.try_acquire(
                tenant_id, api, window_seconds, limit
            )
        except StoreUnavailable as exc:
            degrade = (
                self._override_degrade
                or config.write_policy(tenant_id, api).degrade_mode
            )
            self._degrade_decide(tenant_id, api, limit, window, exc, degrade)
            return
        with self._cache_lock:
            self._last_known[(tenant_id, api, window)] = used
        if not granted:
            logger.info(
                "tenant=%s api=%s window=%s used=%s limit=%s decision=deny "
                "basis=quota_exhausted",
                tenant_id, api, window, used, limit,
            )
            raise GovernanceError(
                DenyReason.QUOTA_EXHAUSTED,
                f"租户 {tenant_id} 调用 {api} 的配额已耗尽 "
                f"({used}/{limit}，窗口 {window_seconds}s)",
            )
        logger.info(
            "tenant=%s api=%s window=%s used=%s limit=%s decision=allow "
            "basis=quota_granted",
            tenant_id, api, window, used, limit,
        )

    def _degrade_decide(
        self,
        tenant_id: str,
        api: str,
        limit: int,
        window: int,
        exc: StoreUnavailable,
        mode: DegradeMode,
    ) -> None:
        if mode is DegradeMode.FAIL_OPEN:
            logger.warning(
                "tenant=%s api=%s decision=allow basis=degrade_fail_open error=%s",
                tenant_id, api, exc,
            )
            return
        if mode is DegradeMode.USE_CACHE:
            with self._cache_lock:
                last = self._last_known.get((tenant_id, api, window))
            if last is not None and last < limit:
                logger.warning(
                    "tenant=%s api=%s window=%s last_known=%s limit=%s "
                    "decision=allow basis=degrade_cache",
                    tenant_id, api, window, last, limit,
                )
                return
            logger.warning(
                "tenant=%s api=%s window=%s last_known=%s decision=deny "
                "basis=degrade_cache_exhausted_or_unknown",
                tenant_id, api, window, last,
            )
        else:
            logger.warning(
                "tenant=%s api=%s decision=deny basis=degrade_fail_closed error=%s",
                tenant_id, api, exc,
            )
        raise GovernanceError(
            DenyReason.DEPENDENCY_UNAVAILABLE,
            "配额服务暂不可用，已按降级策略拒绝该请求",
        )
