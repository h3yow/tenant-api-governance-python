"""治理配置：配额模型、写入策略、降级策略、结论保留策略。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum


class DegradeMode(str, Enum):
    """依赖异常 / 窗口切换不确定时的降级策略。"""

    FAIL_CLOSED = "fail_closed"   # 保守拒绝（默认，安全优先）
    FAIL_OPEN = "fail_open"       # 放行配额闸门（可用性优先；幂等闸门永不放开）
    USE_CACHE = "use_cache"       # 使用最近一次已知状态判定


@dataclass(frozen=True)
class QuotaRule:
    """单条配额规则：某租户对某接口在一个窗口内的最大调用次数。"""

    tenant_id: str
    api: str
    limit: int
    window_seconds: int = 60


@dataclass(frozen=True)
class WritePolicy:
    """某个租户×接口生效中的写入策略（已合并默认值后的最终裁决视图）。

    - ``require_idempotency_key``：写请求是否必须携带提交标识。
    - ``degrade_mode``：治理依赖存储故障时的降级裁决；注意即使
      FAIL_OPEN 也只放开 *配额* 闸门，幂等/结算闸门永远保守拒绝，
      不会因为放行配置而放过重复副作用。
    """

    require_idempotency_key: bool = False
    degrade_mode: DegradeMode = DegradeMode.FAIL_CLOSED


@dataclass(frozen=True)
class WritePolicyRule:
    """租户×接口粒度的写入策略配置项；degrade_mode 为 None 时取全局默认。"""

    tenant_id: str
    api: str
    require_idempotency_key: bool = False
    degrade_mode: DegradeMode | None = None


@dataclass(frozen=True)
class RetentionConfig:
    """已完成提交结论的保留与淘汰规则。

    - ``result_ttl_seconds``：已完成/已拒绝结论的保留时长，按 **最近一次访问**
      （落定、重放、查询都会刷新）滑动计算；None 表示不按时间过期。
    - ``max_completed_records``：每租户已完成结论的容量上限；None 表示不限。
      超容量时按 LRU 淘汰最久未访问的 *已完成* 结论（处理中、刚访问的不淘汰）。
    - ``tombstone_ttl_seconds``：被过期/淘汰的旧标识以“墓碑”形式继续挡住
      静默重放的保留时长；超期后标识彻底消失（按不存在处理，调用方应换新标识）。

    结算存储每次判定都实时读取当前配置（见
    ``InMemoryIdempotencyStore(retention_provider=...)``），因此运行中调整
    立即对后续请求生效。
    """

    result_ttl_seconds: float | None = 86400.0
    max_completed_records: int | None = 10_000
    tombstone_ttl_seconds: float = 86400.0


@dataclass(frozen=True)
class GovernanceConfig:
    """治理总配置（不可变快照）。"""

    quota_rules: tuple[QuotaRule, ...] = field(default_factory=tuple)
    degrade_mode: DegradeMode = DegradeMode.FAIL_CLOSED
    write_policy_rules: tuple[WritePolicyRule, ...] = field(default_factory=tuple)
    retention: RetentionConfig = field(default_factory=RetentionConfig)

    def quota_limit(self, tenant_id: str, api: str) -> tuple[int, int] | None:
        """返回 (上限, 窗口秒数)；未配置则返回 None（不限）。"""
        for rule in self.quota_rules:
            if rule.tenant_id == tenant_id and rule.api == api:
                return rule.limit, rule.window_seconds
        return None

    def write_policy(self, tenant_id: str, api: str) -> WritePolicy:
        """返回某租户×接口合并默认值后的生效写入策略。"""
        for rule in self.write_policy_rules:
            if rule.tenant_id == tenant_id and rule.api == api:
                return WritePolicy(
                    require_idempotency_key=rule.require_idempotency_key,
                    degrade_mode=rule.degrade_mode or self.degrade_mode,
                )
        return WritePolicy(degrade_mode=self.degrade_mode)


class PolicyRegistry:
    """治理配置的原子快照注册表，支持不重启调整策略。

    每个请求在进入判定链时只取一次快照（``get``），整次请求都基于同一
    不可变 ``GovernanceConfig`` 裁决，因此：

    - 策略更新对调整时刻 *之后* 进入的所有并发请求统一生效，不会同一批
      并发一半走旧策略、一半走新策略；
    - 单次请求内部也绝不会混用新旧两份配置。
    """

    def __init__(self, config: GovernanceConfig) -> None:
        self._lock = threading.Lock()
        self._config = config

    def get(self) -> GovernanceConfig:
        with self._lock:
            return self._config

    def update(self, config: GovernanceConfig) -> None:
        """原子替换生效配置；已在处理中的请求继续使用旧快照。"""
        with self._lock:
            self._config = config
