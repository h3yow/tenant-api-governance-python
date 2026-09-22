"""治理配置：配额模型、降级策略。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class DegradeMode(str, Enum):
    """依赖异常 / 窗口切换不确定时的降级策略。"""

    FAIL_CLOSED = "fail_closed"   # 保守拒绝（默认，安全优先）
    FAIL_OPEN = "fail_open"       # 放行（可用性优先，仅幂等安全接口）
    USE_CACHE = "use_cache"       # 使用最近一次已知状态判定


@dataclass(frozen=True)
class QuotaRule:
    """单条配额规则：某租户对某接口在一个窗口内的最大调用次数。"""

    tenant_id: str
    api: str
    limit: int
    window_seconds: int = 60


@dataclass(frozen=True)
class GovernanceConfig:
    """治理总配置。"""

    quota_rules: tuple[QuotaRule, ...] = field(default_factory=tuple)
    degrade_mode: DegradeMode = DegradeMode.FAIL_CLOSED

    def quota_limit(self, tenant_id: str, api: str) -> tuple[int, int] | None:
        """返回 (上限, 窗口秒数)；未配置则返回 None（不限）。"""
        for rule in self.quota_rules:
            if rule.tenant_id == tenant_id and rule.api == api:
                return rule.limit, rule.window_seconds
        return None
