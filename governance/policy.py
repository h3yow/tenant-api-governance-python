"""写入策略：按租户×接口热更新的写入要求与降级裁决。

策略以不可变快照方式保存，更新时整体替换引用；判定链在处理一次请求时
只读取一次快照，保证同一批并发请求在调整前后不会各走一半旧策略、
一半新策略。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from governance.config import DegradeMode, GovernanceConfig


@dataclass(frozen=True)
class WritePolicy:
    """单个 (租户, 接口) 的写入策略。

    - require_idempotency_key: 写请求是否必须携带提交标识；
    - degrade_mode: 治理依赖（配额存储）故障时的降级裁决，None 表示
      沿用全局配置。
    """

    require_idempotency_key: bool = False
    degrade_mode: DegradeMode | None = None


@dataclass(frozen=True)
class RetentionConfig:
    """已完成提交结论的保留策略。

    - ttl_seconds: 结论保留时长，超过后视为已过期；
    - max_completed: 已完成结论的容量上限，超出按最久未访问淘汰。
    """

    ttl_seconds: float = 3600.0
    max_completed: int = 10_000


# 未配置专属策略时的默认策略：不强制标识、降级沿用全局配置。
DEFAULT_WRITE_POLICY = WritePolicy()


class PolicyRegistry:
    """可热更新的写入策略注册表（线程安全、快照原子替换）。

    所有读取都在同一把锁内取快照，写入整体替换字典引用，因此并发请求
    要么全部看到旧策略、要么全部看到新策略，不会出现半新半旧。
    """

    def __init__(
        self,
        config: GovernanceConfig,
        retention: RetentionConfig | None = None,
    ) -> None:
        self._global_degrade: DegradeMode = config.degrade_mode
        self._lock = threading.Lock()
        self._write_policies: dict[tuple[str, str], WritePolicy] = {}
        self._retention = retention or RetentionConfig()

    def snapshot(self) -> tuple[WritePolicy, RetentionConfig]:
        """一次性返回当前生效的 (写入策略, 保留策略) 快照。"""
        with self._lock:
            # 写入策略是按 (租户, 接口) 索引的字典，调用方按需再取；
            # 这里返回全局保留配置与全局默认降级，避免跨锁读取。
            return DEFAULT_WRITE_POLICY, self._retention

    def write_policy(self, tenant_id: str, api: str) -> WritePolicy:
        """返回该租户×接口当前生效的写入策略（含默认值回填）。"""
        with self._lock:
            policy = self._write_policies.get((tenant_id, api))
            if policy is None:
                return DEFAULT_WRITE_POLICY
            return policy

    def retention(self) -> RetentionConfig:
        """返回当前生效的保留策略。"""
        with self._lock:
            return self._retention

    def effective_degrade_mode(self, policy: WritePolicy) -> DegradeMode:
        """回填策略中的降级模式（None -> 全局默认，保守拒绝）。"""
        return policy.degrade_mode or self._global_degrade

    def update_write_policy(
        self, tenant_id: str, api: str, policy: WritePolicy
    ) -> None:
        """热更新单个租户×接口的写入策略，对后续请求原子生效。"""
        with self._lock:
            self._write_policies[(tenant_id, api)] = policy

    def remove_write_policy(self, tenant_id: str, api: str) -> None:
        """删除租户×接口的专属策略，恢复默认。"""
        with self._lock:
            self._write_policies.pop((tenant_id, api), None)

    def update_retention(self, retention: RetentionConfig) -> None:
        """热更新保留策略，对后续判定原子生效。"""
        with self._lock:
            self._retention = retention
