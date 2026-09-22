"""核心领域模型：调用方身份、接口范围、判定结果。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Principal:
    """解析后的调用方身份与权限边界。"""

    tenant_id: str
    caller_id: str
    roles: frozenset[str]


@dataclass(frozen=True)
class ApiScope:
    """接口维度（配额按 租户 + 接口 双维度约束）。"""

    api: str
    write: bool


@dataclass(frozen=True)
class AuthzDecision:
    principal: Principal
    scope: ApiScope
    allowed: bool
    reason: str
