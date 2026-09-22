"""调用方身份解析与权限边界。"""

from __future__ import annotations

from dataclasses import dataclass

from governance.errors import DenyReason, GovernanceError


@dataclass(frozen=True)
class CallerIdentity:
    """解析后的调用方身份及其权限边界（允许调用的接口集合）。"""

    tenant_id: str
    caller: str
    allowed_apis: frozenset[str]


class IdentityResolver:
    """根据请求凭据解析租户身份与权限。

    凭据目录在构造时注入：``credentials -> (租户, 调用方, 允许接口)``。
    凭据缺失或无法识别均判定为 MISSING_IDENTITY（无法建立身份）；
    身份已建立但接口不在授权集合内则判定为 ACCESS_DENIED。
    """

    def __init__(
        self,
        directory: dict[str, tuple[str, str, frozenset[str]]] | None = None,
    ) -> None:
        self._directory = dict(directory or {})

    def resolve(self, credentials: str | None) -> CallerIdentity:
        """凭据缺失/无效 -> MISSING_IDENTITY。"""
        if not credentials:
            raise GovernanceError(
                DenyReason.MISSING_IDENTITY, "缺少调用凭据，无法解析租户身份"
            )
        entry = self._directory.get(credentials)
        if entry is None:
            raise GovernanceError(
                DenyReason.MISSING_IDENTITY, f"凭据无效，无法解析租户身份: {credentials!r}"
            )
        tenant_id, caller, allowed_apis = entry
        return CallerIdentity(
            tenant_id=tenant_id, caller=caller, allowed_apis=frozenset(allowed_apis)
        )

    def authorize(self, identity: CallerIdentity, api: str) -> None:
        """越权访问 -> ACCESS_DENIED；通过则返回 None。"""
        if api not in identity.allowed_apis:
            raise GovernanceError(
                DenyReason.ACCESS_DENIED,
                f"租户 {identity.tenant_id} 无权访问接口 {api}",
            )
