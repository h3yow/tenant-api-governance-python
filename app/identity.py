"""身份解析与权限边界：静态令牌目录（可替换为外部 IdP 适配器）。

规则：
- 令牌缺失或未知 -> 未认证（不可继续解析租户）。
- 已认证但缺少目标接口所需权限 -> 越权拒绝。
- 权限按接口粒度授予，构成调用方可访问的接口边界。
"""

from __future__ import annotations

from typing import Protocol

from app.models import ApiScope, AuthzDecision, Principal


class IdentityResolver(Protocol):
    def resolve(self, token: str | None) -> Principal | None: ...

    def authorize(self, principal: Principal, scope: ApiScope) -> AuthzDecision: ...


class StaticTokenResolver:
    """以内存目录演示：token -> 身份；角色 -> 接口权限集合。"""

    def __init__(
        self,
        directory: dict[str, Principal],
        role_permissions: dict[str, frozenset[str]],
    ) -> None:
        self._directory = dict(directory)
        self._role_permissions = role_permissions

    def resolve(self, token: str | None) -> Principal | None:
        if not token:
            return None
        return self._directory.get(token)

    def _permitted_apis(self, principal: Principal) -> frozenset[str]:
        allowed: set[str] = set()
        for role in principal.roles:
            allowed.update(self._role_permissions.get(role, frozenset()))
        return frozenset(allowed)

    def authorize(self, principal: Principal, scope: ApiScope) -> AuthzDecision:
        permitted = self._permitted_apis(principal)
        allowed = scope.api in permitted
        reason = (
            f"role(s)={sorted(principal.roles)} grant access to api={scope.api}"
            if allowed
            else f"role(s)={sorted(principal.roles)} lack permission for api={scope.api}"
        )
        return AuthzDecision(
            principal=principal, scope=scope, allowed=allowed, reason=reason
        )
