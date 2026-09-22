"""身份解析与权限边界测试。"""

from __future__ import annotations

import pytest

from governance.errors import DenyReason, GovernanceError
from governance.identity import IdentityResolver


@pytest.fixture()
def resolver() -> IdentityResolver:
    return IdentityResolver(
        {
            "token-t1": ("t1", "alice", frozenset({"/orders"})),
        }
    )


def test_missing_credentials_is_missing_identity(resolver: IdentityResolver) -> None:
    with pytest.raises(GovernanceError) as exc:
        resolver.resolve(None)
    assert exc.value.reason is DenyReason.MISSING_IDENTITY


def test_unknown_credentials_is_missing_identity(resolver: IdentityResolver) -> None:
    with pytest.raises(GovernanceError) as exc:
        resolver.resolve("bogus")
    assert exc.value.reason is DenyReason.MISSING_IDENTITY


def test_unauthorized_api_is_access_denied(resolver: IdentityResolver) -> None:
    identity = resolver.resolve("token-t1")
    assert identity.tenant_id == "t1"
    with pytest.raises(GovernanceError) as exc:
        resolver.authorize(identity, "/admin")
    assert exc.value.reason is DenyReason.ACCESS_DENIED


def test_authorized_api_passes(resolver: IdentityResolver) -> None:
    identity = resolver.resolve("token-t1")
    assert resolver.authorize(identity, "/orders") is None
