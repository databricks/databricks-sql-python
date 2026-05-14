"""Translate the connector's ``AuthProvider`` into ``databricks_sql_kernel``
``Session`` auth kwargs.

This phase ships PAT only. The kernel-side PyO3 binding accepts
``auth_type='pat'``; OAuth / federation / custom credentials
providers are reserved but not yet wired in either layer. Non-PAT
auth raises ``NotSupportedError`` from this bridge so the failure
surfaces at session-open time with a clear message rather than
deep inside the kernel.

Token extraction goes through ``AuthProvider.add_headers({})``
rather than touching auth-provider-specific attributes, so the
bridge works uniformly for every PAT shape — including
``AccessTokenAuthProvider`` wrapped in ``TokenFederationProvider``
(which ``get_python_sql_connector_auth_provider`` does for every
provider it builds).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from databricks.sql.auth.authenticators import AccessTokenAuthProvider, AuthProvider
from databricks.sql.auth.token_federation import TokenFederationProvider
from databricks.sql.exc import NotSupportedError

logger = logging.getLogger(__name__)


_BEARER_PREFIX = "Bearer "


def _is_pat(auth_provider: AuthProvider) -> bool:
    """Return True iff this provider ultimately wraps an
    ``AccessTokenAuthProvider``.

    ``get_python_sql_connector_auth_provider`` always wraps the
    base provider in a ``TokenFederationProvider``, so an
    ``isinstance`` check against ``AccessTokenAuthProvider`` alone
    never matches in practice. We peek through the federation
    wrapper to find the real type.
    """
    if isinstance(auth_provider, AccessTokenAuthProvider):
        return True
    if isinstance(auth_provider, TokenFederationProvider) and isinstance(
        auth_provider.external_provider, AccessTokenAuthProvider
    ):
        return True
    return False


def _extract_bearer_token(auth_provider: AuthProvider) -> Optional[str]:
    """Pull the current bearer token out of an ``AuthProvider``.

    The connector's ``AuthProvider.add_headers`` mutates a header
    dict and writes the ``Authorization: Bearer <token>`` value.
    Going through that public surface keeps us insulated from
    provider-specific internals.

    Returns ``None`` if the provider did not write an Authorization
    header or wrote a non-Bearer scheme — neither is representable
    in the kernel's PAT auth surface.
    """
    headers: Dict[str, str] = {}
    auth_provider.add_headers(headers)
    auth = headers.get("Authorization")
    if not auth:
        return None
    if not auth.startswith(_BEARER_PREFIX):
        return None
    return auth[len(_BEARER_PREFIX) :]


def kernel_auth_kwargs(auth_provider: AuthProvider) -> Dict[str, Any]:
    """Build the kwargs passed to ``databricks_sql_kernel.Session(...)``.

    PAT (including ``TokenFederationProvider``-wrapped PAT) routes
    through the kernel's PAT path. Anything else raises
    ``NotSupportedError`` — the kernel binding doesn't accept OAuth
    today, and routing OAuth through PAT would silently break
    token refresh during long-running sessions.
    """
    if _is_pat(auth_provider):
        token = _extract_bearer_token(auth_provider)
        if not token:
            raise ValueError(
                "PAT auth provider did not produce a Bearer Authorization "
                "header; cannot route through the kernel's PAT path"
            )
        return {"auth_type": "pat", "access_token": token}

    raise NotSupportedError(
        f"The kernel backend (use_sea=True) currently only supports PAT auth, "
        f"but got {type(auth_provider).__name__}. Use use_sea=False (Thrift) "
        "for OAuth / federation / custom credential providers."
    )
