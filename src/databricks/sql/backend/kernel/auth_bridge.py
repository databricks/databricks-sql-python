"""Translate the connector's ``AuthProvider`` into ``databricks_sql_kernel``
``Session`` auth kwargs.

The connector already implements every auth flow it supports (PAT,
OAuth M2M, OAuth U2M, external token providers, federation). The
kernel must not re-implement them. Decision D9 in the integration
design: PAT goes through the kernel's PAT path; everything else
delegates back to the connector via the kernel's ``External``
trampoline, with a Python callback that returns a fresh bearer
token.

Token extraction goes through ``AuthProvider.add_headers({})``
rather than touching auth-provider-specific attributes, so the
bridge works for every subclass — including custom providers a
caller may have wired in.

End-to-end limitation: the kernel's
``build_auth_provider`` currently rejects ``AuthConfig::External``
("reserved; v0 wires PAT + OAuthM2M + OAuthU2M only"). Until the
kernel-side follow-up PR lands, non-PAT auth surfaces a clear
``KernelError(code='InvalidArgument', message='AuthConfig::External
is reserved...')`` from ``Session.open_session``. PAT works today.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from databricks.sql.auth.authenticators import AccessTokenAuthProvider, AuthProvider
from databricks.sql.auth.token_federation import TokenFederationProvider

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
    header or wrote a non-Bearer scheme — neither shape is
    representable in the kernel's auth surface today.
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

    Two routing decisions:

    1. ``AccessTokenAuthProvider`` → ``auth_type='pat'`` with the
       static token. Kernel uses it verbatim for every request.
    2. Anything else → ``auth_type='external'`` with a callback that
       calls ``auth_provider.add_headers({})`` and returns the
       fresh bearer token. The connector keeps owning the OAuth /
       MSAL / federation flow; the kernel asks for a token whenever
       it needs one.

    The PAT special-case exists because it's the only path the
    kernel actually serves end-to-end today. Once the kernel-side
    External enablement lands, PAT could collapse into the
    External path too (one callback that returns the static token);
    but keeping the explicit ``pat`` route means the kernel does
    not pay the GIL-reacquire cost on every HTTP request for PAT
    users.
    """
    if _is_pat(auth_provider):
        # PAT case: pull the static token out and feed the kernel's
        # PAT path. We go through ``add_headers`` regardless of
        # whether the provider was wrapped in TokenFederation or
        # not — both shapes write the same Authorization header.
        token = _extract_bearer_token(auth_provider)
        if not token:
            raise ValueError(
                "PAT auth provider did not produce a Bearer Authorization "
                "header; cannot route through the kernel's PAT path"
            )
        return {"auth_type": "pat", "access_token": token}

    # Every other provider: trampoline a callback. The callback is
    # invoked once per HTTP request that needs auth (the kernel does
    # not cache the returned token), so the auth_provider's own
    # caching is what keeps this fast.
    def token_callback() -> str:
        token = _extract_bearer_token(auth_provider)
        if not token:
            raise RuntimeError(
                f"{type(auth_provider).__name__}.add_headers did not produce "
                "a Bearer Authorization header; cannot supply a token to the kernel"
            )
        return token

    logger.debug(
        "Routing %s through kernel External trampoline",
        type(auth_provider).__name__,
    )
    return {"auth_type": "external", "token_callback": token_callback}
