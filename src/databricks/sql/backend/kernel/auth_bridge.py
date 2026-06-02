"""Translate the connector's auth configuration into
``databricks_sql_kernel`` ``Session`` auth kwargs.

Three auth shapes are supported on the kernel path:

- **PAT** — extracted from the built ``AuthProvider`` (works for
  ``AccessTokenAuthProvider``, including the ``TokenFederationProvider``
  wrapper that ``get_python_sql_connector_auth_provider`` always
  applies). Maps to the kernel's ``auth_type='pat'``.
- **OAuth M2M** — when the caller passes ``oauth_client_id`` +
  ``oauth_client_secret``, the *raw* credentials are forwarded to the
  kernel's ``auth_type='oauth-m2m'`` and the kernel owns the full
  token lifecycle (acquire + refresh via workspace OIDC
  client-credentials). We forward the raw pair rather than reusing the
  connector's own OAuth provider because the kernel re-mints tokens
  itself and the client secret is not recoverable from a built
  provider.
- **OAuth U2M** — for ``auth_type`` ``databricks-oauth`` /
  ``azure-oauth`` (the browser authorization-code flow), the optional
  ``oauth_client_id`` / ``oauth_redirect_port`` are forwarded to the
  kernel's ``auth_type='oauth-u2m'`` and the kernel runs the browser
  flow itself.

A user-supplied custom ``credentials_provider`` is **rejected** on the
kernel path with ``NotSupportedError``: it's an opaque token source
with no extractable raw credentials, so the kernel can't own the
lifecycle. Such callers should pass ``oauth_client_id`` /
``oauth_client_secret`` (M2M) instead. Anything else non-PAT also
raises ``NotSupportedError`` so the failure surfaces at session-open
with a clear message rather than deep inside the kernel.

The M2M / U2M decisions are driven by the *raw* connect() kwargs
(``auth_options``), not a built ``AuthProvider``. On the kernel path
the connector deliberately does **not** build its own OAuth provider
(that would eagerly run the U2M browser flow / M2M token exchange at
connect() time, before the kernel is consulted), so ``auth_provider``
is either a minimal PAT provider or ``None`` and the OAuth credentials
are available only from the raw kwargs.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

from databricks.sql.auth.authenticators import AccessTokenAuthProvider, AuthProvider
from databricks.sql.auth.token_federation import TokenFederationProvider
from databricks.sql.exc import NotSupportedError, ProgrammingError

logger = logging.getLogger(__name__)


# RFC 6750 §2.1 defines the Authorization scheme as case-insensitive.
# The connector's auth providers all emit ``Bearer `` exactly today,
# but we match leniently in case a federation proxy or future provider
# normalises the casing differently — failing closed here would surface
# as a confusing ``ProgrammingError`` from the bridge.
_BEARER_PREFIX_LEN = len("Bearer ")

# Defense-in-depth: reject tokens containing ASCII control characters
# or whitespace. CR/LF/NUL in a token would let a misbehaving HTTP
# stack split or terminate the Authorization header line, opening a
# header-injection sink. Space (0x20) is included so leading-/
# embedded-whitespace tokens (e.g. ``"Bearer  doubled-space-token"``,
# tab-prefixed token) get rejected too — RFC 6750 §2.1 forbids
# whitespace within the credential token itself.
_TOKEN_REJECT_RE = re.compile(r"[\x00-\x20\x7f]")


def _is_pat(auth_provider: Optional[AuthProvider]) -> bool:
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


def _extract_bearer_token(auth_provider: Optional[AuthProvider]) -> Optional[str]:
    """Pull the current bearer token out of an ``AuthProvider``.

    The connector's ``AuthProvider.add_headers`` mutates a header
    dict and writes the ``Authorization: Bearer <token>`` value.
    Going through that public surface keeps us insulated from
    provider-specific internals.

    Returns ``None`` if there is no provider, the provider did not
    write an Authorization header, or it wrote a non-Bearer scheme —
    none of which is representable in the kernel's PAT auth surface.
    """
    if auth_provider is None:
        return None
    headers: Dict[str, str] = {}
    auth_provider.add_headers(headers)
    auth = headers.get("Authorization")
    if not auth:
        return None
    if not auth[:_BEARER_PREFIX_LEN].lower() == "bearer ":
        return None
    token = auth[_BEARER_PREFIX_LEN:]
    if _TOKEN_REJECT_RE.search(token):
        raise ProgrammingError(
            "Bearer token contains ASCII control characters or whitespace; "
            "refusing to forward it to the kernel auth bridge."
        )
    return token


def kernel_auth_kwargs(
    auth_provider: Optional[AuthProvider],
    auth_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the kwargs passed to ``databricks_sql_kernel.Session(...)``.

    ``auth_options`` carries the raw connect() kwargs relevant to auth
    (``auth_type``, ``oauth_client_id``, ``oauth_client_secret``,
    ``oauth_redirect_port``, ``credentials_provider``). They drive the
    OAuth decisions because the OAuth secret is consumed during
    ``AuthProvider`` construction and can't be read back off the built
    provider.

    Resolution order:

    0. **Ambiguity guards** — reject conflicting auth signals *before*
       resolving, so an ambiguous request fails loudly at session-open
       rather than silently picking one flow (and failing later as a
       confusing 401 against the wrong principal):
       - a custom ``credentials_provider`` *and* M2M kwargs together;
       - a U2M ``auth_type`` (``databricks-oauth`` / ``azure-oauth``)
         *and* ``oauth_client_secret`` together.
    1. **OAuth M2M** — ``oauth_client_id`` + ``oauth_client_secret``
       both present → forward raw creds to the kernel's ``oauth-m2m``.
    2. **PAT** — the built provider is (or wraps) an
       ``AccessTokenAuthProvider`` → extract the bearer token.
    3. **OAuth U2M** — ``auth_type`` is ``databricks-oauth`` /
       ``azure-oauth`` → forward optional ``oauth_client_id`` /
       ``oauth_redirect_port`` to the kernel's ``oauth-u2m``.
    4. **Custom credentials_provider** → ``NotSupportedError`` (opaque
       token source; no raw creds for the kernel to own).
    5. Anything else → ``NotSupportedError``.

    M2M is checked before PAT so that a workload passing both an
    access token *and* M2M creds resolves to the (refreshing) M2M path
    rather than a static token. (Token + M2M is not treated as
    ambiguous: a PAT is often present as ambient config the caller
    didn't intend as the primary credential, whereas an explicit
    ``oauth_client_secret`` is unambiguous M2M intent.)
    """
    opts = auth_options or {}

    client_id = opts.get("oauth_client_id")
    client_secret = opts.get("oauth_client_secret")
    auth_type = opts.get("auth_type")
    has_m2m = bool(client_id and client_secret)

    # 0. Ambiguity guards — fail before any flow is chosen.
    if client_secret and opts.get("credentials_provider") is not None:
        raise NotSupportedError(
            "Ambiguous auth on use_kernel=True: both a custom "
            "credentials_provider and oauth_client_secret were provided. "
            "Pass exactly one — oauth_client_id + oauth_client_secret for "
            "kernel-managed M2M, or use the Thrift backend (default) for "
            "credentials_provider."
        )
    if client_secret and auth_type in ("databricks-oauth", "azure-oauth"):
        raise NotSupportedError(
            f"Ambiguous auth on use_kernel=True: auth_type={auth_type!r} selects "
            "the U2M browser flow, but oauth_client_secret was also provided "
            "(machine-to-machine). Drop oauth_client_secret for U2M, or drop "
            "auth_type for M2M."
        )

    # 1. OAuth M2M — raw client-credentials pair forwarded to the kernel.
    if has_m2m:
        kwargs: Dict[str, Any] = {
            "auth_type": "oauth-m2m",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        scopes = _normalize_scopes(opts.get("oauth_scopes"))
        if scopes is not None:
            kwargs["oauth_scopes"] = scopes
        return kwargs

    # 2. PAT (including TokenFederationProvider-wrapped PAT).
    if _is_pat(auth_provider):
        token = _extract_bearer_token(auth_provider)
        if not token:
            raise ProgrammingError(
                "PAT auth provider did not produce a Bearer Authorization "
                "header; cannot route through the kernel's PAT path"
            )
        return {"auth_type": "pat", "access_token": token}

    # 3. OAuth U2M — browser authorization-code flow; the kernel runs it.
    if auth_type in ("databricks-oauth", "azure-oauth"):
        kwargs = {"auth_type": "oauth-u2m"}
        if client_id:
            kwargs["client_id"] = client_id
        redirect_port = opts.get("oauth_redirect_port")
        if redirect_port is not None:
            kwargs["redirect_port"] = int(redirect_port)
        scopes = _normalize_scopes(opts.get("oauth_scopes"))
        if scopes is not None:
            kwargs["oauth_scopes"] = scopes
        return kwargs

    # 4. Custom credentials_provider — the connector's primary M2M path
    #    on Thrift/SEA, but unusable on the kernel: it's an opaque token
    #    source with no extractable client_id/secret, so the kernel
    #    can't own the token lifecycle. Point the caller at the raw
    #    M2M kwargs instead.
    if opts.get("credentials_provider") is not None:
        raise NotSupportedError(
            "use_kernel=True does not support a custom credentials_provider. "
            "For OAuth machine-to-machine auth, pass oauth_client_id and "
            "oauth_client_secret so the kernel can manage the token lifecycle "
            "directly; or use the Thrift backend (default) with "
            "credentials_provider."
        )

    # 5. Everything else (including no usable credentials at all —
    #    ``auth_provider`` is None on the kernel path when no access
    #    token was supplied and no OAuth kwargs resolved above).
    provider_desc = (
        type(auth_provider).__name__ if auth_provider is not None else "no credentials"
    )
    raise NotSupportedError(
        f"use_kernel=True requires PAT (access_token), OAuth M2M "
        f"(oauth_client_id + oauth_client_secret), or OAuth U2M "
        f"(auth_type='databricks-oauth' / 'azure-oauth'), but got "
        f"{provider_desc} with auth_type={auth_type!r}. Use the Thrift "
        "backend (default) for other auth flows."
    )


def _normalize_scopes(scopes: Any) -> Optional[list]:
    """Normalise an ``oauth_scopes`` value to a list of strings, or
    ``None`` to let the kernel apply its defaults.

    Accepts a list/tuple of strings or a single space-delimited string
    (the shape ``DatabricksOAuthProvider`` stores internally)."""
    if scopes is None:
        return None
    if isinstance(scopes, str):
        parts = scopes.split()
        return parts or None
    if isinstance(scopes, (list, tuple)):
        parts = [str(s) for s in scopes if s]
        return parts or None
    # Anything else (int, dict, bool, …) is a caller error. Fail loudly
    # rather than silently dropping the scopes to None and surprising
    # the user with default scopes.
    raise ProgrammingError(
        f"oauth_scopes must be a list/tuple of strings or a space-delimited "
        f"string, got {type(scopes).__name__}."
    )
