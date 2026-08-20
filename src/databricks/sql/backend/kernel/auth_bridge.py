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
- **OAuth U2M** — for ``auth_type`` ``databricks-oauth`` (the browser
  authorization-code flow), the connector's ``databricks-sql-python``
  app bundle (``client_id`` + ``redirect_ports`` list, with the optional
  ``oauth_client_id`` / ``oauth_redirect_port`` overriding it) is
  forwarded to the kernel's ``auth_type='oauth-u2m'`` and the kernel
  runs the browser flow itself.
- **Azure Entra (Azure AD)** — the KERNEL is the Azure-aware auth core (it
  owns the endpoints, scopes, app ids, and tenant discovery); the binding
  forwards the selector + credentials and does not construct endpoints:

  - ``azure-oauth`` (U2M) → **not supported on the kernel path**; rejected with
    a pointer to ``databricks-oauth``, whose in-house U2M browser flow works
    against Azure workspaces (the workspace federates login to Entra). A
    dedicated Azure U2M flow may return later (PECOBLR-4120).
  - ``azure-sp-m2m`` (M2M) → forward ``auth_type='azure-sp-m2m'`` with the
    Azure service-principal ``azure_client_id`` / ``azure_client_secret`` (plus
    optional ``azure_tenant_id`` / ``azure_workspace_resource_id``). The kernel
    builds the Entra v2.0 token endpoint and the ``{effective_app_id}/.default``
    scope, auto-discovers the tenant from the workspace's ``/aad/auth`` redirect
    when ``azure_tenant_id`` is omitted, and always sends the Azure SP
    management token (adding the ``X-Databricks-Azure-Workspace-Resource-Id``
    header when ``azure_workspace_resource_id`` is set) — matching the Thrift
    connector, so an RBAC-only SP can authenticate (PECOBLR-4141).

``identity_federation_client_id`` is forwarded with whichever auth shape
wins resolution. It selects mandatory SP-wide workload-identity token
exchange in the kernel; omitting it preserves BYOT / account-wide behavior.

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

from databricks.sql.auth.auth import (
    PYSQL_OAUTH_CLIENT_ID,
    PYSQL_OAUTH_REDIRECT_PORT_RANGE,
    PYSQL_OAUTH_SCOPES,
)
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
    hostname: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the kwargs passed to ``databricks_sql_kernel.Session(...)``.

    ``auth_options`` carries the raw connect() kwargs relevant to auth
    (``auth_type``, ``oauth_client_id``, ``oauth_client_secret``,
    ``oauth_redirect_port``, ``credentials_provider``,
    ``identity_federation_client_id``). They drive the OAuth decisions
    because the OAuth secret is consumed during ``AuthProvider``
    construction and can't be read back off the built provider.

    Resolution order:

    0. **Ambiguity guards** — reject conflicting auth signals *before*
       resolving, so an ambiguous request fails loudly at session-open
       rather than silently picking one flow (and failing later as a
       confusing 401 against the wrong principal):
       - a custom ``credentials_provider`` *and* M2M kwargs together;
       - a U2M ``auth_type`` (``databricks-oauth``) *and*
         ``oauth_client_secret`` together.

    (The Azure Entra auth types are handled up front, before these guards:
    ``azure-sp-m2m`` forwards to the kernel's Azure SP flow; ``azure-oauth``
    is rejected with a pointer to ``databricks-oauth``. See the module
    docstring.)
    1. **OAuth M2M** — ``oauth_client_id`` + ``oauth_client_secret``
       both present → forward raw creds to the kernel's ``oauth-m2m``.
    2. **PAT** — the built provider is (or wraps) an
       ``AccessTokenAuthProvider`` → extract the bearer token.
    3. **OAuth U2M** — ``auth_type`` is ``databricks-oauth`` → forward the
       connector's coupled ``databricks-sql-python`` bundle (``client_id``
       + ``redirect_ports`` list, defaulting scopes to ``PYSQL_OAUTH_SCOPES``
       when the caller supplies none) to the kernel's ``oauth-u2m``, so a
       bare U2M connection authenticates as ``databricks-sql-python`` —
       forwarding the connector's own OAuth app rather than the kernel's
       ``databricks-sql-connector`` default (PECOBLR-4039/4040). Unlike the
       Thrift path, a caller-supplied ``oauth_scopes`` is honored here.
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
    federation_client_id = opts.get("identity_federation_client_id")
    auth_type = opts.get("auth_type")
    has_m2m = bool(client_id and client_secret)

    # Azure Entra (Azure AD) auth types route to the kernel's GENERIC OAuth
    # flows with Azure values supplied as overrides — the kernel needs no
    # Azure-specific code. Handled up front, keyed on the explicit auth_type,
    # before the generic M2M/PAT/U2M routing below (azure-sp-m2m carries its
    # creds in azure_* kwargs, not oauth_client_id/secret, so it would
    # otherwise fall through to the final "unsupported" error).

    # azure-oauth (Azure AD U2M) is NOT supported on the kernel path. The
    # in-house `databricks-oauth` browser flow works against Azure workspaces
    # (the workspace federates the login to Entra), so it is the U2M path on the
    # kernel — reject `azure-oauth` with a clear pointer rather than silently
    # changing the user's selected flow. (A dedicated Azure U2M flow may return
    # later; for now Azure U2M = `databricks-oauth`.)
    if auth_type == "azure-oauth":
        raise NotSupportedError(
            "auth_type='azure-oauth' is not supported on use_kernel=True. Use "
            "auth_type='databricks-oauth' instead — the in-house OAuth U2M "
            "browser flow works against Azure Databricks workspaces (the "
            "workspace federates the login to Microsoft Entra). Or use the "
            "Thrift backend (default) for the dedicated Azure AD U2M flow."
        )

    # azure-sp-m2m (Azure service principal, client-credentials): forward the
    # selector + Azure SP credentials; the KERNEL owns Azure resolution (it is
    # the auth core). The kernel builds the Entra v2.0 token endpoint
    # (`{login}/{tenant}/oauth2/v2.0/token`) and the `{effective_app_id}/.default`
    # scope, and — when azure_tenant_id is omitted — auto-discovers the tenant
    # from the workspace's /aad/auth redirect, matching the Thrift backend
    # (so connect() is byte-identical between Thrift and use_kernel=True).
    # PECOBLR-4141.
    #
    # The Authorization bearer is the Databricks-audience data token; the kernel
    # also always sends the Azure SP management token, and adds the
    # X-Databricks-Azure-Workspace-Resource-Id header when
    # azure_workspace_resource_id is set — matching the Thrift connector, so an
    # RBAC-only SP (Azure role, not a workspace member) can authenticate.
    if auth_type == "azure-sp-m2m":
        azure_client_id = opts.get("azure_client_id")
        azure_client_secret = opts.get("azure_client_secret")
        if not (azure_client_id and azure_client_secret):
            raise ProgrammingError(
                "auth_type='azure-sp-m2m' requires azure_client_id and "
                "azure_client_secret."
            )
        kwargs = {
            "auth_type": "azure-sp-m2m",
            "azure_client_id": azure_client_id,
            "azure_client_secret": azure_client_secret,
        }
        # Optional passthroughs: the kernel auto-discovers the tenant when
        # absent, and always sends the Azure SP management token. When
        # azure_workspace_resource_id is set, the kernel adds the
        # X-Databricks-Azure-Workspace-Resource-Id header alongside it (for an
        # SP with an Azure RBAC role but no workspace membership) — matching the
        # Thrift connector.
        azure_tenant_id = opts.get("azure_tenant_id")
        if azure_tenant_id:
            kwargs["azure_tenant_id"] = azure_tenant_id
        azure_workspace_resource_id = opts.get("azure_workspace_resource_id")
        if azure_workspace_resource_id:
            kwargs["azure_workspace_resource_id"] = azure_workspace_resource_id
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
        return kwargs

    # 0. Ambiguity guards — fail before any flow is chosen.
    if client_secret and opts.get("credentials_provider") is not None:
        raise NotSupportedError(
            "Ambiguous auth on use_kernel=True: both a custom "
            "credentials_provider and oauth_client_secret were provided. "
            "Pass exactly one — oauth_client_id + oauth_client_secret for "
            "kernel-managed M2M, or use the Thrift backend (default) for "
            "credentials_provider."
        )
    if client_secret and auth_type == "databricks-oauth":
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
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
        return kwargs

    # 2. PAT (including TokenFederationProvider-wrapped PAT).
    if _is_pat(auth_provider):
        token = _extract_bearer_token(auth_provider)
        if not token:
            raise ProgrammingError(
                "PAT auth provider did not produce a Bearer Authorization "
                "header; cannot route through the kernel's PAT path"
            )
        kwargs = {"auth_type": "pat", "access_token": token}
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
        return kwargs

    # 3. OAuth U2M — browser authorization-code flow; the kernel runs it.
    #    Only databricks-oauth reaches here (azure-oauth rejected up front).
    #    Forward the connector's own databricks-sql-python bundle instead of
    #    the kernel's databricks-sql-connector default, for parity with the
    #    Thrift path. client_id + redirect ports are coupled per app (each
    #    registers its own redirect URIs): a caller port only overrides the
    #    default when an explicit client_id is also supplied. A caller may
    #    override oauth_scopes; absent one we forward PYSQL_OAUTH_SCOPES as
    #    the default. We forward the FULL PYSQL_OAUTH_REDIRECT_PORT_RANGE as
    #    ``redirect_ports`` so the kernel binds the first free port (busy-port
    #    fallback), mirroring the Thrift DatabricksOAuthProvider which retries
    #    the next port when one is bound. A caller overriding client_id
    #    supplies its own single registered port.
    if auth_type == "databricks-oauth":
        redirect_port = opts.get("oauth_redirect_port")
        # Honor a caller-supplied oauth_scopes (normalized to a list of
        # strings); fall back to the connector default when none is given.
        scopes = _normalize_scopes(opts.get("oauth_scopes"))
        kwargs = {
            "auth_type": "oauth-u2m",
            "client_id": client_id or PYSQL_OAUTH_CLIENT_ID,
            "redirect_ports": (
                [_coerce_redirect_port(redirect_port)]
                if client_id and redirect_port is not None
                else list(PYSQL_OAUTH_REDIRECT_PORT_RANGE)
            ),
            "oauth_scopes": scopes if scopes is not None else list(PYSQL_OAUTH_SCOPES),
        }
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
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
        f"(auth_type='databricks-oauth'), but got "
        f"{provider_desc} with auth_type={auth_type!r}. Use the Thrift "
        "backend (default) for other auth flows."
    )


def _coerce_redirect_port(redirect_port: Any) -> int:
    """Coerce an ``oauth_redirect_port`` value (which may arrive as a string,
    e.g. from a DSN) to an int.

    A non-numeric value is a caller error; surface it as a PEP 249
    ``ProgrammingError`` (as ``_normalize_scopes`` does for malformed
    ``oauth_scopes``) rather than a bare ``ValueError``, so callers get a
    consistent, actionable exception type for garbled input."""
    try:
        return int(redirect_port)
    except (TypeError, ValueError):
        raise ProgrammingError(
            f"oauth_redirect_port must be an integer (or a string parseable as "
            f"one), got {redirect_port!r}."
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
