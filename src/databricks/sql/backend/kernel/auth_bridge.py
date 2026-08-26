"""Translate the connector's auth configuration into
``databricks_sql_kernel`` ``Session`` auth kwargs.

The following auth shapes are supported on the kernel path:

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
- **OAuth U2M** — for ``auth_type`` ``databricks-oauth`` *or* ``azure-oauth``
  (the browser authorization-code flow), the connector's ``databricks-sql-python``
  app bundle (``client_id`` + ``redirect_ports`` list, with the optional
  ``oauth_client_id`` / ``oauth_redirect_port`` overriding it) is
  forwarded to the kernel's ``auth_type='oauth-u2m'`` and the kernel
  runs the browser flow itself.
- **Azure Entra (Azure AD)** — the KERNEL is the Azure-aware auth core (it
  owns the endpoints, scopes, app ids, and tenant discovery); the binding
  forwards the selector + credentials and does not construct endpoints:

  - ``azure-oauth`` (U2M) → routed to the kernel's ``oauth-u2m`` via the shared
    U2M path above, exactly like ``databricks-oauth``. The kernel runs the
    in-house workspace-federated browser flow (the workspace federates login to
    Entra), which Azure Databricks workspaces support; it forwards the
    ``databricks-sql-python`` bundle, **not** the Thrift Azure app
    (``96eecda7`` / redirect port ``8030``) — that app is registered for
    Thrift's *direct-Entra* flow (``login.microsoftonline.com`` config +
    ``{tenant}/user_impersonation`` scopes), which the kernel does not perform.
    So on the kernel path ``azure-oauth`` and ``databricks-oauth`` are the same
    flow (PECOBLR-4120).
  - ``azure-sp-m2m`` (M2M) → forward ``auth_type='azure-sp-m2m'`` with the
    Azure service-principal ``azure_client_id`` / ``azure_client_secret`` (plus
    optional ``azure_tenant_id`` / ``azure_workspace_resource_id``). The kernel
    builds the Entra v2.0 token endpoint and the ``{effective_app_id}/.default``
    scope, and auto-discovers the tenant from the workspace's ``/aad/auth``
    redirect when ``azure_tenant_id`` is omitted. When
    ``azure_workspace_resource_id`` is set, the kernel also sends the Azure SP
    management token + ``X-Databricks-Azure-Workspace-Resource-Id`` header
    (matching the JDBC driver), so an RBAC-only SP — one with an Azure role but
    no workspace membership — can authenticate; omit it for a workspace-member
    SP (PECOBLR-4141).

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
       - a U2M ``auth_type`` (``databricks-oauth`` / ``azure-oauth``) *and*
         ``oauth_client_secret`` together.

    (The ``azure-sp-m2m`` Azure Entra auth type is handled up front, before
    these guards, forwarding to the kernel's Azure SP flow. ``azure-oauth`` is
    a U2M type and is handled by the OAuth U2M step alongside
    ``databricks-oauth``. See the module docstring.)
    1. **OAuth M2M (JWT private key)** — ``oauth_jwt_key_file`` present →
       forward the private-key + ``oauth_client_id`` + ``oauth_jwt_kid``
       to the kernel's ``oauth-m2m-jwt`` (RFC 7523 client assertion). The
       kernel signs the assertion and owns the token lifecycle. Checked
       first because a private-key file is unambiguous JWT M2M intent.
    2. **OAuth M2M** — ``oauth_client_id`` + ``oauth_client_secret``
       both present → forward raw creds to the kernel's ``oauth-m2m``.
    3. **PAT** — the built provider is (or wraps) an
       ``AccessTokenAuthProvider`` → extract the bearer token.
    4. **OAuth U2M** — ``auth_type`` is ``databricks-oauth`` or ``azure-oauth``
       → forward the connector's coupled ``databricks-sql-python`` bundle
       (``client_id`` + ``redirect_ports`` list, defaulting scopes to
       ``PYSQL_OAUTH_SCOPES`` when the caller supplies none) to the kernel's
       ``oauth-u2m``, so a bare U2M connection authenticates as
       ``databricks-sql-python`` — forwarding the connector's own OAuth app
       rather than the kernel's ``databricks-sql-connector`` default
       (PECOBLR-4039/4040). ``azure-oauth`` resolves identically: the kernel's
       workspace-federated flow serves Azure workspaces too (PECOBLR-4120).
       Unlike the Thrift path, a caller-supplied ``oauth_scopes`` is honored here.
    5. **Custom credentials_provider** → ``NotSupportedError`` (opaque
       token source; no raw creds for the kernel to own).
    6. Anything else → ``NotSupportedError``.

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
    jwt_key_file = opts.get("oauth_jwt_key_file")
    has_m2m = bool(client_id and client_secret)
    # A private-key file is unambiguous JWT client-assertion M2M intent
    # (RFC 7523): the kernel signs a short-lived assertion with the key
    # rather than sending a client secret.
    has_jwt_m2m = bool(jwt_key_file)

    # Azure Entra (Azure AD) auth types route to the kernel's GENERIC OAuth
    # flows with Azure values supplied as overrides — the kernel needs no
    # Azure-specific code. Handled up front, keyed on the explicit auth_type,
    # before the generic M2M/PAT/U2M routing below (azure-sp-m2m carries its
    # creds in azure_* kwargs, not oauth_client_id/secret, so it would
    # otherwise fall through to the final "unsupported" error).

    # azure-oauth (Azure AD U2M) routes to the kernel's oauth-u2m, handled by the
    # shared U2M branch below (step 3) alongside databricks-oauth. The kernel runs
    # the in-house workspace-federated browser flow — the workspace federates the
    # login to Microsoft Entra — which Azure Databricks workspaces support. It uses
    # the connector's databricks-sql-python app bundle, NOT the Thrift Azure app
    # (96eecda7 / redirect port 8030): that app is registered for Thrift's
    # direct-Entra flow (login.microsoftonline.com config + {tenant}/user_impersonation
    # scopes), which the kernel does not perform. So on the kernel path azure-oauth
    # and databricks-oauth are the same flow. PECOBLR-4120.

    # azure-sp-m2m (Azure service principal, client-credentials): forward the
    # selector + Azure SP credentials; the KERNEL owns Azure resolution (it is
    # the auth core). The kernel builds the Entra v2.0 token endpoint
    # (`{login}/{tenant}/oauth2/v2.0/token`) and the `{effective_app_id}/.default`
    # scope, and — when azure_tenant_id is omitted — auto-discovers the tenant
    # from the workspace's /aad/auth redirect, matching the Thrift backend
    # (so connect() is byte-identical between Thrift and use_kernel=True).
    # PECOBLR-4141.
    #
    # The Authorization bearer is the Databricks-audience data token, which alone
    # authenticates a workspace-member SP. When azure_workspace_resource_id is
    # set, the kernel also sends the Azure SP management token +
    # X-Databricks-Azure-Workspace-Resource-Id header (matching the JDBC driver),
    # so an RBAC-only SP (Azure role, not a workspace member) can authenticate.
    #
    # This branch returns BEFORE the ambiguity guards below, so an
    # azure-sp-m2m selector paired with a conflicting OAuth signal
    # (oauth_client_secret / oauth_jwt_key_file / credentials_provider) does not
    # fail loudly the way the other flows do. That is intentional: azure-sp-m2m
    # carries its credentials in the azure_* namespace, so there is no routing
    # collision, and an explicit azure-sp-m2m selector is unambiguous intent to
    # use the Azure SP flow. Any oauth_*/credentials_provider values are simply
    # ignored here rather than treated as ambiguous.
    if auth_type == "azure-sp-m2m":
        azure_client_id = opts.get("azure_client_id")
        azure_client_secret = opts.get("azure_client_secret")
        if not (azure_client_id and azure_client_secret):
            raise ProgrammingError(
                "auth_type='azure-sp-m2m' requires azure_client_id and "
                "azure_client_secret."
            )
        # azure-sp-m2m carries its credentials in the azure_* namespace, so a
        # conflicting OAuth signal is not a routing collision and does not fail
        # loudly the way the other flows do (see the routing note above). Still,
        # leave a breadcrumb: a caller who paired azure-sp-m2m with an
        # oauth_*/credentials_provider value likely misunderstood the routing,
        # and those values are silently ignored here.
        ignored_signals = [
            name
            for name in (
                "oauth_client_secret",
                "oauth_jwt_key_file",
                "credentials_provider",
            )
            if opts.get(name) is not None
        ]
        if ignored_signals:
            logger.warning(
                "auth_type='azure-sp-m2m' selected; ignoring conflicting "
                "credential signal(s) %s (Azure SP credentials in the azure_* "
                "namespace take precedence on the kernel path).",
                ", ".join(ignored_signals),
            )
        kwargs: Dict[str, Any] = {
            "auth_type": "azure-sp-m2m",
            "azure_client_id": azure_client_id,
            "azure_client_secret": azure_client_secret,
        }
        # Optional passthroughs: the kernel auto-discovers the tenant when
        # absent. When azure_workspace_resource_id is set, the kernel sends the
        # Azure SP management token + X-Databricks-Azure-Workspace-Resource-Id
        # header (for an SP with an Azure RBAC role but no workspace membership)
        # — matching the JDBC driver; omit it for a workspace-member SP.
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
    if client_secret and auth_type in ("databricks-oauth", "azure-oauth"):
        raise NotSupportedError(
            f"Ambiguous auth on use_kernel=True: auth_type={auth_type!r} selects "
            "the U2M browser flow, but oauth_client_secret was also provided "
            "(machine-to-machine). Drop oauth_client_secret for U2M, or drop "
            "auth_type for M2M."
        )
    if has_jwt_m2m and client_secret:
        raise NotSupportedError(
            "Ambiguous auth on use_kernel=True: both oauth_jwt_key_file "
            "(JWT private-key M2M) and oauth_client_secret (shared-secret "
            "M2M) were provided. Pass exactly one — a private key for "
            "JWT client-assertion M2M, or a client secret for shared-secret M2M."
        )
    if has_jwt_m2m and opts.get("credentials_provider") is not None:
        raise NotSupportedError(
            "Ambiguous auth on use_kernel=True: both a custom "
            "credentials_provider and oauth_jwt_key_file were provided. "
            "Pass exactly one — oauth_client_id + oauth_jwt_key_file for "
            "kernel-managed JWT private-key M2M, or use the Thrift backend "
            "(default) for credentials_provider."
        )
    if has_jwt_m2m and auth_type in ("databricks-oauth", "azure-oauth"):
        raise NotSupportedError(
            f"Ambiguous auth on use_kernel=True: auth_type={auth_type!r} selects "
            "the U2M browser flow, but oauth_jwt_key_file was also provided "
            "(JWT private-key M2M). Drop oauth_jwt_key_file for U2M, or drop "
            "auth_type for JWT M2M."
        )

    # 1. OAuth M2M (JWT private-key client assertion) — the kernel signs a
    #    short-lived assertion with the private key and runs the
    #    client-credentials grant. Checked before shared-secret M2M and PAT
    #    because a private-key file is unambiguous JWT M2M intent. Requires
    #    oauth_client_id (the service principal / OAuth client) and
    #    oauth_jwt_kid (the key id the IdP uses to select the registered
    #    public key). Optional oauth_jwt_passphrase / oauth_jwt_algorithm /
    #    oauth_scopes / token_url are forwarded when present; the kernel
    #    fills defaults (RS256 algorithm, all-apis scope, OIDC discovery)
    #    for any omitted.
    if has_jwt_m2m:
        if not client_id:
            raise ProgrammingError(
                "use_kernel=True JWT private-key M2M (oauth_jwt_key_file) "
                "requires oauth_client_id (the service principal / OAuth "
                "client id used as the assertion issuer and subject)."
            )
        jwt_kid = opts.get("oauth_jwt_kid")
        if not jwt_kid:
            raise ProgrammingError(
                "use_kernel=True JWT private-key M2M (oauth_jwt_key_file) "
                "requires oauth_jwt_kid (the key id written into the JWT "
                "header so the IdP can select the registered public key)."
            )
        kwargs = {
            "auth_type": "oauth-m2m-jwt",
            "client_id": client_id,
            "jwt_key_file": jwt_key_file,
            "jwt_kid": jwt_kid,
        }
        jwt_passphrase = opts.get("oauth_jwt_passphrase")
        if jwt_passphrase:
            kwargs["jwt_passphrase"] = jwt_passphrase
        jwt_algorithm = opts.get("oauth_jwt_algorithm")
        if jwt_algorithm:
            kwargs["jwt_algorithm"] = jwt_algorithm
        token_url = opts.get("token_url")
        if token_url:
            kwargs["token_url"] = token_url
        scopes = _normalize_scopes(opts.get("oauth_scopes"))
        if scopes is not None:
            kwargs["oauth_scopes"] = scopes
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
        return kwargs

    # 2. OAuth M2M — raw client-credentials pair forwarded to the kernel.
    if has_m2m:
        kwargs = {
            "auth_type": "oauth-m2m",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        scopes = _normalize_scopes(opts.get("oauth_scopes"))
        if scopes is not None:
            kwargs["oauth_scopes"] = scopes
        # token_url is an auth-method-agnostic token-endpoint override (JDBC's
        # OAuth2ConnAuthTokenEndpoint applies it to client-secret M2M too), so
        # forward it here as well as on the JWT path.
        token_url = opts.get("token_url")
        if token_url:
            kwargs["token_url"] = token_url
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
        return kwargs

    # 3. PAT (including TokenFederationProvider-wrapped PAT).
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

    # 4. OAuth U2M — browser authorization-code flow; the kernel runs it.
    #    Both databricks-oauth and azure-oauth reach here: on the kernel path
    #    they are the same in-house workspace-federated flow (Azure workspaces
    #    federate the browser login to Entra), so azure-oauth is an alias for
    #    databricks-oauth here — see the note near the top of this function.
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
    if auth_type in ("databricks-oauth", "azure-oauth"):
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
            # OAuth U2M token-cache enable/disable: when present in auth_options,
            # forward to the kernel as token_cache_enabled on the U2M branch.
            # Default disabled for backward compatibility when moving token
            # persistence control to the kernel. This ensures callers must
            # opt-in to on-disk persistence rather than silently enabling it.
            # Coerced via _coerce_bool so a string DSN/env value like "False"
            # is not treated as truthy (bool("False") is True).
            "token_cache_enabled": _coerce_bool(
                opts.get("oauth_token_cache_enabled")
            ),
        }
        if federation_client_id:
            kwargs["identity_federation_client_id"] = federation_client_id
        return kwargs

    # 5. Custom credentials_provider — the connector's primary M2M path
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

    # 6. Everything else (including no usable credentials at all —
    #    ``auth_provider`` is None on the kernel path when no access
    #    token was supplied and no OAuth kwargs resolved above).
    provider_desc = (
        type(auth_provider).__name__ if auth_provider is not None else "no credentials"
    )
    raise NotSupportedError(
        f"use_kernel=True requires PAT (access_token), OAuth M2M "
        f"(oauth_client_id + oauth_client_secret), OAuth M2M JWT "
        f"(oauth_client_id + oauth_jwt_key_file + oauth_jwt_kid), OAuth U2M "
        f"(auth_type='databricks-oauth' or 'azure-oauth'), or Azure SP M2M "
        f"(auth_type='azure-sp-m2m'), but got "
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


def _coerce_bool(value: Any) -> bool:
    """Coerce an opt-in boolean flag (e.g. ``oauth_token_cache_enabled``,
    which may arrive as a string from a DSN/env) to a ``bool``.

    A plain ``bool(value)`` is wrong for string inputs: ``bool("False")`` is
    ``True``, which would silently enable on-disk token persistence whenever
    the flag arrived as the string ``"False"``. Only genuinely truthy values
    enable the flag: real booleans, and the usual textual/numeric truthy
    spellings ("true"/"1"/"yes"/"on"). ``None`` (unset) and anything else
    disable it (opt-in default)."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    if isinstance(value, (int, float)):
        return value != 0
    # Unknown types default to disabled rather than truthy-by-accident.
    return False


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
