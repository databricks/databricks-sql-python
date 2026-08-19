"""Unit tests for the kernel backend's auth bridge.

Tests verify:
  - PAT routes through ``auth_type='pat'``.
  - ``TokenFederationProvider``-wrapped PAT also routes through
    PAT (every provider built by ``get_python_sql_connector_auth_provider``
    is federation-wrapped, so the naive isinstance check has to
    look through the wrapper).
  - OAuth M2M (``oauth_client_id`` + ``oauth_client_secret``) routes
    through ``auth_type='oauth-m2m'`` with the raw creds forwarded.
  - OAuth U2M (``auth_type='databricks-oauth'``) routes through
    ``auth_type='oauth-u2m'``. ``azure-oauth`` (Azure AD) is not yet
    supported on the kernel path and is rejected (PECOBLR-4120).
  - A custom ``credentials_provider`` and any other non-PAT shape raise
    ``NotSupportedError`` with a clear, actionable message.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

# auth_bridge.py itself has no pyarrow or kernel-wheel deps. The
# `databricks.sql.backend.kernel` package's __init__.py deliberately
# does *not* eagerly re-export from .client either (which would
# require the kernel wheel). So this test can run on the
# default-deps CI matrix without any extras. No importorskip needed.

from databricks.sql.auth.auth import (
    PYSQL_OAUTH_CLIENT_ID,
    PYSQL_OAUTH_SCOPES,
    PYSQL_OAUTH_REDIRECT_PORT_RANGE,
)
from databricks.sql.auth.common import get_effective_azure_login_app_id
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    AuthProvider,
)
from databricks.sql.backend.kernel.auth_bridge import (
    _extract_bearer_token,
    kernel_auth_kwargs,
)
from databricks.sql.exc import NotSupportedError, ProgrammingError


class _FakeOAuthProvider(AuthProvider):
    """Stand-in for any non-PAT provider. The bridge should reject
    these with NotSupportedError."""

    def add_headers(self, request_headers):
        request_headers["Authorization"] = "Bearer oauth-token-xyz"


class _MalformedProvider(AuthProvider):
    """Provider that returns a non-Bearer Authorization header."""

    def add_headers(self, request_headers):
        request_headers["Authorization"] = "Basic dXNlcjpwYXNz"


class _SilentProvider(AuthProvider):
    """Provider that writes nothing — misconfigured auth."""

    def add_headers(self, request_headers):
        pass


class TestExtractBearerToken:
    def test_pat_provider_returns_token(self):
        p = AccessTokenAuthProvider("dapi-abc-123")
        assert _extract_bearer_token(p) == "dapi-abc-123"

    def test_non_bearer_auth_returns_none(self):
        assert _extract_bearer_token(_MalformedProvider()) is None

    def test_silent_provider_returns_none(self):
        assert _extract_bearer_token(_SilentProvider()) is None


class TestKernelAuthKwargs:
    def test_pat_routes_to_kernel_pat(self):
        kwargs = kernel_auth_kwargs(AccessTokenAuthProvider("dapi-xyz"))
        assert kwargs == {"auth_type": "pat", "access_token": "dapi-xyz"}

    @pytest.mark.parametrize(
        "scheme",
        ["Bearer ", "bearer ", "BEARER ", "BeArEr "],
        ids=["title", "lower", "upper", "mixed"],
    )
    def test_bearer_prefix_is_case_insensitive(self, scheme):
        """RFC 6750 §2.1: the Authorization scheme is case-insensitive.
        A provider that emits ``bearer`` (lower) or ``BEARER`` (upper)
        must route through PAT, not fall through to a confusing
        ``ProgrammingError`` from the missing-header check."""

        class _CustomCaseProvider(AccessTokenAuthProvider):
            def add_headers(self, request_headers):
                request_headers["Authorization"] = f"{scheme}dapi-xyz"

        kwargs = kernel_auth_kwargs(_CustomCaseProvider("dapi-xyz"))
        assert kwargs == {"auth_type": "pat", "access_token": "dapi-xyz"}

    @pytest.mark.parametrize(
        "bad_token",
        [
            "dapi\x00null",  # NUL
            "dapi\rfoo",  # CR
            "dapi\nfoo",  # LF
            "dapi\x7fdel",  # DEL
            "dapi has space",  # space inside token
            "dapi\tfoo",  # tab
        ],
        ids=["nul", "cr", "lf", "del", "space", "tab"],
    )
    def test_token_with_control_chars_or_whitespace_rejected(self, bad_token):
        """Defense-in-depth: a Bearer token containing CR/LF/NUL would
        let a misbehaving HTTP stack split or terminate the
        Authorization header line. Space/tab are also rejected
        because RFC 6750 forbids whitespace inside the credential
        token. Surface as ``ProgrammingError`` at bridge-build time."""

        class _BadTokenProvider(AccessTokenAuthProvider):
            def add_headers(self, request_headers):
                request_headers["Authorization"] = f"Bearer {bad_token}"

        with pytest.raises(ProgrammingError, match="control characters or whitespace"):
            kernel_auth_kwargs(_BadTokenProvider("ignored"))

    def test_federation_wrapped_pat_routes_to_kernel_pat(self):
        """``get_python_sql_connector_auth_provider`` always wraps
        the base provider in a ``TokenFederationProvider``, so the
        PAT case never reaches us unwrapped in practice. The bridge
        must look through the federation wrapper to find the
        underlying ``AccessTokenAuthProvider``.

        Construct a real ``TokenFederationProvider`` (with a mock
        http_client — `_exchange_token` never fires for a plain
        ``dapi-…`` PAT because it isn't a JWT, so the mock is never
        called). This exercises the real ``add_headers`` path the
        bridge sees in production.
        """
        from databricks.sql.auth.token_federation import TokenFederationProvider

        base = AccessTokenAuthProvider("dapi-abc")
        federated = TokenFederationProvider(
            hostname="https://example.cloud.databricks.com",
            external_provider=base,
            http_client=Mock(),
        )
        kwargs = kernel_auth_kwargs(federated)
        assert kwargs == {"auth_type": "pat", "access_token": "dapi-abc"}

    def test_pat_with_silent_provider_raises_programming_error(self):
        """An AccessTokenAuthProvider that produces no Authorization
        header is misconfigured; surface that at bridge-build time,
        not on the first kernel HTTP request. ``ProgrammingError`` so
        the bridge's error surface is uniformly PEP 249."""
        broken = AccessTokenAuthProvider("dapi-x")
        broken.add_headers = lambda h: None  # type: ignore[method-assign]
        with pytest.raises(ProgrammingError, match="Bearer"):
            kernel_auth_kwargs(broken)

    def test_generic_oauth_provider_raises_not_supported(self):
        # No auth_options → a non-PAT provider with no M2M/U2M signal
        # falls through to the generic "other auth flows" rejection.
        with pytest.raises(NotSupportedError, match="Use the Thrift backend"):
            kernel_auth_kwargs(_FakeOAuthProvider())

    def test_external_credentials_provider_raises_not_supported(self):
        """A user-supplied ``credentials_provider`` is the connector's
        primary M2M path on Thrift/SEA, but it's an opaque token source
        with no extractable raw creds — the kernel can't own the token
        lifecycle, so the bridge rejects it and points at the
        ``oauth_client_id`` / ``oauth_client_secret`` M2M kwargs."""

        def _creds_provider():
            return lambda: {"Authorization": "Bearer noop"}

        with pytest.raises(NotSupportedError, match="oauth_client_secret"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {"credentials_provider": _creds_provider},
            )

    def test_silent_non_pat_provider_also_raises_not_supported(self):
        """Even if a non-PAT provider produces no header, the bridge
        rejects the type itself — we don't try to extract a token
        from something we already know is unsupported."""
        with pytest.raises(NotSupportedError):
            kernel_auth_kwargs(_SilentProvider())


class TestKernelOAuthM2M:
    def test_m2m_forwards_raw_client_credentials(self):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"oauth_client_id": "sp-uuid", "oauth_client_secret": "shh"},
        )
        assert kwargs == {
            "auth_type": "oauth-m2m",
            "client_id": "sp-uuid",
            "client_secret": "shh",
        }

    def test_m2m_includes_scopes_when_provided(self):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "oauth_client_id": "sp-uuid",
                "oauth_client_secret": "shh",
                "oauth_scopes": ["all-apis", "sql"],
            },
        )
        assert kwargs["oauth_scopes"] == ["all-apis", "sql"]

    def test_m2m_normalizes_space_delimited_scopes(self):
        # DatabricksOAuthProvider stores scopes as a single
        # space-delimited string; the bridge splits it to a list.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "oauth_client_id": "sp",
                "oauth_client_secret": "s",
                "oauth_scopes": "all-apis sql",
            },
        )
        assert kwargs["oauth_scopes"] == ["all-apis", "sql"]

    def test_m2m_takes_precedence_over_pat(self):
        # A workload passing both a token and M2M creds resolves to the
        # refreshing M2M path, not the static token.
        kwargs = kernel_auth_kwargs(
            AccessTokenAuthProvider("dapi-xyz"),
            {"oauth_client_id": "id", "oauth_client_secret": "sec"},
        )
        assert kwargs["auth_type"] == "oauth-m2m"

    def test_client_id_without_secret_does_not_trigger_m2m(self):
        # Only oauth_client_id (the U2M custom-client case) must NOT be
        # mistaken for M2M; with a PAT provider it routes to PAT.
        kwargs = kernel_auth_kwargs(
            AccessTokenAuthProvider("dapi-xyz"),
            {"oauth_client_id": "id"},
        )
        assert kwargs == {"auth_type": "pat", "access_token": "dapi-xyz"}


class TestKernelOAuthU2M:
    """Only ``databricks-oauth`` U2M is supported on the kernel path.

    The kernel core default U2M app is ``databricks-sql-connector`` /
    ``sql offline_access`` / port 8030 (see PECOBLR-4039). The Python
    connector is an OVERRIDE: on the kernel path it forwards its OWN
    coupled ``client_id`` + ``redirect_ports`` bundle (the full registered
    port list, for busy-port fallback) so it authenticates as
    ``databricks-sql-python`` rather than the kernel default. A caller
    may override ``oauth_scopes``; absent one, ``PYSQL_OAUTH_SCOPES`` is
    forwarded as the default.

    ``azure-oauth`` (Azure AD U2M) routes here too — see
    ``test_azure_oauth_routes_to_kernel_u2m`` (PECOBLR-4120)."""

    def test_bare_databricks_oauth_forwards_full_python_bundle(self):
        # No overrides → forward the databricks-sql-python bundle in full
        # so the kernel does NOT fall back to its databricks-sql-connector
        # default. This is the parity-with-Thrift acceptance criterion.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": "databricks-oauth"},
        )
        assert kwargs == {
            "auth_type": "oauth-u2m",
            "client_id": PYSQL_OAUTH_CLIENT_ID,
            # Full registered port list → the kernel binds the first free one.
            "redirect_ports": list(PYSQL_OAUTH_REDIRECT_PORT_RANGE),
            "oauth_scopes": list(PYSQL_OAUTH_SCOPES),
        }

    def test_azure_oauth_forwards_selector_kernel_owns_resolution(self):
        # azure-oauth (Azure AD U2M): the bridge forwards ONLY the selector.
        # The kernel owns Azure resolution — it pins the workspace v2.0
        # authorize/token endpoints, the Azure client id, port 8030, and the
        # {app_id}/user_impersonation scope. So the bridge must NOT construct
        # client_id / redirect_ports / oauth_scopes here. PECOBLR-4120.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": "azure-oauth"},
        )
        assert kwargs == {"auth_type": "azure-oauth"}

    def test_azure_oauth_honors_custom_client_id_and_port(self):
        # A caller override still passes through (client_id + its coupled port),
        # but no scopes/endpoints are synthesised by the bridge.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "azure-oauth",
                "oauth_client_id": "custom-azure-app",
                "oauth_redirect_port": 9100,
            },
        )
        assert kwargs == {
            "auth_type": "azure-oauth",
            "client_id": "custom-azure-app",
            "redirect_ports": [9100],
        }

    def test_u2m_custom_client_id_port_and_scopes_honored(self):
        # A caller may override the coupled client_id + redirect port and the
        # oauth_scopes. The single custom port is forwarded as a one-element
        # redirect_ports list; all three are forwarded as supplied.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "databricks-oauth",
                "oauth_client_id": "custom-client",
                "oauth_scopes": ["custom-scope", "offline_access"],
                "oauth_redirect_port": 9999,
            },
        )
        assert kwargs == {
            "auth_type": "oauth-u2m",
            "client_id": "custom-client",
            "redirect_ports": [9999],
            "oauth_scopes": ["custom-scope", "offline_access"],
        }

    def test_u2m_custom_client_id_only_falls_back_to_connector_defaults(self):
        # A custom client_id without explicit scopes/port fills the
        # remaining two from the connector defaults — a custom client_id
        # still uses PYSQL_OAUTH_SCOPES and the default redirect-port list.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "databricks-oauth",
                "oauth_client_id": "custom-client",
            },
        )
        assert kwargs == {
            "auth_type": "oauth-u2m",
            "client_id": "custom-client",
            "redirect_ports": list(PYSQL_OAUTH_REDIRECT_PORT_RANGE),
            "oauth_scopes": list(PYSQL_OAUTH_SCOPES),
        }

    def test_u2m_redirect_port_coerced_to_int(self):
        # oauth_redirect_port may arrive as a string (e.g. from a DSN);
        # the kernel binding wants an int. The port override is coupled to
        # an explicit client_id (see the coupling test below), so supply
        # one here to exercise the coercion path.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "databricks-oauth",
                "oauth_client_id": "custom-client",
                "oauth_redirect_port": "8021",
            },
        )
        assert kwargs["redirect_ports"] == [8021]
        assert isinstance(kwargs["redirect_ports"][0], int)

    def test_u2m_redirect_port_non_numeric_raises_programming_error(self):
        # A garbled oauth_redirect_port is a caller error of the same class
        # as a malformed oauth_scopes, so it must surface as a PEP 249
        # ProgrammingError (not a bare ValueError from int()) for a
        # consistent, actionable exception type.
        with pytest.raises(ProgrammingError, match="oauth_redirect_port must be"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {
                    "auth_type": "databricks-oauth",
                    "oauth_client_id": "custom-client",
                    "oauth_redirect_port": "not-a-port",
                },
            )

    def test_u2m_redirect_port_ignored_without_client_id(self):
        # A bare oauth_redirect_port (no explicit client_id) must NOT replace
        # the default list: it would be paired with the default
        # databricks-sql-python app, so we keep forwarding that app's full
        # registered port list. This mirrors the Thrift path's coupling, where
        # oauth_redirect_port_range is only overridden when both
        # oauth_client_id and oauth_redirect_port are supplied.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": "databricks-oauth", "oauth_redirect_port": 9999},
        )
        assert kwargs["redirect_ports"] == list(PYSQL_OAUTH_REDIRECT_PORT_RANGE)

    def test_u2m_honors_custom_scopes(self):
        # A caller-supplied oauth_scopes is forwarded to the kernel, even
        # without an explicit client_id. Absent one, PYSQL_OAUTH_SCOPES is
        # forwarded as the default (see the bare-bundle test above).
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "databricks-oauth",
                "oauth_scopes": ["all-apis", "offline_access"],
            },
        )
        assert kwargs["oauth_scopes"] == ["all-apis", "offline_access"]

    def test_u2m_normalizes_space_delimited_scopes(self):
        # A space-delimited oauth_scopes string is normalized to a list,
        # mirroring the M2M path.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "databricks-oauth",
                "oauth_scopes": "all-apis offline_access",
            },
        )
        assert kwargs["oauth_scopes"] == ["all-apis", "offline_access"]


class TestKernelIdentityFederationClientId:
    @pytest.mark.parametrize(
        "auth_provider,auth_options",
        [
            pytest.param(AccessTokenAuthProvider("dapi-xyz"), {}, id="pat"),
            pytest.param(
                _FakeOAuthProvider(),
                {"oauth_client_id": "sp-uuid", "oauth_client_secret": "shh"},
                id="m2m",
            ),
            pytest.param(
                _FakeOAuthProvider(),
                {"auth_type": "databricks-oauth"},
                id="u2m",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "federation_client_id",
        [
            pytest.param(None, id="omitted"),
            pytest.param("", id="empty"),
            pytest.param("federation-client", id="supplied"),
        ],
    )
    def test_forwards_only_non_empty_value(
        self, auth_provider, auth_options, federation_client_id
    ):
        options = dict(auth_options)
        if federation_client_id is not None:
            options["identity_federation_client_id"] = federation_client_id

        kwargs = kernel_auth_kwargs(auth_provider, options)

        if federation_client_id:
            assert kwargs["identity_federation_client_id"] == federation_client_id
        else:
            assert "identity_federation_client_id" not in kwargs


class TestKernelAuthAmbiguity:
    """Conflicting auth signals must fail loudly at session-open rather
    than silently resolving to one flow (which would surface later as a
    confusing 401 against the wrong principal)."""

    def test_credentials_provider_plus_m2m_is_rejected(self):
        def _creds_provider():
            return lambda: {"Authorization": "Bearer x"}

        with pytest.raises(NotSupportedError, match="Ambiguous auth"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {
                    "oauth_client_id": "id",
                    "oauth_client_secret": "sec",
                    "credentials_provider": _creds_provider,
                },
            )

    def test_u2m_auth_type_plus_client_secret_is_rejected(self):
        # User asked for U2M (browser) but also passed a secret (M2M).
        # Don't silently route M2M against the wrong principal. (azure-oauth
        # is rejected earlier as unsupported, so it's not exercised here.)
        with pytest.raises(NotSupportedError, match="Ambiguous auth"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {
                    "auth_type": "databricks-oauth",
                    "oauth_client_id": "id",
                    "oauth_client_secret": "sec",
                },
            )


class TestKernelAzureSpM2M:
    """``azure-sp-m2m`` (Azure service-principal, client-credentials) routes to
    the kernel's generic ``oauth-m2m`` with an Entra v2.0 token endpoint and the
    ``{app_id}/.default`` scope. The management-token header is intentionally not
    applied on the kernel path (no SQL connector uses it). PECOBLR-4141."""

    _CREDS = {
        "auth_type": "azure-sp-m2m",
        "azure_client_id": "azure-sp",
        "azure_client_secret": "azure-secret",
        "azure_tenant_id": "tenant-123",
    }

    def test_azure_sp_m2m_routes_to_kernel_m2m(self):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            dict(self._CREDS),
            hostname="adb-1.azuredatabricks.net",
        )
        app_id = get_effective_azure_login_app_id("adb-1.azuredatabricks.net")
        assert kwargs == {
            "auth_type": "oauth-m2m",
            "client_id": "azure-sp",
            "client_secret": "azure-secret",
            "token_url": "https://login.microsoftonline.com/tenant-123/oauth2/v2.0/token",
            "oauth_scopes": [f"{app_id}/.default"],
        }

    def test_azure_sp_m2m_requires_tenant(self):
        opts = {
            "auth_type": "azure-sp-m2m",
            "azure_client_id": "azure-sp",
            "azure_client_secret": "azure-secret",
        }
        with pytest.raises(NotSupportedError, match="azure_tenant_id"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(), opts, hostname="adb-1.azuredatabricks.net"
            )

    def test_azure_sp_m2m_requires_client_id_and_secret(self):
        with pytest.raises(ProgrammingError, match="azure_client_id"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {"auth_type": "azure-sp-m2m", "azure_tenant_id": "t"},
                hostname="adb-1.azuredatabricks.net",
            )

    def test_azure_sp_m2m_forwards_federation_client_id(self):
        opts = dict(self._CREDS, identity_federation_client_id="fed-client")
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(), opts, hostname="adb-1.azuredatabricks.net"
        )
        assert kwargs["identity_federation_client_id"] == "fed-client"


class TestKernelScopesNormalization:
    def test_unknown_scope_type_raises(self):
        # A non-str/list/tuple oauth_scopes is a caller error; fail loudly
        # rather than silently dropping to default scopes.
        with pytest.raises(ProgrammingError, match="oauth_scopes must be"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {
                    "oauth_client_id": "id",
                    "oauth_client_secret": "sec",
                    "oauth_scopes": 123,
                },
            )
