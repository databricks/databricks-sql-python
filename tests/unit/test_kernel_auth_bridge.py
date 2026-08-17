"""Unit tests for the kernel backend's auth bridge.

Tests verify:
  - PAT routes through ``auth_type='pat'``.
  - ``TokenFederationProvider``-wrapped PAT also routes through
    PAT (every provider built by ``get_python_sql_connector_auth_provider``
    is federation-wrapped, so the naive isinstance check has to
    look through the wrapper).
  - OAuth M2M (``oauth_client_id`` + ``oauth_client_secret``) routes
    through ``auth_type='oauth-m2m'`` with the raw creds forwarded.
  - OAuth U2M (``auth_type='databricks-oauth'`` / ``'azure-oauth'``)
    routes through ``auth_type='oauth-u2m'``.
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
    PYSQL_OAUTH_AZURE_CLIENT_ID,
    PYSQL_OAUTH_SCOPES,
    PYSQL_OAUTH_REDIRECT_PORT_RANGE,
    PYSQL_OAUTH_AZURE_REDIRECT_PORT_RANGE,
)
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
    """The kernel core default U2M app is ``databricks-sql-connector`` /
    ``sql offline_access`` / port 8030 (see PECOBLR-4039). The Python
    connector is an OVERRIDE: on the kernel path it must forward its OWN
    full bundle — ``client_id`` + ``oauth_scopes`` + ``redirect_port`` —
    because the three are coupled per OAuth app. Forwarding a partial
    bundle would let the kernel fill the rest from the connector default,
    authenticating as the wrong principal / against an unregistered
    redirect URI. So bare U2M must forward the complete
    ``databricks-sql-python`` bundle for parity with the Thrift path."""

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
            "redirect_port": PYSQL_OAUTH_REDIRECT_PORT_RANGE[0],
            "oauth_scopes": list(PYSQL_OAUTH_SCOPES),
        }

    def test_bare_azure_oauth_forwards_full_azure_bundle(self):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": "azure-oauth"},
        )
        assert kwargs == {
            "auth_type": "oauth-u2m",
            "client_id": PYSQL_OAUTH_AZURE_CLIENT_ID,
            "redirect_port": PYSQL_OAUTH_AZURE_REDIRECT_PORT_RANGE[0],
            "oauth_scopes": list(PYSQL_OAUTH_SCOPES),
        }

    def test_u2m_custom_client_id_and_port_honored_scopes_fixed(self):
        # A caller overriding the app supplies the coupled client_id +
        # redirect_port, which are forwarded verbatim. oauth_scopes is NOT
        # caller-overridable: the Thrift path hardcodes PYSQL_OAUTH_SCOPES
        # for U2M (it never reads an oauth_scopes kwarg), so the kernel
        # path forwards the same fixed scopes for parity even when the
        # caller passes their own.
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
            "redirect_port": 9999,
            "oauth_scopes": list(PYSQL_OAUTH_SCOPES),
        }

    def test_u2m_custom_client_id_only_falls_back_to_connector_defaults(self):
        # A custom client_id without explicit scopes/port fills the
        # remaining two from the connector defaults — matching the Thrift
        # path, where a custom client_id still uses PYSQL_OAUTH_SCOPES and
        # the default redirect-port range.
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
            "redirect_port": PYSQL_OAUTH_REDIRECT_PORT_RANGE[0],
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
        assert kwargs["redirect_port"] == 8021
        assert isinstance(kwargs["redirect_port"], int)

    def test_u2m_redirect_port_ignored_without_client_id(self):
        # A bare oauth_redirect_port (no explicit client_id) must NOT be
        # forwarded: it would be paired with the default databricks-sql-python
        # app, whose registered redirect URIs only cover the default port
        # range, so an arbitrary port would resolve to an unregistered URI
        # and fail the U2M flow. This mirrors the Thrift path's coupling,
        # where oauth_redirect_port_range is only overridden when both
        # oauth_client_id and oauth_redirect_port are supplied.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": "databricks-oauth", "oauth_redirect_port": 9999},
        )
        assert kwargs["redirect_port"] == PYSQL_OAUTH_REDIRECT_PORT_RANGE[0]

    @pytest.mark.parametrize("auth_type", ["databricks-oauth", "azure-oauth"])
    def test_u2m_ignores_custom_scopes_for_thrift_parity(self, auth_type):
        # The Thrift path hardcodes PYSQL_OAUTH_SCOPES for U2M and never
        # reads a caller's oauth_scopes; the kernel path forwards the same
        # fixed scopes for parity rather than honoring an override the
        # other backend silently ignores.
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": auth_type, "oauth_scopes": ["all-apis", "offline_access"]},
        )
        assert kwargs["oauth_scopes"] == list(PYSQL_OAUTH_SCOPES)


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

    @pytest.mark.parametrize("auth_type", ["databricks-oauth", "azure-oauth"])
    def test_u2m_auth_type_plus_client_secret_is_rejected(self, auth_type):
        # User asked for U2M (browser) but also passed a secret (M2M).
        # Don't silently route M2M against the wrong principal.
        with pytest.raises(NotSupportedError, match="Ambiguous auth"):
            kernel_auth_kwargs(
                _FakeOAuthProvider(),
                {
                    "auth_type": auth_type,
                    "oauth_client_id": "id",
                    "oauth_client_secret": "sec",
                },
            )


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
