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
    @pytest.mark.parametrize("auth_type", ["databricks-oauth", "azure-oauth"])
    def test_u2m_routes_to_kernel_u2m(self, auth_type):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": auth_type},
        )
        assert kwargs == {"auth_type": "oauth-u2m"}

    def test_u2m_forwards_client_id_and_redirect_port(self):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {
                "auth_type": "databricks-oauth",
                "oauth_client_id": "custom-client",
                "oauth_redirect_port": 8030,
            },
        )
        assert kwargs == {
            "auth_type": "oauth-u2m",
            "client_id": "custom-client",
            "redirect_port": 8030,
        }

    @pytest.mark.parametrize("auth_type", ["databricks-oauth", "azure-oauth"])
    def test_u2m_forwards_scopes(self, auth_type):
        kwargs = kernel_auth_kwargs(
            _FakeOAuthProvider(),
            {"auth_type": auth_type, "oauth_scopes": ["all-apis", "offline_access"]},
        )
        assert kwargs["oauth_scopes"] == ["all-apis", "offline_access"]


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
