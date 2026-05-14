"""Unit tests for the kernel backend's auth bridge.

Phase 1 ships PAT only. Tests verify:
  - PAT routes through ``auth_type='pat'``.
  - ``TokenFederationProvider``-wrapped PAT also routes through
    PAT (every provider built by ``get_python_sql_connector_auth_provider``
    is federation-wrapped, so the naive isinstance check has to
    look through the wrapper).
  - Anything else raises ``NotSupportedError`` with a clear message.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    AuthProvider,
    DatabricksOAuthProvider,
    ExternalAuthProvider,
)
from databricks.sql.backend.kernel.auth_bridge import (
    _extract_bearer_token,
    kernel_auth_kwargs,
)
from databricks.sql.exc import NotSupportedError


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

    def test_federation_wrapped_pat_routes_to_kernel_pat(self):
        """``get_python_sql_connector_auth_provider`` always wraps
        the base provider in a ``TokenFederationProvider``, so the
        PAT case never reaches us unwrapped in practice. The bridge
        must look through the federation wrapper to find the
        underlying ``AccessTokenAuthProvider``."""
        from databricks.sql.auth.token_federation import TokenFederationProvider

        base = AccessTokenAuthProvider("dapi-abc")
        # TokenFederationProvider's __init__ requires an http_client
        # to construct cleanly; for this unit test we only exercise
        # the add_headers passthrough + the external_provider
        # attribute. Bypass __init__ with __new__ and stash just
        # the fields the bridge touches.
        federated = TokenFederationProvider.__new__(TokenFederationProvider)
        federated.external_provider = base
        federated.add_headers = base.add_headers
        kwargs = kernel_auth_kwargs(federated)
        assert kwargs == {"auth_type": "pat", "access_token": "dapi-abc"}

    def test_pat_with_silent_provider_raises_value_error(self):
        """An AccessTokenAuthProvider that produces no Authorization
        header is misconfigured; surface that at bridge-build time,
        not on the first kernel HTTP request."""
        broken = AccessTokenAuthProvider("dapi-x")
        broken.add_headers = lambda h: None  # type: ignore[method-assign]
        with pytest.raises(ValueError, match="Bearer"):
            kernel_auth_kwargs(broken)

    def test_generic_oauth_provider_raises_not_supported(self):
        with pytest.raises(NotSupportedError, match="only supports PAT"):
            kernel_auth_kwargs(_FakeOAuthProvider())

    def test_external_credentials_provider_raises_not_supported(self):
        """``ExternalAuthProvider`` wraps user-supplied
        credentials_provider — kernel doesn't accept these today,
        and the bridge surfaces that explicitly."""
        # ExternalAuthProvider's __init__ calls the credentials
        # provider; supply a noop one.
        from databricks.sql.auth.authenticators import CredentialsProvider

        class _NoopCreds(CredentialsProvider):
            def auth_type(self):
                return "noop"

            def __call__(self, *args, **kwargs):
                return lambda: {"Authorization": "Bearer noop"}

        ext = ExternalAuthProvider(_NoopCreds())
        with pytest.raises(NotSupportedError, match="only supports PAT"):
            kernel_auth_kwargs(ext)

    def test_silent_non_pat_provider_also_raises_not_supported(self):
        """Even if a non-PAT provider produces no header, the bridge
        rejects the type itself — we don't try to extract a token
        from something we already know is unsupported."""
        with pytest.raises(NotSupportedError):
            kernel_auth_kwargs(_SilentProvider())
