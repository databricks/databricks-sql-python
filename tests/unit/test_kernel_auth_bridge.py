"""Unit tests for the kernel backend's auth bridge.

The bridge translates the connector's ``AuthProvider`` hierarchy
into ``databricks_sql_kernel.Session`` auth kwargs. PAT goes through
the kernel's PAT path; everything else trampolines through the
``External`` path with a Python callback.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks.sql.auth.authenticators import AccessTokenAuthProvider, AuthProvider
from databricks.sql.backend.kernel.auth_bridge import (
    _extract_bearer_token,
    kernel_auth_kwargs,
)


class _FakeOAuthProvider(AuthProvider):
    """Stand-in for OAuth/MSAL/federation providers — anything that
    isn't ``AccessTokenAuthProvider``. Returns a counter-stamped
    token so tests can prove the callback is invoked each call."""

    def __init__(self):
        self.calls = 0

    def add_headers(self, request_headers):
        self.calls += 1
        request_headers["Authorization"] = f"Bearer token-{self.calls}"


class _MalformedProvider(AuthProvider):
    """Provider that returns a non-Bearer Authorization header
    (e.g. Basic auth). The bridge should reject this rather than
    silently sending the wrong shape to the kernel."""

    def add_headers(self, request_headers):
        request_headers["Authorization"] = "Basic dXNlcjpwYXNz"


class _SilentProvider(AuthProvider):
    """Provider that writes nothing — represents misconfigured
    auth or a placeholder. The bridge must surface this clearly."""

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

        # TokenFederationProvider needs an http_client; a MagicMock
        # is sufficient because we don't trigger any token exchange
        # in the test (the cached-token path is never hit).
        base = AccessTokenAuthProvider("dapi-abc")
        federated = TokenFederationProvider.__new__(TokenFederationProvider)
        federated.external_provider = base
        # The bridge only touches `add_headers` (delegated to the
        # base) and `external_provider`. Other attrs would be set
        # by __init__ but aren't exercised here.
        federated.add_headers = base.add_headers
        kwargs = kernel_auth_kwargs(federated)
        assert kwargs == {"auth_type": "pat", "access_token": "dapi-abc"}

    def test_pat_with_silent_provider_raises(self):
        """An AccessTokenAuthProvider that produces no Authorization
        header is misconfigured; surface that at bridge-build time,
        not on the first kernel HTTP request."""
        broken = AccessTokenAuthProvider("dapi-x")
        # Force the broken state by monkey-patching add_headers.
        broken.add_headers = lambda h: None  # type: ignore[method-assign]
        with pytest.raises(ValueError, match="Bearer"):
            kernel_auth_kwargs(broken)

    def test_oauth_routes_to_external_trampoline(self):
        provider = _FakeOAuthProvider()
        kwargs = kernel_auth_kwargs(provider)
        assert kwargs["auth_type"] == "external"
        callback = kwargs["token_callback"]
        assert callable(callback)
        # First call -> token-1, second call -> token-2. Proves the
        # callback delegates to the live auth_provider each time
        # rather than caching.
        assert callback() == "token-1"
        assert callback() == "token-2"
        assert provider.calls == 2

    def test_external_callback_raises_on_missing_header(self):
        kwargs = kernel_auth_kwargs(_SilentProvider())
        with pytest.raises(RuntimeError, match="Bearer"):
            kwargs["token_callback"]()
