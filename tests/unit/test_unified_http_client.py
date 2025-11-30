"""
Unit tests for UnifiedHttpClient, specifically testing MaxRetryError handling
and HTTP status code extraction.
"""

import pytest
from unittest.mock import Mock, patch
from urllib3.exceptions import MaxRetryError

from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.common.http import HttpMethod
from databricks.sql.exc import RequestError
from databricks.sql.auth.common import ClientContext
from databricks.sql.types import SSLOptions


class TestUnifiedHttpClientMaxRetryError:
    """Test MaxRetryError handling and HTTP status code extraction."""

    @pytest.fixture
    def client_context(self):
        """Create a minimal ClientContext for testing."""
        context = Mock(spec=ClientContext)
        context.hostname = "https://test.databricks.com"
        context.ssl_options = SSLOptions(
            tls_verify=True,
            tls_verify_hostname=True,
            tls_trusted_ca_file=None,
            tls_client_cert_file=None,
            tls_client_cert_key_file=None,
            tls_client_cert_key_password=None,
        )
        context.socket_timeout = 30
        context.retry_stop_after_attempts_count = 3
        context.retry_delay_min = 1.0
        context.retry_delay_max = 10.0
        context.retry_stop_after_attempts_duration = 300.0
        context.retry_delay_default = 5.0
        context.retry_dangerous_codes = []
        context.proxy_auth_method = None
        context.pool_connections = 10
        context.pool_maxsize = 20
        context.user_agent = "test-agent"
        return context

    @pytest.fixture
    def http_client(self, client_context):
        """Create UnifiedHttpClient instance."""
        return UnifiedHttpClient(client_context)

    @pytest.mark.parametrize("status_code,path", [
        (429, "reason.response"),
        (503, "reason.response"),
        (500, "direct_response"),
    ])
    def test_max_retry_error_with_status_codes(self, http_client, status_code, path):
        """Test MaxRetryError with various status codes and response paths."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        if path == "reason.response":
            max_retry_error.reason = Mock()
            max_retry_error.reason.response = Mock()
            max_retry_error.reason.response.status = status_code
        else:  # direct_response
            max_retry_error.response = Mock()
            max_retry_error.response.status = status_code

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(
                    HttpMethod.POST, "http://test.com", headers={"test": "header"}
                )

            error = exc_info.value
            assert hasattr(error, "context")
            assert "http-code" in error.context
            assert error.context["http-code"] == status_code

    @pytest.mark.parametrize("setup_func", [
        lambda e: None,  # No setup - error with no attributes
        lambda e: setattr(e, "reason", None),  # reason=None
        lambda e: (setattr(e, "reason", Mock()), setattr(e.reason, "response", None)),  # reason.response=None
        lambda e: (setattr(e, "reason", Mock()), setattr(e.reason, "response", Mock(spec=[]))),  # No status attr
    ])
    def test_max_retry_error_missing_status(self, http_client, setup_func):
        """Test MaxRetryError without status code (no crash, empty context)."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        setup_func(max_retry_error)

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.GET, "http://test.com")

            error = exc_info.value
            assert error.context == {}

    def test_max_retry_error_prefers_reason_response(self, http_client):
        """Test that e.reason.response.status is preferred over e.response.status."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        # Set both structures with different status codes
        max_retry_error.reason = Mock()
        max_retry_error.reason.response = Mock()
        max_retry_error.reason.response.status = 429  # Should use this
        
        max_retry_error.response = Mock()
        max_retry_error.response.status = 500  # Should be ignored

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.GET, "http://test.com")

            error = exc_info.value
            assert error.context["http-code"] == 429

    def test_generic_exception_no_crash(self, http_client):
        """Test that generic exceptions don't crash when checking for status code."""
        generic_error = Exception("Network error")

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=generic_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.POST, "http://test.com")

            error = exc_info.value
            assert "HTTP request error" in str(error)
