"""
Unit tests for UnifiedHttpClient, specifically testing MaxRetryError handling
and HTTP status code extraction.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from urllib3.exceptions import MaxRetryError
from urllib3 import HTTPResponse

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

    def test_max_retry_error_with_reason_response_status_429(self, http_client):
        """Test MaxRetryError with reason.response.status = 429."""
        # Create a MaxRetryError with nested response containing status code
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        # Set up the nested structure: e.reason.response.status
        max_retry_error.reason = Mock()
        max_retry_error.reason.response = Mock()
        max_retry_error.reason.response.status = 429

        # Mock the pool manager to raise our error
        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            # Verify RequestError is raised with http-code in context
            with pytest.raises(RequestError) as exc_info:
                http_client.request(
                    HttpMethod.POST, "http://test.com", headers={"test": "header"}
                )

            # Verify the context contains the HTTP status code
            error = exc_info.value
            assert hasattr(error, "context")
            assert "http-code" in error.context
            assert error.context["http-code"] == 429

    def test_max_retry_error_with_reason_response_status_503(self, http_client):
        """Test MaxRetryError with reason.response.status = 503."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        # Set up the nested structure for 503
        max_retry_error.reason = Mock()
        max_retry_error.reason.response = Mock()
        max_retry_error.reason.response.status = 503

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(
                    HttpMethod.GET, "http://test.com", headers={"test": "header"}
                )

            error = exc_info.value
            assert error.context["http-code"] == 503

    def test_max_retry_error_with_direct_response_status(self, http_client):
        """Test MaxRetryError with e.response.status (alternate structure)."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        # Set up direct response on error (e.response.status)
        max_retry_error.response = Mock()
        max_retry_error.response.status = 500

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.POST, "http://test.com")

            error = exc_info.value
            assert error.context["http-code"] == 500

    def test_max_retry_error_without_status_code(self, http_client):
        """Test MaxRetryError without any status code (no crash)."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        # No reason or response set - should not crash

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.GET, "http://test.com")

            error = exc_info.value
            # Context should be empty (no http-code)
            assert error.context == {}

    def test_max_retry_error_with_none_reason(self, http_client):
        """Test MaxRetryError with reason=None (no crash)."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        max_retry_error.reason = None  # Explicitly None

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.POST, "http://test.com")

            error = exc_info.value
            # Should not crash, context should be empty
            assert error.context == {}

    def test_max_retry_error_with_none_response(self, http_client):
        """Test MaxRetryError with reason.response=None (no crash)."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        max_retry_error.reason = Mock()
        max_retry_error.reason.response = None  # Explicitly None

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.GET, "http://test.com")

            error = exc_info.value
            # Should not crash, context should be empty
            assert error.context == {}

    def test_max_retry_error_missing_status_attribute(self, http_client):
        """Test MaxRetryError when response exists but has no status attribute."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        max_retry_error.reason = Mock()
        max_retry_error.reason.response = Mock(spec=[])  # Mock with no attributes

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.POST, "http://test.com")

            error = exc_info.value
            # getattr with default should return None, context should be empty
            assert error.context == {}

    def test_max_retry_error_prefers_reason_response_over_direct_response(
        self, http_client
    ):
        """Test that e.reason.response.status is preferred over e.response.status."""
        mock_pool = Mock()
        max_retry_error = MaxRetryError(pool=mock_pool, url="http://test.com")
        
        # Set both structures with different status codes
        max_retry_error.reason = Mock()
        max_retry_error.reason.response = Mock()
        max_retry_error.reason.response.status = 429  # Should use this one
        
        max_retry_error.response = Mock()
        max_retry_error.response.status = 500  # Should be ignored

        with patch.object(
            http_client._direct_pool_manager, "request", side_effect=max_retry_error
        ):
            with pytest.raises(RequestError) as exc_info:
                http_client.request(HttpMethod.GET, "http://test.com")

            error = exc_info.value
            # Should prefer reason.response.status (429) over response.status (500)
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
            # Should raise RequestError but not crash trying to extract status
            assert "HTTP request error" in str(error)

