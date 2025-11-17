"""
Unit tests specifically for telemetry_push_client RequestError handling
with http-code context extraction for rate limiting detection.
"""

import pytest
from unittest.mock import Mock, patch

from databricks.sql.telemetry.telemetry_push_client import (
    CircuitBreakerTelemetryPushClient,
    TelemetryPushClient,
)
from databricks.sql.common.http import HttpMethod
from databricks.sql.exc import RequestError, TelemetryRateLimitError
from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager


class TestTelemetryPushClientRequestErrorHandling:
    """Test RequestError handling and http-code context extraction."""

    @pytest.fixture
    def setup_circuit_breaker(self):
        """Setup circuit breaker for testing."""
        CircuitBreakerManager._instances.clear()
        yield
        CircuitBreakerManager._instances.clear()

    @pytest.fixture
    def mock_delegate(self):
        """Create mock delegate client."""
        return Mock(spec=TelemetryPushClient)

    @pytest.fixture
    def client(self, mock_delegate, setup_circuit_breaker):
        """Create CircuitBreakerTelemetryPushClient instance."""
        return CircuitBreakerTelemetryPushClient(mock_delegate, "test-host.example.com")

    def test_request_error_with_http_code_429_triggers_rate_limit_error(
        self, client, mock_delegate
    ):
        """Test that RequestError with http-code=429 raises TelemetryRateLimitError."""
        # Create RequestError with http-code in context
        request_error = RequestError("HTTP request failed", context={"http-code": 429})
        mock_delegate.request.side_effect = request_error

        # Should raise TelemetryRateLimitError (circuit breaker counts it)
        with pytest.raises(TelemetryRateLimitError):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_with_http_code_503_triggers_rate_limit_error(
        self, client, mock_delegate
    ):
        """Test that RequestError with http-code=503 raises TelemetryRateLimitError."""
        request_error = RequestError("HTTP request failed", context={"http-code": 503})
        mock_delegate.request.side_effect = request_error

        # Should raise TelemetryRateLimitError (circuit breaker counts it)
        with pytest.raises(TelemetryRateLimitError):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_with_http_code_500_raises_original_error(
        self, client, mock_delegate
    ):
        """Test that RequestError with http-code=500 raises original RequestError."""
        request_error = RequestError("HTTP request failed", context={"http-code": 500})
        mock_delegate.request.side_effect = request_error

        # Should raise original RequestError (500 is NOT rate limiting)
        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_without_http_code_raises_original_error(
        self, client, mock_delegate
    ):
        """Test that RequestError without http-code context raises original error."""
        # RequestError with empty context
        request_error = RequestError("HTTP request failed", context={})
        mock_delegate.request.side_effect = request_error

        # Should raise original RequestError (no rate limiting)
        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_with_none_context_raises_original_error(
        self, client, mock_delegate
    ):
        """Test that RequestError with None context raises original error."""
        # RequestError with no context attribute
        request_error = RequestError("HTTP request failed")
        request_error.context = None
        mock_delegate.request.side_effect = request_error

        # Should raise original RequestError (no crash)
        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_missing_context_attribute(self, client, mock_delegate):
        """Test RequestError without context attribute raises original error."""
        request_error = RequestError("HTTP request failed")
        # Ensure no context attribute exists
        if hasattr(request_error, "context"):
            delattr(request_error, "context")
        mock_delegate.request.side_effect = request_error

        # Should raise original RequestError (no crash checking hasattr)
        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_with_http_code_429_logs_warning(self, client, mock_delegate):
        """Test that rate limit errors log at warning level and raise exception."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            request_error = RequestError(
                "HTTP request failed", context={"http-code": 429}
            )
            mock_delegate.request.side_effect = request_error

            with pytest.raises(TelemetryRateLimitError):
                client.request(HttpMethod.POST, "https://test.com", {})

            # Should log warning for rate limiting
            mock_logger.warning.assert_called()
            warning_args = mock_logger.warning.call_args[0]
            assert "429" in str(warning_args)
            assert "circuit breaker" in warning_args[0].lower()

    def test_request_error_with_http_code_500_logs_debug(self, client, mock_delegate):
        """Test that non-rate-limit errors log at debug level and raise original error."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            request_error = RequestError(
                "HTTP request failed", context={"http-code": 500}
            )
            mock_delegate.request.side_effect = request_error

            with pytest.raises(RequestError):
                client.request(HttpMethod.POST, "https://test.com", {})

            # Should log debug for wrapping/unwrapping
            assert mock_logger.debug.call_count >= 1

    def test_request_error_with_string_http_code(self, client, mock_delegate):
        """Test RequestError with http-code as string (edge case)."""
        # Edge case: http-code as string instead of int
        request_error = RequestError(
            "HTTP request failed", context={"http-code": "429"}
        )
        mock_delegate.request.side_effect = request_error

        # Should handle gracefully and raise original error (string "429" not in [429, 503])
        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_http_code_extraction_prioritization(self, client, mock_delegate):
        """Test that http-code from RequestError context is correctly extracted."""
        # This test verifies the exact code path in telemetry_push_client
        request_error = RequestError(
            "HTTP request failed after retries", context={"http-code": 503}
        )
        mock_delegate.request.side_effect = request_error

        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            with pytest.raises(TelemetryRateLimitError):
                client.request(HttpMethod.POST, "https://test.com", {})

            # Verify warning logged with correct status code
            mock_logger.warning.assert_called()
            warning_call = mock_logger.warning.call_args[0]
            assert "503" in str(warning_call)
            assert "retries exhausted" in warning_call[0].lower()

    def test_non_request_error_exceptions_raised(self, client, mock_delegate):
        """Test that non-RequestError exceptions are wrapped then unwrapped."""
        # Generic exception (not RequestError)
        generic_error = ValueError("Network timeout")
        mock_delegate.request.side_effect = generic_error

        # Should raise original ValueError (wrapped then unwrapped)
        with pytest.raises(ValueError, match="Network timeout"):
            client.request(HttpMethod.POST, "https://test.com", {})
