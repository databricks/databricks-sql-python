"""
Unit tests specifically for telemetry_push_client RequestError handling
with http-code context extraction for rate limiting detection.
"""

import pytest
from unittest.mock import Mock

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

    @pytest.mark.parametrize("status_code", [429, 503])
    def test_request_error_with_rate_limit_codes(self, client, mock_delegate, status_code):
        """Test that RequestError with rate-limit codes raises TelemetryRateLimitError."""
        request_error = RequestError("HTTP request failed", context={"http-code": status_code})
        mock_delegate.request.side_effect = request_error

        with pytest.raises(TelemetryRateLimitError):
            client.request(HttpMethod.POST, "https://test.com", {})

    @pytest.mark.parametrize("status_code", [500, 400, 404])
    def test_request_error_with_non_rate_limit_codes(self, client, mock_delegate, status_code):
        """Test that RequestError with non-rate-limit codes raises original RequestError."""
        request_error = RequestError("HTTP request failed", context={"http-code": status_code})
        mock_delegate.request.side_effect = request_error

        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    @pytest.mark.parametrize("context", [{}, None, "429"])
    def test_request_error_with_invalid_context(self, client, mock_delegate, context):
        """Test RequestError with invalid/missing context raises original error."""
        request_error = RequestError("HTTP request failed")
        if context == "429":
            # Edge case: http-code as string instead of int
            request_error.context = {"http-code": context}
        else:
            request_error.context = context
        mock_delegate.request.side_effect = request_error

        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_error_missing_context_attribute(self, client, mock_delegate):
        """Test RequestError without context attribute raises original error."""
        request_error = RequestError("HTTP request failed")
        if hasattr(request_error, "context"):
            delattr(request_error, "context")
        mock_delegate.request.side_effect = request_error

        with pytest.raises(RequestError, match="HTTP request failed"):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_http_code_extraction_prioritization(self, client, mock_delegate):
        """Test that http-code from RequestError context is correctly extracted."""
        request_error = RequestError(
            "HTTP request failed after retries", context={"http-code": 503}
        )
        mock_delegate.request.side_effect = request_error

        with pytest.raises(TelemetryRateLimitError):
            client.request(HttpMethod.POST, "https://test.com", {})

    def test_non_request_error_exceptions_raised(self, client, mock_delegate):
        """Test that non-RequestError exceptions are wrapped then unwrapped."""
        generic_error = ValueError("Network timeout")
        mock_delegate.request.side_effect = generic_error

        with pytest.raises(ValueError, match="Network timeout"):
            client.request(HttpMethod.POST, "https://test.com", {})
