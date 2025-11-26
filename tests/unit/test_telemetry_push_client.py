"""
Unit tests for telemetry push client functionality.
"""

import pytest
from unittest.mock import Mock, patch

from databricks.sql.telemetry.telemetry_push_client import (
    ITelemetryPushClient,
    TelemetryPushClient,
    CircuitBreakerTelemetryPushClient,
)
from databricks.sql.common.http import HttpMethod
from databricks.sql.exc import TelemetryRateLimitError
from pybreaker import CircuitBreakerError


class TestTelemetryPushClient:
    """Test cases for TelemetryPushClient."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_http_client = Mock()
        self.client = TelemetryPushClient(self.mock_http_client)

    def test_initialization(self):
        """Test client initialization."""
        assert self.client._http_client == self.mock_http_client

    def test_request_delegates_to_http_client(self):
        """Test that request delegates to underlying HTTP client."""
        mock_response = Mock()
        self.mock_http_client.request.return_value = mock_response

        response = self.client.request(HttpMethod.POST, "https://test.com", {})

        assert response == mock_response
        self.mock_http_client.request.assert_called_once()


class TestCircuitBreakerTelemetryPushClient:
    """Test cases for CircuitBreakerTelemetryPushClient."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock(spec=ITelemetryPushClient)
        self.host = "test-host.example.com"
        self.client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

    def test_initialization(self):
        """Test client initialization."""
        assert self.client._delegate == self.mock_delegate
        assert self.client._host == self.host
        assert self.client._circuit_breaker is not None

    def test_request_success(self):
        """Test successful request when circuit breaker is enabled."""
        mock_response = Mock()
        self.mock_delegate.request.return_value = mock_response

        response = self.client.request(HttpMethod.POST, "https://test.com", {})

        assert response == mock_response
        self.mock_delegate.request.assert_called_once()

    def test_request_circuit_breaker_open(self):
        """Test request when circuit breaker is open raises CircuitBreakerError."""
        with patch.object(
            self.client._circuit_breaker,
            "call",
            side_effect=CircuitBreakerError("Circuit is open"),
        ):
            with pytest.raises(CircuitBreakerError):
                self.client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_other_error(self):
        """Test request when other error occurs raises original exception."""
        self.mock_delegate.request.side_effect = ValueError("Network error")

        with pytest.raises(ValueError, match="Network error"):
            self.client.request(HttpMethod.POST, "https://test.com", {})

    @pytest.mark.parametrize("status_code,expected_error", [
        (429, TelemetryRateLimitError),
        (503, TelemetryRateLimitError),
    ])
    def test_request_rate_limit_codes(self, status_code, expected_error):
        """Test that rate-limit status codes raise TelemetryRateLimitError."""
        mock_response = Mock()
        mock_response.status = status_code
        self.mock_delegate.request.return_value = mock_response

        with pytest.raises(expected_error):
            self.client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_non_rate_limit_code(self):
        """Test that non-rate-limit status codes return response."""
        mock_response = Mock()
        mock_response.status = 500
        mock_response.data = b'Server error'
        self.mock_delegate.request.return_value = mock_response

        response = self.client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 500

    def test_rate_limit_error_logging(self):
        """Test that rate limit errors are logged with circuit breaker context."""
        with patch("databricks.sql.telemetry.telemetry_push_client.logger") as mock_logger:
            mock_response = Mock()
            mock_response.status = 429
            self.mock_delegate.request.return_value = mock_response

            with pytest.raises(TelemetryRateLimitError):
                self.client.request(HttpMethod.POST, "https://test.com", {})

            mock_logger.warning.assert_called()
            warning_args = mock_logger.warning.call_args[0]
            assert "429" in str(warning_args)
            assert "circuit breaker" in warning_args[0]

    def test_other_error_logging(self):
        """Test that other errors are logged during wrapping/unwrapping."""
        with patch("databricks.sql.telemetry.telemetry_push_client.logger") as mock_logger:
            self.mock_delegate.request.side_effect = ValueError("Network error")

            with pytest.raises(ValueError, match="Network error"):
                self.client.request(HttpMethod.POST, "https://test.com", {})

            assert mock_logger.debug.call_count >= 1


class TestCircuitBreakerTelemetryPushClientIntegration:
    """Integration tests for CircuitBreakerTelemetryPushClient."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock()
        self.host = "test-host.example.com"
        from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager
        CircuitBreakerManager._instances.clear()

    def test_circuit_breaker_opens_after_failures(self):
        """Test that circuit breaker opens after repeated failures (429/503 errors)."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerManager,
            MINIMUM_CALLS,
        )

        CircuitBreakerManager._instances.clear()
        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        mock_response = Mock()
        mock_response.status = 429
        self.mock_delegate.request.return_value = mock_response

        rate_limit_error_count = 0
        circuit_breaker_error_count = 0

        for _ in range(MINIMUM_CALLS + 5):
            try:
                client.request(HttpMethod.POST, "https://test.com", {})
            except TelemetryRateLimitError:
                rate_limit_error_count += 1
            except CircuitBreakerError:
                circuit_breaker_error_count += 1

        assert rate_limit_error_count >= MINIMUM_CALLS - 1
        assert circuit_breaker_error_count > 0

    def test_circuit_breaker_recovers_after_success(self):
        """Test that circuit breaker recovers after successful calls."""
        import time
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerManager,
            MINIMUM_CALLS,
            RESET_TIMEOUT,
        )

        CircuitBreakerManager._instances.clear()
        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        # Trigger failures
        mock_rate_limit_response = Mock()
        mock_rate_limit_response.status = 429
        self.mock_delegate.request.return_value = mock_rate_limit_response

        for _ in range(MINIMUM_CALLS + 5):
            try:
                client.request(HttpMethod.POST, "https://test.com", {})
            except (TelemetryRateLimitError, CircuitBreakerError):
                pass

        # Circuit should be open
        with pytest.raises(CircuitBreakerError):
            client.request(HttpMethod.POST, "https://test.com", {})

        # Wait for reset timeout
        time.sleep(RESET_TIMEOUT + 1.0)

        # Simulate success
        mock_success_response = Mock()
        mock_success_response.status = 200
        self.mock_delegate.request.return_value = mock_success_response

        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200

    def test_urllib3_import_fallback(self):
        """Test that the urllib3 import fallback works correctly."""
        from databricks.sql.telemetry.telemetry_push_client import BaseHTTPResponse
        assert BaseHTTPResponse is not None
