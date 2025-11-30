"""
Unit tests for telemetry push client functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from databricks.sql.telemetry.telemetry_push_client import (
    ITelemetryPushClient,
    TelemetryPushClient,
    CircuitBreakerTelemetryPushClient,
)
from databricks.sql.common.http import HttpMethod
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

    def test_direct_client_has_no_circuit_breaker(self):
        """Test that direct client does not have circuit breaker functionality."""
        # Direct client should work without circuit breaker
        assert isinstance(self.client, TelemetryPushClient)


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

    def test_request_enabled_success(self):
        """Test successful request when circuit breaker is enabled."""
        mock_response = Mock()
        self.mock_delegate.request.return_value = mock_response

        response = self.client.request(HttpMethod.POST, "https://test.com", {})

        assert response == mock_response
        self.mock_delegate.request.assert_called_once()

    def test_request_enabled_circuit_breaker_error(self):
        """Test request when circuit breaker is open - should raise CircuitBreakerError."""
        # Mock circuit breaker to raise CircuitBreakerError
        with patch.object(
            self.client._circuit_breaker,
            "call",
            side_effect=CircuitBreakerError("Circuit is open"),
        ):
            # Circuit breaker open should raise (caller handles it)
            with pytest.raises(CircuitBreakerError):
                self.client.request(HttpMethod.POST, "https://test.com", {})

    def test_request_enabled_other_error(self):
        """Test request when other error occurs - should raise original exception."""
        # Mock delegate to raise a different error (not rate limiting)
        self.mock_delegate.request.side_effect = ValueError("Network error")

        # Non-rate-limit errors are unwrapped and raised
        with pytest.raises(ValueError, match="Network error"):
            self.client.request(HttpMethod.POST, "https://test.com", {})

    def test_is_circuit_breaker_enabled(self):
        """Test checking if circuit breaker is enabled."""
        assert self.client._circuit_breaker is not None

    def test_circuit_breaker_state_logging(self):
        """Test that circuit breaker errors are raised (no longer silent)."""
        with patch.object(
            self.client._circuit_breaker,
            "call",
            side_effect=CircuitBreakerError("Circuit is open"),
        ):
            # Should raise CircuitBreakerError (caller handles it)
            with pytest.raises(CircuitBreakerError):
                self.client.request(HttpMethod.POST, "https://test.com", {})

    def test_other_error_logging(self):
        """Test that other errors are wrapped, logged, then unwrapped and raised."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            self.mock_delegate.request.side_effect = ValueError("Network error")

            # Should raise the original ValueError
            with pytest.raises(ValueError, match="Network error"):
                self.client.request(HttpMethod.POST, "https://test.com", {})

            # Check that debug was logged (for wrapping and/or unwrapping)
            assert mock_logger.debug.call_count >= 1


class TestCircuitBreakerTelemetryPushClientIntegration:
    """Integration tests for CircuitBreakerTelemetryPushClient."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock()
        self.host = "test-host.example.com"

    def test_circuit_breaker_opens_after_failures(self):
        """Test that circuit breaker opens after repeated failures (429/503 errors)."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerManager,
            MINIMUM_CALLS,
        )
        from databricks.sql.exc import TelemetryRateLimitError

        # Clear any existing state
        CircuitBreakerManager._instances.clear()

        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        # Simulate rate limit failures (429)
        mock_response = Mock()
        mock_response.status = 429
        self.mock_delegate.request.return_value = mock_response

        # All calls should raise TelemetryRateLimitError
        # After MINIMUM_CALLS failures, circuit breaker opens
        rate_limit_error_count = 0
        circuit_breaker_error_count = 0

        for i in range(MINIMUM_CALLS + 5):
            try:
                client.request(HttpMethod.POST, "https://test.com", {})
            except TelemetryRateLimitError:
                rate_limit_error_count += 1
            except CircuitBreakerError:
                circuit_breaker_error_count += 1

        # Should have some rate limit errors before circuit opens, then circuit breaker errors
        assert rate_limit_error_count >= MINIMUM_CALLS - 1
        assert circuit_breaker_error_count > 0

    def test_circuit_breaker_recovers_after_success(self):
        """Test that circuit breaker recovers after successful calls."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerManager,
            MINIMUM_CALLS,
            RESET_TIMEOUT,
        )
        import time

        # Clear any existing state
        CircuitBreakerManager._instances.clear()

        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        # Simulate rate limit failures first (429)
        from databricks.sql.exc import TelemetryRateLimitError
        from pybreaker import CircuitBreakerError

        mock_rate_limit_response = Mock()
        mock_rate_limit_response.status = 429
        self.mock_delegate.request.return_value = mock_rate_limit_response

        # Trigger enough rate limit failures to open circuit
        for i in range(MINIMUM_CALLS + 5):
            try:
                client.request(HttpMethod.POST, "https://test.com", {})
            except (TelemetryRateLimitError, CircuitBreakerError):
                pass  # Expected - circuit breaker opens after MINIMUM_CALLS failures

        # Circuit should be open now - raises CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            client.request(HttpMethod.POST, "https://test.com", {})

        # Wait for reset timeout
        time.sleep(RESET_TIMEOUT + 1.0)

        # Simulate successful calls (200 response)
        mock_success_response = Mock()
        mock_success_response.status = 200
        self.mock_delegate.request.return_value = mock_success_response

        # Should work again with actual success response
        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200
