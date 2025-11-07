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
        """Test request when circuit breaker is open - should return mock response."""
        # Mock circuit breaker to raise CircuitBreakerError
        with patch.object(
            self.client._circuit_breaker,
            "call",
            side_effect=CircuitBreakerError("Circuit is open"),
        ):
            # Circuit breaker open should return mock response, not raise
            response = self.client.request(HttpMethod.POST, "https://test.com", {})
            # Should get a mock success response
            assert response is not None
            assert response.status == 200
            assert b"numProtoSuccess" in response.data

    def test_request_enabled_other_error(self):
        """Test request when other error occurs."""
        # Mock delegate to raise a different error
        self.mock_delegate.request.side_effect = ValueError("Network error")

        with pytest.raises(ValueError):
            self.client.request(HttpMethod.POST, "https://test.com", {})

    def test_is_circuit_breaker_enabled(self):
        """Test checking if circuit breaker is enabled."""
        assert self.client._circuit_breaker is not None

    def test_circuit_breaker_state_logging(self):
        """Test that circuit breaker state changes are logged."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            with patch.object(
                self.client._circuit_breaker,
                "call",
                side_effect=CircuitBreakerError("Circuit is open"),
            ):
                # Should return mock response, not raise
                response = self.client.request(HttpMethod.POST, "https://test.com", {})
                assert response is not None

            # Check that debug was logged (not warning - telemetry silently drops)
            mock_logger.debug.assert_called()
            debug_call = mock_logger.debug.call_args[0]
            assert "Circuit breaker is open" in debug_call[0]
            assert self.host in debug_call[1]

    def test_other_error_logging(self):
        """Test that other errors are logged appropriately."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            self.mock_delegate.request.side_effect = ValueError("Network error")

            with pytest.raises(ValueError):
                self.client.request(HttpMethod.POST, "https://test.com", {})

            # Check that debug was logged
            mock_logger.debug.assert_called()
            debug_call = mock_logger.debug.call_args[0]
            assert "Telemetry request failed" in debug_call[0]
            assert self.host in debug_call[1]


class TestCircuitBreakerTelemetryPushClientIntegration:
    """Integration tests for CircuitBreakerTelemetryPushClient."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock()
        self.host = "test-host.example.com"

    def test_circuit_breaker_opens_after_failures(self):
        """Test that circuit breaker opens after repeated failures."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerManager,
            MINIMUM_CALLS,
        )

        # Clear any existing state
        CircuitBreakerManager._instances.clear()

        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        # Simulate failures
        self.mock_delegate.request.side_effect = Exception("Network error")

        # Trigger failures - some will raise, some will return mock response once circuit opens
        exception_count = 0
        mock_response_count = 0
        for i in range(MINIMUM_CALLS + 5):
            try:
                response = client.request(HttpMethod.POST, "https://test.com", {})
                # Got a mock response - circuit is open
                assert response.status == 200
                mock_response_count += 1
            except Exception:
                # Got an exception - circuit is still closed
                exception_count += 1
        
        # Should have some exceptions before circuit opened, then mock responses after
        # Circuit opens around MINIMUM_CALLS failures (might be MINIMUM_CALLS or MINIMUM_CALLS-1)
        assert exception_count >= MINIMUM_CALLS - 1
        assert mock_response_count > 0

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

        # Simulate failures first
        self.mock_delegate.request.side_effect = Exception("Network error")

        # Trigger enough failures to open circuit
        for i in range(MINIMUM_CALLS + 5):
            try:
                client.request(HttpMethod.POST, "https://test.com", {})
            except Exception:
                pass  # Expected during failures

        # Circuit should be open now - returns mock response
        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200  # Mock success response

        # Wait for reset timeout
        time.sleep(RESET_TIMEOUT + 1.0)

        # Simulate successful calls
        self.mock_delegate.request.side_effect = None
        self.mock_delegate.request.return_value = Mock()

        # Should work again
        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
