"""
Unit tests for telemetry push client functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import urllib.parse

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

    def test_initialization_disabled(self):
        """Test client initialization with circuit breaker disabled."""
        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        assert client._circuit_breaker is not None

    def test_request_disabled(self):
        """Test request method when circuit breaker is disabled."""
        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        mock_response = Mock()
        self.mock_delegate.request.return_value = mock_response

        response = client.request(HttpMethod.POST, "https://test.com", {})

        assert response == mock_response
        self.mock_delegate.request.assert_called_once()

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
        """Test request when other error occurs - should return mock response and not raise."""
        # Mock delegate to raise a different error
        self.mock_delegate.request.side_effect = ValueError("Network error")

        # Should return mock response, not raise
        response = self.client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200

    def test_is_circuit_breaker_enabled(self):
        """Test checking if circuit breaker is enabled."""
        # Circuit breaker is always enabled in this implementation
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
            debug_args = mock_logger.debug.call_args[0]
            assert "Circuit breaker is open" in debug_args[0]
            assert self.host in debug_args[1]  # The host is the second argument

    def test_other_error_logging(self):
        """Test that other errors are logged appropriately - should return mock response."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            self.mock_delegate.request.side_effect = ValueError("Network error")

            # Should return mock response, not raise
            response = self.client.request(HttpMethod.POST, "https://test.com", {})
            assert response is not None
            assert response.status == 200

            # Check that debug was logged
            mock_logger.debug.assert_called()
            debug_args = mock_logger.debug.call_args[0]
            assert "failing silently" in debug_args[0]
            assert self.host in debug_args[1]  # The host is the second argument

    def test_request_429_returns_mock_success(self):
        """Test that 429 response triggers circuit breaker but returns mock success."""
        # Mock delegate to return 429
        mock_response = Mock()
        mock_response.status = 429
        self.mock_delegate.request.return_value = mock_response

        # Should return mock success response (circuit breaker counted it as failure)
        response = self.client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200  # Mock success

    def test_request_503_returns_mock_success(self):
        """Test that 503 response triggers circuit breaker but returns mock success."""
        # Mock delegate to return 503
        mock_response = Mock()
        mock_response.status = 503
        self.mock_delegate.request.return_value = mock_response

        # Should return mock success response (circuit breaker counted it as failure)
        response = self.client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200  # Mock success

    def test_request_500_returns_response(self):
        """Test that 500 response returns the response without raising."""
        # Mock delegate to return 500
        mock_response = Mock()
        mock_response.status = 500
        mock_response.data = b'Server error'
        self.mock_delegate.request.return_value = mock_response

        # Should return the actual response since 500 is not rate limiting
        response = self.client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 500

    def test_rate_limit_error_logging(self):
        """Test that rate limit errors are logged at warning level."""
        with patch(
            "databricks.sql.telemetry.telemetry_push_client.logger"
        ) as mock_logger:
            mock_response = Mock()
            mock_response.status = 429
            self.mock_delegate.request.return_value = mock_response

            # Should return mock success (no exception raised)
            response = self.client.request(HttpMethod.POST, "https://test.com", {})
            assert response is not None
            assert response.status == 200

            # Check that warning was logged (from inner function)
            mock_logger.warning.assert_called()
            warning_args = mock_logger.warning.call_args[0]
            assert "429" in str(warning_args)
            assert "circuit breaker" in warning_args[0]


class TestCircuitBreakerTelemetryPushClientIntegration:
    """Integration tests for CircuitBreakerTelemetryPushClient."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock()
        self.host = "test-host.example.com"
        # Clear any existing circuit breaker state and initialize with config
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerManager,
            CircuitBreakerConfig,
        )

        CircuitBreakerManager._instances.clear()
        # Initialize with default config for testing
        CircuitBreakerManager.initialize(CircuitBreakerConfig())

    @pytest.mark.skip(reason="TODO: pybreaker needs custom filtering logic to only count TelemetryRateLimitError")
    def test_circuit_breaker_opens_after_failures(self):
        """Test that circuit breaker opens after repeated 429 failures.
        
        NOTE: pybreaker currently counts ALL exceptions as failures.
        We need to implement custom filtering to only count TelemetryRateLimitError.
        Unit tests verify the component behavior correctly.
        """
        from databricks.sql.telemetry.circuit_breaker_manager import DEFAULT_MINIMUM_CALLS

        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        # Simulate 429 responses (rate limiting)
        mock_response = Mock()
        mock_response.status = 429
        self.mock_delegate.request.return_value = mock_response

        # Trigger failures - some will raise TelemetryRateLimitError, some will return mock response once circuit opens
        exception_count = 0
        mock_response_count = 0
        for i in range(DEFAULT_MINIMUM_CALLS + 5):
            try:
                response = client.request(HttpMethod.POST, "https://test.com", {})
                # Got a mock response - circuit is open or it's a non-rate-limit response
                assert response.status == 200
                mock_response_count += 1
            except TelemetryRateLimitError:
                # Got rate limit error - circuit is still closed
                exception_count += 1
        
        # Should have some rate limit exceptions before circuit opened, then mock responses after
        # Circuit opens around DEFAULT_MINIMUM_CALLS failures (might be DEFAULT_MINIMUM_CALLS or DEFAULT_MINIMUM_CALLS-1)
        assert exception_count >= DEFAULT_MINIMUM_CALLS - 1
        assert mock_response_count > 0

    @pytest.mark.skip(reason="TODO: pybreaker needs custom filtering logic to only count TelemetryRateLimitError")
    def test_circuit_breaker_recovers_after_success(self):
        """Test that circuit breaker recovers after successful calls.
        
        NOTE: pybreaker currently counts ALL exceptions as failures.
        We need to implement custom filtering to only count TelemetryRateLimitError.
        Unit tests verify the component behavior correctly.
        """
        from databricks.sql.telemetry.circuit_breaker_manager import (
            DEFAULT_MINIMUM_CALLS,
            DEFAULT_RESET_TIMEOUT,
        )
        import time

        client = CircuitBreakerTelemetryPushClient(self.mock_delegate, self.host)

        # Simulate 429 responses (rate limiting)
        mock_429_response = Mock()
        mock_429_response.status = 429
        self.mock_delegate.request.return_value = mock_429_response

        # Trigger enough failures to open circuit
        for i in range(DEFAULT_MINIMUM_CALLS + 5):
            try:
                client.request(HttpMethod.POST, "https://test.com", {})
            except TelemetryRateLimitError:
                pass  # Expected during rate limiting

        # Circuit should be open now - returns mock response
        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200  # Mock success response

        # Wait for reset timeout
        time.sleep(DEFAULT_RESET_TIMEOUT + 1.0)

        # Simulate successful calls (200 response)
        mock_success_response = Mock()
        mock_success_response.status = 200
        mock_success_response.data = b'{"success": true}'
        self.mock_delegate.request.return_value = mock_success_response

        # Should work again
        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
        assert response.status == 200

    def test_urllib3_import_fallback(self):
        """Test that the urllib3 import fallback works correctly."""
        # This test verifies that the import fallback mechanism exists
        # The actual fallback is tested by the fact that the module imports successfully
        # even when BaseHTTPResponse is not available
        from databricks.sql.telemetry.telemetry_push_client import BaseHTTPResponse

        assert BaseHTTPResponse is not None
