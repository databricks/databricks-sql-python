"""
Unit tests for telemetry push client functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import urllib.parse

from databricks.sql.telemetry.telemetry_push_client import (
    ITelemetryPushClient,
    TelemetryPushClient,
    CircuitBreakerTelemetryPushClient
)
from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerConfig
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
    
    def test_circuit_breaker_state_methods(self):
        """Test circuit breaker state methods return appropriate values."""
        assert self.client.get_circuit_breaker_state() == "not_available"
        assert self.client.is_circuit_breaker_open() is False
        # Should not raise exception
        self.client.reset_circuit_breaker()


class TestCircuitBreakerTelemetryPushClient:
    """Test cases for CircuitBreakerTelemetryPushClient."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock(spec=ITelemetryPushClient)
        self.host = "test-host.example.com"
        self.config = CircuitBreakerConfig(
            failure_threshold=0.5,
            minimum_calls=10,
            timeout=30,
            reset_timeout=30
        )
        self.client = CircuitBreakerTelemetryPushClient(
            self.mock_delegate,
            self.host,
            self.config
        )
    
    def test_initialization(self):
        """Test client initialization."""
        assert self.client._delegate == self.mock_delegate
        assert self.client._host == self.host
        assert self.client._config == self.config
        assert self.client._circuit_breaker is not None
    
    def test_initialization_disabled(self):
        """Test client initialization with circuit breaker disabled."""
        config = CircuitBreakerConfig(enabled=False)
        client = CircuitBreakerHttpClient(self.mock_delegate, self.host, config)
        
        assert client._config.enabled is False
    
    def test_request_context_disabled(self):
        """Test request context when circuit breaker is disabled."""
        config = CircuitBreakerConfig(enabled=False)
        client = CircuitBreakerHttpClient(self.mock_delegate, self.host, config)
        
        mock_response = Mock()
        self.mock_delegate.request_context.return_value.__enter__.return_value = mock_response
        self.mock_delegate.request_context.return_value.__exit__.return_value = None
        
        with client.request_context(HttpMethod.POST, "https://test.com", {}) as response:
            assert response == mock_response
        
        self.mock_delegate.request_context.assert_called_once()
    
    def test_request_context_enabled_success(self):
        """Test successful request context when circuit breaker is enabled."""
        mock_response = Mock()
        self.mock_delegate.request_context.return_value.__enter__.return_value = mock_response
        self.mock_delegate.request_context.return_value.__exit__.return_value = None
        
        with client.request_context(HttpMethod.POST, "https://test.com", {}) as response:
            assert response == mock_response
        
        self.mock_delegate.request_context.assert_called_once()
    
    def test_request_context_enabled_circuit_breaker_error(self):
        """Test request context when circuit breaker is open."""
        # Mock circuit breaker to raise CircuitBreakerError
        with patch.object(self.client._circuit_breaker, '__enter__', side_effect=CircuitBreakerError("Circuit is open")):
            with pytest.raises(CircuitBreakerError):
                with self.client.request_context(HttpMethod.POST, "https://test.com", {}):
                    pass
    
    def test_request_context_enabled_other_error(self):
        """Test request context when other error occurs."""
        # Mock delegate to raise a different error
        self.mock_delegate.request_context.side_effect = ValueError("Network error")
        
        with pytest.raises(ValueError):
            with self.client.request_context(HttpMethod.POST, "https://test.com", {}):
                pass
    
    def test_request_disabled(self):
        """Test request method when circuit breaker is disabled."""
        config = CircuitBreakerConfig(enabled=False)
        client = CircuitBreakerHttpClient(self.mock_delegate, self.host, config)
        
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
        """Test request when circuit breaker is open."""
        # Mock circuit breaker to raise CircuitBreakerError
        with patch.object(self.client._circuit_breaker, '__enter__', side_effect=CircuitBreakerError("Circuit is open")):
            with pytest.raises(CircuitBreakerError):
                self.client.request(HttpMethod.POST, "https://test.com", {})
    
    def test_request_enabled_other_error(self):
        """Test request when other error occurs."""
        # Mock delegate to raise a different error
        self.mock_delegate.request.side_effect = ValueError("Network error")
        
        with pytest.raises(ValueError):
            self.client.request(HttpMethod.POST, "https://test.com", {})
    
    def test_get_circuit_breaker_state(self):
        """Test getting circuit breaker state."""
        with patch.object(self.client._circuit_breaker, 'current_state', 'open'):
            state = self.client.get_circuit_breaker_state()
            assert state == 'open'
    
    def test_reset_circuit_breaker(self):
        """Test resetting circuit breaker."""
        with patch.object(self.client._circuit_breaker, 'reset') as mock_reset:
            self.client.reset_circuit_breaker()
            mock_reset.assert_called_once()
    
    def test_is_circuit_breaker_open(self):
        """Test checking if circuit breaker is open."""
        with patch.object(self.client, 'get_circuit_breaker_state', return_value='open'):
            assert self.client.is_circuit_breaker_open() is True
        
        with patch.object(self.client, 'get_circuit_breaker_state', return_value='closed'):
            assert self.client.is_circuit_breaker_open() is False
    
    def test_is_circuit_breaker_enabled(self):
        """Test checking if circuit breaker is enabled."""
        assert self.client.is_circuit_breaker_enabled() is True
        
        config = CircuitBreakerConfig(enabled=False)
        client = CircuitBreakerHttpClient(self.mock_delegate, self.host, config)
        assert client.is_circuit_breaker_enabled() is False
    
    def test_circuit_breaker_state_logging(self):
        """Test that circuit breaker state changes are logged."""
        with patch('databricks.sql.telemetry.circuit_breaker_http_client.logger') as mock_logger:
            with patch.object(self.client._circuit_breaker, '__enter__', side_effect=CircuitBreakerError("Circuit is open")):
                with pytest.raises(CircuitBreakerError):
                    self.client.request(HttpMethod.POST, "https://test.com", {})
                
                # Check that warning was logged
                mock_logger.warning.assert_called()
                warning_call = mock_logger.warning.call_args[0][0]
                assert "Circuit breaker is open" in warning_call
                assert self.host in warning_call
    
    def test_other_error_logging(self):
        """Test that other errors are logged appropriately."""
        with patch('databricks.sql.telemetry.circuit_breaker_http_client.logger') as mock_logger:
            self.mock_delegate.request.side_effect = ValueError("Network error")
            
            with pytest.raises(ValueError):
                self.client.request(HttpMethod.POST, "https://test.com", {})
            
            # Check that debug was logged
            mock_logger.debug.assert_called()
            debug_call = mock_logger.debug.call_args[0][0]
            assert "Telemetry request failed" in debug_call
            assert self.host in debug_call


class TestCircuitBreakerHttpClientIntegration:
    """Integration tests for CircuitBreakerHttpClient."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_delegate = Mock()
        self.host = "test-host.example.com"
    
    def test_circuit_breaker_opens_after_failures(self):
        """Test that circuit breaker opens after repeated failures."""
        config = CircuitBreakerConfig(
            failure_threshold=0.1,  # 10% failure rate
            minimum_calls=2,        # Only 2 calls needed
            reset_timeout=1         # 1 second reset timeout
        )
        client = CircuitBreakerHttpClient(self.mock_delegate, self.host, config)
        
        # Simulate failures
        self.mock_delegate.request.side_effect = Exception("Network error")
        
        # First few calls should fail with the original exception
        for _ in range(2):
            with pytest.raises(Exception, match="Network error"):
                client.request(HttpMethod.POST, "https://test.com", {})
        
        # After enough failures, circuit breaker should open
        with pytest.raises(CircuitBreakerError):
            client.request(HttpMethod.POST, "https://test.com", {})
    
    def test_circuit_breaker_recovers_after_success(self):
        """Test that circuit breaker recovers after successful calls."""
        config = CircuitBreakerConfig(
            failure_threshold=0.1,
            minimum_calls=2,
            reset_timeout=1
        )
        client = CircuitBreakerHttpClient(self.mock_delegate, self.host, config)
        
        # Simulate failures first
        self.mock_delegate.request.side_effect = Exception("Network error")
        
        for _ in range(2):
            with pytest.raises(Exception):
                client.request(HttpMethod.POST, "https://test.com", {})
        
        # Circuit breaker should be open now
        with pytest.raises(CircuitBreakerError):
            client.request(HttpMethod.POST, "https://test.com", {})
        
        # Wait for reset timeout
        import time
        time.sleep(1.1)
        
        # Simulate successful calls
        self.mock_delegate.request.side_effect = None
        self.mock_delegate.request.return_value = Mock()
        
        # Should work again
        response = client.request(HttpMethod.POST, "https://test.com", {})
        assert response is not None
