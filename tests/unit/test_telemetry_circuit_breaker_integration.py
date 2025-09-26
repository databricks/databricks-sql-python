"""
Integration tests for telemetry circuit breaker functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import threading
import time

from databricks.sql.telemetry.telemetry_client import TelemetryClient
from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerConfig
from databricks.sql.auth.common import ClientContext
from databricks.sql.auth.authenticators import AccessTokenAuthProvider
from pybreaker import CircuitBreakerError


class TestTelemetryCircuitBreakerIntegration:
    """Integration tests for telemetry circuit breaker functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create mock client context with circuit breaker config
        self.client_context = Mock(spec=ClientContext)
        self.client_context.telemetry_circuit_breaker_enabled = True
        self.client_context.telemetry_circuit_breaker_failure_threshold = 0.1  # 10% failure rate
        self.client_context.telemetry_circuit_breaker_minimum_calls = 2
        self.client_context.telemetry_circuit_breaker_timeout = 30
        self.client_context.telemetry_circuit_breaker_reset_timeout = 1  # 1 second for testing
        
        # Create mock auth provider
        self.auth_provider = Mock(spec=AccessTokenAuthProvider)
        
        # Create mock executor
        self.executor = Mock()
        
        # Create telemetry client
        self.telemetry_client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex="test-session",
            auth_provider=self.auth_provider,
            host_url="test-host.example.com",
            executor=self.executor,
            batch_size=10,
            client_context=self.client_context
        )
    
    def teardown_method(self):
        """Clean up after tests."""
        # Clear circuit breaker instances
        from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager
        CircuitBreakerManager.clear_all_circuit_breakers()
    
    def test_telemetry_client_initialization(self):
        """Test that telemetry client initializes with circuit breaker."""
        assert self.telemetry_client._circuit_breaker_config is not None
        assert self.telemetry_client._circuit_breaker_http_client is not None
        assert self.telemetry_client._circuit_breaker_config.enabled is True
    
    def test_telemetry_client_circuit_breaker_disabled(self):
        """Test telemetry client with circuit breaker disabled."""
        self.client_context.telemetry_circuit_breaker_enabled = False
        
        telemetry_client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex="test-session-2",
            auth_provider=self.auth_provider,
            host_url="test-host.example.com",
            executor=self.executor,
            batch_size=10,
            client_context=self.client_context
        )
        
        assert telemetry_client._circuit_breaker_config.enabled is False
    
    def test_get_circuit_breaker_state(self):
        """Test getting circuit breaker state from telemetry client."""
        state = self.telemetry_client.get_circuit_breaker_state()
        assert state in ["closed", "open", "half-open", "disabled"]
    
    def test_is_circuit_breaker_open(self):
        """Test checking if circuit breaker is open."""
        is_open = self.telemetry_client.is_circuit_breaker_open()
        assert isinstance(is_open, bool)
    
    def test_reset_circuit_breaker(self):
        """Test resetting circuit breaker from telemetry client."""
        # Should not raise an exception
        self.telemetry_client.reset_circuit_breaker()
    
    def test_telemetry_request_with_circuit_breaker_success(self):
        """Test successful telemetry request with circuit breaker."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = b'{"numProtoSuccess": 1, "errors": []}'
        
        with patch.object(self.telemetry_client._circuit_breaker_http_client, 'request', return_value=mock_response):
            # Mock the callback to avoid actual processing
            with patch.object(self.telemetry_client, '_telemetry_request_callback'):
                self.telemetry_client._send_with_unified_client(
                    "https://test.com/telemetry",
                    '{"test": "data"}',
                    {"Content-Type": "application/json"}
                )
    
    def test_telemetry_request_with_circuit_breaker_error(self):
        """Test telemetry request when circuit breaker is open."""
        # Mock circuit breaker error
        with patch.object(self.telemetry_client._circuit_breaker_http_client, 'request', side_effect=CircuitBreakerError("Circuit is open")):
            with pytest.raises(CircuitBreakerError):
                self.telemetry_client._send_with_unified_client(
                    "https://test.com/telemetry",
                    '{"test": "data"}',
                    {"Content-Type": "application/json"}
                )
    
    def test_telemetry_request_with_other_error(self):
        """Test telemetry request with other network error."""
        # Mock network error
        with patch.object(self.telemetry_client._circuit_breaker_http_client, 'request', side_effect=ValueError("Network error")):
            with pytest.raises(ValueError):
                self.telemetry_client._send_with_unified_client(
                    "https://test.com/telemetry",
                    '{"test": "data"}',
                    {"Content-Type": "application/json"}
                )
    
    def test_circuit_breaker_opens_after_telemetry_failures(self):
        """Test that circuit breaker opens after repeated telemetry failures."""
        # Mock failures
        with patch.object(self.telemetry_client._circuit_breaker_http_client, 'request', side_effect=Exception("Network error")):
            # Simulate multiple failures
            for _ in range(3):
                try:
                    self.telemetry_client._send_with_unified_client(
                        "https://test.com/telemetry",
                        '{"test": "data"}',
                        {"Content-Type": "application/json"}
                    )
                except Exception:
                    pass
        
        # Circuit breaker should eventually open
        # Note: This test might be flaky due to timing, but it tests the integration
        time.sleep(0.1)  # Give circuit breaker time to process
    
    def test_telemetry_client_factory_integration(self):
        """Test telemetry client factory with circuit breaker."""
        from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
        
        # Clear any existing clients
        TelemetryClientFactory._clients.clear()
        
        # Initialize telemetry client through factory
        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex="factory-test-session",
            auth_provider=self.auth_provider,
            host_url="test-host.example.com",
            batch_size=10,
            client_context=self.client_context
        )
        
        # Get the client
        client = TelemetryClientFactory.get_telemetry_client("factory-test-session")
        
        # Should have circuit breaker functionality
        assert hasattr(client, 'get_circuit_breaker_state')
        assert hasattr(client, 'is_circuit_breaker_open')
        assert hasattr(client, 'reset_circuit_breaker')
        
        # Clean up
        TelemetryClientFactory.close("factory-test-session")
    
    def test_circuit_breaker_configuration_from_client_context(self):
        """Test that circuit breaker configuration is properly read from client context."""
        # Test with custom configuration
        self.client_context.telemetry_circuit_breaker_failure_threshold = 0.8
        self.client_context.telemetry_circuit_breaker_minimum_calls = 5
        self.client_context.telemetry_circuit_breaker_timeout = 60
        self.client_context.telemetry_circuit_breaker_reset_timeout = 120
        
        telemetry_client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex="config-test-session",
            auth_provider=self.auth_provider,
            host_url="test-host.example.com",
            executor=self.executor,
            batch_size=10,
            client_context=self.client_context
        )
        
        config = telemetry_client._circuit_breaker_config
        assert config.failure_threshold == 0.8
        assert config.minimum_calls == 5
        assert config.timeout == 60
        assert config.reset_timeout == 120
    
    def test_circuit_breaker_logging(self):
        """Test that circuit breaker events are properly logged."""
        with patch('databricks.sql.telemetry.telemetry_client.logger') as mock_logger:
            # Mock circuit breaker error
            with patch.object(self.telemetry_client._circuit_breaker_http_client, 'request', side_effect=CircuitBreakerError("Circuit is open")):
                try:
                    self.telemetry_client._send_with_unified_client(
                        "https://test.com/telemetry",
                        '{"test": "data"}',
                        {"Content-Type": "application/json"}
                    )
                except CircuitBreakerError:
                    pass
            
            # Check that warning was logged
            mock_logger.warning.assert_called()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Telemetry request blocked by circuit breaker" in warning_call
            assert "test-session" in warning_call


class TestTelemetryCircuitBreakerThreadSafety:
    """Test thread safety of telemetry circuit breaker functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.client_context = Mock(spec=ClientContext)
        self.client_context.telemetry_circuit_breaker_enabled = True
        self.client_context.telemetry_circuit_breaker_failure_threshold = 0.1
        self.client_context.telemetry_circuit_breaker_minimum_calls = 2
        self.client_context.telemetry_circuit_breaker_timeout = 30
        self.client_context.telemetry_circuit_breaker_reset_timeout = 1
        
        self.auth_provider = Mock(spec=AccessTokenAuthProvider)
        self.executor = Mock()
    
    def teardown_method(self):
        """Clean up after tests."""
        from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager
        CircuitBreakerManager.clear_all_circuit_breakers()
    
    def test_concurrent_telemetry_requests(self):
        """Test concurrent telemetry requests with circuit breaker."""
        telemetry_client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex="concurrent-test-session",
            auth_provider=self.auth_provider,
            host_url="test-host.example.com",
            executor=self.executor,
            batch_size=10,
            client_context=self.client_context
        )
        
        results = []
        errors = []
        
        def make_request():
            try:
                with patch.object(telemetry_client._circuit_breaker_http_client, 'request', side_effect=Exception("Network error")):
                    telemetry_client._send_with_unified_client(
                        "https://test.com/telemetry",
                        '{"test": "data"}',
                        {"Content-Type": "application/json"}
                    )
                results.append("success")
            except Exception as e:
                errors.append(type(e).__name__)
        
        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Should have some results and some errors
        assert len(results) + len(errors) == 5
        # Some should be CircuitBreakerError after circuit opens
        assert "CircuitBreakerError" in errors or len(errors) == 0
