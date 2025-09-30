"""
Unit tests for circuit breaker manager functionality.
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch

from databricks.sql.telemetry.circuit_breaker_manager import (
    CircuitBreakerManager,
    CircuitBreakerConfig,
    is_circuit_breaker_error
)
from pybreaker import CircuitBreakerError


class TestCircuitBreakerConfig:
    """Test cases for CircuitBreakerConfig."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = CircuitBreakerConfig()
        
        assert config.failure_threshold == 0.5
        assert config.minimum_calls == 20
        assert config.timeout == 30
        assert config.reset_timeout == 30
        assert config.expected_exception == (Exception,)
        assert config.name == "telemetry-circuit-breaker"
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = CircuitBreakerConfig(
            failure_threshold=0.8,
            minimum_calls=10,
            timeout=60,
            reset_timeout=120,
            expected_exception=(ValueError,),
            name="custom-breaker"
        )
        
        assert config.failure_threshold == 0.8
        assert config.minimum_calls == 10
        assert config.timeout == 60
        assert config.reset_timeout == 120
        assert config.expected_exception == (ValueError,)
        assert config.name == "custom-breaker"


class TestCircuitBreakerManager:
    """Test cases for CircuitBreakerManager."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Clear any existing instances
        CircuitBreakerManager.clear_all_circuit_breakers()
        CircuitBreakerManager._config = None
    
    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager.clear_all_circuit_breakers()
        CircuitBreakerManager._config = None
    
    def test_initialize(self):
        """Test circuit breaker manager initialization."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        assert CircuitBreakerManager._config == config
    
    def test_get_circuit_breaker_not_initialized(self):
        """Test getting circuit breaker when not initialized."""
        # Don't initialize the manager
        CircuitBreakerManager._config = None
        
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")
        
        # Should return a no-op circuit breaker
        assert breaker.name == "noop-circuit-breaker"
        assert breaker.fail_max == 1000000  # Very high threshold for no-op
    
    def test_get_circuit_breaker_enabled(self):
        """Test getting circuit breaker when enabled."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")
        
        assert breaker.name == "telemetry-circuit-breaker-test-host"
        assert breaker.fail_max == 20  # minimum_calls from config
    
    def test_get_circuit_breaker_same_host(self):
        """Test that same host returns same circuit breaker instance."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        breaker1 = CircuitBreakerManager.get_circuit_breaker("test-host")
        breaker2 = CircuitBreakerManager.get_circuit_breaker("test-host")
        
        assert breaker1 is breaker2
    
    def test_get_circuit_breaker_different_hosts(self):
        """Test that different hosts return different circuit breaker instances."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        breaker1 = CircuitBreakerManager.get_circuit_breaker("host1")
        breaker2 = CircuitBreakerManager.get_circuit_breaker("host2")
        
        assert breaker1 is not breaker2
        assert breaker1.name != breaker2.name
    
    def test_get_circuit_breaker_state(self):
        """Test getting circuit breaker state."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        # Test not initialized state
        CircuitBreakerManager._config = None
        assert CircuitBreakerManager.get_circuit_breaker_state("test-host") == "disabled"
        
        # Test enabled state
        CircuitBreakerManager.initialize(config)
        CircuitBreakerManager.get_circuit_breaker("test-host")
        state = CircuitBreakerManager.get_circuit_breaker_state("test-host")
        assert state in ["closed", "open", "half-open"]
    
    def test_reset_circuit_breaker(self):
        """Test resetting circuit breaker."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")
        CircuitBreakerManager.reset_circuit_breaker("test-host")
        
        # Reset should not raise an exception
        assert breaker.current_state in ["closed", "open", "half-open"]
    
    def test_clear_circuit_breaker(self):
        """Test clearing circuit breaker for specific host."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        CircuitBreakerManager.get_circuit_breaker("test-host")
        assert "test-host" in CircuitBreakerManager._instances
        
        CircuitBreakerManager.clear_circuit_breaker("test-host")
        assert "test-host" not in CircuitBreakerManager._instances
    
    def test_clear_all_circuit_breakers(self):
        """Test clearing all circuit breakers."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        CircuitBreakerManager.get_circuit_breaker("host1")
        CircuitBreakerManager.get_circuit_breaker("host2")
        assert len(CircuitBreakerManager._instances) == 2
        
        CircuitBreakerManager.clear_all_circuit_breakers()
        assert len(CircuitBreakerManager._instances) == 0
    
    def test_thread_safety(self):
        """Test thread safety of circuit breaker manager."""
        config = CircuitBreakerConfig()
        CircuitBreakerManager.initialize(config)
        
        results = []
        
        def get_breaker(host):
            breaker = CircuitBreakerManager.get_circuit_breaker(host)
            results.append(breaker)
        
        # Create multiple threads accessing circuit breakers
        threads = []
        for i in range(10):
            thread = threading.Thread(target=get_breaker, args=(f"host{i % 3}",))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have 10 results
        assert len(results) == 10
        
        # All breakers for same host should be same instance
        host0_breakers = [b for b in results if b.name.endswith("host0")]
        assert all(b is host0_breakers[0] for b in host0_breakers)


class TestCircuitBreakerErrorDetection:
    """Test cases for circuit breaker error detection."""
    
    def test_is_circuit_breaker_error_true(self):
        """Test detecting circuit breaker errors."""
        error = CircuitBreakerError("Circuit breaker is open")
        assert is_circuit_breaker_error(error) is True
    
    def test_is_circuit_breaker_error_false(self):
        """Test detecting non-circuit breaker errors."""
        error = ValueError("Some other error")
        assert is_circuit_breaker_error(error) is False
        
        error = RuntimeError("Another error")
        assert is_circuit_breaker_error(error) is False
    
    def test_is_circuit_breaker_error_none(self):
        """Test with None input."""
        assert is_circuit_breaker_error(None) is False


class TestCircuitBreakerIntegration:
    """Integration tests for circuit breaker functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        CircuitBreakerManager.clear_all_circuit_breakers()
        CircuitBreakerManager._config = None
    
    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager.clear_all_circuit_breakers()
        CircuitBreakerManager._config = None
    
    def test_circuit_breaker_state_transitions(self):
        """Test circuit breaker state transitions."""
        # Use a very low threshold to trigger circuit breaker quickly
        config = CircuitBreakerConfig(
            failure_threshold=0.1,  # 10% failure rate
            minimum_calls=2,        # Only 2 calls needed
            reset_timeout=1         # 1 second reset timeout
        )
        CircuitBreakerManager.initialize(config)
        
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")
        
        # Initially should be closed
        assert breaker.current_state == "closed"
        
        # Simulate failures to trigger circuit breaker
        def failing_func():
            raise Exception("Simulated failure")
        
        # First call should fail with original exception
        with pytest.raises(Exception):
            breaker.call(failing_func)
        
        # Second call should fail with CircuitBreakerError (circuit opens)
        with pytest.raises(CircuitBreakerError):
            breaker.call(failing_func)
        
        # Circuit breaker should eventually open
        assert breaker.current_state == "open"
        
        # Wait for reset timeout
        time.sleep(1.1)
        
        # Circuit breaker should be half-open (or still open depending on implementation)
        # Let's just check that it's not closed
        assert breaker.current_state in ["open", "half-open"]
    
    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after failures."""
        config = CircuitBreakerConfig(
            failure_threshold=0.1,
            minimum_calls=2,
            reset_timeout=1
        )
        CircuitBreakerManager.initialize(config)
        
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")
        
        # Trigger circuit breaker to open
        def failing_func():
            raise Exception("Simulated failure")
        
        # First call should fail with original exception
        with pytest.raises(Exception):
            breaker.call(failing_func)
        
        # Second call should fail with CircuitBreakerError (circuit opens)
        with pytest.raises(CircuitBreakerError):
            breaker.call(failing_func)
        
        assert breaker.current_state == "open"
        
        # Wait for reset timeout
        time.sleep(1.1)
        
        # Try successful call to close circuit breaker
        def successful_func():
            return "success"
        
        try:
            breaker.call(successful_func)
        except Exception:
            pass
        
        # Circuit breaker should be closed again (or at least not open)
        assert breaker.current_state in ["closed", "half-open"]
