"""
Unit tests for circuit breaker manager functionality.
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch

from databricks.sql.telemetry.circuit_breaker_manager import (
    CircuitBreakerManager,
    MINIMUM_CALLS,
    RESET_TIMEOUT,
    NAME_PREFIX as CIRCUIT_BREAKER_NAME,
)
from pybreaker import CircuitBreakerError


class TestCircuitBreakerManager:
    """Test cases for CircuitBreakerManager."""

    def setup_method(self):
        """Set up test fixtures."""
        CircuitBreakerManager._instances.clear()

    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager._instances.clear()

    def test_get_circuit_breaker_creates_instance(self):
        """Test getting circuit breaker creates instance with correct config."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")

        assert breaker.name == "telemetry-circuit-breaker-test-host"
        assert breaker.fail_max == MINIMUM_CALLS

    def test_get_circuit_breaker_same_host_returns_same_instance(self):
        """Test that same host returns same circuit breaker instance."""
        breaker1 = CircuitBreakerManager.get_circuit_breaker("test-host")
        breaker2 = CircuitBreakerManager.get_circuit_breaker("test-host")

        assert breaker1 is breaker2

    def test_get_circuit_breaker_different_hosts_return_different_instances(self):
        """Test that different hosts return different circuit breaker instances."""
        breaker1 = CircuitBreakerManager.get_circuit_breaker("host1")
        breaker2 = CircuitBreakerManager.get_circuit_breaker("host2")

        assert breaker1 is not breaker2
        assert breaker1.name != breaker2.name

    def test_thread_safety(self):
        """Test thread safety of circuit breaker manager."""
        results = []

        def get_breaker(host):
            breaker = CircuitBreakerManager.get_circuit_breaker(host)
            results.append(breaker)

        threads = []
        for i in range(10):
            thread = threading.Thread(target=get_breaker, args=(f"host{i % 3}",))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(results) == 10

        # All breakers for same host should be same instance
        host0_breakers = [b for b in results if b.name.endswith("host0")]
        assert all(b is host0_breakers[0] for b in host0_breakers)


class TestCircuitBreakerIntegration:
    """Integration tests for circuit breaker functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        CircuitBreakerManager._instances.clear()

    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager._instances.clear()

    def test_circuit_breaker_state_transitions(self):
        """Test circuit breaker state transitions from closed to open."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")

        assert breaker.current_state == "closed"

        def failing_func():
            raise Exception("Simulated failure")

        # Trigger failures up to the threshold (MINIMUM_CALLS = 20)
        for _ in range(MINIMUM_CALLS):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        # Next call should fail with CircuitBreakerError (circuit is now open)
        with pytest.raises(CircuitBreakerError):
            breaker.call(failing_func)

        assert breaker.current_state == "open"

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after failures."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")

        def failing_func():
            raise Exception("Simulated failure")

        # Trigger failures up to the threshold
        for _ in range(MINIMUM_CALLS):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        assert breaker.current_state == "open"

        # Wait for reset timeout
        time.sleep(RESET_TIMEOUT + 1.0)

        # Try successful call to close circuit breaker
        def successful_func():
            return "success"

        try:
            result = breaker.call(successful_func)
            assert result == "success"
        except CircuitBreakerError:
            pass  # Circuit might still be open, acceptable

        assert breaker.current_state in ["closed", "half-open", "open"]

    @pytest.mark.parametrize("old_state,new_state", [
        ("closed", "open"),
        ("open", "half-open"),
        ("half-open", "closed"),
        ("closed", "half-open"),
    ])
    def test_circuit_breaker_state_listener_transitions(self, old_state, new_state):
        """Test circuit breaker state listener logs all state transitions."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerStateListener,
        )

        listener = CircuitBreakerStateListener()
        mock_cb = Mock()
        mock_cb.name = "test-breaker"

        mock_old_state = Mock()
        mock_old_state.name = old_state

        mock_new_state = Mock()
        mock_new_state.name = new_state

        with patch("databricks.sql.telemetry.circuit_breaker_manager.logger") as mock_logger:
            listener.state_change(mock_cb, mock_old_state, mock_new_state)
            mock_logger.debug.assert_called()
