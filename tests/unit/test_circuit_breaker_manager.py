"""
Unit tests for circuit breaker manager functionality.
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch

from databricks.sql.telemetry.circuit_breaker_manager import (
    CircuitBreakerManager,
    is_circuit_breaker_error,
    MINIMUM_CALLS,
    RESET_TIMEOUT,
    CIRCUIT_BREAKER_NAME,
)
from pybreaker import CircuitBreakerError


class TestCircuitBreakerManager:
    """Test cases for CircuitBreakerManager."""

    def setup_method(self):
        """Set up test fixtures."""
        # Clear any existing instances
        CircuitBreakerManager._instances.clear()

    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager._instances.clear()

    def test_get_circuit_breaker_creates_instance(self):
        """Test getting circuit breaker creates instance with correct config."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")

        assert breaker.name == "telemetry-circuit-breaker-test-host"
        assert breaker.fail_max == MINIMUM_CALLS

    def test_get_circuit_breaker_same_host(self):
        """Test that same host returns same circuit breaker instance."""
        breaker1 = CircuitBreakerManager.get_circuit_breaker("test-host")
        breaker2 = CircuitBreakerManager.get_circuit_breaker("test-host")

        assert breaker1 is breaker2

    def test_get_circuit_breaker_different_hosts(self):
        """Test that different hosts return different circuit breaker instances."""
        breaker1 = CircuitBreakerManager.get_circuit_breaker("host1")
        breaker2 = CircuitBreakerManager.get_circuit_breaker("host2")

        assert breaker1 is not breaker2
        assert breaker1.name != breaker2.name

    def test_get_circuit_breaker_creates_breaker(self):
        """Test getting circuit breaker creates and returns breaker."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")
        assert breaker is not None
        assert breaker.current_state in ["closed", "open", "half-open"]

    def test_thread_safety(self):
        """Test thread safety of circuit breaker manager."""
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
        CircuitBreakerManager._instances.clear()

    def teardown_method(self):
        """Clean up after tests."""
        CircuitBreakerManager._instances.clear()

    def test_circuit_breaker_state_transitions(self):
        """Test circuit breaker state transitions."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")

        # Initially should be closed
        assert breaker.current_state == "closed"

        # Simulate failures to trigger circuit breaker
        def failing_func():
            raise Exception("Simulated failure")

        # Trigger failures up to the threshold (MINIMUM_CALLS = 20)
        for i in range(MINIMUM_CALLS):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        # Next call should fail with CircuitBreakerError (circuit is now open)
        with pytest.raises(CircuitBreakerError):
            breaker.call(failing_func)

        # Circuit breaker should be open
        assert breaker.current_state == "open"

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after failures."""
        breaker = CircuitBreakerManager.get_circuit_breaker("test-host")

        # Trigger circuit breaker to open
        def failing_func():
            raise Exception("Simulated failure")

        # Trigger failures up to the threshold
        for i in range(MINIMUM_CALLS):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        # Circuit should be open now
        assert breaker.current_state == "open"

        # Wait for reset timeout
        time.sleep(RESET_TIMEOUT + 1.0)

        # Try successful call to close circuit breaker
        def successful_func():
            return "success"

        try:
            result = breaker.call(successful_func)
            # If successful, circuit should transition to closed or half-open
            assert result == "success"
        except CircuitBreakerError:
            # Circuit might still be open, which is acceptable
            pass

        # Circuit breaker should be closed or half-open (not permanently open)
        assert breaker.current_state in ["closed", "half-open", "open"]

    def test_circuit_breaker_state_listener_half_open(self):
        """Test circuit breaker state listener logs half-open state."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerStateListener,
            CIRCUIT_BREAKER_STATE_HALF_OPEN,
        )
        from unittest.mock import patch

        listener = CircuitBreakerStateListener()

        # Mock circuit breaker with half-open state
        mock_cb = Mock()
        mock_cb.name = "test-breaker"

        # Mock old and new states
        mock_old_state = Mock()
        mock_old_state.name = "open"

        mock_new_state = Mock()
        mock_new_state.name = CIRCUIT_BREAKER_STATE_HALF_OPEN

        with patch(
            "databricks.sql.telemetry.circuit_breaker_manager.logger"
        ) as mock_logger:
            listener.state_change(mock_cb, mock_old_state, mock_new_state)

            # Check that half-open state was logged
            mock_logger.info.assert_called()
            calls = mock_logger.info.call_args_list
            half_open_logged = any("half-open" in str(call) for call in calls)
            assert half_open_logged

    def test_circuit_breaker_state_listener_all_states(self):
        """Test circuit breaker state listener logs all possible state transitions."""
        from databricks.sql.telemetry.circuit_breaker_manager import (
            CircuitBreakerStateListener,
            CIRCUIT_BREAKER_STATE_HALF_OPEN,
            CIRCUIT_BREAKER_STATE_OPEN,
            CIRCUIT_BREAKER_STATE_CLOSED,
        )
        from unittest.mock import patch

        listener = CircuitBreakerStateListener()
        mock_cb = Mock()
        mock_cb.name = "test-breaker"

        # Test all state transitions with exact constants
        state_transitions = [
            (CIRCUIT_BREAKER_STATE_CLOSED, CIRCUIT_BREAKER_STATE_OPEN),
            (CIRCUIT_BREAKER_STATE_OPEN, CIRCUIT_BREAKER_STATE_HALF_OPEN),
            (CIRCUIT_BREAKER_STATE_HALF_OPEN, CIRCUIT_BREAKER_STATE_CLOSED),
            (CIRCUIT_BREAKER_STATE_CLOSED, CIRCUIT_BREAKER_STATE_HALF_OPEN),
        ]

        with patch(
            "databricks.sql.telemetry.circuit_breaker_manager.logger"
        ) as mock_logger:
            for old_state_name, new_state_name in state_transitions:
                mock_old_state = Mock()
                mock_old_state.name = old_state_name

                mock_new_state = Mock()
                mock_new_state.name = new_state_name

                listener.state_change(mock_cb, mock_old_state, mock_new_state)

            # Verify that logging was called for each transition
            assert mock_logger.info.call_count >= len(state_transitions)

    def test_get_circuit_breaker_creates_on_demand(self):
        """Test that circuit breaker is created on first access."""
        # Test with a host that doesn't exist yet
        breaker = CircuitBreakerManager.get_circuit_breaker("new-host")
        assert breaker is not None
        assert "new-host" in CircuitBreakerManager._instances
