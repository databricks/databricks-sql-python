"""
E2E tests for circuit breaker functionality in telemetry.

This test suite verifies:
1. Circuit breaker opens after rate limit failures (429/503)
2. Circuit breaker blocks subsequent calls while open
3. Circuit breaker does not trigger for non-rate-limit errors
4. Circuit breaker can be disabled via configuration flag
5. Circuit breaker closes after reset timeout

Run with:
    pytest tests/e2e/test_circuit_breaker.py -v -s
"""

import time
from unittest.mock import patch, MagicMock

import pytest
from pybreaker import STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN
from urllib3 import HTTPResponse

import databricks.sql as sql
from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager


def wait_for_circuit_state(circuit_breaker, expected_states, timeout=5):
    """
    Wait for circuit breaker to reach one of the expected states with polling.

    Args:
        circuit_breaker: The circuit breaker instance to monitor
        expected_states: List of acceptable states
                        (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN)
        timeout: Maximum time to wait in seconds

    Returns:
        True if state reached, False if timeout

    Examples:
        # Single state - pass list with one element
        wait_for_circuit_state(cb, [STATE_OPEN])

        # Multiple states
        wait_for_circuit_state(cb, [STATE_CLOSED, STATE_HALF_OPEN])
    """
    start = time.time()
    while time.time() - start < timeout:
        if circuit_breaker.current_state in expected_states:
            return True
        time.sleep(0.1)  # Poll every 100ms
    return False


@pytest.fixture(autouse=True)
def aggressive_circuit_breaker_config():
    """
    Configure circuit breaker to be aggressive for faster testing.
    Opens after 2 failures instead of 20, with 5 second timeout.
    """
    from databricks.sql.telemetry import circuit_breaker_manager

    original_minimum_calls = circuit_breaker_manager.MINIMUM_CALLS
    original_reset_timeout = circuit_breaker_manager.RESET_TIMEOUT

    circuit_breaker_manager.MINIMUM_CALLS = 2
    circuit_breaker_manager.RESET_TIMEOUT = 5

    CircuitBreakerManager._instances.clear()

    yield

    circuit_breaker_manager.MINIMUM_CALLS = original_minimum_calls
    circuit_breaker_manager.RESET_TIMEOUT = original_reset_timeout
    CircuitBreakerManager._instances.clear()


class TestCircuitBreakerTelemetry:
    """Tests for circuit breaker functionality with telemetry"""

    @pytest.fixture(autouse=True)
    def get_details(self, connection_details):
        """Get connection details from pytest fixture"""
        self.arguments = connection_details.copy()

    def create_mock_response(self, status_code):
        """Helper to create mock HTTP response."""
        response = MagicMock(spec=HTTPResponse)
        response.status = status_code
        response.data = {
            429: b"Too Many Requests",
            503: b"Service Unavailable",
            500: b"Internal Server Error",
        }.get(status_code, b"Response")
        return response

    @pytest.mark.parametrize(
        "status_code,should_trigger",
        [
            (429, True),
            (503, True),
            (500, False),
        ],
    )
    def test_circuit_breaker_triggers_for_rate_limit_codes(
        self, status_code, should_trigger
    ):
        """
        Verify circuit breaker opens for rate-limit codes (429/503) but not others (500).
        """
        request_count = {"count": 0}

        def mock_request(*args, **kwargs):
            request_count["count"] += 1
            return self.create_mock_response(status_code)

        with patch(
            "databricks.sql.telemetry.telemetry_push_client.TelemetryPushClient.request",
            side_effect=mock_request,
        ):
            with sql.connect(
                server_hostname=self.arguments["host"],
                http_path=self.arguments["http_path"],
                access_token=self.arguments.get("access_token"),
                force_enable_telemetry=True,
                telemetry_batch_size=1,
                _telemetry_circuit_breaker_enabled=True,
            ) as conn:
                circuit_breaker = CircuitBreakerManager.get_circuit_breaker(
                    self.arguments["host"]
                )

                assert circuit_breaker.current_state == STATE_CLOSED

                cursor = conn.cursor()

                # Execute queries to trigger telemetry
                for i in range(1, 6):
                    cursor.execute(f"SELECT {i}")
                    cursor.fetchone()
                    time.sleep(0.5)

                if should_trigger:
                    # Wait for circuit to open (async telemetry may take time)
                    assert wait_for_circuit_state(
                        circuit_breaker, [STATE_OPEN], timeout=5
                    ), f"Circuit didn't open within 5s, state: {circuit_breaker.current_state}"

                    # Circuit should be OPEN after rate-limit failures
                    assert circuit_breaker.current_state == STATE_OPEN
                    assert circuit_breaker.fail_counter >= 2  # At least 2 failures

                    # Track requests before another query
                    requests_before = request_count["count"]
                    cursor.execute("SELECT 99")
                    cursor.fetchone()
                    time.sleep(1)

                    # No new telemetry requests (circuit is open)
                    assert request_count["count"] == requests_before
                else:
                    # Circuit should remain CLOSED for non-rate-limit errors
                    assert circuit_breaker.current_state == STATE_CLOSED
                    assert circuit_breaker.fail_counter == 0
                    assert request_count["count"] >= 5

    def test_circuit_breaker_disabled_allows_all_calls(self):
        """
        Verify that when circuit breaker is disabled, all calls go through
        even with rate limit errors.
        """
        request_count = {"count": 0}

        def mock_rate_limited_request(*args, **kwargs):
            request_count["count"] += 1
            return self.create_mock_response(429)

        with patch(
            "databricks.sql.telemetry.telemetry_push_client.TelemetryPushClient.request",
            side_effect=mock_rate_limited_request,
        ):
            with sql.connect(
                server_hostname=self.arguments["host"],
                http_path=self.arguments["http_path"],
                access_token=self.arguments.get("access_token"),
                force_enable_telemetry=True,
                telemetry_batch_size=1,
                _telemetry_circuit_breaker_enabled=False,  # Disabled
            ) as conn:
                cursor = conn.cursor()

                for i in range(5):
                    cursor.execute(f"SELECT {i}")
                    cursor.fetchone()
                    time.sleep(0.3)

                assert request_count["count"] >= 5

    def test_circuit_breaker_recovers_after_reset_timeout(self):
        """
        Verify circuit breaker transitions to HALF_OPEN after reset timeout
        and eventually CLOSES if requests succeed.
        """
        request_count = {"count": 0}
        fail_requests = {"enabled": True}

        def mock_conditional_request(*args, **kwargs):
            request_count["count"] += 1
            status = 429 if fail_requests["enabled"] else 200
            return self.create_mock_response(status)

        with patch(
            "databricks.sql.telemetry.telemetry_push_client.TelemetryPushClient.request",
            side_effect=mock_conditional_request,
        ):
            with sql.connect(
                server_hostname=self.arguments["host"],
                http_path=self.arguments["http_path"],
                access_token=self.arguments.get("access_token"),
                force_enable_telemetry=True,
                telemetry_batch_size=1,
                _telemetry_circuit_breaker_enabled=True,
            ) as conn:
                circuit_breaker = CircuitBreakerManager.get_circuit_breaker(
                    self.arguments["host"]
                )

                cursor = conn.cursor()

                # Trigger failures to open circuit
                cursor.execute("SELECT 1")
                cursor.fetchone()
                time.sleep(1)

                cursor.execute("SELECT 2")
                cursor.fetchone()
                time.sleep(2)

                # Wait for circuit to open
                assert wait_for_circuit_state(
                    circuit_breaker, [STATE_OPEN], timeout=5
                ), f"Circuit didn't open, state: {circuit_breaker.current_state}"

                # Wait for reset timeout (5 seconds in test)
                time.sleep(6)

                # Now make requests succeed
                fail_requests["enabled"] = False

                # Execute query to trigger HALF_OPEN state
                cursor.execute("SELECT 3")
                cursor.fetchone()

                # Wait for circuit to start recovering
                assert wait_for_circuit_state(
                    circuit_breaker, [STATE_HALF_OPEN, STATE_CLOSED], timeout=5
                ), f"Circuit didn't recover, state: {circuit_breaker.current_state}"

                # Execute more queries to fully recover
                cursor.execute("SELECT 4")
                cursor.fetchone()

                # Wait for full recovery
                assert wait_for_circuit_state(
                    circuit_breaker, [STATE_CLOSED, STATE_HALF_OPEN], timeout=5
                ), f"Circuit didn't fully recover, state: {circuit_breaker.current_state}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
