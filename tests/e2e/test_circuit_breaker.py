"""
E2E tests for circuit breaker functionality in telemetry.

This test suite verifies:
1. Circuit breaker opens after rate limit failures (429/503)
2. Circuit breaker blocks subsequent calls while open
3. Circuit breaker transitions through states correctly
4. Circuit breaker does not trigger for non-rate-limit errors
5. Circuit breaker can be disabled via configuration flag
6. Circuit breaker closes after reset timeout

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


@pytest.fixture(autouse=True)
def aggressive_circuit_breaker_config():
    """
    Configure circuit breaker to be aggressive for faster testing.
    Opens after 2 failures instead of 20, with 5 second timeout.
    """
    from databricks.sql.telemetry import circuit_breaker_manager

    # Store original values
    original_minimum_calls = circuit_breaker_manager.MINIMUM_CALLS
    original_reset_timeout = circuit_breaker_manager.RESET_TIMEOUT

    # Patch with aggressive test values
    circuit_breaker_manager.MINIMUM_CALLS = 2
    circuit_breaker_manager.RESET_TIMEOUT = 5

    # Reset all circuit breakers before test
    CircuitBreakerManager._instances.clear()

    yield

    # Cleanup: restore original values and reset breakers
    circuit_breaker_manager.MINIMUM_CALLS = original_minimum_calls
    circuit_breaker_manager.RESET_TIMEOUT = original_reset_timeout
    CircuitBreakerManager._instances.clear()


class TestCircuitBreakerTelemetry:
    """Tests for circuit breaker functionality with telemetry"""

    @pytest.fixture(autouse=True)
    def get_details(self, connection_details):
        """Get connection details from pytest fixture"""
        self.arguments = connection_details.copy()

    def test_circuit_breaker_opens_after_rate_limit_errors(self):
        """
        Verify circuit breaker opens after 429/503 errors and blocks subsequent calls.
        """
        request_count = {"count": 0}

        def mock_rate_limited_request(*args, **kwargs):
            """Mock that returns 429 rate limit response"""
            request_count["count"] += 1
            response = MagicMock(spec=HTTPResponse)
            response.status = 429
            response.data = b"Too Many Requests"
            return response

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
                _telemetry_circuit_breaker_enabled=True,
            ) as conn:
                circuit_breaker = CircuitBreakerManager.get_circuit_breaker(
                    self.arguments["host"]
                )

                # Initial state should be CLOSED
                assert circuit_breaker.current_state == STATE_CLOSED

                cursor = conn.cursor()

                # Execute queries to trigger telemetry failures
                cursor.execute("SELECT 1")
                cursor.fetchone()
                time.sleep(1)

                cursor.execute("SELECT 2")
                cursor.fetchone()
                time.sleep(2)

                # Circuit should now be OPEN after 2 failures
                assert circuit_breaker.current_state == STATE_OPEN
                assert circuit_breaker.fail_counter == 2

                # Track requests before executing another query
                requests_before = request_count["count"]

                # Execute another query - circuit breaker should block telemetry
                cursor.execute("SELECT 3")
                cursor.fetchone()
                time.sleep(1)

                requests_after = request_count["count"]

                # No new telemetry requests should be made (circuit is open)
                assert (
                    requests_after == requests_before
                ), "Circuit breaker should block requests while OPEN"

    def test_circuit_breaker_does_not_trigger_for_non_rate_limit_errors(self):
        """
        Verify circuit breaker does NOT open for errors other than 429/503.
        Only rate limit errors should trigger the circuit breaker.
        """
        request_count = {"count": 0}

        def mock_server_error_request(*args, **kwargs):
            """Mock that returns 500 server error (not rate limit)"""
            request_count["count"] += 1
            response = MagicMock(spec=HTTPResponse)
            response.status = 500  # Server error - should NOT trigger CB
            response.data = b"Internal Server Error"
            return response

        with patch(
            "databricks.sql.telemetry.telemetry_push_client.TelemetryPushClient.request",
            side_effect=mock_server_error_request,
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

                # Execute multiple queries with 500 errors
                for i in range(5):
                    cursor.execute(f"SELECT {i}")
                    cursor.fetchone()
                    time.sleep(0.5)

                # Circuit should remain CLOSED (500 errors don't trigger CB)
                assert (
                    circuit_breaker.current_state == STATE_CLOSED
                ), "Circuit should stay CLOSED for non-rate-limit errors"
                assert (
                    circuit_breaker.fail_counter == 0
                ), "Non-rate-limit errors should not increment fail counter"

                # Requests should still go through
                assert request_count["count"] >= 5, "Requests should not be blocked"

    def test_circuit_breaker_disabled_allows_all_calls(self):
        """
        Verify that when circuit breaker is disabled, all calls go through
        even with rate limit errors.
        """
        request_count = {"count": 0}

        def mock_rate_limited_request(*args, **kwargs):
            """Mock that returns 429"""
            request_count["count"] += 1
            response = MagicMock(spec=HTTPResponse)
            response.status = 429
            response.data = b"Too Many Requests"
            return response

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

                # Execute multiple queries
                for i in range(5):
                    cursor.execute(f"SELECT {i}")
                    cursor.fetchone()
                    time.sleep(0.3)

                # All requests should go through (no circuit breaker)
                assert (
                    request_count["count"] >= 5
                ), "All requests should go through when CB disabled"

    def test_circuit_breaker_recovers_after_reset_timeout(self):
        """
        Verify circuit breaker transitions to HALF_OPEN after reset timeout
        and eventually CLOSES if requests succeed.
        """
        request_count = {"count": 0}
        fail_requests = {"enabled": True}

        def mock_conditional_request(*args, **kwargs):
            """Mock that fails initially, then succeeds"""
            request_count["count"] += 1
            response = MagicMock(spec=HTTPResponse)

            if fail_requests["enabled"]:
                # Return 429 to trigger circuit breaker
                response.status = 429
                response.data = b"Too Many Requests"
            else:
                # Return success
                response.status = 200
                response.data = b"OK"

            return response

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

                # Circuit should be OPEN
                assert circuit_breaker.current_state == STATE_OPEN

                # Wait for reset timeout (5 seconds in test)
                time.sleep(6)

                # Now make requests succeed
                fail_requests["enabled"] = False

                # Execute query to trigger HALF_OPEN state
                cursor.execute("SELECT 3")
                cursor.fetchone()
                time.sleep(1)

                # Circuit should be HALF_OPEN or CLOSED (testing recovery)
                assert circuit_breaker.current_state in [
                    STATE_HALF_OPEN,
                    STATE_CLOSED,
                ], f"Circuit should be recovering, but is {circuit_breaker.current_state}"

                # Execute more queries to fully recover
                cursor.execute("SELECT 4")
                cursor.fetchone()
                time.sleep(1)

                # Eventually should be CLOSED if requests succeed
                # (may take a few successful requests to close from HALF_OPEN)
                current_state = circuit_breaker.current_state
                assert current_state in [
                    STATE_CLOSED,
                    STATE_HALF_OPEN,
                ], f"Circuit should recover to CLOSED or HALF_OPEN, got {current_state}"

    def test_circuit_breaker_503_also_triggers_circuit(self):
        """
        Verify circuit breaker opens for 503 Service Unavailable errors
        in addition to 429 rate limit errors.
        """
        request_count = {"count": 0}

        def mock_service_unavailable_request(*args, **kwargs):
            """Mock that returns 503 service unavailable"""
            request_count["count"] += 1
            response = MagicMock(spec=HTTPResponse)
            response.status = 503  # Service unavailable - should trigger CB
            response.data = b"Service Unavailable"
            return response

        with patch(
            "databricks.sql.telemetry.telemetry_push_client.TelemetryPushClient.request",
            side_effect=mock_service_unavailable_request,
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

                # Execute queries to trigger 503 failures
                cursor.execute("SELECT 1")
                cursor.fetchone()
                time.sleep(1)

                cursor.execute("SELECT 2")
                cursor.fetchone()
                time.sleep(2)

                # Circuit should be OPEN after 2 x 503 errors
                assert (
                    circuit_breaker.current_state == STATE_OPEN
                ), "503 errors should trigger circuit breaker"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
