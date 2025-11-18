"""
E2E test for telemetry - verifies telemetry successfully sends to backend
"""
import time
import json
import threading
from unittest.mock import patch
import pytest
from concurrent.futures import wait

from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    TelemetryClientFactory,
)
from tests.e2e.test_driver import PySQLPytestTestCase


class TestTelemetryE2E(PySQLPytestTestCase):
    """E2E tests for telemetry"""

    @pytest.fixture(autouse=True)
    def telemetry_setup_teardown(self):
        """
        This fixture ensures the TelemetryClientFactory is in a clean state
        before each test and shuts it down afterward.
        """
        try:
            yield
        finally:
            if TelemetryClientFactory._executor:
                TelemetryClientFactory._executor.shutdown(wait=True)
                TelemetryClientFactory._executor = None
            TelemetryClientFactory._stop_flush_thread()
            TelemetryClientFactory._initialized = False

    def test_telemetry_sends_successfully_with_200_response(self):
        """
        E2E test to verify telemetry successfully sends to backend and receives 200 response.

        This test:
        1. Enables telemetry with force_enable_telemetry
        2. Sets telemetry_batch_size=1 for immediate flushing
        3. Executes a simple query
        4. Captures the telemetry response
        5. Verifies response status is 200 (success)

        With batch_size=1, telemetry is sent immediately after each event.
        """
        capture_lock = threading.Lock()
        captured_futures = []

        # Store original callback
        original_callback = TelemetryClient._telemetry_request_callback

        def callback_wrapper(self_client, future, sent_count):
            """
            Wraps the original callback to capture the server's response.
            """
            with capture_lock:
                captured_futures.append(future)
            original_callback(self_client, future, sent_count)

        with patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            # Execute a query with telemetry enabled and batch_size=1
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,  # Immediate flushing for test
                }
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    assert result[0] == 1

            # Wait for telemetry to complete (max 30 seconds)
            # With batch_size=1, we expect 2 events: initial_log and latency_log
            timeout_seconds = 30
            start_time = time.time()
            expected_event_count = 2  # initial_log + latency_log

            while (
                len(captured_futures) < expected_event_count
                and time.time() - start_time < timeout_seconds
            ):
                time.sleep(0.1)

            # Wait for all futures to complete
            done, not_done = wait(captured_futures, timeout=timeout_seconds)
            assert (
                not not_done
            ), f"Telemetry requests timed out: {len(not_done)} still pending"

            # Verify all responses are successful (status 200)
            captured_exceptions = []
            captured_responses = []

            for future in done:
                try:
                    response = future.result()

                    # Verify status is 200
                    assert (
                        200 <= response.status < 300
                    ), f"Expected 2xx status, got {response.status}"

                    # Parse JSON response
                    response_data = (
                        json.loads(response.data.decode()) if response.data else {}
                    )
                    captured_responses.append(response_data)

                except Exception as e:
                    captured_exceptions.append(e)

            # Assert no exceptions occurred
            assert (
                not captured_exceptions
            ), f"Telemetry requests failed with exceptions: {captured_exceptions}"

            # Assert we got responses
            assert len(captured_responses) > 0, "No telemetry responses received"

            # Verify response structure
            for response in captured_responses:
                # Should not have errors
                assert (
                    "errors" not in response or not response["errors"]
                ), f"Telemetry response contained errors: {response.get('errors')}"

    def test_telemetry_does_not_break_driver_on_query_execution(self):
        """
        E2E test to verify telemetry doesn't break driver functionality.

        This is a simpler test that just ensures:
        1. Driver works fine with telemetry enabled
        2. Query executes successfully
        3. Results are returned correctly

        If telemetry has issues, they're logged but don't break the driver.
        """
        with self.connection(
            extra_params={
                "force_enable_telemetry": True,
                "telemetry_batch_size": 1,  # Immediate flushing for test
            }
        ) as conn:
            with conn.cursor() as cursor:
                # Execute a simple query
                cursor.execute("SELECT 1 as col1, 'test' as col2")
                result = cursor.fetchone()

                # Verify query worked correctly
                assert result[0] == 1
                assert result[1] == "test"

                # Execute another query to generate more telemetry
                cursor.execute("SELECT 42")
                result = cursor.fetchone()
                assert result[0] == 42

        # Test passes = telemetry didn't break driver ✅
