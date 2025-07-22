import threading
from unittest.mock import patch
import pytest

from databricks.sql.telemetry.telemetry_client import TelemetryClient, TelemetryClientFactory
from tests.e2e.test_driver import PySQLPytestTestCase

def run_in_threads(target, num_threads, pass_index=False):
    """Helper to run target function in multiple threads."""
    threads = [
        threading.Thread(target=target, args=(i,) if pass_index else ())
        for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class TestE2ETelemetry(PySQLPytestTestCase):
    
    @pytest.fixture(autouse=True)
    def telemetry_setup_teardown(self):
        """
        This fixture ensures the TelemetryClientFactory is in a clean state
        before each test and shuts it down afterward. Using a fixture makes
        this robust and automatic.
        """
        # --- SETUP ---
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
        TelemetryClientFactory._clients.clear()
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False

        yield  # This is where the test runs

        # --- TEARDOWN ---
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)
            TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False

    def test_concurrent_queries_sends_telemetry(self):
        """
        An E2E test where concurrent threads execute real queries against
        the staging endpoint, while we capture and verify the generated telemetry.
        """
        num_threads = 5
        captured_telemetry = []
        captured_telemetry_lock = threading.Lock()

        original_send_telemetry = TelemetryClient._send_telemetry

        def send_telemetry_wrapper(self_client, events):
            with captured_telemetry_lock:
                captured_telemetry.extend(events)
            original_send_telemetry(self_client, events)

        with patch.object(TelemetryClient, "_send_telemetry", send_telemetry_wrapper):

            def execute_query_worker(thread_id):
                """Each thread creates a connection and executes a query."""
                with self.connection(extra_params={"enable_telemetry": True}) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"SELECT {thread_id}")
                        cursor.fetchall()

            # Run the workers concurrently
            run_in_threads(execute_query_worker, num_threads, pass_index=True)

            if TelemetryClientFactory._executor:
                TelemetryClientFactory._executor.shutdown(wait=True)

            # --- VERIFICATION ---
            assert len(captured_telemetry) == num_threads * 2 # 2 events per thread (initial_telemetry_log, latency_log (execute))

            events_with_latency = [
                e for e in captured_telemetry
                if e.entry.sql_driver_log.operation_latency_ms is not None and e.entry.sql_driver_log.sql_statement_id is not None
            ]
            assert len(events_with_latency) == num_threads # 1 event per thread (execute)