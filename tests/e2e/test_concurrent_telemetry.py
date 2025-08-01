from concurrent.futures import wait
import random
import threading
import time
from unittest.mock import patch
import pytest

from databricks.sql.telemetry.models.enums import StatementType
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
        try:
            yield 
        finally:
            if TelemetryClientFactory._executor:
                TelemetryClientFactory._executor.shutdown(wait=True)
                TelemetryClientFactory._executor = None
            TelemetryClientFactory._stop_flush_thread()
            TelemetryClientFactory._initialized = False

    def test_concurrent_queries_sends_telemetry(self):
        """
        An E2E test where concurrent threads execute real queries against
        the staging endpoint, while we capture and verify the generated telemetry.
        """
        num_threads = 30
        capture_lock = threading.Lock()
        captured_telemetry = []
        captured_session_ids = []
        captured_statement_ids = []
        captured_futures = []

        original_send_telemetry = TelemetryClient._send_telemetry
        original_callback = TelemetryClient._telemetry_request_callback

        def send_telemetry_wrapper(self_client, events):
            with capture_lock:
                captured_telemetry.extend(events)
            original_send_telemetry(self_client, events)

        def callback_wrapper(self_client, future, sent_count):
            """
            Wraps the original callback to capture the server's response
            or any exceptions from the async network call.
            """
            with capture_lock:
                captured_futures.append(future)
            original_callback(self_client, future, sent_count)

        with patch.object(TelemetryClient, "_send_telemetry", send_telemetry_wrapper), \
             patch.object(TelemetryClient, "_telemetry_request_callback", callback_wrapper):

            def execute_query_worker(thread_id):
                """Each thread creates a connection and executes a query."""

                time.sleep(random.uniform(0, 0.05))
                
                with self.connection(extra_params={"enable_telemetry": True}) as conn:
                    # Capture the session ID from the connection before executing the query
                    session_id_hex = conn.get_session_id_hex()
                    with capture_lock:
                        captured_session_ids.append(session_id_hex)
                    
                    with conn.cursor() as cursor:
                        cursor.execute(f"SELECT {thread_id}")
                        # Capture the statement ID after executing the query
                        statement_id = cursor.query_id
                        with capture_lock:
                            captured_statement_ids.append(statement_id)
                        cursor.fetchall()

            # Run the workers concurrently
            run_in_threads(execute_query_worker, num_threads, pass_index=True)

            timeout_seconds = 60
            start_time = time.time()
            expected_event_count = num_threads

            while len(captured_futures) < expected_event_count and time.time() - start_time < timeout_seconds:
                time.sleep(0.1)

            done, not_done = wait(captured_futures, timeout=timeout_seconds)
            assert not not_done

            captured_exceptions = []
            captured_responses = []
            for future in done:
                try:
                    response = future.result()
                    response.raise_for_status()
                    captured_responses.append(response.json())
                except Exception as e:
                    captured_exceptions.append(e)

            assert not captured_exceptions
            assert len(captured_responses) > 0
            
            total_successful_events = 0
            for response in captured_responses:
                assert "errors" not in response or not response["errors"]
                if "numProtoSuccess" in response:
                    total_successful_events += response["numProtoSuccess"]
            assert total_successful_events == num_threads * 2

            assert len(captured_telemetry) == num_threads * 2  # 2 events per thread (initial_telemetry_log, latency_log (execute))
            assert len(captured_session_ids) == num_threads  # One session ID per thread
            assert len(captured_statement_ids) == num_threads  # One statement ID per thread (per query)

            # Separate initial logs from latency logs
            initial_logs = [
                e for e in captured_telemetry
                if e.entry.sql_driver_log.operation_latency_ms is None
                and e.entry.sql_driver_log.driver_connection_params is not None
                and e.entry.sql_driver_log.system_configuration is not None
            ]
            latency_logs = [
                e for e in captured_telemetry
                if e.entry.sql_driver_log.operation_latency_ms is not None 
                and e.entry.sql_driver_log.sql_statement_id is not None 
                and e.entry.sql_driver_log.sql_operation.statement_type == StatementType.QUERY
            ]

            # Verify counts
            assert len(initial_logs) == num_threads
            assert len(latency_logs) == num_threads

            # Verify that telemetry events contain the exact session IDs we captured from connections
            telemetry_session_ids = set()
            for event in captured_telemetry:
                session_id = event.entry.sql_driver_log.session_id
                assert session_id is not None
                telemetry_session_ids.add(session_id)

            captured_session_ids_set = set(captured_session_ids)
            assert telemetry_session_ids == captured_session_ids_set
            assert len(captured_session_ids_set) == num_threads

            # Verify that telemetry latency logs contain the exact statement IDs we captured from cursors
            telemetry_statement_ids = set()
            for event in latency_logs:
                statement_id = event.entry.sql_driver_log.sql_statement_id
                assert statement_id is not None
                telemetry_statement_ids.add(statement_id)

            captured_statement_ids_set = set(captured_statement_ids)
            assert telemetry_statement_ids == captured_statement_ids_set
            assert len(captured_statement_ids_set) == num_threads

            # Verify that each latency log has a statement ID from our captured set
            for event in latency_logs:
                log = event.entry.sql_driver_log
                assert log.sql_statement_id in captured_statement_ids
                assert log.session_id in captured_session_ids