"""
E2E test for telemetry - verifies telemetry behavior with different scenarios
"""
import threading
import logging
from contextlib import contextmanager
from unittest.mock import patch
import pytest

import databricks.sql as sql
from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    TelemetryClientFactory,
)

log = logging.getLogger(__name__)


class TelemetryTestBase:
    """Simplified test base class for telemetry e2e tests"""

    @pytest.fixture(autouse=True)
    def get_details(self, connection_details):
        self.arguments = connection_details.copy()

    def connection_params(self):
        return {
            "server_hostname": self.arguments["host"],
            "http_path": self.arguments["http_path"],
            "access_token": self.arguments.get("access_token"),
        }

    @contextmanager
    def connection(self, extra_params=()):
        connection_params = dict(self.connection_params(), **dict(extra_params))
        log.info("Connecting with args: {}".format(connection_params))
        conn = sql.connect(**connection_params)
        try:
            yield conn
        finally:
            conn.close()


@pytest.mark.serial
@pytest.mark.xdist_group(name="serial_telemetry")
class TestTelemetryE2E(TelemetryTestBase):
    """E2E tests for telemetry scenarios - must run serially due to shared host-level telemetry client"""

    @pytest.fixture(autouse=True)
    def telemetry_setup_teardown(self):
        """Clean up telemetry client state before and after each test"""
        # Clean up BEFORE test starts
        # Use wait=True to ensure all pending telemetry from previous tests completes
        if TelemetryClientFactory._executor:
            TelemetryClientFactory._executor.shutdown(wait=True)  # WAIT for pending telemetry
            TelemetryClientFactory._executor = None
        TelemetryClientFactory._stop_flush_thread()
        TelemetryClientFactory._flush_event.clear()  # Clear the event flag
        TelemetryClientFactory._clients.clear()
        TelemetryClientFactory._initialized = False

        # Clear feature flags cache before test starts
        from databricks.sql.common.feature_flag import FeatureFlagsContextFactory
        with FeatureFlagsContextFactory._lock:
            FeatureFlagsContextFactory._context_map.clear()
            if FeatureFlagsContextFactory._executor:
                FeatureFlagsContextFactory._executor.shutdown(wait=False)
                FeatureFlagsContextFactory._executor = None

        try:
            yield
        finally:
            # Clean up AFTER test ends
            # Use wait=True to ensure this test's telemetry completes
            if TelemetryClientFactory._executor:
                TelemetryClientFactory._executor.shutdown(wait=True)  # WAIT for this test's telemetry
                TelemetryClientFactory._executor = None
            TelemetryClientFactory._stop_flush_thread()
            TelemetryClientFactory._flush_event.clear()  # Clear the event flag
            TelemetryClientFactory._clients.clear()
            TelemetryClientFactory._initialized = False

            # Clear feature flags cache after test ends
            with FeatureFlagsContextFactory._lock:
                FeatureFlagsContextFactory._context_map.clear()
                if FeatureFlagsContextFactory._executor:
                    FeatureFlagsContextFactory._executor.shutdown(wait=False)
                    FeatureFlagsContextFactory._executor = None

    @pytest.fixture
    def telemetry_interceptors(self):
        """Setup reusable telemetry interceptors as a fixture.

        Captures two signals:
          * captured_events — every call to TelemetryClient._export_event
            (the connector intended to send this event).
          * captured_submissions — every call to TelemetryClient._send_telemetry
            (the connector submitted this batch to the executor).

        We deliberately do NOT capture inside _telemetry_request_callback. That
        callback only fires after the HTTP round-trip completes, and the
        connector's TelemetryClientFactory.close() shuts down the shared
        executor with wait=False — so on the last connection close, in-flight
        futures may never get to run their callbacks before the executor is
        gone. That's intentional for connection-close latency in production,
        but it means a callback-time assertion is racing the connector's
        shutdown path. Capturing at submission time tests what the connector
        actually controls.
        """
        capture_lock = threading.Lock()
        captured_events = []
        captured_submissions = []

        original_export = TelemetryClient._export_event
        original_send = TelemetryClient._send_telemetry

        def export_wrapper(self_client, event):
            with capture_lock:
                captured_events.append(event)
            return original_export(self_client, event)

        def send_wrapper(self_client, events):
            with capture_lock:
                # Record the batch (list of events) the connector submitted.
                captured_submissions.append(list(events))
            return original_send(self_client, events)

        return captured_events, captured_submissions, export_wrapper, send_wrapper

    # ==================== ASSERTION HELPERS ====================

    def assert_system_config(self, event):
        """Assert system configuration fields"""
        sys_config = event.entry.sql_driver_log.system_configuration
        assert sys_config is not None

        # Check all required fields are non-empty
        for field in ['driver_name', 'driver_version', 'os_name', 'os_version', 
                      'os_arch', 'runtime_name', 'runtime_version', 'runtime_vendor',
                      'locale_name', 'char_set_encoding']:
            value = getattr(sys_config, field)
            assert value and len(value) > 0, f"{field} should not be None or empty"
        
        assert sys_config.driver_name == "Databricks SQL Python Connector"

    def assert_connection_params(self, event, expected_http_path=None):
        """Assert connection parameters"""
        conn_params = event.entry.sql_driver_log.driver_connection_params
        assert conn_params is not None
        assert conn_params.http_path
        assert conn_params.host_info is not None
        assert conn_params.auth_mech is not None
        
        if expected_http_path:
            assert conn_params.http_path == expected_http_path
        
        if conn_params.socket_timeout is not None:
            assert conn_params.socket_timeout > 0

    def assert_statement_execution(self, event):
        """Assert statement execution details"""
        sql_op = event.entry.sql_driver_log.sql_operation
        assert sql_op is not None
        assert sql_op.statement_type is not None
        assert sql_op.execution_result is not None
        assert hasattr(sql_op, "retry_count")
        
        if sql_op.retry_count is not None:
            assert sql_op.retry_count >= 0

        latency = event.entry.sql_driver_log.operation_latency_ms
        assert latency is not None and latency >= 0

    def assert_error_info(self, event, expected_error_name=None):
        """Assert error information"""
        error_info = event.entry.sql_driver_log.error_info
        assert error_info is not None
        assert error_info.error_name and len(error_info.error_name) > 0
        assert error_info.stack_trace and len(error_info.stack_trace) > 0
        
        if expected_error_name:
            assert error_info.error_name == expected_error_name

    def verify_events(self, captured_events, captured_submissions, expected_count):
        """Common verification for event count and submission count.

        Asserts on what the connector did — exported events and submitted
        batches — not on what the server returned. Server-side HTTP success
        is asserted via end-to-end behavior elsewhere (and would race the
        connector's wait=False executor shutdown on connection close, see
        the docstring on telemetry_interceptors).

        Because these tests use telemetry_batch_size=1, each exported event
        triggers its own batch submission, so the submission count should
        equal the event count.
        """
        if expected_count == 0:
            assert len(captured_events) == 0, \
                f"Expected 0 events, got {len(captured_events)}"
            assert len(captured_submissions) == 0, \
                f"Expected 0 submissions, got {len(captured_submissions)}"
        else:
            assert len(captured_events) == expected_count, \
                f"Expected {expected_count} events, got {len(captured_events)}"
            # batch_size=1, so one submission per event.
            submitted_event_count = sum(len(batch) for batch in captured_submissions)
            assert submitted_event_count == expected_count, \
                f"Expected {expected_count} submitted events across batches, " \
                f"got {submitted_event_count} (batches: {[len(b) for b in captured_submissions]})"

            # Assert common fields for all events
            for event in captured_events:
                self.assert_system_config(event)
                self.assert_connection_params(event, self.arguments["http_path"])

    # ==================== PARAMETERIZED TESTS ====================

    @pytest.mark.parametrize("enable_telemetry,force_enable,expected_count,test_id", [
        (True, False, 2, "enable_on_force_off"),
        (False, True, 2, "enable_off_force_on"),
        (False, False, 0, "both_off"),
        (None, None, 2, "default_behavior"),
    ])
    def test_telemetry_flags(self, telemetry_interceptors, enable_telemetry, 
                            force_enable, expected_count, test_id):
        """Test telemetry behavior with different flag combinations"""
        captured_events, captured_submissions, export_wrapper, send_wrapper = \
            telemetry_interceptors

        with patch.object(TelemetryClient, "_export_event", export_wrapper), \
             patch.object(TelemetryClient, "_send_telemetry", send_wrapper):
            
            extra_params = {"telemetry_batch_size": 1}
            if enable_telemetry is not None:
                extra_params["enable_telemetry"] = enable_telemetry
            if force_enable is not None:
                extra_params["force_enable_telemetry"] = force_enable

            with self.connection(extra_params=extra_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

            self.verify_events(captured_events, captured_submissions, expected_count)
            
            # Assert statement execution on latency event (if events exist)
            if expected_count > 0:
                self.assert_statement_execution(captured_events[-1])

    @pytest.mark.parametrize("query,expected_error", [
        ("SELECT * FROM WHERE INVALID SYNTAX 12345", "ServerOperationError"),
        ("SELECT * FROM non_existent_table_xyz_12345", None),
    ])
    def test_sql_errors(self, telemetry_interceptors, query, expected_error):
        """Test telemetry captures error information for different SQL errors"""
        captured_events, captured_submissions, export_wrapper, send_wrapper = \
            telemetry_interceptors

        with patch.object(TelemetryClient, "_export_event", export_wrapper), \
             patch.object(TelemetryClient, "_send_telemetry", send_wrapper):
            
            with self.connection(extra_params={
                "force_enable_telemetry": True,
                "telemetry_batch_size": 1,
            }) as conn:
                with conn.cursor() as cursor:
                    with pytest.raises(Exception):
                        cursor.execute(query)
                        cursor.fetchone()

            assert len(captured_events) >= 1

            # Find event with error_info
            error_event = next((e for e in captured_events 
                               if e.entry.sql_driver_log.error_info), None)
            assert error_event is not None

            self.assert_system_config(error_event)
            self.assert_connection_params(error_event, self.arguments["http_path"])
            self.assert_error_info(error_event, expected_error)

    def test_metadata_operation(self, telemetry_interceptors):
        """Test telemetry for metadata operations (getCatalogs)"""
        captured_events, captured_submissions, export_wrapper, send_wrapper = \
            telemetry_interceptors

        with patch.object(TelemetryClient, "_export_event", export_wrapper), \
             patch.object(TelemetryClient, "_send_telemetry", send_wrapper):
            
            with self.connection(extra_params={
                "force_enable_telemetry": True,
                "telemetry_batch_size": 1,
            }) as conn:
                with conn.cursor() as cursor:
                    catalogs = cursor.catalogs()
                    catalogs.fetchall()

            assert len(captured_events) >= 1
            for event in captured_events:
                self.assert_system_config(event)
                self.assert_connection_params(event, self.arguments["http_path"])

    def test_direct_results(self, telemetry_interceptors):
        """Test telemetry with direct results (use_cloud_fetch=False)"""
        captured_events, captured_submissions, export_wrapper, send_wrapper = \
            telemetry_interceptors

        with patch.object(TelemetryClient, "_export_event", export_wrapper), \
             patch.object(TelemetryClient, "_send_telemetry", send_wrapper):
            
            with self.connection(extra_params={
                "force_enable_telemetry": True,
                "telemetry_batch_size": 1,
                "use_cloud_fetch": False,
            }) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 100")
                    result = cursor.fetchall()
                    assert len(result) == 1 and result[0][0] == 100

            assert len(captured_events) >= 2
            for event in captured_events:
                self.assert_system_config(event)
                self.assert_connection_params(event, self.arguments["http_path"])
            
            self.assert_statement_execution(captured_events[-1])

    @pytest.mark.parametrize("close_type", [
        "context_manager",
        "explicit_cursor",
        "explicit_connection",
        "implicit_fetchall",
    ])
    def test_cloudfetch_with_different_close_patterns(self, telemetry_interceptors, 
                                                       close_type):
        """Test telemetry with cloud fetch using different resource closing patterns"""
        captured_events, captured_submissions, export_wrapper, send_wrapper = \
            telemetry_interceptors

        with patch.object(TelemetryClient, "_export_event", export_wrapper), \
             patch.object(TelemetryClient, "_send_telemetry", send_wrapper):
            
            if close_type == "explicit_connection":
                # Test explicit connection close
                conn = sql.connect(
                    **self.connection_params(),
                    force_enable_telemetry=True,
                    telemetry_batch_size=1,
                    use_cloud_fetch=True,
                )
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM range(1000)")
                result = cursor.fetchall()
                assert len(result) == 1000
                conn.close()
            else:
                # Other patterns use connection context manager
                with self.connection(extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                    "use_cloud_fetch": True,
                }) as conn:
                    if close_type == "context_manager":
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT * FROM range(1000)")
                            result = cursor.fetchall()
                            assert len(result) == 1000
                    
                    elif close_type == "explicit_cursor":
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM range(1000)")
                        result = cursor.fetchall()
                        assert len(result) == 1000
                        cursor.close()
                    
                    elif close_type == "implicit_fetchall":
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM range(1000)")
                        result = cursor.fetchall()
                        assert len(result) == 1000

            assert len(captured_events) >= 2
            for event in captured_events:
                self.assert_system_config(event)
                self.assert_connection_params(event, self.arguments["http_path"])
            
            self.assert_statement_execution(captured_events[-1])
