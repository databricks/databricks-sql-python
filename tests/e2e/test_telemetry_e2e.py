"""
E2E test for telemetry - verifies telemetry behavior with different scenarios
"""
import time
import json
import threading
import logging
from contextlib import contextmanager
from unittest.mock import patch
import pytest
from concurrent.futures import wait

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


class TestTelemetryE2E(TelemetryTestBase):
    """E2E tests for telemetry scenarios"""

    @pytest.fixture(autouse=True)
    def telemetry_setup_teardown(self):
        """Clean up telemetry client state before and after each test"""
        try:
            yield
        finally:
            if TelemetryClientFactory._executor:
                TelemetryClientFactory._executor.shutdown(wait=True)
                TelemetryClientFactory._executor = None
            TelemetryClientFactory._stop_flush_thread()
            TelemetryClientFactory._initialized = False

    # ==================== REUSABLE WRAPPER METHODS ====================

    @pytest.fixture
    def telemetry_interceptors(self):
        """
        Setup reusable telemetry interceptors as a fixture.

        Returns:
            Tuple of (captured_events, captured_futures, export_wrapper, callback_wrapper)
        """
        capture_lock = threading.Lock()
        captured_events = []
        captured_futures = []

        original_export = TelemetryClient._export_event
        original_callback = TelemetryClient._telemetry_request_callback

        def export_wrapper(self_client, event):
            """Intercept telemetry events to capture payload"""
            with capture_lock:
                captured_events.append(event)
            return original_export(self_client, event)

        def callback_wrapper(self_client, future, sent_count):
            """Capture HTTP response futures"""
            with capture_lock:
                captured_futures.append(future)
            original_callback(self_client, future, sent_count)

        return captured_events, captured_futures, export_wrapper, callback_wrapper

    # ==================== ASSERTION METHODS ====================

    def assertSystemConfiguration(self, event):
        """Assert base system configuration fields"""
        sys_config = event.entry.sql_driver_log.system_configuration

        assert sys_config is not None, "system_configuration should not be None"

        # Driver information
        assert (
            sys_config.driver_name == "Databricks SQL Python Connector"
        ), f"Expected driver_name 'Databricks SQL Python Connector', got '{sys_config.driver_name}'"
        assert (
            sys_config.driver_version is not None and len(sys_config.driver_version) > 0
        ), "driver_version should not be None or empty"

        # OS information
        assert (
            sys_config.os_name is not None and len(sys_config.os_name) > 0
        ), "os_name should not be None or empty"
        assert (
            sys_config.os_version is not None and len(sys_config.os_version) > 0
        ), "os_version should not be None or empty"
        assert (
            sys_config.os_arch is not None and len(sys_config.os_arch) > 0
        ), "os_arch should not be None or empty"

        # Runtime information
        assert (
            sys_config.runtime_name is not None and len(sys_config.runtime_name) > 0
        ), "runtime_name should not be None or empty"
        assert (
            sys_config.runtime_version is not None
            and len(sys_config.runtime_version) > 0
        ), "runtime_version should not be None or empty"
        assert (
            sys_config.runtime_vendor is not None and len(sys_config.runtime_vendor) > 0
        ), "runtime_vendor should not be None or empty"

        # Locale and encoding
        assert (
            sys_config.locale_name is not None and len(sys_config.locale_name) > 0
        ), "locale_name should not be None or empty"
        assert (
            sys_config.char_set_encoding is not None
            and len(sys_config.char_set_encoding) > 0
        ), "char_set_encoding should not be None or empty"

    def assertConnectionParams(
        self,
        event,
        expected_http_path=None,
        expected_mode=None,
        expected_auth_mech=None,
        expected_auth_flow=None,
    ):
        """Assert connection parameters"""
        conn_params = event.entry.sql_driver_log.driver_connection_params

        assert conn_params is not None, "driver_connection_params should not be None"

        # HTTP Path
        if expected_http_path:
            assert (
                conn_params.http_path == expected_http_path
            ), f"Expected http_path '{expected_http_path}', got '{conn_params.http_path}'"
        assert (
            conn_params.http_path is not None and len(conn_params.http_path) > 0
        ), "http_path should not be None or empty"

        # Mode
        if expected_mode:
            assert (
                conn_params.mode == expected_mode
            ), f"Expected mode '{expected_mode}', got '{conn_params.mode}'"
        # Mode is optional, so don't assert it must exist

        # Host Info (HostDetails object)
        assert conn_params.host_info is not None, "host_info should not be None"

        # Auth Mechanism (AuthMech object)
        if expected_auth_mech:
            assert (
                conn_params.auth_mech == expected_auth_mech
            ), f"Expected auth_mech '{expected_auth_mech}', got '{conn_params.auth_mech}'"
        assert conn_params.auth_mech is not None, "auth_mech should not be None"

        # Auth Flow (optional string)
        if expected_auth_flow:
            assert (
                conn_params.auth_flow == expected_auth_flow
            ), f"Expected auth_flow '{expected_auth_flow}', got '{conn_params.auth_flow}'"
        # auth_flow is optional, so don't assert it must exist

        # Socket Timeout
        # socket_timeout is optional and can be None
        if conn_params.socket_timeout is not None:
            assert (
                conn_params.socket_timeout > 0
            ), f"socket_timeout should be positive, got {conn_params.socket_timeout}"

    def assertStatementExecution(
        self, event, statement_type=None, execution_result=None
    ):
        """Assert statement execution details including operation latency"""
        sql_op = event.entry.sql_driver_log.sql_operation

        assert sql_op is not None, "sql_operation should not be None for SQL execution"

        # Statement Type
        if statement_type:
            assert (
                sql_op.statement_type == statement_type
            ), f"Expected statement_type '{statement_type}', got '{sql_op.statement_type}'"
        else:
            # At minimum, statement_type should exist
            assert (
                sql_op.statement_type is not None
            ), "statement_type should not be None"

        # Execution Result
        if execution_result:
            assert (
                sql_op.execution_result == execution_result
            ), f"Expected execution_result '{execution_result}', got '{sql_op.execution_result}'"
        else:
            # At minimum, execution_result should exist
            assert (
                sql_op.execution_result is not None
            ), "execution_result should not be None"

        # Retry Count
        assert hasattr(sql_op, "retry_count"), "sql_operation should have retry_count"
        if sql_op.retry_count is not None:
            assert (
                sql_op.retry_count >= 0
            ), f"retry_count should be non-negative, got {sql_op.retry_count}"

        # Operation Latency
        latency = event.entry.sql_driver_log.operation_latency_ms
        assert latency is not None, "operation_latency_ms should not be None"
        assert (
            latency >= 0
        ), f"operation_latency_ms should be non-negative, got {latency}"

    def assertErrorInfo(self, event, expected_error_name=None):
        """Assert error information"""
        error_info = event.entry.sql_driver_log.error_info

        assert error_info is not None, "error_info should not be None for error events"

        # Error Name
        assert (
            error_info.error_name is not None and len(error_info.error_name) > 0
        ), "error_name should not be None or empty"
        if expected_error_name:
            assert (
                error_info.error_name == expected_error_name
            ), f"Expected error_name '{expected_error_name}', got '{error_info.error_name}'"

        # Stack Trace
        assert (
            error_info.stack_trace is not None and len(error_info.stack_trace) > 0
        ), "stack_trace should not be None or empty"

    def assertOperationLatency(self, event):
        """Assert operation latency exists"""
        latency = event.entry.sql_driver_log.operation_latency_ms
        assert latency is not None, "operation_latency_ms should not be None"
        assert latency >= 0, "operation_latency_ms should be non-negative"

    def assertBaseTelemetryEvent(self, captured_events):
        """Assert basic telemetry event payload fields"""
        assert len(captured_events) > 0, "No events captured to assert"

        for event in captured_events:
            telemetry_event = event.entry.sql_driver_log
            assert telemetry_event.session_id is not None

    # ==================== TEST SCENARIOS ====================

    def test_enable_telemetry_on_with_server_on_sends_events(
        self, telemetry_interceptors
    ):
        """
        Scenario: enable_telemetry=ON, force_enable_telemetry=OFF, server=ON
        Expected: 2 events (initial_log + latency_log)
        """
        from databricks.sql.telemetry.telemetry_client import TelemetryHelper

        print(f"\n{'='*80}")
        print(f"TEST: test_enable_telemetry_on_with_server_on_sends_events")
        print(
            f"Feature flag being checked: {TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME}"
        )
        print(f"{'='*80}\n")

        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "enable_telemetry": True,
                    "force_enable_telemetry": False,
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                print(f"\nConnection created:")
                print(f"  enable_telemetry: {conn.enable_telemetry}")
                print(f"  force_enable_telemetry: {conn.force_enable_telemetry}")
                print(f"  telemetry_enabled (computed): {conn.telemetry_enabled}")
                print(
                    f"  telemetry_client type: {type(conn._telemetry_client).__name__}\n"
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    statement_id = cursor.query_id

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Exact count assertion
            assert (
                len(captured_events) == 2
            ), f"Expected exactly 2 events, got {len(captured_events)}"
            assert len(done) == 2, f"Expected exactly 2 responses, got {len(done)}"

            # Verify HTTP responses
            for future in done:
                response = future.result()
                assert 200 <= response.status < 300

            # Assert payload for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert latency event (second event)
            self.assertStatementExecution(captured_events[1])

            print(f"\nStatement ID: {statement_id}")

    def test_force_enable_on_with_enable_off_sends_events(self, telemetry_interceptors):
        """
        Scenario: enable_telemetry=OFF, force_enable_telemetry=ON, server=ON
        Expected: 2 events (initial_log + latency_log)
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "enable_telemetry": False,
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 2")
                    cursor.fetchone()
                    statement_id = cursor.query_id

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Exact count assertion
            assert (
                len(captured_events) == 2
            ), f"Expected exactly 2 events, got {len(captured_events)}"
            assert len(done) == 2, f"Expected exactly 2 responses, got {len(done)}"

            # Verify HTTP responses
            for future in done:
                response = future.result()
                assert 200 <= response.status < 300

            # Assert payload
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            self.assertStatementExecution(captured_events[1])

            print(f"\nStatement ID: {statement_id}")

    def test_both_flags_off_does_not_send_events(self, telemetry_interceptors):
        """
        Scenario: enable_telemetry=OFF, force_enable_telemetry=OFF, server=ON
        Expected: 0 events
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "enable_telemetry": False,
                    "force_enable_telemetry": False,
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 3")
                    cursor.fetchone()
                    statement_id = cursor.query_id

            time.sleep(2)

            # Exact count assertion
            assert (
                len(captured_events) == 0
            ), f"Expected 0 events, got {len(captured_events)}"
            assert (
                len(captured_futures) == 0
            ), f"Expected 0 responses, got {len(captured_futures)}"

            print(f"\nStatement ID: {statement_id}")

    def test_default_behavior_sends_events_with_server_flag_on(
        self, telemetry_interceptors
    ):
        """
        Scenario: Neither enable_telemetry nor force_enable_telemetry passed (uses defaults)
        Expected: 2 events (initial_log + latency_log) when server feature flag is ON

        Default behavior:
        - enable_telemetry defaults to True
        - force_enable_telemetry defaults to False
        - Telemetry will be sent if server feature flag is enabled
        """
        from databricks.sql.telemetry.telemetry_client import TelemetryHelper

        print(f"\n{'='*80}")
        print(f"TEST: test_default_behavior_sends_events_with_server_flag_on")
        print(
            f"Feature flag being checked: {TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME}"
        )
        print(f"Testing DEFAULT behavior (no flags passed explicitly)")
        print(f"{'='*80}\n")

        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            # Connection without explicit telemetry flags - relies on defaults
            with self.connection(
                extra_params={
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                # Verify defaults are as expected
                print(f"\nConnection created with DEFAULT settings:")
                print(f"  enable_telemetry (default): {conn.enable_telemetry}")
                print(
                    f"  force_enable_telemetry (default): {conn.force_enable_telemetry}"
                )
                print(f"  telemetry_enabled (computed): {conn.telemetry_enabled}")
                print(
                    f"  telemetry_client type: {type(conn._telemetry_client).__name__}\n"
                )

                with conn.cursor() as cursor:
                    cursor.execute("SELECT 99")
                    cursor.fetchone()
                    statement_id = cursor.query_id

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # With default enable_telemetry=True and server flag ON, expect 2 events
            assert (
                len(captured_events) == 2
            ), f"Expected exactly 2 events with default settings, got {len(captured_events)}"
            assert len(done) == 2, f"Expected exactly 2 responses, got {len(done)}"

            # Verify HTTP responses
            for future in done:
                response = future.result()
                assert 200 <= response.status < 300

            # Assert payload for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert latency event (second event)
            self.assertStatementExecution(captured_events[1])

            print(f"\nStatement ID: {statement_id}")

    def test_sql_error_sends_telemetry_with_error_info(self, telemetry_interceptors):
        """
        Scenario: SQL query with invalid syntax causes error
        Expected: Telemetry event with error_name and stack_trace
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                with conn.cursor() as cursor:
                    # Execute query with invalid syntax to trigger immediate parse error
                    try:
                        cursor.execute("SELECT * FROM WHERE INVALID SYNTAX 12345")
                        cursor.fetchone()
                        assert False, "Query should have failed"
                    except Exception as e:
                        # Expected to fail
                        print(f"\nExpected error occurred: {type(e).__name__}")

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 1 event
            assert (
                len(captured_events) >= 1
            ), f"Expected at least 1 event, got {len(captured_events)}"

            print(f"\nCaptured {len(captured_events)} events")

            # Find event with error_info (typically the middle event)
            error_event = None
            for idx, event in enumerate(captured_events):
                error_info = event.entry.sql_driver_log.error_info
                if error_info:
                    error_event = event
                    print(f"\nFound error_info in event {idx}")
                    break

            assert (
                error_event is not None
            ), "Expected at least one event with error_info"

            # Assert system configuration
            self.assertSystemConfiguration(error_event)

            # Assert connection params
            self.assertConnectionParams(
                error_event, expected_http_path=self.arguments["http_path"]
            )

            # Assert error info with ServerOperationError
            self.assertErrorInfo(
                error_event, expected_error_name="ServerOperationError"
            )

            print(
                f"✅ Error telemetry successfully captured with error_name and stack_trace"
            )

    def test_non_existent_table_error_sends_telemetry(self, telemetry_interceptors):
        """
        Scenario: SQL query on non-existent table causes error
        Expected: Telemetry event with error_name and stack_trace
        Note: This test checks timing - querying non-existent table vs invalid syntax
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                with conn.cursor() as cursor:
                    # Execute query on non-existent table
                    try:
                        cursor.execute("SELECT * FROM non_existent_table_xyz_12345")
                        cursor.fetchone()
                        assert False, "Query should have failed"
                    except Exception as e:
                        # Expected to fail
                        print(f"\nExpected error occurred: {type(e).__name__}")

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 1 event
            assert (
                len(captured_events) >= 1
            ), f"Expected at least 1 event, got {len(captured_events)}"

            print(f"\nCaptured {len(captured_events)} events")

            # Find event with error_info
            error_event = None
            for idx, event in enumerate(captured_events):
                error_info = event.entry.sql_driver_log.error_info
                if error_info:
                    error_event = event
                    print(f"\nFound error_info in event {idx}")
                    break

            assert (
                error_event is not None
            ), "Expected at least one event with error_info"

            # Assert system configuration
            self.assertSystemConfiguration(error_event)

            # Assert connection params
            self.assertConnectionParams(
                error_event, expected_http_path=self.arguments["http_path"]
            )

            # Assert error info exists
            self.assertErrorInfo(error_event)

            print(f"✅ Non-existent table error telemetry captured")

    def test_metadata_get_catalogs_sends_telemetry(self, telemetry_interceptors):
        """
        Scenario: Statement created and metadata (getCatalogs) called
        Expected: Telemetry events with system config and connection params
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                }
            ) as conn:
                with conn.cursor() as cursor:
                    # Call metadata operation
                    catalogs = cursor.catalogs()
                    catalogs.fetchall()

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 1 event
            assert (
                len(captured_events) >= 1
            ), f"Expected at least 1 event, got {len(captured_events)}"

            print(f"\nCaptured {len(captured_events)} events for getCatalogs")

            # Assert system configuration and connection params for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            print(f"✅ Metadata getCatalogs telemetry captured")

    def test_direct_results_sends_telemetry(self, telemetry_interceptors):
        """
        Scenario: ResultSet created with directResults (use_cloud_fetch=False)
        Expected: Telemetry events with SQL execution metrics
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                    "use_cloud_fetch": False,  # Force direct results
                }
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 100")
                    result = cursor.fetchall()
                    assert len(result) == 1
                    assert result[0][0] == 100

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 2 events (initial + latency)
            assert (
                len(captured_events) >= 2
            ), f"Expected at least 2 events, got {len(captured_events)}"

            print(f"\nCaptured {len(captured_events)} events for direct results")

            # Assert system configuration and connection params for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert SQL execution metrics on latency event (last event)
            latency_event = captured_events[-1]
            self.assertStatementExecution(latency_event)

            print(f"✅ Direct results telemetry captured")

    def test_cloudfetch_no_explicit_close_sends_telemetry(self, telemetry_interceptors):
        """
        Scenario: ResultSet created with cloudfetch, Statement/Connection not explicitly closed
        Expected: Telemetry events sent when context managers exit
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                    "use_cloud_fetch": True,  # Enable cloud fetch
                }
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM range(1000)")
                    result = cursor.fetchall()
                    assert len(result) == 1000
                    # Statement and connection close automatically via context managers

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 2 events (initial + latency)
            assert (
                len(captured_events) >= 2
            ), f"Expected at least 2 events, got {len(captured_events)}"

            print(
                f"\nCaptured {len(captured_events)} events for cloudfetch (auto close)"
            )

            # Assert system configuration and connection params for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert SQL execution metrics on latency event
            latency_event = captured_events[-1]
            self.assertStatementExecution(latency_event)

            print(f"✅ Cloudfetch (auto close) telemetry captured")

    def test_cloudfetch_statement_closed_sends_telemetry(self, telemetry_interceptors):
        """
        Scenario: ResultSet created with cloudfetch, Statement explicitly closed
        Expected: Telemetry events sent when statement closes
        Note: With batch_size=1, immediate flush. With larger batch, may need connection close.
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                    "use_cloud_fetch": True,
                }
            ) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM range(1000)")
                result = cursor.fetchall()
                assert len(result) == 1000
                cursor.close()  # Explicitly close statement

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 2 events (initial + latency)
            assert (
                len(captured_events) >= 2
            ), f"Expected at least 2 events, got {len(captured_events)}"

            print(
                f"\nCaptured {len(captured_events)} events for cloudfetch (statement close)"
            )

            # Assert system configuration and connection params for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert SQL execution metrics on latency event
            latency_event = captured_events[-1]
            self.assertStatementExecution(latency_event)

            print(f"✅ Cloudfetch (statement close) telemetry captured")

    def test_cloudfetch_connection_closed_sends_telemetry(self, telemetry_interceptors):
        """
        Scenario: ResultSet created with cloudfetch, Connection explicitly closed
        Expected: Telemetry events sent when connection closes (forces flush)
        Note: Connection.close() always flushes all pending telemetry events.
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
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
            conn.close()  # Explicitly close connection (forces flush)

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 2 events (initial + latency)
            assert (
                len(captured_events) >= 2
            ), f"Expected at least 2 events, got {len(captured_events)}"

            print(
                f"\nCaptured {len(captured_events)} events for cloudfetch (connection close)"
            )

            # Assert system configuration and connection params for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert SQL execution metrics on latency event
            latency_event = captured_events[-1]
            self.assertStatementExecution(latency_event)

            print(f"✅ Cloudfetch (connection close) telemetry captured")

    def test_cloudfetch_only_resultset_closed_sends_telemetry(
        self, telemetry_interceptors
    ):
        """
        Scenario: ResultSet created with cloudfetch, only ResultSet closed (implicit via fetchall)
        Expected: Telemetry events sent (batch_size=1 ensures immediate flush)
        Note: ResultSet closes after fetchall(). Events flush due to batch_size=1.
        """
        (
            captured_events,
            captured_futures,
            export_wrapper,
            callback_wrapper,
        ) = telemetry_interceptors

        with patch.object(
            TelemetryClient, "_export_event", export_wrapper
        ), patch.object(
            TelemetryClient, "_telemetry_request_callback", callback_wrapper
        ):
            with self.connection(
                extra_params={
                    "force_enable_telemetry": True,
                    "telemetry_batch_size": 1,
                    "use_cloud_fetch": True,
                }
            ) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM range(1000)")
                result = cursor.fetchall()  # ResultSet implicitly closed after fetchall
                assert len(result) == 1000
                # Don't explicitly close cursor or connection (context manager will)

            time.sleep(2)
            done, not_done = wait(captured_futures, timeout=10)

            # Should have at least 2 events (initial + latency)
            assert (
                len(captured_events) >= 2
            ), f"Expected at least 2 events, got {len(captured_events)}"

            print(
                f"\nCaptured {len(captured_events)} events for cloudfetch (resultset close)"
            )

            # Assert system configuration and connection params for all events
            for event in captured_events:
                self.assertSystemConfiguration(event)
                self.assertConnectionParams(
                    event, expected_http_path=self.arguments["http_path"]
                )

            # Assert SQL execution metrics on latency event
            latency_event = captured_events[-1]
            self.assertStatementExecution(latency_event)

            print(f"✅ Cloudfetch (resultset close) telemetry captured")
