"""
Integration tests for ResultHeartbeatManager wiring into ThriftResultSet.

These verify the lifecycle hooks (start in __init__, stop in close(), stop
when _fill_results_buffer flips has_more_rows to False) — not the manager
itself, which is covered in test_result_heartbeat_manager.py.
"""

import unittest
from unittest.mock import Mock, patch

from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.backend.types import (
    BackendType,
    CommandId,
    CommandState,
    ExecuteResponse,
)
from databricks.sql.result_set import ThriftResultSet
from databricks.sql.thrift_api.TCLIService import ttypes


def _make_thrift_command_id() -> CommandId:
    handle = ttypes.TOperationHandle(
        operationId=ttypes.THandleIdentifier(guid=b"a" * 16, secret=b"b" * 16),
        operationType=0,
        hasResultSet=True,
        modifiedRowCount=None,
    )
    return CommandId.from_thrift_handle(handle)


def _make_connection(enable_heartbeat=True, interval=60):
    conn = Mock()
    conn.enable_heartbeat = enable_heartbeat
    conn.heartbeat_interval_seconds = interval
    conn.heartbeat_request_timeout_seconds = 30
    conn.disable_pandas = False
    conn.open = True
    return conn


def _execute_response(*, has_been_closed_server_side=False):
    return ExecuteResponse(
        command_id=_make_thrift_command_id(),
        status=CommandState.SUCCEEDED,
        description=[("c", "int", None, None, None, None, None)],
        has_been_closed_server_side=has_been_closed_server_side,
        lz4_compressed=False,
        is_staging_operation=False,
        arrow_schema_bytes=None,
        result_format=None,
    )


def _make_backend():
    backend = Mock(spec=ThriftDatabricksClient)
    backend.fetch_results.return_value = (Mock(), True, 0)
    return backend


@patch(
    "databricks.sql.backend.thrift_result_heartbeat_manager." "ResultHeartbeatManager"
)
class ThriftResultSetHeartbeatWiringTest(unittest.TestCase):
    def _make_rs(
        self,
        *,
        has_more_rows=True,
        has_been_closed_server_side=False,
        enable_heartbeat=True,
        backend=None,
    ):
        backend = backend or _make_backend()
        # Provide a pre-built results queue so __init__ doesn't call
        # _fill_results_buffer() before we get to the eligibility check.
        results_queue = Mock()
        return (
            ThriftResultSet(
                connection=_make_connection(enable_heartbeat=enable_heartbeat),
                execute_response=_execute_response(
                    has_been_closed_server_side=has_been_closed_server_side
                ),
                thrift_client=backend,
                has_more_rows=has_more_rows,
            ),
            backend,
            results_queue,
        )

    def test_starts_when_has_more_rows_true(self, mgr_cls):
        rs, _, _ = self._make_rs()
        mgr_cls.assert_called_once()
        rs._heartbeat_manager.start.assert_called_once()

    def test_does_not_start_when_no_more_rows(self, mgr_cls):
        # When has_more_rows=False the eligibility check should short-circuit
        # BEFORE the initial _fill_results_buffer can flip anything.
        backend = _make_backend()
        backend.fetch_results.return_value = (Mock(), False, 0)
        rs, _, _ = self._make_rs(has_more_rows=False, backend=backend)
        mgr_cls.assert_not_called()
        self.assertIsNone(rs._heartbeat_manager)

    def test_does_not_start_when_closed_server_side(self, mgr_cls):
        rs, _, _ = self._make_rs(has_been_closed_server_side=True)
        mgr_cls.assert_not_called()

    def test_does_not_start_when_flag_disabled(self, mgr_cls):
        rs, _, _ = self._make_rs(enable_heartbeat=False)
        mgr_cls.assert_not_called()

    def test_close_stops_heartbeat_first(self, mgr_cls):
        rs, backend, _ = self._make_rs()
        manager = rs._heartbeat_manager
        self.assertIsNotNone(manager)

        rs.close()
        manager.stop.assert_called_once()
        self.assertIsNone(rs._heartbeat_manager)

    def test_close_twice_is_idempotent(self, mgr_cls):
        rs, _, _ = self._make_rs()
        manager = rs._heartbeat_manager

        rs.close()
        rs.close()
        manager.stop.assert_called_once()

    def test_fill_results_buffer_stops_when_server_finished(self, mgr_cls):
        backend = _make_backend()
        rs, _, _ = self._make_rs(backend=backend)
        manager = rs._heartbeat_manager

        # Server delivers its last batch and signals no more rows.
        backend.fetch_results.return_value = (Mock(), False, 0)
        rs._fill_results_buffer()

        manager.stop.assert_called_once()
        self.assertIsNone(rs._heartbeat_manager)

    def test_fill_results_buffer_keeps_running_when_more_rows(self, mgr_cls):
        backend = _make_backend()
        rs, _, _ = self._make_rs(backend=backend)
        manager = rs._heartbeat_manager

        backend.fetch_results.return_value = (Mock(), True, 0)
        rs._fill_results_buffer()

        manager.stop.assert_not_called()
        self.assertIs(rs._heartbeat_manager, manager)


if __name__ == "__main__":
    unittest.main()
