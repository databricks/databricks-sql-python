import gc
import re
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock, PropertyMock
import itertools
from decimal import Decimal
from datetime import datetime, date
from uuid import UUID

from databricks.sql.thrift_api.TCLIService.ttypes import (
    TOpenSessionResp,
    TExecuteStatementResp,
    TOperationHandle,
    THandleIdentifier,
    TOperationState,
    TOperationType,
    TOperationState,
)
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient

import databricks.sql
import databricks.sql.client as client
from databricks.sql import (
    InterfaceError,
    DatabaseError,
    Error,
    NotSupportedError,
    TransactionError,
)
from databricks.sql.types import Row
from databricks.sql.result_set import ResultSet, ThriftResultSet
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.backend.types import ExecuteResponse

from tests.unit.test_fetches import FetchTests
from tests.unit.test_thrift_backend import ThriftBackendTestSuite
from tests.unit.test_arrow_queue import ArrowQueueSuite


class ThriftDatabricksClientMockFactory:
    @classmethod
    def new(cls):
        ThriftBackendMock = Mock(spec=ThriftDatabricksClient)
        ThriftBackendMock.return_value = ThriftBackendMock

        mock_result_set = Mock(spec=ThriftResultSet)
        cls.apply_property_to_mock(
            mock_result_set,
            description=None,
            is_staging_operation=False,
            command_id=None,
            has_been_closed_server_side=True,
            has_more_rows=True,
            lz4_compressed=True,
            arrow_schema_bytes=b"schema",
        )

        ThriftBackendMock.execute_command.return_value = mock_result_set

        return ThriftBackendMock

    @classmethod
    def apply_property_to_mock(self, mock_obj, **kwargs):
        """
        Apply a property to a mock object.
        """

        for key, value in kwargs.items():
            if value is not None:
                kwargs = {"return_value": value}
            else:
                kwargs = {}

            prop = PropertyMock(**kwargs)
            setattr(type(mock_obj), key, prop)


class ClientTestSuite(unittest.TestCase):
    """
    Unit tests for isolated client behaviour.
    """

    PACKAGE_NAME = "databricks.sql"
    DUMMY_CONNECTION_ARGS = {
        "server_hostname": "foo",
        "http_path": "dummy_path",
        "access_token": "tok",
    }

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_closing_connection_closes_commands(self, mock_thrift_client_class):
        """Test that closing a connection properly closes commands.

        This test verifies that when a connection is closed:
        1. the active result set is marked as closed server-side
        2. The operation state is set to CLOSED
        3. backend.close_command is called only for commands that weren't already closed

        Args:
            mock_thrift_client_class: Mock for ThriftBackend class
        """

        for closed in (True, False):
            with self.subTest(closed=closed):
                # Set initial state based on whether the command is already closed
                initial_state = (
                    CommandState.CLOSED if closed else CommandState.SUCCEEDED
                )

                # Mock the execute response with controlled state
                mock_execute_response = Mock(spec=ExecuteResponse)
                mock_execute_response.status = initial_state
                mock_execute_response.has_been_closed_server_side = closed
                mock_execute_response.is_staging_operation = False
                mock_execute_response.command_id = Mock(spec=CommandId)
                mock_execute_response.description = []

                # Mock the backend that will be used
                mock_backend = Mock(spec=ThriftDatabricksClient)
                mock_backend.staging_allowed_local_path = None
                mock_backend.fetch_results.return_value = (Mock(), False, 0)

                # Configure the decorator's mock to return our specific mock_backend
                mock_thrift_client_class.return_value = mock_backend

                # Create connection and cursor
                connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
                cursor = connection.cursor()

                real_result_set = ThriftResultSet(
                    connection=connection,
                    execute_response=mock_execute_response,
                    thrift_client=mock_backend,
                )

                # Mock execute_command to return our real result set
                cursor.backend.execute_command = Mock(return_value=real_result_set)

                # Execute a command
                cursor.execute("SELECT 1")

                # Close the connection
                connection.close()

                # Verify the close logic worked:
                assert real_result_set.has_been_closed_server_side is True

                # 2. op_state should always be CLOSED after close()
                assert real_result_set.status == CommandState.CLOSED

                # 3. Backend close_command should be called appropriately
                if not closed:
                    # Should have called backend.close_command during the close chain
                    mock_backend.close_command.assert_called_once_with(
                        mock_execute_response.command_id
                    )
                else:
                    # Should NOT have called backend.close_command (already closed)
                    mock_backend.close_command.assert_not_called()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_cant_open_cursor_on_closed_connection(self, mock_client_class):
        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        self.assertTrue(connection.open)
        connection.close()
        self.assertFalse(connection.open)
        with self.assertRaises(Error) as cm:
            connection.cursor()
        self.assertIn("closed", str(cm.exception))

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    @patch("%s.client.Cursor" % PACKAGE_NAME)
    def test_arraysize_buffer_size_passthrough(
        self, mock_cursor_class, mock_client_class
    ):
        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        connection.cursor(arraysize=999, buffer_size_bytes=1234)
        kwargs = mock_cursor_class.call_args[1]

        self.assertEqual(kwargs["arraysize"], 999)
        self.assertEqual(kwargs["result_buffer_size_bytes"], 1234)

    def test_closing_result_set_with_closed_connection_soft_closes_commands(self):
        mock_connection = Mock()
        mock_backend = Mock()
        mock_results = Mock()
        mock_backend.fetch_results.return_value = (Mock(), False, 0)

        result_set = ThriftResultSet(
            connection=mock_connection,
            execute_response=Mock(),
            thrift_client=mock_backend,
        )
        result_set.results = mock_results

        # Setup session mock on the mock_connection
        mock_session = Mock()
        mock_session.open = False
        type(mock_connection).session = PropertyMock(return_value=mock_session)

        result_set.close()

        self.assertFalse(mock_backend.close_command.called)
        self.assertTrue(result_set.has_been_closed_server_side)

    def test_closing_result_set_hard_closes_commands(self):
        mock_results_response = Mock()
        mock_results_response.has_been_closed_server_side = False
        mock_connection = Mock()
        mock_thrift_backend = Mock()
        mock_results = Mock()
        # Setup session mock on the mock_connection
        mock_session = Mock()
        mock_session.open = True
        type(mock_connection).session = PropertyMock(return_value=mock_session)

        mock_thrift_backend.fetch_results.return_value = (Mock(), False, 0)
        result_set = ThriftResultSet(
            mock_connection,
            mock_results_response,
            mock_thrift_backend,
        )
        result_set.results = mock_results

        result_set.close()

        mock_thrift_backend.close_command.assert_called_once_with(
            mock_results_response.command_id
        )
        mock_results.close.assert_called_once()

    def test_executing_multiple_commands_uses_the_most_recent_command(self):
        mock_result_sets = [Mock(), Mock()]
        # Set is_staging_operation to False to avoid _handle_staging_operation being called
        for mock_rs in mock_result_sets:
            mock_rs.is_staging_operation = False

        mock_backend = ThriftDatabricksClientMockFactory.new()
        mock_backend.execute_command.side_effect = mock_result_sets

        cursor = client.Cursor(connection=Mock(), backend=mock_backend)
        cursor.execute("SELECT 1;")
        cursor.execute("SELECT 1;")

        mock_result_sets[0].close.assert_called_once_with()
        mock_result_sets[1].close.assert_not_called()

        cursor.fetchall()

        mock_result_sets[0].fetchall.assert_not_called()
        mock_result_sets[1].fetchall.assert_called_once_with()

    def test_closed_cursor_doesnt_allow_operations(self):
        cursor = client.Cursor(Mock(), Mock())
        cursor.close()

        with self.assertRaises(Error) as e:
            cursor.execute("SELECT 1;")
            self.assertIn("closed", e.msg)

        with self.assertRaises(Error) as e:
            cursor.fetchall()
            self.assertIn("closed", e.msg)

    def test_negative_fetch_throws_exception(self):
        mock_backend = Mock()
        mock_backend.fetch_results.return_value = (Mock(), False, 0)

        result_set = ThriftResultSet(Mock(), Mock(), mock_backend)

        with self.assertRaises(ValueError) as e:
            result_set.fetchmany(-1)

    def test_context_manager_closes_cursor(self):
        mock_close = Mock()
        with client.Cursor(Mock(), Mock()) as cursor:
            cursor.close = mock_close
        mock_close.assert_called_once_with()

    def dict_product(self, dicts):
        """
        Generate cartesion product of values in input dictionary, outputting a dictionary
        for each combination.
        >>> list(dict_product(dict(number=[1,2], character='ab')))
        [{'character': 'a', 'number': 1},
        {'character': 'a', 'number': 2},
        {'character': 'b', 'number': 1},
        {'character': 'b', 'number': 2}]
        """
        return (dict(zip(dicts.keys(), x)) for x in itertools.product(*dicts.values()))

    @patch("%s.client.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_get_schemas_parameters_passed_to_thrift_backend(self, mock_thrift_backend):
        req_args_combinations = self.dict_product(
            dict(
                catalog_name=["NOT_SET", None, "catalog_pattern"],
                schema_name=["NOT_SET", None, "schema_pattern"],
            )
        )

        for req_args in req_args_combinations:
            req_args = {k: v for k, v in req_args.items() if v != "NOT_SET"}
            with self.subTest(req_args=req_args):
                mock_thrift_backend = Mock()

                cursor = client.Cursor(Mock(), mock_thrift_backend)
                cursor.schemas(**req_args)

                call_args = mock_thrift_backend.get_schemas.call_args[1]
                for k, v in req_args.items():
                    self.assertEqual(v, call_args[k])

    @patch("%s.client.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_get_tables_parameters_passed_to_thrift_backend(self, mock_thrift_backend):
        req_args_combinations = self.dict_product(
            dict(
                catalog_name=["NOT_SET", None, "catalog_pattern"],
                schema_name=["NOT_SET", None, "schema_pattern"],
                table_name=["NOT_SET", None, "table_pattern"],
                table_types=["NOT_SET", [], ["type1", "type2"]],
            )
        )

        for req_args in req_args_combinations:
            req_args = {k: v for k, v in req_args.items() if v != "NOT_SET"}
            with self.subTest(req_args=req_args):
                mock_thrift_backend = Mock()

                cursor = client.Cursor(Mock(), mock_thrift_backend)
                cursor.tables(**req_args)

                call_args = mock_thrift_backend.get_tables.call_args[1]
                for k, v in req_args.items():
                    self.assertEqual(v, call_args[k])

    @patch("%s.client.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_get_columns_parameters_passed_to_thrift_backend(self, mock_thrift_backend):
        req_args_combinations = self.dict_product(
            dict(
                catalog_name=["NOT_SET", None, "catalog_pattern"],
                schema_name=["NOT_SET", None, "schema_pattern"],
                table_name=["NOT_SET", None, "table_pattern"],
                column_name=["NOT_SET", None, "column_pattern"],
            )
        )

        for req_args in req_args_combinations:
            req_args = {k: v for k, v in req_args.items() if v != "NOT_SET"}
            with self.subTest(req_args=req_args):
                mock_thrift_backend = Mock()

                cursor = client.Cursor(Mock(), mock_thrift_backend)
                cursor.columns(**req_args)

                call_args = mock_thrift_backend.get_columns.call_args[1]
                for k, v in req_args.items():
                    self.assertEqual(v, call_args[k])

    def test_cancel_command_calls_the_backend(self):
        mock_thrift_backend = Mock()
        cursor = client.Cursor(Mock(), mock_thrift_backend)
        mock_command_id = Mock()
        cursor.active_command_id = mock_command_id
        cursor.cancel()
        mock_thrift_backend.cancel_command.assert_called_with(mock_command_id)

    @patch("databricks.sql.client.logger")
    def test_cancel_command_will_issue_warning_for_cancel_with_no_executing_command(
        self, logger_instance
    ):
        mock_thrift_backend = Mock()
        cursor = client.Cursor(Mock(), mock_thrift_backend)
        cursor.cancel()

        self.assertTrue(logger_instance.warning.called)
        self.assertFalse(mock_thrift_backend.cancel_command.called)

    def test_version_is_canonical(self):
        version = databricks.sql.__version__
        canonical_version_re = (
            r"^([1-9][0-9]*!)?(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*((a|b|rc)"
            r"(0|[1-9][0-9]*))?(\.post(0|[1-9][0-9]*))?(\.dev(0|[1-9][0-9]*))?$"
        )
        self.assertIsNotNone(re.match(canonical_version_re, version))

    def test_execute_parameter_passthrough(self):
        mock_thrift_backend = ThriftDatabricksClientMockFactory.new()
        cursor = client.Cursor(Mock(), mock_thrift_backend)

        tests = [
            ("SELECT %(string_v)s", "SELECT 'foo_12345'", {"string_v": "foo_12345"}),
            ("SELECT %(x)s", "SELECT NULL", {"x": None}),
            ("SELECT %(int_value)d", "SELECT 48", {"int_value": 48}),
            ("SELECT %(float_value).2f", "SELECT 48.20", {"float_value": 48.2}),
            ("SELECT %(iter)s", "SELECT ARRAY(1,2,3,4,5)", {"iter": [1, 2, 3, 4, 5]}),
            (
                "SELECT %(datetime)s",
                "SELECT '2022-02-01 10:23:00.000000'",
                {"datetime": datetime(2022, 2, 1, 10, 23)},
            ),
            ("SELECT %(date)s", "SELECT '2022-02-01'", {"date": date(2022, 2, 1)}),
        ]

        for query, expected_query, params in tests:
            cursor.execute(query, parameters=params)
            self.assertEqual(
                mock_thrift_backend.execute_command.call_args[1]["operation"],
                expected_query,
            )

    def test_executemany_parameter_passhthrough_and_uses_last_result_set(self):
        # Create a new mock result set each time the class is instantiated
        mock_result_set_instances = [Mock(), Mock(), Mock()]
        # Set is_staging_operation to False to avoid _handle_staging_operation being called
        for mock_rs in mock_result_set_instances:
            mock_rs.is_staging_operation = False

        mock_backend = ThriftDatabricksClientMockFactory.new()
        mock_backend.execute_command.side_effect = mock_result_set_instances

        cursor = client.Cursor(Mock(), mock_backend)

        params = [{"x": None}, {"x": "foo1"}, {"x": "bar2"}]
        expected_queries = ["SELECT NULL", "SELECT 'foo1'", "SELECT 'bar2'"]

        cursor.executemany("SELECT %(x)s", seq_of_parameters=params)

        self.assertEqual(
            len(mock_backend.execute_command.call_args_list),
            len(expected_queries),
            "Expected execute_command to be called the same number of times as params were passed",
        )

        for expected_query, call_args in zip(
            expected_queries, mock_backend.execute_command.call_args_list
        ):
            self.assertEqual(call_args[1]["operation"], expected_query)

        self.assertEqual(
            cursor.active_result_set,
            mock_result_set_instances[2],
            "Expected the active result set to be the result set corresponding to the"
            "last operation",
        )

    def test_setinputsizes_a_noop(self):
        cursor = client.Cursor(Mock(), Mock())
        cursor.setinputsizes(1)

    def test_setoutputsizes_a_noop(self):
        cursor = client.Cursor(Mock(), Mock())
        cursor.setoutputsize(1)

    @unittest.skip("JDW: skipping winter 2024 as we're about to rewrite this interface")
    @patch("%s.client.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_row_number_respected(self, mock_thrift_backend_class):
        def make_fake_row_slice(n_rows):
            mock_slice = Mock()
            mock_slice.num_rows = n_rows
            return mock_slice

        mock_thrift_backend = mock_thrift_backend_class.return_value
        mock_aq = Mock()
        mock_aq.next_n_rows.side_effect = make_fake_row_slice
        mock_thrift_backend.execute_command.return_value.arrow_queue = mock_aq

        cursor = client.Cursor(Mock(), mock_thrift_backend)
        cursor.execute("foo")

        self.assertEqual(cursor.rownumber, 0)
        cursor.fetchmany_arrow(10)
        self.assertEqual(cursor.rownumber, 10)
        cursor.fetchmany_arrow(13)
        self.assertEqual(cursor.rownumber, 23)
        cursor.fetchmany_arrow(6)
        self.assertEqual(cursor.rownumber, 29)

    @unittest.skip("JDW: skipping winter 2024 as we're about to rewrite this interface")
    @patch("%s.client.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_disable_pandas_respected(self, mock_thrift_backend_class):
        mock_thrift_backend = mock_thrift_backend_class.return_value
        mock_table = Mock()
        mock_table.num_rows = 10
        mock_table.itercolumns.return_value = []
        mock_table.rename_columns.return_value = mock_table
        mock_aq = Mock()
        mock_aq.remaining_rows.return_value = mock_table
        mock_thrift_backend.execute_command.return_value.arrow_queue = mock_aq
        mock_thrift_backend.execute_command.return_value.has_been_closed_server_side = (
            True
        )
        mock_con = Mock()
        mock_con.disable_pandas = True

        cursor = client.Cursor(mock_con, mock_thrift_backend)
        cursor.execute("foo")
        cursor.fetchall()

        mock_table.itercolumns.assert_called_once_with()

    def test_column_name_api(self):
        ResultRow = Row("first_col", "second_col", "third_col")
        data = [
            ResultRow("val1", 321, 52.32),
            ResultRow("val2", 2321, 252.32),
        ]

        expected_values = [["val1", 321, 52.32], ["val2", 2321, 252.32]]

        for (row, expected) in zip(data, expected_values):
            self.assertEqual(row.first_col, expected[0])
            self.assertEqual(row.second_col, expected[1])
            self.assertEqual(row.third_col, expected[2])

            self.assertEqual(row["first_col"], expected[0])
            self.assertEqual(row["second_col"], expected[1])
            self.assertEqual(row["third_col"], expected[2])

            self.assertEqual(row[0], expected[0])
            self.assertEqual(row[1], expected[1])
            self.assertEqual(row[2], expected[2])

            self.assertEqual(
                row.asDict(),
                {
                    "first_col": expected[0],
                    "second_col": expected[1],
                    "third_col": expected[2],
                },
            )

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_cursor_keeps_connection_alive(self, mock_client_class):
        instance = mock_client_class.return_value

        mock_open_session_resp = MagicMock(spec=TOpenSessionResp)()
        mock_open_session_resp.sessionHandle.sessionId = b"\x22"
        instance.open_session.return_value = mock_open_session_resp

        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        cursor = connection.cursor()
        del connection

        gc.collect()

        self.assertEqual(instance.close_session.call_count, 0)
        cursor.close()

    @patch("%s.backend.types.ExecuteResponse" % PACKAGE_NAME)
    @patch("%s.client.Cursor._handle_staging_operation" % PACKAGE_NAME)
    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_staging_operation_response_is_handled(
        self,
        mock_client_class,
        mock_handle_staging_operation,
        mock_execute_response,
    ):
        # If server sets ExecuteResponse.is_staging_operation True then _handle_staging_operation should be called

        ThriftDatabricksClientMockFactory.apply_property_to_mock(
            mock_execute_response, is_staging_operation=True
        )
        mock_client = mock_client_class.return_value
        mock_client.execute_command.return_value = Mock(is_staging_operation=True)
        mock_client_class.return_value = mock_client

        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        cursor = connection.cursor()
        cursor.execute("Text of some staging operation command;")
        connection.close()

        mock_handle_staging_operation.call_count == 1

    @patch(
        "%s.session.ThriftDatabricksClient" % PACKAGE_NAME,
        ThriftDatabricksClientMockFactory.new(),
    )
    def test_access_current_query_id(self):
        operation_id = "EE6A8778-21FC-438B-92D8-96AC51EE3821"

        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        cursor = connection.cursor()

        self.assertIsNone(cursor.query_id)

        cursor.active_command_id = CommandId.from_thrift_handle(
            TOperationHandle(
                operationId=THandleIdentifier(
                    guid=UUID(operation_id).bytes, secret=0x00
                ),
                operationType=TOperationType.EXECUTE_STATEMENT,
            )
        )
        self.assertEqual(cursor.query_id.upper(), operation_id.upper())

        cursor.close()
        self.assertIsNone(cursor.query_id)

    def test_connection_close_handles_cursor_close_exception(self):
        """Test that _close handles exceptions from cursor.close() properly."""
        cursors_closed = []

        def mock_close_with_exception():
            cursors_closed.append(1)
            raise Exception("Test error during close")

        cursor1 = Mock()
        cursor1.close = mock_close_with_exception

        def mock_close_normal():
            cursors_closed.append(2)

        cursor2 = Mock()
        cursor2.close = mock_close_normal

        mock_backend = Mock()
        mock_session_handle = Mock()

        try:
            for cursor in [cursor1, cursor2]:
                try:
                    cursor.close()
                except Exception:
                    pass

            mock_backend.close_session(mock_session_handle)
        except Exception as e:
            self.fail(f"Connection close should handle exceptions: {e}")

        self.assertEqual(
            cursors_closed, [1, 2], "Both cursors should have close called"
        )


class TransactionTestSuite(unittest.TestCase):
    """
    Unit tests for transaction control methods (MST support).
    """

    PACKAGE_NAME = "databricks.sql"
    DUMMY_CONNECTION_ARGS = {
        "server_hostname": "foo",
        "http_path": "dummy_path",
        "access_token": "tok",
    }

    def _setup_mock_session_with_http_client(self, mock_session):
        """
        Helper to configure a mock session with HTTP client mocks.
        This prevents feature flag network requests during Connection initialization.
        """
        mock_session.host = "foo"

        # Mock HTTP client to prevent feature flag network requests
        mock_http_client = Mock()
        mock_session.http_client = mock_http_client

        # Mock feature flag response to prevent blocking HTTP calls
        mock_ff_response = Mock()
        mock_ff_response.status = 200
        mock_ff_response.data = b'{"flags": [], "ttl_seconds": 900}'
        mock_http_client.request.return_value = mock_ff_response

    def _create_mock_connection(self, mock_session_class):
        """Helper to create a mocked connection for transaction tests."""
        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"
        mock_session.get_autocommit.return_value = True

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        # Create connection with ignore_transactions=False to test actual transaction functionality
        conn = client.Connection(
            ignore_transactions=False, **self.DUMMY_CONNECTION_ARGS
        )
        return conn

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_autocommit_getter_returns_cached_value(self, mock_session_class):
        """Test that autocommit property returns cached session value by default."""
        conn = self._create_mock_connection(mock_session_class)

        # Get autocommit (should use cached value)
        result = conn.autocommit

        conn.session.get_autocommit.assert_called_once()
        self.assertTrue(result)

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_autocommit_setter_executes_sql(self, mock_session_class):
        """Test that setting autocommit executes SET AUTOCOMMIT command."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        with patch.object(conn, "cursor", return_value=mock_cursor):
            conn.autocommit = False

            # Verify SQL was executed
            mock_cursor.execute.assert_called_once_with("SET AUTOCOMMIT = FALSE")
            mock_cursor.close.assert_called_once()

            conn.session.set_autocommit.assert_called_once_with(False)

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_autocommit_setter_with_true_value(self, mock_session_class):
        """Test setting autocommit to True."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        with patch.object(conn, "cursor", return_value=mock_cursor):
            conn.autocommit = True

            mock_cursor.execute.assert_called_once_with("SET AUTOCOMMIT = TRUE")
            conn.session.set_autocommit.assert_called_once_with(True)

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_autocommit_setter_wraps_database_error(self, mock_session_class):
        """Test that autocommit setter wraps DatabaseError in TransactionError."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        server_error = DatabaseError(
            "AUTOCOMMIT_SET_DURING_ACTIVE_TRANSACTION",
            context={"sql_state": "25000"},
            host_url="test-host",
        )
        mock_cursor.execute.side_effect = server_error

        with patch.object(conn, "cursor", return_value=mock_cursor):
            with self.assertRaises(TransactionError) as ctx:
                conn.autocommit = False

            self.assertIn("Failed to set autocommit", str(ctx.exception))
            self.assertEqual(ctx.exception.context["operation"], "set_autocommit")
            self.assertEqual(ctx.exception.context["autocommit_value"], False)

            mock_cursor.close.assert_called_once()

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_autocommit_setter_preserves_exception_chain(self, mock_session_class):
        """Test that exception chaining is preserved."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        original_error = DatabaseError("Original error", host_url="test-host")
        mock_cursor.execute.side_effect = original_error

        with patch.object(conn, "cursor", return_value=mock_cursor):
            with self.assertRaises(TransactionError) as ctx:
                conn.autocommit = False

            self.assertEqual(ctx.exception.__cause__, original_error)

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_commit_executes_sql(self, mock_session_class):
        """Test that commit() executes COMMIT command."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        with patch.object(conn, "cursor", return_value=mock_cursor):
            conn.commit()

            mock_cursor.execute.assert_called_once_with("COMMIT")
            mock_cursor.close.assert_called_once()

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_commit_wraps_database_error(self, mock_session_class):
        """Test that commit() wraps DatabaseError in TransactionError."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        server_error = DatabaseError(
            "MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION",
            context={"sql_state": "25000"},
            host_url="test-host",
        )
        mock_cursor.execute.side_effect = server_error

        with patch.object(conn, "cursor", return_value=mock_cursor):
            with self.assertRaises(TransactionError) as ctx:
                conn.commit()

            self.assertIn("Failed to commit", str(ctx.exception))
            self.assertEqual(ctx.exception.context["operation"], "commit")
            mock_cursor.close.assert_called_once()

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_commit_on_closed_connection_raises_interface_error(
        self, mock_session_class
    ):
        """Test that commit() on closed connection raises InterfaceError."""
        conn = self._create_mock_connection(mock_session_class)
        conn.session.is_open = False

        with self.assertRaises(InterfaceError) as ctx:
            conn.commit()

        self.assertIn("Cannot commit on closed connection", str(ctx.exception))

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_rollback_executes_sql(self, mock_session_class):
        """Test that rollback() executes ROLLBACK command."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        with patch.object(conn, "cursor", return_value=mock_cursor):
            conn.rollback()

            mock_cursor.execute.assert_called_once_with("ROLLBACK")
            mock_cursor.close.assert_called_once()

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_rollback_wraps_database_error(self, mock_session_class):
        """Test that rollback() wraps DatabaseError in TransactionError."""
        conn = self._create_mock_connection(mock_session_class)

        mock_cursor = Mock()
        server_error = DatabaseError(
            "Unexpected rollback error",
            context={"sql_state": "HY000"},
            host_url="test-host",
        )
        mock_cursor.execute.side_effect = server_error

        with patch.object(conn, "cursor", return_value=mock_cursor):
            with self.assertRaises(TransactionError) as ctx:
                conn.rollback()

            self.assertIn("Failed to rollback", str(ctx.exception))
            self.assertEqual(ctx.exception.context["operation"], "rollback")
            mock_cursor.close.assert_called_once()

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_rollback_on_closed_connection_raises_interface_error(
        self, mock_session_class
    ):
        """Test that rollback() on closed connection raises InterfaceError."""
        conn = self._create_mock_connection(mock_session_class)
        conn.session.is_open = False

        with self.assertRaises(InterfaceError) as ctx:
            conn.rollback()

        self.assertIn("Cannot rollback on closed connection", str(ctx.exception))

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_get_transaction_isolation_returns_repeatable_read(
        self, mock_session_class
    ):
        """Test that get_transaction_isolation() returns REPEATABLE_READ."""
        conn = self._create_mock_connection(mock_session_class)

        result = conn.get_transaction_isolation()

        self.assertEqual(result, "REPEATABLE_READ")

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_get_transaction_isolation_on_closed_connection_raises_interface_error(
        self, mock_session_class
    ):
        """Test that get_transaction_isolation() on closed connection raises InterfaceError."""
        conn = self._create_mock_connection(mock_session_class)
        conn.session.is_open = False

        with self.assertRaises(InterfaceError) as ctx:
            conn.get_transaction_isolation()

        self.assertIn(
            "Cannot get transaction isolation on closed connection", str(ctx.exception)
        )

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_set_transaction_isolation_accepts_repeatable_read(
        self, mock_session_class
    ):
        """Test that set_transaction_isolation() accepts REPEATABLE_READ."""
        conn = self._create_mock_connection(mock_session_class)

        # Should not raise
        conn.set_transaction_isolation("REPEATABLE_READ")
        conn.set_transaction_isolation("REPEATABLE READ")  # With space
        conn.set_transaction_isolation("repeatable_read")  # Lowercase with underscore
        conn.set_transaction_isolation("repeatable read")  # Lowercase with space

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_set_transaction_isolation_rejects_other_levels(self, mock_session_class):
        """Test that set_transaction_isolation() rejects non-REPEATABLE_READ levels."""
        conn = self._create_mock_connection(mock_session_class)

        with self.assertRaises(NotSupportedError) as ctx:
            conn.set_transaction_isolation("READ_COMMITTED")

        self.assertIn("not supported", str(ctx.exception))
        self.assertIn("READ_COMMITTED", str(ctx.exception))

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_set_transaction_isolation_on_closed_connection_raises_interface_error(
        self, mock_session_class
    ):
        """Test that set_transaction_isolation() on closed connection raises InterfaceError."""
        conn = self._create_mock_connection(mock_session_class)
        conn.session.is_open = False

        with self.assertRaises(InterfaceError) as ctx:
            conn.set_transaction_isolation("REPEATABLE_READ")

        self.assertIn(
            "Cannot set transaction isolation on closed connection", str(ctx.exception)
        )

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_fetch_autocommit_from_server_queries_server(self, mock_session_class):
        """Test that fetch_autocommit_from_server=True queries server."""
        # Create connection with fetch_autocommit_from_server=True
        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        conn = client.Connection(
            fetch_autocommit_from_server=True,
            ignore_transactions=False,
            **self.DUMMY_CONNECTION_ARGS,
        )

        mock_cursor = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="true")
        mock_cursor.fetchone.return_value = mock_row

        with patch.object(conn, "cursor", return_value=mock_cursor):
            result = conn.autocommit

            mock_cursor.execute.assert_called_once_with("SET AUTOCOMMIT")
            mock_cursor.fetchone.assert_called_once()
            mock_cursor.close.assert_called_once()

            conn.session.set_autocommit.assert_called_once_with(True)

            self.assertTrue(result)

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_fetch_autocommit_from_server_handles_false_value(self, mock_session_class):
        """Test that fetch_autocommit_from_server correctly parses false value."""
        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        conn = client.Connection(
            fetch_autocommit_from_server=True,
            ignore_transactions=False,
            **self.DUMMY_CONNECTION_ARGS,
        )

        mock_cursor = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="false")
        mock_cursor.fetchone.return_value = mock_row

        with patch.object(conn, "cursor", return_value=mock_cursor):
            result = conn.autocommit

            conn.session.set_autocommit.assert_called_once_with(False)
            self.assertFalse(result)

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_fetch_autocommit_from_server_raises_on_no_result(self, mock_session_class):
        """Test that fetch_autocommit_from_server raises error when no result."""
        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        conn = client.Connection(
            fetch_autocommit_from_server=True,
            ignore_transactions=False,
            **self.DUMMY_CONNECTION_ARGS,
        )

        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None

        with patch.object(conn, "cursor", return_value=mock_cursor):
            with self.assertRaises(TransactionError) as ctx:
                _ = conn.autocommit

            self.assertIn("No result returned", str(ctx.exception))
            mock_cursor.close.assert_called_once()

        conn.close()

    # ==================== IGNORE_TRANSACTIONS TESTS ====================

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_commit_is_noop_when_ignore_transactions_true(self, mock_session_class):
        """Test that commit() is a no-op when ignore_transactions=True."""

        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        # Create connection with ignore_transactions=True (default)
        conn = client.Connection(**self.DUMMY_CONNECTION_ARGS)

        # Verify ignore_transactions is True by default
        self.assertTrue(conn.ignore_transactions)

        mock_cursor = Mock()
        with patch.object(conn, "cursor", return_value=mock_cursor):
            # Call commit - should be no-op
            conn.commit()

            # Verify that execute was NOT called (no-op)
            mock_cursor.execute.assert_not_called()
            mock_cursor.close.assert_not_called()

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_rollback_raises_not_supported_when_ignore_transactions_true(
        self, mock_session_class
    ):
        """Test that rollback() raises NotSupportedError when ignore_transactions=True."""

        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        # Create connection with ignore_transactions=True (default)
        conn = client.Connection(**self.DUMMY_CONNECTION_ARGS)

        # Verify ignore_transactions is True by default
        self.assertTrue(conn.ignore_transactions)

        # Call rollback - should raise NotSupportedError
        with self.assertRaises(NotSupportedError) as ctx:
            conn.rollback()

        self.assertIn("Transactions are not supported", str(ctx.exception))

        conn.close()

    @patch("%s.client.Session" % PACKAGE_NAME)
    def test_autocommit_setter_is_noop_when_ignore_transactions_true(
        self, mock_session_class
    ):
        """Test that autocommit setter is a no-op when ignore_transactions=True."""

        mock_session = Mock()
        mock_session.is_open = True
        mock_session.guid_hex = "test-session-id"

        self._setup_mock_session_with_http_client(mock_session)
        mock_session_class.return_value = mock_session

        # Create connection with ignore_transactions=True (default)
        conn = client.Connection(**self.DUMMY_CONNECTION_ARGS)

        # Verify ignore_transactions is True by default
        self.assertTrue(conn.ignore_transactions)

        mock_cursor = Mock()
        with patch.object(conn, "cursor", return_value=mock_cursor):
            # Set autocommit - should be no-op
            conn.autocommit = False

            # Verify that execute was NOT called (no-op)
            mock_cursor.execute.assert_not_called()
            mock_cursor.close.assert_not_called()

            # Session set_autocommit should also not be called
            conn.session.set_autocommit.assert_not_called()

        conn.close()


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    loader = unittest.TestLoader()
    test_classes = [
        ClientTestSuite,
        TransactionTestSuite,
        FetchTests,
        ThriftBackendTestSuite,
        ArrowQueueSuite,
    ]
    suites_list = []
    for test_class in test_classes:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    suite = unittest.TestSuite(suites_list)
    test_result = unittest.TextTestRunner().run(suite)

    if len(test_result.errors) != 0 or len(test_result.failures) != 0:
        sys.exit(1)
