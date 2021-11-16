import unittest
from unittest.mock import patch, MagicMock, Mock
import itertools
import pyarrow

from databricks.sql.thrift_api.TCLIService import ttypes

from databricks.sql import *
from databricks.sql.thrift_backend import ThriftBackend


class TestThriftBackend(unittest.TestCase):
    okay_status = ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS)

    bad_status = ttypes.TStatus(
        statusCode=ttypes.TStatusCode.ERROR_STATUS,
        errorMessage="this is a bad error",
    )

    operation_handle = ttypes.TOperationHandle(
        operationId=ttypes.THandleIdentifier(guid=0x33, secret=0x35),
        operationType=ttypes.TOperationType.EXECUTE_STATEMENT)

    session_handle = ttypes.TSessionHandle(
        sessionId=ttypes.THandleIdentifier(guid=0x36, secret=0x37))

    open_session_resp = ttypes.TOpenSessionResp(
        status=okay_status,
        serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4)

    metadata_resp = ttypes.TGetResultSetMetadataResp(
        status=okay_status,
        resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET,
        schema=MagicMock(),
    )

    execute_response_types = [
        ttypes.TExecuteStatementResp, ttypes.TGetCatalogsResp, ttypes.TGetSchemasResp,
        ttypes.TGetTablesResp, ttypes.TGetColumnsResp
    ]

    def test_make_request_checks_thrift_status_code(self):
        mock_response = Mock()
        mock_response.status.statusCode = ttypes.TStatusCode.ERROR_STATUS
        mock_method = lambda _: mock_response
        with self.assertRaises(DatabaseError):
            ThriftBackend.make_request(mock_method, Mock())

    def _make_type_desc(self, type):
        return ttypes.TTypeDesc(types=[ttypes.TTypeEntry(ttypes.TPrimitiveTypeEntry(type=type))])

    def _make_fake_thrift_backend(self):
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend._hive_schema_to_arrow_schema = Mock()
        thrift_backend._hive_schema_to_description = Mock()
        thrift_backend._create_arrow_table = MagicMock()
        thrift_backend._create_arrow_table.return_value = (Mock(), Mock())
        return thrift_backend

    def test_hive_schema_to_arrow_schema_preserves_column_names(self):
        columns = [
            ttypes.TColumnDesc(
                columnName="column 1", typeDesc=self._make_type_desc(ttypes.TTypeId.INT_TYPE)),
            ttypes.TColumnDesc(
                columnName="column 2", typeDesc=self._make_type_desc(ttypes.TTypeId.INT_TYPE)),
            ttypes.TColumnDesc(
                columnName="column 2", typeDesc=self._make_type_desc(ttypes.TTypeId.INT_TYPE)),
            ttypes.TColumnDesc(
                columnName="", typeDesc=self._make_type_desc(ttypes.TTypeId.INT_TYPE))
        ]

        t_table_schema = ttypes.TTableSchema(columns)
        arrow_schema = ThriftBackend._hive_schema_to_arrow_schema(t_table_schema)

        self.assertEqual(arrow_schema.field(0).name, "column 1")
        self.assertEqual(arrow_schema.field(1).name, "column 2")
        self.assertEqual(arrow_schema.field(2).name, "column 2")
        self.assertEqual(arrow_schema.field(3).name, "")

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_bad_protocol_versions_are_rejected(self, tcli_service_client_cass):
        t_http_client_instance = tcli_service_client_cass.return_value
        bad_protocol_versions = [
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9,
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1,
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2,
        ]

        for protocol_version in bad_protocol_versions:
            t_http_client_instance.OpenSession.return_value = ttypes.TOpenSessionResp(
                status=self.okay_status, serverProtocolVersion=protocol_version)

            with self.assertRaises(OperationalError) as cm:
                thrift_backend = self._make_fake_thrift_backend()
                thrift_backend.open_session()

            self.assertIn("expected server to use a protocol version", str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_okay_protocol_versions_succeed(self, tcli_service_client_cass):
        t_http_client_instance = tcli_service_client_cass.return_value
        good_protocol_versions = [
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3,
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4,
        ]

        for protocol_version in good_protocol_versions:
            t_http_client_instance.OpenSession.return_value = ttypes.TOpenSessionResp(
                status=self.okay_status, serverProtocolVersion=protocol_version)

            thrift_backend = self._make_fake_thrift_backend()
            thrift_backend.open_session()

    @patch("thrift.transport.THttpClient.THttpClient")
    def test_headers_are_set(self, t_http_client_class):
        ThriftBackend("foo", 123, "bar", [("header", "value")])
        t_http_client_class.return_value.setCustomHeaders.assert_called_with({"header": "value"})

    @patch("thrift.transport.THttpClient.THttpClient")
    def test_port_and_host_are_respected(self, t_http_client_class):
        ThriftBackend("hostname", 123, "path_value", [])
        self.assertEqual(t_http_client_class.call_args[1]["uri_or_host"],
                         "https://hostname:123/path_value")

    def test_non_primitive_types_raise_error(self):
        columns = [
            ttypes.TColumnDesc(
                columnName="column 1", typeDesc=self._make_type_desc(ttypes.TTypeId.INT_TYPE)),
            ttypes.TColumnDesc(
                columnName="column 2",
                typeDesc=ttypes.TTypeDesc(types=[
                    ttypes.TTypeEntry(userDefinedTypeEntry=ttypes.TUserDefinedTypeEntry("foo"))
                ]))
        ]

        t_table_schema = ttypes.TTableSchema(columns)
        with self.assertRaises(OperationalError):
            ThriftBackend._hive_schema_to_arrow_schema(t_table_schema)
        with self.assertRaises(OperationalError):
            ThriftBackend._hive_schema_to_description(t_table_schema)

    def test_hive_schema_to_description_preserves_column_names_and_types(self):
        # Full coverage of all types is done in integration tests, this is just a
        # canary test
        columns = [
            ttypes.TColumnDesc(
                columnName="column 1", typeDesc=self._make_type_desc(ttypes.TTypeId.INT_TYPE)),
            ttypes.TColumnDesc(
                columnName="column 2", typeDesc=self._make_type_desc(ttypes.TTypeId.BOOLEAN_TYPE)),
            ttypes.TColumnDesc(
                columnName="column 2", typeDesc=self._make_type_desc(ttypes.TTypeId.MAP_TYPE)),
            ttypes.TColumnDesc(
                columnName="", typeDesc=self._make_type_desc(ttypes.TTypeId.STRUCT_TYPE))
        ]

        t_table_schema = ttypes.TTableSchema(columns)
        description = ThriftBackend._hive_schema_to_description(t_table_schema)

        self.assertEqual(description, [
            ("column 1", "int", None, None, None, None, None),
            ("column 2", "boolean", None, None, None, None, None),
            ("column 2", "map", None, None, None, None, None),
            ("", "struct", None, None, None, None, None),
        ])

    def test_make_request_checks_status_code(self):
        error_codes = [ttypes.TStatusCode.ERROR_STATUS, ttypes.TStatusCode.INVALID_HANDLE_STATUS]
        for code in error_codes:
            mock_error_response = Mock()
            mock_error_response.status.statusCode = code
            mock_error_response.status.errorMessage = "a detailed error message"
            with self.assertRaises(DatabaseError) as cm:
                ThriftBackend.make_request(lambda _: mock_error_response, Mock())
            self.assertIn("a detailed error message", str(cm.exception))

        success_codes = [
            ttypes.TStatusCode.SUCCESS_STATUS, ttypes.TStatusCode.SUCCESS_WITH_INFO_STATUS,
            ttypes.TStatusCode.STILL_EXECUTING_STATUS
        ]
        for code in success_codes:
            mock_response = Mock()
            mock_response.status.statusCode = code
            ThriftBackend.make_request(lambda _: mock_response, Mock())

    def test_handle_execute_response_checks_operation_state_in_direct_results(self):
        for resp_type in self.execute_response_types:
            with self.subTest(resp_type=resp_type):
                t_execute_resp = resp_type(
                    status=self.okay_status,
                    directResults=ttypes.TSparkDirectResults(
                        operationStatus=ttypes.TGetOperationStatusResp(
                            status=self.okay_status,
                            operationState=ttypes.TOperationState.ERROR_STATE,
                            errorMessage="some information about the error"),
                        resultSetMetadata=None,
                        resultSet=None,
                        closeOperation=None))
                thrift_backend = ThriftBackend("foobar", 443, "path", [])

                with self.assertRaises(DatabaseError) as cm:
                    thrift_backend._handle_execute_response(t_execute_resp, Mock())
                self.assertIn("some information about the error", str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_handle_execute_response_checks_operation_state_in_polls(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value

        error_resp = ttypes.TGetOperationStatusResp(
            status=self.okay_status,
            operationState=ttypes.TOperationState.ERROR_STATE,
            errorMessage="some information about the error")

        closed_resp = ttypes.TGetOperationStatusResp(
            status=self.okay_status, operationState=ttypes.TOperationState.CLOSED_STATE)

        for op_state_resp, exec_resp_type in itertools.product([error_resp, closed_resp],
                                                               self.execute_response_types):
            with self.subTest(op_state_resp=op_state_resp, exec_resp_type=exec_resp_type):
                tcli_service_instance = tcli_service_class.return_value
                t_execute_resp = exec_resp_type(
                    status=self.okay_status,
                    directResults=None,
                    operationHandle=self.operation_handle)

                tcli_service_instance.GetOperationStatus.return_value = op_state_resp
                thrift_backend = ThriftBackend("foobar", 443, "path", [])

                with self.assertRaises(DatabaseError) as cm:
                    thrift_backend._handle_execute_response(t_execute_resp, Mock())
                if op_state_resp.errorMessage:
                    self.assertIn(op_state_resp.errorMessage, str(cm.exception))

    def test_handle_execute_response_checks_direct_results_for_error_statuses(self):
        for resp_type in self.execute_response_types:
            resp_1 = resp_type(
                status=self.okay_status,
                directResults=ttypes.TSparkDirectResults(
                    operationStatus=ttypes.TGetOperationStatusResp(status=self.bad_status),
                    resultSetMetadata=None,
                    resultSet=None,
                    closeOperation=None))

            resp_2 = resp_type(
                status=self.okay_status,
                directResults=ttypes.TSparkDirectResults(
                    operationStatus=None,
                    resultSetMetadata=ttypes.TGetResultSetMetadataResp(status=self.bad_status),
                    resultSet=None,
                    closeOperation=None))

            resp_3 = resp_type(
                status=self.okay_status,
                directResults=ttypes.TSparkDirectResults(
                    operationStatus=None,
                    resultSetMetadata=None,
                    resultSet=ttypes.TFetchResultsResp(status=self.bad_status),
                    closeOperation=None))

            resp_4 = resp_type(
                status=self.okay_status,
                directResults=ttypes.TSparkDirectResults(
                    operationStatus=None,
                    resultSetMetadata=None,
                    resultSet=None,
                    closeOperation=ttypes.TCloseOperationResp(status=self.bad_status)))

            for error_resp in [resp_1, resp_2, resp_3, resp_4]:
                with self.subTest(error_resp=error_resp):
                    thrift_backend = ThriftBackend("foobar", 443, "path", [])

                    with self.assertRaises(DatabaseError) as cm:
                        thrift_backend._handle_execute_response(error_resp, Mock())
                    self.assertIn("this is a bad error", str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_handle_execute_response_can_handle_without_direct_results(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value

        for resp_type in self.execute_response_types:
            with self.subTest(resp_type=resp_type):

                execute_resp = resp_type(
                    status=self.okay_status,
                    directResults=None,
                    operationHandle=self.operation_handle,
                )

                op_state_1 = ttypes.TGetOperationStatusResp(
                    status=self.okay_status,
                    operationState=ttypes.TOperationState.RUNNING_STATE,
                )

                op_state_2 = ttypes.TGetOperationStatusResp(
                    status=self.okay_status,
                    operationState=ttypes.TOperationState.PENDING_STATE,
                )

                op_state_3 = ttypes.TGetOperationStatusResp(
                    status=self.okay_status, operationState=ttypes.TOperationState.FINISHED_STATE)

                tcli_service_instance.GetResultSetMetadata.return_value = self.metadata_resp
                tcli_service_instance.GetOperationStatus.side_effect = [
                    op_state_1, op_state_2, op_state_3
                ]
                thrift_backend = ThriftBackend("foobar", 443, "path", [])
                results_message_response = thrift_backend._handle_execute_response(
                    execute_resp, Mock())
                self.assertEqual(results_message_response.status,
                                 ttypes.TOperationState.FINISHED_STATE)

    def test_handle_execute_response_can_handle_with_direct_results(self):
        result_set_metadata_mock = Mock()

        direct_results_message = ttypes.TSparkDirectResults(
            operationStatus=ttypes.TGetOperationStatusResp(
                status=self.okay_status,
                operationState=ttypes.TOperationState.FINISHED_STATE,
            ),
            resultSetMetadata=result_set_metadata_mock,
            resultSet=Mock(),
            closeOperation=Mock())

        for resp_type in self.execute_response_types:
            with self.subTest(resp_type=resp_type):
                execute_resp = resp_type(
                    status=self.okay_status,
                    directResults=direct_results_message,
                    operationHandle=self.operation_handle)

                thrift_backend = ThriftBackend("foobar", 443, "path", [])
                thrift_backend._results_message_to_execute_response = Mock()

                thrift_backend._handle_execute_response(execute_resp, Mock())

                thrift_backend._results_message_to_execute_response.assert_called_with(
                    execute_resp,
                    ttypes.TOperationState.FINISHED_STATE,
                )

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_handle_execute_response_reads_has_more_rows_in_direct_results(
            self, tcli_service_class):
        for has_more_rows, resp_type in itertools.product([True, False],
                                                          self.execute_response_types):
            with self.subTest(has_more_rows=has_more_rows, resp_type=resp_type):
                tcli_service_instance = tcli_service_class.return_value
                results_mock = Mock()
                results_mock.startRowOffset = 0
                direct_results_message = ttypes.TSparkDirectResults(
                    operationStatus=ttypes.TGetOperationStatusResp(
                        status=self.okay_status,
                        operationState=ttypes.TOperationState.FINISHED_STATE,
                    ),
                    resultSetMetadata=self.metadata_resp,
                    resultSet=ttypes.TFetchResultsResp(
                        status=self.okay_status,
                        hasMoreRows=has_more_rows,
                        results=results_mock,
                    ),
                    closeOperation=Mock())
                execute_resp = resp_type(
                    status=self.okay_status,
                    directResults=direct_results_message,
                    operationHandle=self.operation_handle)

                tcli_service_instance.GetResultSetMetadata.return_value = self.metadata_resp
                thrift_backend = self._make_fake_thrift_backend()

                execute_response = thrift_backend._handle_execute_response(execute_resp, Mock())

                self.assertEqual(has_more_rows, execute_response.has_more_rows)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_handle_execute_response_reads_has_more_rows_in_result_response(
            self, tcli_service_class):
        for has_more_rows, resp_type in itertools.product([True, False],
                                                          self.execute_response_types):
            with self.subTest(has_more_rows=has_more_rows, resp_type=resp_type):
                tcli_service_instance = tcli_service_class.return_value
                results_mock = Mock()
                results_mock.startRowOffset = 0

                execute_resp = resp_type(
                    status=self.okay_status,
                    directResults=None,
                    operationHandle=self.operation_handle)

                fetch_results_resp = ttypes.TFetchResultsResp(
                    status=self.okay_status,
                    hasMoreRows=has_more_rows,
                    results=results_mock,
                )

                operation_status_resp = ttypes.TGetOperationStatusResp(
                    status=self.okay_status,
                    operationState=ttypes.TOperationState.FINISHED_STATE,
                    errorMessage="some information about the error")

                tcli_service_instance.FetchResults.return_value = fetch_results_resp
                tcli_service_instance.GetOperationStatus.return_value = operation_status_resp
                tcli_service_instance.GetResultSetMetadata.return_value = self.metadata_resp
                thrift_backend = self._make_fake_thrift_backend()

                thrift_backend._handle_execute_response(execute_resp, Mock())
                _, has_more_rows_resp = thrift_backend.fetch_results(Mock(), 1, 1, 0, Mock())

                self.assertEqual(has_more_rows, has_more_rows_resp)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_execute_statement_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.ExecuteStatement.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.execute_command("foo", Mock(), 100, 200, cursor_mock)
        # Check call to client
        req = tcli_service_instance.ExecuteStatement.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        self.assertEqual(req.statement, "foo")
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_get_catalogs_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetCatalogs.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.get_catalogs(Mock(), 100, 200, cursor_mock)
        # Check call to client
        req = tcli_service_instance.GetCatalogs.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_get_schemas_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetSchemas.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.get_schemas(
            Mock(),
            100,
            200,
            cursor_mock,
            catalog_name="catalog_pattern",
            schema_name="schema_pattern")
        # Check call to client
        req = tcli_service_instance.GetSchemas.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        self.assertEqual(req.catalogName, "catalog_pattern")
        self.assertEqual(req.schemaName, "schema_pattern")
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_get_tables_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetTables.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.get_tables(
            Mock(),
            100,
            200,
            cursor_mock,
            catalog_name="catalog_pattern",
            schema_name="schema_pattern",
            table_name="table_pattern",
            table_types=["type1", "type2"])
        # Check call to client
        req = tcli_service_instance.GetTables.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        self.assertEqual(req.catalogName, "catalog_pattern")
        self.assertEqual(req.schemaName, "schema_pattern")
        self.assertEqual(req.tableName, "table_pattern")
        self.assertEqual(req.tableTypes, ["type1", "type2"])
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_get_columns_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetColumns.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.get_columns(
            Mock(),
            100,
            200,
            cursor_mock,
            catalog_name="catalog_pattern",
            schema_name="schema_pattern",
            table_name="table_pattern",
            column_name="column_pattern")
        # Check call to client
        req = tcli_service_instance.GetColumns.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        self.assertEqual(req.catalogName, "catalog_pattern")
        self.assertEqual(req.schemaName, "schema_pattern")
        self.assertEqual(req.tableName, "table_pattern")
        self.assertEqual(req.columnName, "column_pattern")
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_open_session_uses_user_provided_session_id(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        tcli_service_instance.OpenSession.return_value = self.open_session_resp

        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend.open_session(0b111)
        self.assertEqual(tcli_service_instance.OpenSession.call_args[0][0].sessionId.guid, 0b111)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_open_session_user_provided_session_id_optional(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        tcli_service_instance.OpenSession.return_value = self.open_session_resp

        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend.open_session()
        self.assertEqual(len(tcli_service_instance.OpenSession.call_args_list), 1)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_op_handle_respected_in_close_command(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend.close_command(self.operation_handle)
        self.assertEqual(tcli_service_instance.CloseOperation.call_args[0][0].operationHandle,
                         self.operation_handle)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_session_handle_respected_in_close_session(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        thrift_backend.close_session(self.session_handle)
        self.assertEqual(tcli_service_instance.CloseSession.call_args[0][0].sessionHandle,
                         self.session_handle)

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_non_arrow_non_column_based_set_triggers_exception(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        results_mock = Mock()
        results_mock.startRowOffset = 0

        execute_statement_resp = ttypes.TExecuteStatementResp(
            status=self.okay_status, directResults=None, operationHandle=self.operation_handle)

        metadata_resp = ttypes.TGetResultSetMetadataResp(
            status=self.okay_status,
            resultFormat=ttypes.TSparkRowSetType.ROW_BASED_SET,
        )

        operation_status_resp = ttypes.TGetOperationStatusResp(
            status=self.okay_status,
            operationState=ttypes.TOperationState.FINISHED_STATE,
            errorMessage="some information about the error")

        tcli_service_instance.ExecuteStatement.return_value = execute_statement_resp
        tcli_service_instance.GetResultSetMetadata.return_value = metadata_resp
        tcli_service_instance.GetOperationStatus.return_value = operation_status_resp
        thrift_backend = self._make_fake_thrift_backend()

        with self.assertRaises(OperationalError) as cm:
            thrift_backend.execute_command("foo", Mock(), 100, 100, Mock())
        self.assertIn("Expected results to be in Arrow or column based format", str(cm.exception))

    def test_create_arrow_table_raises_error_for_unsupported_type(self):
        t_row_set = ttypes.TRowSet()
        thrift_backend = ThriftBackend("foobar", 443, "path", [])
        with self.assertRaises(OperationalError):
            thrift_backend._create_arrow_table(t_row_set, None)

    @patch.object(ThriftBackend, "_convert_arrow_based_set_to_arrow_table")
    @patch.object(ThriftBackend, "_convert_column_based_set_to_arrow_table")
    def test_create_arrow_table_calls_correct_conversion_method(self, convert_col_mock,
                                                                convert_arrow_mock):
        thrift_backend = ThriftBackend("foobar", 443, "path", [])

        schema = Mock()
        cols = Mock()
        arrow_batches = Mock()

        t_col_set = ttypes.TRowSet(columns=cols)
        thrift_backend._create_arrow_table(t_col_set, schema)
        convert_arrow_mock.assert_not_called()
        convert_col_mock.assert_called_once_with(cols, schema)

        t_arrow_set = ttypes.TRowSet(arrowBatches=arrow_batches)
        thrift_backend._create_arrow_table(t_arrow_set, schema)
        convert_arrow_mock.assert_called_once_with(arrow_batches, schema)
        convert_col_mock.assert_called_once_with(cols, schema)

    def test_convert_column_based_set_to_arrow_table_without_nulls(self):
        schema = pyarrow.schema([
            pyarrow.field("column1", pyarrow.int32()),
            pyarrow.field("column2", pyarrow.string()),
            pyarrow.field("column3", pyarrow.float64()),
            pyarrow.field("column3", pyarrow.binary())
        ])

        t_cols = [
            ttypes.TColumn(i32Val=ttypes.TI32Column(values=[1, 2, 3], nulls=bytes(1))),
            ttypes.TColumn(
                stringVal=ttypes.TStringColumn(values=["s1", "s2", "s3"], nulls=bytes(1))),
            ttypes.TColumn(doubleVal=ttypes.TDoubleColumn(values=[1.15, 2.2, 3.3], nulls=bytes(1))),
            ttypes.TColumn(
                binaryVal=ttypes.TBinaryColumn(values=[b'\x11', b'\x22', b'\x33'], nulls=bytes(1)))
        ]

        arrow_table, n_rows = ThriftBackend._convert_column_based_set_to_arrow_table(t_cols, schema)
        self.assertEqual(n_rows, 3)

        # Check schema, column names and types
        self.assertEqual(arrow_table.field(0).name, "column1")
        self.assertEqual(arrow_table.field(1).name, "column2")
        self.assertEqual(arrow_table.field(2).name, "column3")
        self.assertEqual(arrow_table.field(3).name, "column3")

        self.assertEqual(arrow_table.field(0).type, pyarrow.int32())
        self.assertEqual(arrow_table.field(1).type, pyarrow.string())
        self.assertEqual(arrow_table.field(2).type, pyarrow.float64())
        self.assertEqual(arrow_table.field(3).type, pyarrow.binary())

        # Check data
        self.assertEqual(arrow_table.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(arrow_table.column(1).to_pylist(), ["s1", "s2", "s3"])
        self.assertEqual(arrow_table.column(2).to_pylist(), [1.15, 2.2, 3.3])
        self.assertEqual(arrow_table.column(3).to_pylist(), [b'\x11', b'\x22', b'\x33'])

    def test_convert_column_based_set_to_arrow_table_with_nulls(self):
        schema = pyarrow.schema([
            pyarrow.field("column1", pyarrow.int32()),
            pyarrow.field("column2", pyarrow.string()),
            pyarrow.field("column3", pyarrow.float64()),
            pyarrow.field("column3", pyarrow.binary())
        ])

        t_cols = [
            ttypes.TColumn(i32Val=ttypes.TI32Column(values=[1, 2, 3], nulls=bytes([1]))),
            ttypes.TColumn(
                stringVal=ttypes.TStringColumn(values=["s1", "s2", "s3"], nulls=bytes([2]))),
            ttypes.TColumn(
                doubleVal=ttypes.TDoubleColumn(values=[1.15, 2.2, 3.3], nulls=bytes([4]))),
            ttypes.TColumn(
                binaryVal=ttypes.TBinaryColumn(
                    values=[b'\x11', b'\x22', b'\x33'], nulls=bytes([3])))
        ]

        arrow_table, n_rows = ThriftBackend._convert_column_based_set_to_arrow_table(t_cols, schema)
        self.assertEqual(n_rows, 3)

        # Check data
        self.assertEqual(arrow_table.column(0).to_pylist(), [None, 2, 3])
        self.assertEqual(arrow_table.column(1).to_pylist(), ["s1", None, "s3"])
        self.assertEqual(arrow_table.column(2).to_pylist(), [1.15, 2.2, None])
        self.assertEqual(arrow_table.column(3).to_pylist(), [None, None, b'\x33'])

    def test_convert_column_based_set_to_arrow_table_uses_types_from_col_set(self):
        schema = pyarrow.schema([
            pyarrow.field("column1", pyarrow.string()),
            pyarrow.field("column2", pyarrow.string()),
            pyarrow.field("column3", pyarrow.string()),
            pyarrow.field("column3", pyarrow.string())
        ])

        t_cols = [
            ttypes.TColumn(i32Val=ttypes.TI32Column(values=[1, 2, 3], nulls=bytes(1))),
            ttypes.TColumn(
                stringVal=ttypes.TStringColumn(values=["s1", "s2", "s3"], nulls=bytes(1))),
            ttypes.TColumn(doubleVal=ttypes.TDoubleColumn(values=[1.15, 2.2, 3.3], nulls=bytes(1))),
            ttypes.TColumn(
                binaryVal=ttypes.TBinaryColumn(values=[b'\x11', b'\x22', b'\x33'], nulls=bytes(1)))
        ]

        arrow_table, n_rows = ThriftBackend._convert_column_based_set_to_arrow_table(t_cols, schema)
        self.assertEqual(n_rows, 3)

        # Check schema, column names and types
        self.assertEqual(arrow_table.field(0).name, "column1")
        self.assertEqual(arrow_table.field(1).name, "column2")
        self.assertEqual(arrow_table.field(2).name, "column3")
        self.assertEqual(arrow_table.field(3).name, "column3")

        self.assertEqual(arrow_table.field(0).type, pyarrow.int32())
        self.assertEqual(arrow_table.field(1).type, pyarrow.string())
        self.assertEqual(arrow_table.field(2).type, pyarrow.float64())
        self.assertEqual(arrow_table.field(3).type, pyarrow.binary())

        # Check data
        self.assertEqual(arrow_table.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(arrow_table.column(1).to_pylist(), ["s1", "s2", "s3"])
        self.assertEqual(arrow_table.column(2).to_pylist(), [1.15, 2.2, 3.3])
        self.assertEqual(arrow_table.column(3).to_pylist(), [b'\x11', b'\x22', b'\x33'])

    @patch("databricks.sql.thrift_backend.TCLIService.Client")
    def test_cancel_command_uses_active_op_handle(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value

        thrift_backend = self._make_fake_thrift_backend()
        active_op_handle_mock = Mock()
        thrift_backend.cancel_command(active_op_handle_mock)

        self.assertEqual(tcli_service_instance.CancelOperation.call_args[0][0].operationHandle,
                         active_op_handle_mock)

    def test_handle_execute_response_sets_active_op_handle(self):
        thrift_backend = self._make_fake_thrift_backend()
        thrift_backend._check_direct_results_for_error = Mock()
        thrift_backend._wait_until_command_done = Mock()
        thrift_backend._results_message_to_execute_response = Mock()
        mock_resp = Mock()
        mock_cursor = Mock()

        thrift_backend._handle_execute_response(mock_resp, mock_cursor)

        self.assertEqual(mock_resp.operationHandle, mock_cursor.active_op_handle)


if __name__ == '__main__':
    unittest.main()
