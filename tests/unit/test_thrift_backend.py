from collections import OrderedDict
from decimal import Decimal
import itertools
import unittest
from unittest.mock import patch, MagicMock, Mock
from ssl import CERT_NONE, CERT_REQUIRED

import pyarrow

import databricks.sql
from databricks.sql import utils
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql import *
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.thrift_backend import ThriftBackend


def retry_policy_factory():
    return {                          # (type, default, min, max)
        "_retry_delay_min":                     (float, 1, None, None),
        "_retry_delay_max":                     (float, 60, None, None),
        "_retry_stop_after_attempts_count":     (int, 30, None, None),
        "_retry_stop_after_attempts_duration":  (float, 900, None, None),
        "_retry_delay_default":                 (float, 5, 1, 60)
    }


class ThriftBackendTestSuite(unittest.TestCase):
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
        mock_method = Mock()
        mock_method.__name__ = "method name"
        mock_method.return_value = mock_response
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        with self.assertRaises(DatabaseError):
            thrift_backend.make_request(mock_method, Mock())

    def _make_type_desc(self, type):
        return ttypes.TTypeDesc(types=[ttypes.TTypeEntry(ttypes.TTAllowedParameterValueEntry(type=type))])

    def _make_fake_thrift_backend(self):
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        thrift_backend._hive_schema_to_arrow_schema = Mock()
        thrift_backend._hive_schema_to_description = Mock()
        thrift_backend._create_arrow_table = MagicMock()
        thrift_backend._create_arrow_table.return_value = (MagicMock(), Mock())
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

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
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
        ]

        for protocol_version in bad_protocol_versions:
            t_http_client_instance.OpenSession.return_value = ttypes.TOpenSessionResp(
                status=self.okay_status, serverProtocolVersion=protocol_version)

            with self.assertRaises(OperationalError) as cm:
                thrift_backend = self._make_fake_thrift_backend()
                thrift_backend.open_session({}, None, None)

            self.assertIn("expected server to use a protocol version", str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_okay_protocol_versions_succeed(self, tcli_service_client_cass):
        t_http_client_instance = tcli_service_client_cass.return_value
        good_protocol_versions = [
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2,
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3,
            ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4
        ]

        for protocol_version in good_protocol_versions:
            t_http_client_instance.OpenSession.return_value = ttypes.TOpenSessionResp(
                status=self.okay_status, serverProtocolVersion=protocol_version)

            thrift_backend = self._make_fake_thrift_backend()
            thrift_backend.open_session({}, None, None)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    def test_headers_are_set(self, t_http_client_class):
        ThriftBackend("foo", 123, "bar", [("header", "value")], auth_provider=AuthProvider())
        t_http_client_class.return_value.setCustomHeaders.assert_called_with({"header": "value"})

    def test_proxy_headers_are_set(self):

        from databricks.sql.auth.thrift_http_client import THttpClient
        from urllib.parse import urlparse

        fake_proxy_spec = "https://someuser:somepassword@8.8.8.8:12340"
        parsed_proxy = urlparse(fake_proxy_spec)
        
        try:
            result = THttpClient.basic_proxy_auth_header(parsed_proxy)
        except TypeError as e:
            assert False

        assert isinstance(result, type(str()))

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    @patch("databricks.sql.thrift_backend.create_default_context")
    def test_tls_cert_args_are_propagated(self, mock_create_default_context, t_http_client_class):
        mock_cert_key_file = Mock()
        mock_cert_key_password = Mock()
        mock_trusted_ca_file = Mock()
        mock_cert_file = Mock()

        ThriftBackend(
            "foo",
            123,
            "bar", [],
            auth_provider=AuthProvider(),
            _tls_client_cert_file=mock_cert_file,
            _tls_client_cert_key_file=mock_cert_key_file,
            _tls_client_cert_key_password=mock_cert_key_password,
            _tls_trusted_ca_file=mock_trusted_ca_file)

        mock_create_default_context.assert_called_once_with(cafile=mock_trusted_ca_file)
        mock_ssl_context = mock_create_default_context.return_value
        mock_ssl_context.load_cert_chain.assert_called_once_with(
            certfile=mock_cert_file, keyfile=mock_cert_key_file, password=mock_cert_key_password)
        self.assertTrue(mock_ssl_context.check_hostname)
        self.assertEqual(mock_ssl_context.verify_mode, CERT_REQUIRED)
        self.assertEqual(t_http_client_class.call_args[1]["ssl_context"], mock_ssl_context)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    @patch("databricks.sql.thrift_backend.create_default_context")
    def test_tls_no_verify_is_respected(self, mock_create_default_context, t_http_client_class):
        ThriftBackend("foo", 123, "bar", [], auth_provider=AuthProvider(), _tls_no_verify=True)

        mock_ssl_context = mock_create_default_context.return_value
        self.assertFalse(mock_ssl_context.check_hostname)
        self.assertEqual(mock_ssl_context.verify_mode, CERT_NONE)
        self.assertEqual(t_http_client_class.call_args[1]["ssl_context"], mock_ssl_context)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    @patch("databricks.sql.thrift_backend.create_default_context")
    def test_tls_verify_hostname_is_respected(self, mock_create_default_context,
                                              t_http_client_class):
        ThriftBackend("foo", 123, "bar", [], auth_provider=AuthProvider(), _tls_verify_hostname=False)

        mock_ssl_context = mock_create_default_context.return_value
        self.assertFalse(mock_ssl_context.check_hostname)
        self.assertEqual(mock_ssl_context.verify_mode, CERT_REQUIRED)
        self.assertEqual(t_http_client_class.call_args[1]["ssl_context"], mock_ssl_context)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    def test_port_and_host_are_respected(self, t_http_client_class):
        ThriftBackend("hostname", 123, "path_value", [], auth_provider=AuthProvider())
        self.assertEqual(t_http_client_class.call_args[1]["uri_or_host"],
                         "https://hostname:123/path_value")

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    def test_host_with_https_does_not_duplicate(self, t_http_client_class):
        ThriftBackend("https://hostname", 123, "path_value", [], auth_provider=AuthProvider())
        self.assertEqual(t_http_client_class.call_args[1]["uri_or_host"],
                         "https://hostname:123/path_value")
        
    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    def test_host_with_trailing_backslash_does_not_duplicate(self, t_http_client_class):
        ThriftBackend("https://hostname/", 123, "path_value", [], auth_provider=AuthProvider())
        self.assertEqual(t_http_client_class.call_args[1]["uri_or_host"],
                         "https://hostname:123/path_value")        

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    def test_socket_timeout_is_propagated(self, t_http_client_class):
        ThriftBackend("hostname", 123, "path_value", [], auth_provider=AuthProvider(), _socket_timeout=129)
        self.assertEqual(t_http_client_class.return_value.setTimeout.call_args[0][0], 129 * 1000)
        ThriftBackend("hostname", 123, "path_value", [], auth_provider=AuthProvider(), _socket_timeout=0)
        self.assertEqual(t_http_client_class.return_value.setTimeout.call_args[0][0], 0)
        ThriftBackend("hostname", 123, "path_value", [], auth_provider=AuthProvider())
        self.assertEqual(t_http_client_class.return_value.setTimeout.call_args[0][0], 900 * 1000)
        ThriftBackend("hostname", 123, "path_value", [], auth_provider=AuthProvider(), _socket_timeout=None)
        self.assertEqual(t_http_client_class.return_value.setTimeout.call_args[0][0], None)

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

    def test_hive_schema_to_description_preserves_scale_and_precision(self):
        columns = [
            ttypes.TColumnDesc(
                columnName="column 1",
                typeDesc=ttypes.TTypeDesc(types=[
                    ttypes.TTypeEntry(
                        ttypes.TTAllowedParameterValueEntry(
                            type=ttypes.TTypeId.DECIMAL_TYPE,
                            typeQualifiers=ttypes.TTypeQualifiers(
                                qualifiers={
                                    "precision": ttypes.TTypeQualifierValue(i32Value=10),
                                    "scale": ttypes.TTypeQualifierValue(i32Value=100),
                                })))
                ])),
        ]
        t_table_schema = ttypes.TTableSchema(columns)

        description = ThriftBackend._hive_schema_to_description(t_table_schema)
        self.assertEqual(description, [
            ("column 1", "decimal", None, None, 10, 100, None),
        ])

    def test_make_request_checks_status_code(self):
        error_codes = [ttypes.TStatusCode.ERROR_STATUS, ttypes.TStatusCode.INVALID_HANDLE_STATUS]
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

        for code in error_codes:
            mock_error_response = Mock()
            mock_error_response.status.statusCode = code
            mock_error_response.status.errorMessage = "a detailed error message"
            with self.assertRaises(DatabaseError) as cm:
                thrift_backend.make_request(lambda _: mock_error_response, Mock())
            self.assertIn("a detailed error message", str(cm.exception))

        success_codes = [
            ttypes.TStatusCode.SUCCESS_STATUS, ttypes.TStatusCode.SUCCESS_WITH_INFO_STATUS,
            ttypes.TStatusCode.STILL_EXECUTING_STATUS
        ]

        for code in success_codes:
            mock_response = Mock()
            mock_response.status.statusCode = code
            thrift_backend.make_request(lambda _: mock_response, Mock())

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
                thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

                with self.assertRaises(DatabaseError) as cm:
                    thrift_backend._handle_execute_response(t_execute_resp, Mock())
                self.assertIn("some information about the error", str(cm.exception))

    @patch("databricks.sql.utils.ResultSetQueueFactory.build_queue", return_value=Mock())
    def test_handle_execute_response_sets_compression_in_direct_results(self, build_queue):
        for resp_type in self.execute_response_types:
            lz4Compressed=Mock()
            resultSet=MagicMock()
            resultSet.results.startRowOffset = 0
            t_execute_resp = resp_type(
                status=Mock(),
                operationHandle=Mock(),
                directResults=ttypes.TSparkDirectResults(
                    operationStatus= Mock(),
                    resultSetMetadata=ttypes.TGetResultSetMetadataResp(
                        status=self.okay_status,
                        resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET,
                        schema=MagicMock(),
                        arrowSchema=MagicMock(),
                        lz4Compressed=lz4Compressed),
                    resultSet=resultSet,
                    closeOperation=None))
            thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

            execute_response = thrift_backend._handle_execute_response(t_execute_resp, Mock())
            self.assertEqual(execute_response.lz4_compressed, lz4Compressed)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
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
                thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

                with self.assertRaises(DatabaseError) as cm:
                    thrift_backend._handle_execute_response(t_execute_resp, Mock())
                if op_state_resp.errorMessage:
                    self.assertIn(op_state_resp.errorMessage, str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_get_status_uses_display_message_if_available(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value

        display_message = "simple message"
        diagnostic_info = "diagnostic info"
        t_get_operation_status_resp = ttypes.TGetOperationStatusResp(
            status=self.okay_status,
            operationState=ttypes.TOperationState.ERROR_STATE,
            errorMessage="foo",
            displayMessage=display_message,
            diagnosticInfo=diagnostic_info)

        t_execute_resp = ttypes.TExecuteStatementResp(
            status=self.okay_status, directResults=None, operationHandle=self.operation_handle)
        tcli_service_instance.GetOperationStatus.return_value = t_get_operation_status_resp
        tcli_service_instance.ExecuteStatement.return_value = t_execute_resp

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        with self.assertRaises(DatabaseError) as cm:
            thrift_backend.execute_command(Mock(), Mock(), 100, 100, Mock(), Mock())

        self.assertEqual(display_message, str(cm.exception))
        self.assertIn(diagnostic_info, str(cm.exception.message_with_context()))

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_direct_results_uses_display_message_if_available(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value

        display_message = "simple message"
        diagnostic_info = "diagnostic info"
        t_get_operation_status_resp = ttypes.TGetOperationStatusResp(
            status=self.okay_status,
            operationState=ttypes.TOperationState.ERROR_STATE,
            errorMessage="foo",
            displayMessage=display_message,
            diagnosticInfo=diagnostic_info)

        t_execute_resp = ttypes.TExecuteStatementResp(
            status=self.okay_status,
            directResults=ttypes.TSparkDirectResults(
                operationStatus=t_get_operation_status_resp,
                resultSetMetadata=None,
                resultSet=None,
                closeOperation=None))

        tcli_service_instance.ExecuteStatement.return_value = t_execute_resp

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        with self.assertRaises(DatabaseError) as cm:
            thrift_backend.execute_command(Mock(), Mock(), 100, 100, Mock(), Mock())

        self.assertEqual(display_message, str(cm.exception))
        self.assertIn(diagnostic_info, str(cm.exception.message_with_context()))

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
                    thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

                    with self.assertRaises(DatabaseError) as cm:
                        thrift_backend._handle_execute_response(error_resp, Mock())
                    self.assertIn("this is a bad error", str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
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
                thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
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

                thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
                thrift_backend._results_message_to_execute_response = Mock()

                thrift_backend._handle_execute_response(execute_resp, Mock())

                thrift_backend._results_message_to_execute_response.assert_called_with(
                    execute_resp,
                    ttypes.TOperationState.FINISHED_STATE,
                )

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_use_arrow_schema_if_available(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        arrow_schema_mock = MagicMock(name="Arrow schema mock")
        hive_schema_mock = MagicMock(name="Hive schema mock")

        t_get_result_set_metadata_resp = ttypes.TGetResultSetMetadataResp(
            status=self.okay_status,
            resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET,
            schema=hive_schema_mock,
            arrowSchema=arrow_schema_mock)

        t_execute_resp = ttypes.TExecuteStatementResp(
            status=self.okay_status,
            directResults=None,
            operationHandle=self.operation_handle,
        )

        tcli_service_instance.GetResultSetMetadata.return_value = t_get_result_set_metadata_resp
        thrift_backend = self._make_fake_thrift_backend()
        execute_response = thrift_backend._handle_execute_response(t_execute_resp, Mock())

        self.assertEqual(execute_response.arrow_schema_bytes, arrow_schema_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_fall_back_to_hive_schema_if_no_arrow_schema(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        hive_schema_mock = MagicMock(name="Hive schema mock")

        hive_schema_req = ttypes.TGetResultSetMetadataResp(
            status=self.okay_status,
            resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET,
            arrowSchema=None,
            schema=hive_schema_mock)

        t_execute_resp = ttypes.TExecuteStatementResp(
            status=self.okay_status,
            directResults=None,
            operationHandle=self.operation_handle,
        )

        tcli_service_instance.GetResultSetMetadata.return_value = hive_schema_req
        thrift_backend = self._make_fake_thrift_backend()
        thrift_backend._handle_execute_response(t_execute_resp, Mock())

        self.assertEqual(hive_schema_mock,
                         thrift_backend._hive_schema_to_arrow_schema.call_args[0][0])

    @patch("databricks.sql.utils.ResultSetQueueFactory.build_queue", return_value=Mock())
    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_handle_execute_response_reads_has_more_rows_in_direct_results(
            self, tcli_service_class, build_queue):
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

    @patch("databricks.sql.utils.ResultSetQueueFactory.build_queue", return_value=Mock())
    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_handle_execute_response_reads_has_more_rows_in_result_response(
            self, tcli_service_class, build_queue):
        for has_more_rows, resp_type in itertools.product([True, False],
                                                          self.execute_response_types):
            with self.subTest(has_more_rows=has_more_rows, resp_type=resp_type):
                tcli_service_instance = tcli_service_class.return_value
                results_mock = MagicMock()
                results_mock.startRowOffset = 0

                execute_resp = resp_type(
                    status=self.okay_status,
                    directResults=None,
                    operationHandle=self.operation_handle)

                fetch_results_resp = ttypes.TFetchResultsResp(
                    status=self.okay_status,
                    hasMoreRows=has_more_rows,
                    results=results_mock,
                    resultSetMetadata=ttypes.TGetResultSetMetadataResp(
                        resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET
                    )
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
                _, has_more_rows_resp = thrift_backend.fetch_results(
                    op_handle=Mock(),
                    max_rows=1,
                    max_bytes=1,
                    expected_row_start_offset=0,
                    lz4_compressed=False,
                    arrow_schema_bytes=Mock(),
                    description=Mock())

                self.assertEqual(has_more_rows, has_more_rows_resp)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_arrow_batches_row_count_are_respected(self, tcli_service_class):
        # make some semi-real arrow batches and check the number of rows is correct in the queue
        tcli_service_instance = tcli_service_class.return_value
        t_fetch_results_resp = ttypes.TFetchResultsResp(
            status=self.okay_status,
            hasMoreRows=False,
            results=ttypes.TRowSet(
                startRowOffset=0,
                rows=[],
                arrowBatches=[
                    ttypes.TSparkArrowBatch(batch=bytearray(), rowCount=15) for _ in range(10)
                ]
            ),
            resultSetMetadata=ttypes.TGetResultSetMetadataResp(
                resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET
            )
        )
        tcli_service_instance.FetchResults.return_value = t_fetch_results_resp
        schema = pyarrow.schema([
            pyarrow.field("column1", pyarrow.int32()),
            pyarrow.field("column2", pyarrow.string()),
            pyarrow.field("column3", pyarrow.float64()),
            pyarrow.field("column3", pyarrow.binary())
        ]).serialize().to_pybytes()

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        arrow_queue, has_more_results = thrift_backend.fetch_results(
            op_handle=Mock(),
            max_rows=1,
            max_bytes=1,
            expected_row_start_offset=0,
            lz4_compressed=False,
            arrow_schema_bytes=schema,
            description=MagicMock())

        self.assertEqual(arrow_queue.n_valid_rows, 15 * 10)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_execute_statement_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.ExecuteStatement.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.execute_command("foo", Mock(), 100, 200, Mock(), cursor_mock)
        # Check call to client
        req = tcli_service_instance.ExecuteStatement.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        self.assertEqual(req.statement, "foo")
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_get_catalogs_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetCatalogs.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        thrift_backend._handle_execute_response = Mock()
        cursor_mock = Mock()

        thrift_backend.get_catalogs(Mock(), 100, 200, cursor_mock)
        # Check call to client
        req = tcli_service_instance.GetCatalogs.call_args[0][0]
        get_direct_results = ttypes.TSparkGetDirectResults(maxRows=100, maxBytes=200)
        self.assertEqual(req.getDirectResults, get_direct_results)
        # Check response handling
        thrift_backend._handle_execute_response.assert_called_with(response, cursor_mock)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_get_schemas_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetSchemas.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
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

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_get_tables_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetTables.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
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

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_get_columns_calls_client_and_handle_execute_response(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        response = Mock()
        tcli_service_instance.GetColumns.return_value = response
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
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

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_open_session_user_provided_session_id_optional(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        tcli_service_instance.OpenSession.return_value = self.open_session_resp

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        thrift_backend.open_session({}, None, None)
        self.assertEqual(len(tcli_service_instance.OpenSession.call_args_list), 1)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_op_handle_respected_in_close_command(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        thrift_backend.close_command(self.operation_handle)
        self.assertEqual(tcli_service_instance.CloseOperation.call_args[0][0].operationHandle,
                         self.operation_handle)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_session_handle_respected_in_close_session(self, tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        thrift_backend.close_session(self.session_handle)
        self.assertEqual(tcli_service_instance.CloseSession.call_args[0][0].sessionHandle,
                         self.session_handle)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
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
            thrift_backend.execute_command("foo", Mock(), 100, 100, Mock(), Mock())
        self.assertIn("Expected results to be in Arrow or column based format", str(cm.exception))

    def test_create_arrow_table_raises_error_for_unsupported_type(self):
        t_row_set = ttypes.TRowSet()
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        with self.assertRaises(OperationalError):
            thrift_backend._create_arrow_table(t_row_set, Mock(), None, Mock())

    @patch("databricks.sql.thrift_backend.convert_arrow_based_set_to_arrow_table")
    @patch("databricks.sql.thrift_backend.convert_column_based_set_to_arrow_table")
    def test_create_arrow_table_calls_correct_conversion_method(self, convert_col_mock,
                                                                convert_arrow_mock):
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        convert_arrow_mock.return_value = (MagicMock(), Mock())
        convert_col_mock.return_value = (MagicMock(), Mock())

        schema = Mock()
        cols = Mock()
        arrow_batches = Mock()
        lz4_compressed = Mock()
        description = Mock()

        t_col_set = ttypes.TRowSet(columns=cols)
        thrift_backend._create_arrow_table(t_col_set, lz4_compressed, schema, description)
        convert_arrow_mock.assert_not_called()
        convert_col_mock.assert_called_once_with(cols, description)

        t_arrow_set = ttypes.TRowSet(arrowBatches=arrow_batches)
        thrift_backend._create_arrow_table(t_arrow_set, lz4_compressed, schema, Mock())
        convert_arrow_mock.assert_called_once_with(arrow_batches, lz4_compressed, schema)

    @patch("lz4.frame.decompress")
    @patch("pyarrow.ipc.open_stream")
    def test_convert_arrow_based_set_to_arrow_table(self, open_stream_mock, lz4_decompress_mock):
        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        
        lz4_decompress_mock.return_value = bytearray('Testing','utf-8')

        schema = pyarrow.schema([
            pyarrow.field("column1", pyarrow.int32()),
        ]).serialize().to_pybytes()
        
        arrow_batches = [ttypes.TSparkArrowBatch(batch=bytearray('Testing','utf-8'), rowCount=1) for _ in range(10)]
        utils.convert_arrow_based_set_to_arrow_table(arrow_batches, False, schema)
        lz4_decompress_mock.assert_not_called()

        utils.convert_arrow_based_set_to_arrow_table(arrow_batches, True, schema)
        lz4_decompress_mock.assert_called()

    def test_convert_column_based_set_to_arrow_table_without_nulls(self):
        # Deliberately duplicate the column name to check that dups work
        field_names = ["column1", "column2", "column3", "column3"]
        description = [(name, ) for name in field_names]

        t_cols = [
            ttypes.TColumn(i32Val=ttypes.TI32Column(values=[1, 2, 3], nulls=bytes(1))),
            ttypes.TColumn(
                stringVal=ttypes.TStringColumn(values=["s1", "s2", "s3"], nulls=bytes(1))),
            ttypes.TColumn(doubleVal=ttypes.TDoubleColumn(values=[1.15, 2.2, 3.3], nulls=bytes(1))),
            ttypes.TColumn(
                binaryVal=ttypes.TBinaryColumn(values=[b'\x11', b'\x22', b'\x33'], nulls=bytes(1)))
        ]

        arrow_table, n_rows = utils.convert_column_based_set_to_arrow_table(
            t_cols, description)
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
        field_names = ["column1", "column2", "column3", "column3"]
        description = [(name, ) for name in field_names]

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

        arrow_table, n_rows = utils.convert_column_based_set_to_arrow_table(
            t_cols, description)
        self.assertEqual(n_rows, 3)

        # Check data
        self.assertEqual(arrow_table.column(0).to_pylist(), [None, 2, 3])
        self.assertEqual(arrow_table.column(1).to_pylist(), ["s1", None, "s3"])
        self.assertEqual(arrow_table.column(2).to_pylist(), [1.15, 2.2, None])
        self.assertEqual(arrow_table.column(3).to_pylist(), [None, None, b'\x33'])

    def test_convert_column_based_set_to_arrow_table_uses_types_from_col_set(self):
        field_names = ["column1", "column2", "column3", "column3"]
        description = [(name, ) for name in field_names]

        t_cols = [
            ttypes.TColumn(i32Val=ttypes.TI32Column(values=[1, 2, 3], nulls=bytes(1))),
            ttypes.TColumn(
                stringVal=ttypes.TStringColumn(values=["s1", "s2", "s3"], nulls=bytes(1))),
            ttypes.TColumn(doubleVal=ttypes.TDoubleColumn(values=[1.15, 2.2, 3.3], nulls=bytes(1))),
            ttypes.TColumn(
                binaryVal=ttypes.TBinaryColumn(values=[b'\x11', b'\x22', b'\x33'], nulls=bytes(1)))
        ]

        arrow_table, n_rows = utils.convert_column_based_set_to_arrow_table(
            t_cols, description)
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

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
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

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    @patch("databricks.sql.thrift_api.TCLIService.TCLIService.Client.GetOperationStatus")
    @patch("databricks.sql.thrift_backend._retry_policy", new_callable=retry_policy_factory)
    def test_make_request_will_retry_GetOperationStatus(
            self, mock_retry_policy, mock_GetOperationStatus, t_transport_class):

        import thrift, errno
        from databricks.sql.thrift_api.TCLIService.TCLIService import Client
        from databricks.sql.exc import RequestError
        from databricks.sql.utils import NoRetryReason

        this_gos_name = "GetOperationStatus"
        mock_GetOperationStatus.__name__ = this_gos_name
        mock_GetOperationStatus.side_effect = OSError(errno.ETIMEDOUT, "Connection timed out")

        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(t_transport_class)
        client = Client(protocol)

        req = ttypes.TGetOperationStatusReq(
            operationHandle=self.operation_handle,
            getProgressUpdate=False,
        )

        EXPECTED_RETRIES = 2

        thrift_backend = ThriftBackend(
            "foobar",
            443,
            "path", [],
            auth_provider=AuthProvider(),
            _retry_stop_after_attempts_count=EXPECTED_RETRIES,
            _retry_delay_default=1)


        with self.assertRaises(RequestError) as cm:
            thrift_backend.make_request(client.GetOperationStatus, req)

        self.assertEqual(NoRetryReason.OUT_OF_ATTEMPTS.value, cm.exception.context["no-retry-reason"])
        self.assertEqual(f'{EXPECTED_RETRIES}/{EXPECTED_RETRIES}', cm.exception.context["attempt"])

        # Unusual OSError code
        mock_GetOperationStatus.side_effect = OSError(errno.EEXIST, "File does not exist")

        with self.assertLogs("databricks.sql.thrift_backend", level=logging.WARNING) as cm:
            with self.assertRaises(RequestError):
                thrift_backend.make_request(client.GetOperationStatus, req)

        # There should be two warning log messages: one for each retry
        self.assertEqual(len(cm.output), EXPECTED_RETRIES)

        # The warnings should be identical
        self.assertEqual(cm.output[1], cm.output[0])

        # The warnings should include this text
        self.assertIn(f"{this_gos_name} failed with code {errno.EEXIST} and will attempt to retry", cm.output[0])

    @patch("databricks.sql.thrift_api.TCLIService.TCLIService.Client.GetOperationStatus")
    @patch("databricks.sql.thrift_backend._retry_policy", new_callable=retry_policy_factory)
    def test_make_request_will_retry_GetOperationStatus_for_http_error(
            self, mock_retry_policy, mock_gos):

        import urllib3.exceptions
        mock_gos.side_effect = urllib3.exceptions.HTTPError("Read timed out")

        import thrift, errno
        from databricks.sql.thrift_api.TCLIService.TCLIService import Client
        from databricks.sql.exc import RequestError
        from databricks.sql.utils import NoRetryReason
        from databricks.sql.auth.thrift_http_client import THttpClient

        this_gos_name = "GetOperationStatus"
        mock_gos.__name__ = this_gos_name

        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(THttpClient)
        client = Client(protocol)

        req = ttypes.TGetOperationStatusReq(
            operationHandle=self.operation_handle,
            getProgressUpdate=False,
        )

        EXPECTED_RETRIES = 2

        thrift_backend = ThriftBackend(
            "foobar",
            443,
            "path", [],
            auth_provider=AuthProvider(),
            _retry_stop_after_attempts_count=EXPECTED_RETRIES,
            _retry_delay_default=1)


        with self.assertRaises(RequestError) as cm:
            thrift_backend.make_request(client.GetOperationStatus, req)


        self.assertEqual(NoRetryReason.OUT_OF_ATTEMPTS.value, cm.exception.context["no-retry-reason"])
        self.assertEqual(f'{EXPECTED_RETRIES}/{EXPECTED_RETRIES}', cm.exception.context["attempt"])




    @patch("thrift.transport.THttpClient.THttpClient")
    def test_make_request_wont_retry_if_headers_not_present(self, t_transport_class):
        t_transport_instance = t_transport_class.return_value
        t_transport_instance.code = 429
        t_transport_instance.headers = {"foo": "bar"}
        mock_method = Mock()
        mock_method.__name__ = "method name"
        mock_method.side_effect = Exception("This method fails")

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

        with self.assertRaises(OperationalError) as cm:
            thrift_backend.make_request(mock_method, Mock())

        self.assertIn("This method fails", str(cm.exception.message_with_context()))

    @patch("thrift.transport.THttpClient.THttpClient")
    def test_make_request_wont_retry_if_error_code_not_429_or_503(self, t_transport_class):
        t_transport_instance = t_transport_class.return_value
        t_transport_instance.code = 430
        t_transport_instance.headers = {"Retry-After": "1"}
        mock_method = Mock()
        mock_method.__name__ = "method name"
        mock_method.side_effect = Exception("This method fails")

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

        with self.assertRaises(OperationalError) as cm:
            thrift_backend.make_request(mock_method, Mock())

        self.assertIn("This method fails", str(cm.exception.message_with_context()))

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    @patch("databricks.sql.thrift_backend._retry_policy", new_callable=retry_policy_factory)
    def test_make_request_will_retry_stop_after_attempts_count_if_retryable(
            self, mock_retry_policy, t_transport_class):
        t_transport_instance = t_transport_class.return_value
        t_transport_instance.code = 429
        t_transport_instance.headers = {"Retry-After": "0"}
        mock_method = Mock()
        mock_method.__name__ = "method name"
        mock_method.side_effect = Exception("This method fails")

        thrift_backend = ThriftBackend(
            "foobar",
            443,
            "path", [],
            auth_provider=AuthProvider(),
            _retry_stop_after_attempts_count=14,
            _retry_delay_max=0,
            _retry_delay_min=0)

        with self.assertRaises(OperationalError) as cm:
            thrift_backend.make_request(mock_method, Mock())

        self.assertIn("This method fails", cm.exception.message_with_context())
        self.assertIn("14/14", cm.exception.message_with_context())

        self.assertEqual(mock_method.call_count, 14)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient")
    def test_make_request_will_read_error_message_headers_if_set(self, t_transport_class):
        t_transport_instance = t_transport_class.return_value
        mock_method = Mock()
        mock_method.__name__ = "method name"
        mock_method.side_effect = Exception("This method fails")

        thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

        error_headers = [[("x-thriftserver-error-message", "thrift server error message")],
                         [("x-databricks-error-or-redirect-message", "databricks error message")],
                         [("x-databricks-error-or-redirect-message", "databricks error message"),
                          ("x-databricks-reason-phrase", "databricks error reason")],
                         [("x-thriftserver-error-message", "thrift server error message"),
                          ("x-databricks-error-or-redirect-message", "databricks error message"),
                          ("x-databricks-reason-phrase", "databricks error reason")],
                         [("x-thriftserver-error-message", "thrift server error message"),
                          ("x-databricks-error-or-redirect-message", "databricks error message")]]

        for headers in error_headers:
            t_transport_instance.headers = dict(headers)
            with self.assertRaises(OperationalError) as cm:
                thrift_backend.make_request(mock_method, Mock())

            for header in headers:
                self.assertIn(header[1], str(cm.exception))

    @staticmethod
    def make_table_and_desc(height, n_decimal_cols, width, precision, scale, int_constant,
                            decimal_constant):
        int_col = [int_constant for _ in range(height)]
        decimal_col = [decimal_constant for _ in range(height)]
        data = OrderedDict({"col{}".format(i): int_col for i in range(width - n_decimal_cols)})
        decimals = OrderedDict({"col_dec{}".format(i): decimal_col for i in range(n_decimal_cols)})
        data.update(decimals)

        int_desc = ([("", "int")] * (width - n_decimal_cols))
        decimal_desc = ([("", "decimal", None, None, precision, scale, None)] * n_decimal_cols)
        description = int_desc + decimal_desc

        table = pyarrow.Table.from_pydict(data)
        return table, description

    def test_arrow_decimal_conversion(self):
        # Broader tests in DecimalTestSuite
        width = 10
        int_constant = 12345
        precision, scale = 10, 5
        decimal_constant = "1.345"

        for n_decimal_cols in [0, 1, 10]:
            for height in [0, 1, 10]:
                with self.subTest(n_decimal_cols=n_decimal_cols, height=height):
                    table, description = self.make_table_and_desc(height, n_decimal_cols, width,
                                                                  precision, scale, int_constant,
                                                                  decimal_constant)
                    decimal_converted_table = utils.convert_decimals_in_arrow_table(
                        table, description)

                    for i in range(width):
                        if height > 0:
                            if i < width - n_decimal_cols:
                                self.assertEqual(
                                    decimal_converted_table.field(i).type, pyarrow.int64())
                            else:
                                self.assertEqual(
                                    decimal_converted_table.field(i).type,
                                    pyarrow.decimal128(precision=precision, scale=scale))

                    int_col = [int_constant for _ in range(height)]
                    decimal_col = [Decimal(decimal_constant) for _ in range(height)]
                    expected_result = OrderedDict(
                        {"col{}".format(i): int_col
                         for i in range(width - n_decimal_cols)})
                    decimals = OrderedDict(
                        {"col_dec{}".format(i): decimal_col
                         for i in range(n_decimal_cols)})
                    expected_result.update(decimals)

                    self.assertEqual(decimal_converted_table.to_pydict(), expected_result)

    @patch("thrift.transport.THttpClient.THttpClient")
    def test_retry_args_passthrough(self, mock_http_client):
        retry_delay_args = {
            "_retry_delay_min": 6,
            "_retry_delay_max": 10,
            "_retry_stop_after_attempts_count": 1,
            "_retry_stop_after_attempts_duration": 100
        }
        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider(), **retry_delay_args)
        for (arg, val) in retry_delay_args.items():
            self.assertEqual(getattr(backend, arg), val)

    @patch("thrift.transport.THttpClient.THttpClient")
    def test_retry_args_bounding(self, mock_http_client):
        retry_delay_test_args_and_expected_values = {}
        for (k, (_, _, min, max)) in databricks.sql.thrift_backend._retry_policy.items():
            retry_delay_test_args_and_expected_values[k] = ((min - 1, min), (max + 1, max))

        for i in range(2):
            retry_delay_args = {
                k: v[i][0]
                for (k, v) in retry_delay_test_args_and_expected_values.items()
            }
            backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider(), **retry_delay_args)
            retry_delay_expected_vals = {
                k: v[i][1]
                for (k, v) in retry_delay_test_args_and_expected_values.items()
            }
            for (arg, val) in retry_delay_expected_vals.items():
                self.assertEqual(getattr(backend, arg), val)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_configuration_passthrough(self, tcli_client_class):
        tcli_service_instance = tcli_client_class.return_value
        tcli_service_instance.OpenSession.return_value = self.open_session_resp
        mock_config = {"foo": "bar", "baz": True, "42": 42}
        expected_config = {
            "spark.thriftserver.arrowBasedRowSet.timestampAsString": "false",
            "foo": "bar",
            "baz": "True",
            "42": "42"
        }

        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        backend.open_session(mock_config, None, None)

        open_session_req = tcli_client_class.return_value.OpenSession.call_args[0][0]
        self.assertEqual(open_session_req.configuration, expected_config)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_cant_set_timestamp_as_string_to_true(self, tcli_client_class):
        tcli_service_instance = tcli_client_class.return_value
        tcli_service_instance.OpenSession.return_value = self.open_session_resp
        mock_config = {"spark.thriftserver.arrowBasedRowSet.timestampAsString": True}
        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

        with self.assertRaises(databricks.sql.Error) as cm:
            backend.open_session(mock_config, None, None)

        self.assertIn("timestampAsString cannot be changed", str(cm.exception))

    def _construct_open_session_with_namespace(self, can_use_multiple_cats, cat, schem):
        return ttypes.TOpenSessionResp(
            status=self.okay_status,
            serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4,
            canUseMultipleCatalogs=can_use_multiple_cats,
            initialNamespace=ttypes.TNamespace(catalogName=cat, schemaName=schem))

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_initial_namespace_passthrough_to_open_session(self, tcli_client_class):
        tcli_service_instance = tcli_client_class.return_value

        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        initial_cat_schem_args = [("cat", None), (None, "schem"), ("cat", "schem")]

        for cat, schem in initial_cat_schem_args:
            with self.subTest(cat=cat, schem=schem):
                tcli_service_instance.OpenSession.return_value = \
                    self._construct_open_session_with_namespace(True, cat, schem)

                backend.open_session({}, cat, schem)

                open_session_req = tcli_client_class.return_value.OpenSession.call_args[0][0]
                self.assertEqual(open_session_req.initialNamespace.catalogName, cat)
                self.assertEqual(open_session_req.initialNamespace.schemaName, schem)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_can_use_multiple_catalogs_is_set_in_open_session_req(self, tcli_client_class):
        tcli_service_instance = tcli_client_class.return_value
        tcli_service_instance.OpenSession.return_value = self.open_session_resp

        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        backend.open_session({}, None, None)

        open_session_req = tcli_client_class.return_value.OpenSession.call_args[0][0]
        self.assertTrue(open_session_req.canUseMultipleCatalogs)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_can_use_multiple_catalogs_is_false_fails_with_initial_catalog(self, tcli_client_class):
        tcli_service_instance = tcli_client_class.return_value

        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())
        # If the initial catalog is set, but server returns canUseMultipleCatalogs=False, we
        # expect failure. If the initial catalog isn't set, then canUseMultipleCatalogs=False
        # is fine
        failing_ns_args = [("cat", None), ("cat", "schem")]
        passing_ns_args = [(None, None), (None, "schem")]

        for cat, schem in failing_ns_args:
            tcli_service_instance.OpenSession.return_value = \
                self._construct_open_session_with_namespace(False, cat, schem)

            with self.assertRaises(InvalidServerResponseError) as cm:
                backend.open_session({}, cat, schem)

            self.assertIn("server does not support multiple catalogs", str(cm.exception),
                          "incorrect error thrown for initial namespace {}".format((cat, schem)))

        for cat, schem in passing_ns_args:
            tcli_service_instance.OpenSession.return_value = \
                self._construct_open_session_with_namespace(False, cat, schem)
            backend.open_session({}, cat, schem)

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    def test_protocol_v3_fails_if_initial_namespace_set(self, tcli_client_class):
        tcli_service_instance = tcli_client_class.return_value

        tcli_service_instance.OpenSession.return_value = \
            ttypes.TOpenSessionResp(
                status=self.okay_status,
                serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3,
                canUseMultipleCatalogs=True,
                initialNamespace=ttypes.TNamespace(catalogName="cat", schemaName="schem")
            )

        backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider())

        with self.assertRaises(InvalidServerResponseError) as cm:
            backend.open_session({}, "cat", "schem")

        self.assertIn("Setting initial namespace not supported by the DBR version",
                      str(cm.exception))

    @patch("databricks.sql.thrift_backend.TCLIService.Client", autospec=True)
    @patch("databricks.sql.thrift_backend.ThriftBackend._handle_execute_response")
    def test_execute_command_sets_complex_type_fields_correctly(self, mock_handle_execute_response,
                                                                tcli_service_class):
        tcli_service_instance = tcli_service_class.return_value
        # Iterate through each possible combination of native types (True, False and unset)
        for (complex, timestamp, decimals) in itertools.product(
            [True, False, None], [True, False, None], [True, False, None]):
            complex_arg_types = {}
            if complex is not None:
                complex_arg_types["_use_arrow_native_complex_types"] = complex
            if timestamp is not None:
                complex_arg_types["_use_arrow_native_timestamps"] = timestamp
            if decimals is not None:
                complex_arg_types["_use_arrow_native_decimals"] = decimals

            thrift_backend = ThriftBackend("foobar", 443, "path", [], auth_provider=AuthProvider(), **complex_arg_types)
            thrift_backend.execute_command(Mock(), Mock(), 100, 100, Mock(), Mock())
            t_execute_statement_req = tcli_service_instance.ExecuteStatement.call_args[0][0]
            # If the value is unset, the native type should default to True
            self.assertEqual(t_execute_statement_req.useArrowNativeTypes.timestampAsArrow,
                             complex_arg_types.get("_use_arrow_native_timestamps", True))
            self.assertEqual(t_execute_statement_req.useArrowNativeTypes.decimalAsArrow,
                             complex_arg_types.get("_use_arrow_native_decimals", True))
            self.assertEqual(t_execute_statement_req.useArrowNativeTypes.complexTypesAsArrow,
                             complex_arg_types.get("_use_arrow_native_complex_types", True))
            self.assertFalse(t_execute_statement_req.useArrowNativeTypes.intervalTypesAsArrow)


if __name__ == '__main__':
    unittest.main()
