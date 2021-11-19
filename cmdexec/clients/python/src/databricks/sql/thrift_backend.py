import logging
import time
import threading
from uuid import uuid4
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED, create_default_context

import pyarrow
import thrift.transport.THttpClient
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport
from thrift.Thrift import TException

from databricks.sql.thrift_api.TCLIService import TCLIService, ttypes
from databricks.sql import *
from databricks.sql.utils import ArrowQueue, ExecuteResponse

logger = logging.getLogger(__name__)


class ThriftBackend:
    CLOSED_OP_STATE = ttypes.TOperationState.CLOSED_STATE
    ERROR_OP_STATE = ttypes.TOperationState.ERROR_STATE
    BIT_MASKS = [1, 2, 4, 8, 16, 32, 64, 128]
    ERROR_MSG_HEADER = "X-Thriftserver-Error-Message"

    def __init__(self, server_hostname: str, port, http_path: str, http_headers, **kwargs):
        # Internal arguments in **kwargs:
        # _user_agent_entry
        #   Tag to add to User-Agent header. For use by partners.
        # _username, _password
        #   Username and password Basic authentication (no official support)
        # _tls_no_verify
        #   Set to True (Boolean) to completely disable SSL verification.
        # _tls_verify_hostname
        #   Set to False (Boolean) to disable SSL hostname verification, but check certificate.
        # _tls_trusted_ca_file
        #   Set to the path of the file containing trusted CA certificates for server certificate
        #   verification. If not provide, uses system truststore.
        # _tls_client_cert_file, _tls_client_cert_key_file, _tls_client_cert_key_password
        #   Set client SSL certificate.
        #   See https://docs.python.org/3/library/ssl.html#ssl.SSLContext.load_cert_chain
        # _connection_uri
        #   Overrides server_hostname and http_path.
        # _max_number_of_retries
        #  The maximum number of times we should retry retryable requests (defaults to 25)

        port = port or 443
        if kwargs.get("_connection_uri"):
            uri = kwargs.get("_connection_uri")
        elif server_hostname and http_path:
            uri = "https://{host}:{port}/{path}".format(
                host=server_hostname, port=port, path=http_path.lstrip("/"))
        else:
            raise ValueError("No valid connection settings.")

        self._max_number_of_retries = kwargs.get("_max_number_of_retries", 25)

        # Configure tls context
        ssl_context = create_default_context(cafile=kwargs.get("_tls_trusted_ca_file"))
        if kwargs.get("_tls_no_verify") is True:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE
        elif kwargs.get("_tls_verify_hostname") is False:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.check_hostname = True
            ssl_context.verify_mode = CERT_REQUIRED

        tls_client_cert_file = kwargs.get("_tls_client_cert_file")
        tls_client_cert_key_file = kwargs.get("__tls_client_cert_key_file")
        tls_client_cert_key_password = kwargs.get("_tls_client_cert_key_password")
        if tls_client_cert_file:
            ssl_context.load_cert_chain(
                certfile=tls_client_cert_file,
                keyfile=tls_client_cert_key_file,
                password=tls_client_cert_key_password)

        self._transport = thrift.transport.THttpClient.THttpClient(
            uri_or_host=uri,
            ssl_context=ssl_context,
        )

        self._transport.setCustomHeaders(dict(http_headers))
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = TCLIService.Client(protocol)

        try:
            self._transport.open()
        except:
            self._transport.close()
            raise

        self._request_lock = threading.RLock()

    @staticmethod
    def _check_response_for_error(response):
        if response.status and response.status.statusCode in \
                [ttypes.TStatusCode.ERROR_STATUS, ttypes.TStatusCode.INVALID_HANDLE_STATUS]:
            raise DatabaseError(response.status.errorMessage)

    def make_request(self, method, request, attempt_number=1):
        try:
            # We have a lock here because .cancel can be called from a separate thread.
            # We do not want threads to be simultaneously sharing the Thrift Transport
            # because we use its state to determine retries
            self._request_lock.acquire()
            response = method(request)
            logger.debug("Received response: {}".format(response))
            ThriftBackend._check_response_for_error(response)
            return response
        except Exception as error:
            # _transport.code isn't necessarily set :(
            code_and_headers_is_set = hasattr(self._transport, 'code') \
                                      and hasattr(self._transport, 'headers')
            # We only retry if a Retry-After header is set
            if code_and_headers_is_set and self._transport.code in [503, 429] and \
                    "Retry-After" in self._transport.headers and \
                    attempt_number <= self._max_number_of_retries:
                retry_time_seconds = int(self._transport.headers["Retry-After"])
                if self.ERROR_MSG_HEADER in self._transport.headers:
                    error_message = self._transport.headers[self.ERROR_MSG_HEADER]
                else:
                    error_message = str(error)
                logger.warning("Received retryable error during {}. Request: {} Error: {}".format(
                    method, request, error_message))
                logger.warning("Retrying in {} seconds. This is attempt number {}".format(
                    retry_time_seconds, attempt_number))
                time.sleep(retry_time_seconds)
                return self.make_request(method, request, attempt_number + 1)
            else:
                logger.error("Received error when issuing: {}".format(request))
                if hasattr(self._transport, "headers") and \
                   self.ERROR_MSG_HEADER in self._transport.headers:
                    error_message = self._transport.headers[self.ERROR_MSG_HEADER]
                    raise OperationalError("Error during Thrift request: {}".format(error_message))
                else:
                    raise OperationalError("Error during Thrift request", error)
        finally:
            self._request_lock.release()

    def _check_protocol_version(self, t_open_session_resp):
        protocol_version = t_open_session_resp.serverProtocolVersion

        if protocol_version < ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3:
            raise OperationalError("Error: expected server to use a protocol version >= "
                                   "SPARK_CLI_SERVICE_PROTOCOL_V3, "
                                   "instead got: {}".format(protocol_version))

    def open_session(self, session_id=None):
        try:
            self._transport.open()
            handle_identifier = session_id and ttypes.THandleIdentifier(session_id, uuid4().bytes)
            open_session_req = ttypes.TOpenSessionReq(
                sessionId=handle_identifier,
                client_protocol_i64=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4,
                client_protocol=None,
                configuration={
                    # We want to receive proper Timestamp arrow types.
                    # We set it also in confOverlay in TExecuteStatementReq on a per query basic,
                    # but it doesn't hurt to also set for the whole session.
                    "spark.thriftserver.arrowBasedRowSet.timestampAsString": "false"
                })
            response = self.make_request(self._client.OpenSession, open_session_req)
            self._check_protocol_version(response)
            return response.sessionHandle
        except:
            self._transport.close()
            raise

    def close_session(self, session_handle) -> None:
        req = ttypes.TCloseSessionReq(sessionHandle=session_handle)
        try:
            self.make_request(self._client.CloseSession, req)
        finally:
            self._transport.close()

    def _check_command_not_in_error_or_closed_state(self, op_handle, get_operations_resp):
        if get_operations_resp.operationState == ttypes.TOperationState.ERROR_STATE:
            raise DatabaseError(get_operations_resp.errorMessage)
        elif get_operations_resp.operationState == ttypes.TOperationState.CLOSED_STATE:
            raise DatabaseError("Command {} unexpectedly closed server side".format(
                op_handle and op_handle.operationId.guid))

    def _poll_for_status(self, op_handle):
        req = ttypes.TGetOperationStatusReq(
            operationHandle=op_handle,
            getProgressUpdate=False,
        )
        return self.make_request(self._client.GetOperationStatus, req)

    def _create_arrow_table(self, t_row_set, schema):
        if t_row_set.columns is not None:
            return ThriftBackend._convert_column_based_set_to_arrow_table(t_row_set.columns, schema)
        if t_row_set.arrowBatches is not None:
            return ThriftBackend._convert_arrow_based_set_to_arrow_table(
                t_row_set.arrowBatches, schema)
        raise OperationalError("Unsupported TRowSet instance {}".format(t_row_set))

    @staticmethod
    def _convert_arrow_based_set_to_arrow_table(arrow_batches, schema):
        ba = bytearray()
        schema_bytes = schema.serialize().to_pybytes()
        ba += schema_bytes
        n_rows = 0
        for arrow_batch in arrow_batches:
            n_rows += arrow_batch.rowCount
            ba += arrow_batch.batch
        arrow_table = pyarrow.ipc.open_stream(ba).read_all()
        return arrow_table, n_rows

    @staticmethod
    def _convert_column_based_set_to_arrow_table(columns, schema):
        arrow_table = pyarrow.Table.from_arrays(
            [ThriftBackend._convert_column_to_arrow_array(c) for c in columns],
            # Only use the column names from the schema, the types are determined by the
            # physical types used in column based set, as they can differ from the
            # mapping used in _hive_schema_to_arrow_schema.
            names=[c.name for c in schema])
        return arrow_table, arrow_table.num_rows

    @staticmethod
    def _convert_column_to_arrow_array(t_col):
        """
        Return a pyarrow array from the values in a TColumn instance.
        Note that ColumnBasedSet has no native support for complex types, so they will be converted
        to strings server-side.
        """
        field_name_to_arrow_type = {
            "boolVal": pyarrow.bool_(),
            "byteVal": pyarrow.int8(),
            "i16Val": pyarrow.int16(),
            "i32Val": pyarrow.int32(),
            "i64Val": pyarrow.int64(),
            "doubleVal": pyarrow.float64(),
            "stringVal": pyarrow.string(),
            "binaryVal": pyarrow.binary()
        }
        for field in field_name_to_arrow_type.keys():
            wrapper = getattr(t_col, field)
            if wrapper:
                return ThriftBackend._create_arrow_array(wrapper, field_name_to_arrow_type[field])

        raise OperationalError("Empty TColumn instance {}".format(t_col))

    @staticmethod
    def _create_arrow_array(t_col_value_wrapper, arrow_type):
        result = t_col_value_wrapper.values
        nulls = t_col_value_wrapper.nulls  # bitfield describing which values are null
        assert isinstance(nulls, bytes)

        # The number of bits in nulls can be both larger or smaller than the number of
        # elements in result, so take the minimum of both to iterate over.
        length = min(len(result), len(nulls) * 8)

        for i in range(length):
            if nulls[i >> 3] & ThriftBackend.BIT_MASKS[i & 0x7]:
                result[i] = None

        return pyarrow.array(result, type=arrow_type)

    def _get_metadata_resp(self, op_handle):
        req = ttypes.TGetResultSetMetadataReq(operationHandle=op_handle)
        return self.make_request(self._client.GetResultSetMetadata, req)

    @staticmethod
    def _hive_schema_to_arrow_schema(t_table_schema):
        def map_type(t_type_entry):
            if t_type_entry.primitiveEntry:
                return {
                    ttypes.TTypeId.BOOLEAN_TYPE: pyarrow.bool_(),
                    ttypes.TTypeId.TINYINT_TYPE: pyarrow.int8(),
                    ttypes.TTypeId.SMALLINT_TYPE: pyarrow.int16(),
                    ttypes.TTypeId.INT_TYPE: pyarrow.int32(),
                    ttypes.TTypeId.BIGINT_TYPE: pyarrow.int64(),
                    ttypes.TTypeId.FLOAT_TYPE: pyarrow.float32(),
                    ttypes.TTypeId.DOUBLE_TYPE: pyarrow.float64(),
                    ttypes.TTypeId.STRING_TYPE: pyarrow.string(),
                    ttypes.TTypeId.TIMESTAMP_TYPE: pyarrow.timestamp('us', None),
                    ttypes.TTypeId.BINARY_TYPE: pyarrow.binary(),
                    ttypes.TTypeId.ARRAY_TYPE: pyarrow.string(),
                    ttypes.TTypeId.MAP_TYPE: pyarrow.string(),
                    ttypes.TTypeId.STRUCT_TYPE: pyarrow.string(),
                    ttypes.TTypeId.UNION_TYPE: pyarrow.string(),
                    ttypes.TTypeId.USER_DEFINED_TYPE: pyarrow.string(),
                    ttypes.TTypeId.DECIMAL_TYPE: pyarrow.string(),
                    ttypes.TTypeId.NULL_TYPE: pyarrow.null(),
                    ttypes.TTypeId.DATE_TYPE: pyarrow.date32(),
                    ttypes.TTypeId.VARCHAR_TYPE: pyarrow.string(),
                    ttypes.TTypeId.CHAR_TYPE: pyarrow.string(),
                    ttypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE: pyarrow.string(),
                    ttypes.TTypeId.INTERVAL_DAY_TIME_TYPE: pyarrow.string()
                }[t_type_entry.primitiveEntry.type]
            else:
                # Current thriftserver implementation should always return a primitiveEntry,
                # even for complex types
                raise OperationalError("Thrift protocol error: t_type_entry not a primitiveEntry")

        def convert_col(t_column_desc):
            return pyarrow.field(t_column_desc.columnName,
                                 map_type(t_column_desc.typeDesc.types[0]))

        return pyarrow.schema([convert_col(col) for col in t_table_schema.columns])

    @staticmethod
    def _hive_schema_to_description(t_table_schema):
        def clean_type(typeEntry):
            if typeEntry.primitiveEntry:
                name = ttypes.TTypeId._VALUES_TO_NAMES[typeEntry.primitiveEntry.type]
                # Drop _TYPE suffix
                return (name[:-5] if name.endswith("_TYPE") else name).lower()
            else:
                raise OperationalError("Thrift protocol error: t_type_entry not a primitiveEntry")

        return [(col.columnName, clean_type(col.typeDesc.types[0]), None, None, None, None, None)
                for col in t_table_schema.columns]

    def _results_message_to_execute_response(self, resp, operation_state):
        if resp.directResults and resp.directResults.resultSetMetadata:
            t_result_set_metadata_resp = resp.directResults.resultSetMetadata
        else:
            t_result_set_metadata_resp = self._get_metadata_resp(resp.operationHandle)

        if t_result_set_metadata_resp.resultFormat not in [
                ttypes.TSparkRowSetType.ARROW_BASED_SET, ttypes.TSparkRowSetType.COLUMN_BASED_SET
        ]:
            raise OperationalError("Expected results to be in Arrow or column based format, "
                                   "instead they are: {}".format(
                                       ttypes.TSparkRowSetType._VALUES_TO_NAMES[
                                           t_result_set_metadata_resp.resultFormat]))

        direct_results = resp.directResults
        has_been_closed_server_side = direct_results and direct_results.closeOperation
        has_more_rows = (not direct_results) or (not direct_results.resultSet) \
                        or direct_results.resultSet.hasMoreRows
        description = self._hive_schema_to_description(t_result_set_metadata_resp.schema)
        arrow_schema = self._hive_schema_to_arrow_schema(t_result_set_metadata_resp.schema)

        if direct_results and direct_results.resultSet:
            assert (direct_results.resultSet.results.startRowOffset == 0)
            assert (direct_results.resultSetMetadata)
            arrow_results, n_rows = self._create_arrow_table(direct_results.resultSet.results,
                                                             arrow_schema)
            arrow_queue_opt = ArrowQueue(arrow_results, n_rows, 0)
        else:
            arrow_queue_opt = None
        return ExecuteResponse(
            arrow_queue=arrow_queue_opt,
            status=operation_state,
            has_been_closed_server_side=has_been_closed_server_side,
            has_more_rows=has_more_rows,
            command_handle=resp.operationHandle,
            description=description,
            arrow_schema=arrow_schema)

    def _wait_until_command_done(self, op_handle, initial_operation_status_resp):
        if initial_operation_status_resp:
            self._check_command_not_in_error_or_closed_state(op_handle,
                                                             initial_operation_status_resp)
        operation_state = initial_operation_status_resp and initial_operation_status_resp.operationState
        while not operation_state or operation_state in [
                ttypes.TOperationState.RUNNING_STATE, ttypes.TOperationState.PENDING_STATE
        ]:
            poll_resp = self._poll_for_status(op_handle)
            operation_state = poll_resp.operationState
            self._check_command_not_in_error_or_closed_state(op_handle, poll_resp)
        return operation_state

    @staticmethod
    def _check_direct_results_for_error(t_spark_direct_results):
        if t_spark_direct_results:
            if t_spark_direct_results.operationStatus:
                ThriftBackend._check_response_for_error(t_spark_direct_results.operationStatus)
            if t_spark_direct_results.resultSetMetadata:
                ThriftBackend._check_response_for_error(t_spark_direct_results.resultSetMetadata)
            if t_spark_direct_results.resultSet:
                ThriftBackend._check_response_for_error(t_spark_direct_results.resultSet)
            if t_spark_direct_results.closeOperation:
                ThriftBackend._check_response_for_error(t_spark_direct_results.closeOperation)

    def execute_command(self, operation, session_handle, max_rows, max_bytes, cursor):
        assert (session_handle is not None)

        req = ttypes.TExecuteStatementReq(
            sessionHandle=session_handle,
            statement=operation,
            runAsync=True,
            getDirectResults=ttypes.TSparkGetDirectResults(maxRows=max_rows, maxBytes=max_bytes),
            canReadArrowResult=True,
            canDecompressLZ4Result=False,
            canDownloadResult=False,
            confOverlay={
                # We want to receive proper Timestamp arrow types.
                "spark.thriftserver.arrowBasedRowSet.timestampAsString": "false"
            })
        resp = self.make_request(self._client.ExecuteStatement, req)
        return self._handle_execute_response(resp, cursor)

    def get_catalogs(self, session_handle, max_rows, max_bytes, cursor):
        assert (session_handle is not None)

        req = ttypes.TGetCatalogsReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(maxRows=max_rows, maxBytes=max_bytes))
        resp = self.make_request(self._client.GetCatalogs, req)
        return self._handle_execute_response(resp, cursor)

    def get_schemas(self,
                    session_handle,
                    max_rows,
                    max_bytes,
                    cursor,
                    catalog_name=None,
                    schema_name=None):
        assert (session_handle is not None)

        req = ttypes.TGetSchemasReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(maxRows=max_rows, maxBytes=max_bytes),
            catalogName=catalog_name,
            schemaName=schema_name,
        )
        resp = self.make_request(self._client.GetSchemas, req)
        return self._handle_execute_response(resp, cursor)

    def get_tables(self,
                   session_handle,
                   max_rows,
                   max_bytes,
                   cursor,
                   catalog_name=None,
                   schema_name=None,
                   table_name=None,
                   table_types=None):
        if table_types is None:
            table_types = []
        assert (session_handle is not None)

        req = ttypes.TGetTablesReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(maxRows=max_rows, maxBytes=max_bytes),
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            tableTypes=table_types)
        resp = self.make_request(self._client.GetTables, req)
        return self._handle_execute_response(resp, cursor)

    def get_columns(self,
                    session_handle,
                    max_rows,
                    max_bytes,
                    cursor,
                    catalog_name=None,
                    schema_name=None,
                    table_name=None,
                    column_name=None):
        assert (session_handle is not None)

        req = ttypes.TGetColumnsReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(maxRows=max_rows, maxBytes=max_bytes),
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            columnName=column_name)
        resp = self.make_request(self._client.GetColumns, req)
        return self._handle_execute_response(resp, cursor)

    def _handle_execute_response(self, resp, cursor):
        cursor.active_op_handle = resp.operationHandle
        self._check_direct_results_for_error(resp.directResults)

        final_operation_state = self._wait_until_command_done(
            resp.operationHandle, resp.directResults and resp.directResults.operationStatus)

        return self._results_message_to_execute_response(resp, final_operation_state)

    def fetch_results(self, op_handle, max_rows, max_bytes, row_offset, arrow_schema):
        assert (op_handle is not None)

        req = ttypes.TFetchResultsReq(
            operationHandle=ttypes.TOperationHandle(
                op_handle.operationId,
                op_handle.operationType,
                False,
                op_handle.modifiedRowCount,
            ),
            maxRows=max_rows,
            maxBytes=max_bytes,
            startRowOffset=row_offset,
        )

        resp = self.make_request(self._client.FetchResults, req)
        arrow_results, n_rows = self._create_arrow_table(resp.results, arrow_schema)
        arrow_queue = ArrowQueue(arrow_results, n_rows, row_offset - resp.results.startRowOffset)

        return arrow_queue, resp.hasMoreRows

    def close_command(self, op_handle):
        req = ttypes.TCloseOperationReq(operationHandle=op_handle)
        resp = self.make_request(self._client.CloseOperation, req)
        return resp.status

    def cancel_command(self, active_op_handle):
        logger.debug("Cancelling command {}".format(active_op_handle.operationId.guid))
        req = ttypes.TCancelOperationReq(active_op_handle)
        self.make_request(self._client.CancelOperation, req)

    @staticmethod
    def handle_to_id(session_handle):
        return session_handle.sessionId.guid
