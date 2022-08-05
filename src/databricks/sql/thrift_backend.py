from decimal import Decimal
import errno
import logging
import math
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
from databricks.sql.thrift_api.TCLIService.TCLIService import (
    Client as TCLIServiceClient,
)
from databricks.sql.utils import (
    ArrowQueue,
    ExecuteResponse,
    _bound,
    RequestErrorInfo,
    NoRetryReason,
)

logger = logging.getLogger(__name__)

THRIFT_ERROR_MESSAGE_HEADER = "x-thriftserver-error-message"
DATABRICKS_ERROR_OR_REDIRECT_HEADER = "x-databricks-error-or-redirect-message"
DATABRICKS_REASON_HEADER = "x-databricks-reason-phrase"

TIMESTAMP_AS_STRING_CONFIG = "spark.thriftserver.arrowBasedRowSet.timestampAsString"

# see Connection.__init__ for parameter descriptions.
# - Min/Max avoids unsustainable configs (sane values are far more constrained)
# - 900s attempts-duration lines up w ODBC/JDBC drivers (for cluster startup > 10 mins)
_retry_policy = {  # (type, default, min, max)
    "_retry_delay_min": (float, 1, 0.1, 60),
    "_retry_delay_max": (float, 60, 5, 3600),
    "_retry_stop_after_attempts_count": (int, 30, 1, 60),
    "_retry_stop_after_attempts_duration": (float, 900, 1, 86400),
    "_retry_delay_default": (float, 5, 1, 60),
}


class ThriftBackend:
    CLOSED_OP_STATE = ttypes.TOperationState.CLOSED_STATE
    ERROR_OP_STATE = ttypes.TOperationState.ERROR_STATE
    BIT_MASKS = [1, 2, 4, 8, 16, 32, 64, 128]

    def __init__(
        self, server_hostname: str, port, http_path: str, http_headers, **kwargs
    ):
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
        # RETRY/ATTEMPT POLICY
        # _retry_delay_min                      (default: 1)
        # _retry_delay_max                      (default: 60)
        #   {min,max} pre-retry delay bounds
        # _retry_delay_default                   (default: 5)
        #   Only used when GetOperationStatus fails due to a TCP/OS Error.
        # _retry_stop_after_attempts_count      (default: 30)
        #   total max attempts during retry sequence
        # _retry_stop_after_attempts_duration   (default: 900)
        #   total max wait duration during retry sequence
        #   (Note this will stop _before_ intentionally exceeding; thus if the
        #   next calculated pre-retry delay would go past
        #   _retry_stop_after_attempts_duration, stop now.)
        #
        # _retry_stop_after_attempts_count
        #  The maximum number of times we should retry retryable requests (defaults to 24)
        # _socket_timeout
        #  The timeout in seconds for socket send, recv and connect operations. Defaults to None for
        #  no timeout. Should be a positive float or integer.

        port = port or 443
        if kwargs.get("_connection_uri"):
            uri = kwargs.get("_connection_uri")
        elif server_hostname and http_path:
            uri = "https://{host}:{port}/{path}".format(
                host=server_hostname, port=port, path=http_path.lstrip("/")
            )
        else:
            raise ValueError("No valid connection settings.")

        self._initialize_retry_args(kwargs)
        self._use_arrow_native_complex_types = kwargs.get(
            "_use_arrow_native_complex_types", True
        )
        self._use_arrow_native_decimals = kwargs.get("_use_arrow_native_decimals", True)
        self._use_arrow_native_timestamps = kwargs.get(
            "_use_arrow_native_timestamps", True
        )

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
        tls_client_cert_key_file = kwargs.get("_tls_client_cert_key_file")
        tls_client_cert_key_password = kwargs.get("_tls_client_cert_key_password")
        if tls_client_cert_file:
            ssl_context.load_cert_chain(
                certfile=tls_client_cert_file,
                keyfile=tls_client_cert_key_file,
                password=tls_client_cert_key_password,
            )

        self._transport = thrift.transport.THttpClient.THttpClient(
            uri_or_host=uri,
            ssl_context=ssl_context,
        )

        timeout = kwargs.get("_socket_timeout")
        # setTimeout defaults to None (i.e. no timeout), and is expected in ms
        self._transport.setTimeout(timeout and (float(timeout) * 1000.0))

        self._transport.setCustomHeaders(dict(http_headers))
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = TCLIService.Client(protocol)

        try:
            self._transport.open()
        except:
            self._transport.close()
            raise

        self._request_lock = threading.RLock()

    def _initialize_retry_args(self, kwargs):
        # Configure retries & timing: use user-settings or defaults, and bound
        # by policy. Log.warn when given param gets restricted.
        for (key, (type_, default, min, max)) in _retry_policy.items():
            given_or_default = type_(kwargs.get(key, default))
            bound = _bound(min, max, given_or_default)
            setattr(self, key, bound)
            logger.debug(
                "retry parameter: {} given_or_default {}".format(key, given_or_default)
            )
            if bound != given_or_default:
                logger.warning(
                    "Override out of policy retry parameter: "
                    + "{} given {}, restricted to {}".format(
                        key, given_or_default, bound
                    )
                )

        # Fail on retry delay min > max; consider later adding fail on min > duration?
        if (
            self._retry_stop_after_attempts_count > 1
            and self._retry_delay_min > self._retry_delay_max
        ):
            raise ValueError(
                "Invalid configuration enables retries with retry delay min(={}) > max(={})".format(
                    self._retry_delay_min, self._retry_delay_max
                )
            )

    @staticmethod
    def _check_response_for_error(response):
        if response.status and response.status.statusCode in [
            ttypes.TStatusCode.ERROR_STATUS,
            ttypes.TStatusCode.INVALID_HANDLE_STATUS,
        ]:
            raise DatabaseError(response.status.errorMessage)

    @staticmethod
    def _extract_error_message_from_headers(headers):
        err_msg = ""
        if THRIFT_ERROR_MESSAGE_HEADER in headers:
            err_msg = headers[THRIFT_ERROR_MESSAGE_HEADER]
        if DATABRICKS_ERROR_OR_REDIRECT_HEADER in headers:
            if (
                err_msg
            ):  # We don't expect both to be set, but log both here just in case
                err_msg = "Thriftserver error: {}, Databricks error: {}".format(
                    err_msg, headers[DATABRICKS_ERROR_OR_REDIRECT_HEADER]
                )
            else:
                err_msg = headers[DATABRICKS_ERROR_OR_REDIRECT_HEADER]
            if DATABRICKS_REASON_HEADER in headers:
                err_msg += ": " + headers[DATABRICKS_REASON_HEADER]
        return err_msg

    def _handle_request_error(self, error_info, attempt, elapsed):
        max_attempts = self._retry_stop_after_attempts_count
        max_duration_s = self._retry_stop_after_attempts_duration

        if (
            error_info.retry_delay is not None
            and elapsed + error_info.retry_delay > max_duration_s
        ):
            no_retry_reason = NoRetryReason.OUT_OF_TIME
        elif error_info.retry_delay is not None and attempt >= max_attempts:
            no_retry_reason = NoRetryReason.OUT_OF_ATTEMPTS
        elif error_info.retry_delay is None:
            no_retry_reason = NoRetryReason.NOT_RETRYABLE
        else:
            no_retry_reason = None

        full_error_info_context = error_info.full_info_logging_context(
            no_retry_reason, attempt, max_attempts, elapsed, max_duration_s
        )

        if no_retry_reason is not None:
            user_friendly_error_message = error_info.user_friendly_error_message(
                no_retry_reason, attempt, elapsed
            )
            network_request_error = RequestError(
                user_friendly_error_message, full_error_info_context, error_info.error
            )
            logger.info(network_request_error.message_with_context())

            raise network_request_error

        logger.info(
            "Retrying request after error in {} seconds: {}".format(
                error_info.retry_delay, full_error_info_context
            )
        )
        time.sleep(error_info.retry_delay)

    # FUTURE: Consider moving to https://github.com/litl/backoff or
    # https://github.com/jd/tenacity for retry logic.
    def make_request(self, method, request):
        """Execute given request, attempting retries when
            1. Receiving HTTP 429/503 from server
            2. OSError is raised during a GetOperationStatus

        For delay between attempts, honor the given Retry-After header, but with bounds.
        Use lower bound of expontial-backoff based on _retry_delay_min,
        and upper bound of _retry_delay_max.
        Will stop retry attempts if total elapsed time + next retry delay would exceed
        _retry_stop_after_attempts_duration.
        """
        # basic strategy: build range iterator rep'ing number of available
        # retries. bounds can be computed from there. iterate over it with
        # retries until success or final failure achieved.

        t0 = time.time()

        def get_elapsed():
            return time.time() - t0

        def bound_retry_delay(attempt, proposed_delay):
            """bound delay (seconds) by [min_delay*1.5^(attempt-1), max_delay]"""
            delay = int(proposed_delay)
            delay = max(delay, self._retry_delay_min * math.pow(1.5, attempt - 1))
            delay = min(delay, self._retry_delay_max)
            return delay

        def extract_retry_delay(attempt):
            # encapsulate retry checks, returns None || delay-in-secs
            # Retry IFF 429/503 code + Retry-After header set
            http_code = getattr(self._transport, "code", None)
            retry_after = getattr(self._transport, "headers", {}).get("Retry-After")
            if http_code in [429, 503] and retry_after:
                # bound delay (seconds) by [min_delay*1.5^(attempt-1), max_delay]
                return bound_retry_delay(attempt, int(retry_after))
            return None

        def attempt_request(attempt):
            # splits out lockable attempt, from delay & retry loop
            # returns tuple: (method_return, delay_fn(), error, error_message)
            # - non-None method_return -> success, return and be done
            # - non-None retry_delay -> sleep delay before retry
            # - error, error_message always set when available

            error, error_message, retry_delay = None, None, None
            try:
                logger.debug("Sending request: {}".format(request))
                response = method(request)
                logger.debug("Received response: {}".format(response))
                return response
            except OSError as err:
                error = err
                error_message = str(err)

                gos_name = TCLIServiceClient.GetOperationStatus.__name__
                if method.__name__ == gos_name:
                    retry_delay = bound_retry_delay(attempt, self._retry_delay_default)

                    # fmt: off
                    # The built-in errno package encapsulates OSError codes, which are OS-specific.
                    # log.info for errors we believe are not unusual or unexpected. log.warn for
                    # for others like EEXIST, EBADF, ERANGE which are not expected in this context.
                    #
                    # I manually tested this retry behaviour using mitmweb and confirmed that 
                    # GetOperationStatus requests are retried when I forced network connection
                    # interruptions / timeouts / reconnects. See #24 for more info.
                                            # | Debian | Darwin |
                    info_errs = [           # |--------|--------|         
                        errno.ESHUTDOWN,    # |   32   |   32   |
                        errno.EAFNOSUPPORT, # |   97   |   47   |
                        errno.ECONNRESET,   # |   104  |   54   |
                        errno.ETIMEDOUT,    # |   110  |   60   |
                    ]

                    # fmt: on
                    log_string = f"{gos_name} failed with code {err.errno} and will attempt to retry"
                    if err.errno in info_errs:
                        logger.info(log_string)
                    else:
                        logger.warning(log_string)
            except Exception as err:
                error = err
                retry_delay = extract_retry_delay(attempt)
                error_message = ThriftBackend._extract_error_message_from_headers(
                    getattr(self._transport, "headers", {})
                )
            return RequestErrorInfo(
                error=error,
                error_message=error_message,
                retry_delay=retry_delay,
                http_code=getattr(self._transport, "code", None),
                method=method.__name__,
                request=request,
            )

        # The real work:
        # - for each available attempt:
        #       lock-and-attempt
        #       return on success
        #       if available: bounded delay and retry
        #       if not: raise error
        max_attempts = self._retry_stop_after_attempts_count

        # use index-1 counting for logging/human consistency
        for attempt in range(1, max_attempts + 1):
            # We have a lock here because .cancel can be called from a separate thread.
            # We do not want threads to be simultaneously sharing the Thrift Transport
            # because we use its state to determine retries
            with self._request_lock:
                response_or_error_info = attempt_request(attempt)
            elapsed = get_elapsed()

            # conditions: success, non-retry-able, no-attempts-left, no-time-left, delay+retry
            if not isinstance(response_or_error_info, RequestErrorInfo):
                # log nothing here, presume that main request logging covers
                response = response_or_error_info
                ThriftBackend._check_response_for_error(response)
                return response

            error_info = response_or_error_info
            # The error handler will either sleep or throw an exception
            self._handle_request_error(error_info, attempt, elapsed)

    def _check_protocol_version(self, t_open_session_resp):
        protocol_version = t_open_session_resp.serverProtocolVersion

        if protocol_version < ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2:
            raise OperationalError(
                "Error: expected server to use a protocol version >= "
                "SPARK_CLI_SERVICE_PROTOCOL_V2, "
                "instead got: {}".format(protocol_version)
            )

    def _check_initial_namespace(self, catalog, schema, response):
        if not (catalog or schema):
            return

        if (
            response.serverProtocolVersion
            < ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4
        ):
            raise InvalidServerResponseError(
                "Setting initial namespace not supported by the DBR version, "
                "Please use a Databricks SQL endpoint or a cluster with DBR >= 9.0."
            )

        if catalog:
            if not response.canUseMultipleCatalogs:
                raise InvalidServerResponseError(
                    "Unexpected response from server: Trying to set initial catalog to {}, "
                    + "but server does not support multiple catalogs.".format(catalog)  # type: ignore
                )

    def _check_session_configuration(self, session_configuration):
        # This client expects timetampsAsString to be false, so we do not allow users to modify that
        if (
            session_configuration.get(TIMESTAMP_AS_STRING_CONFIG, "false").lower()
            != "false"
        ):
            raise Error(
                "Invalid session configuration: {} cannot be changed "
                "while using the Databricks SQL connector, it must be false not {}".format(
                    TIMESTAMP_AS_STRING_CONFIG,
                    session_configuration[TIMESTAMP_AS_STRING_CONFIG],
                )
            )

    def open_session(self, session_configuration, catalog, schema):
        try:
            self._transport.open()
            session_configuration = {
                k: str(v) for (k, v) in (session_configuration or {}).items()
            }
            self._check_session_configuration(session_configuration)
            # We want to receive proper Timestamp arrow types.
            # We set it also in confOverlay in TExecuteStatementReq on a per query basic,
            # but it doesn't hurt to also set for the whole session.
            session_configuration[TIMESTAMP_AS_STRING_CONFIG] = "false"
            if catalog or schema:
                initial_namespace = ttypes.TNamespace(
                    catalogName=catalog, schemaName=schema
                )
            else:
                initial_namespace = None

            open_session_req = ttypes.TOpenSessionReq(
                client_protocol_i64=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5,
                client_protocol=None,
                initialNamespace=initial_namespace,
                canUseMultipleCatalogs=True,
                configuration=session_configuration,
            )
            response = self.make_request(self._client.OpenSession, open_session_req)
            self._check_initial_namespace(catalog, schema, response)
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

    def _check_command_not_in_error_or_closed_state(
        self, op_handle, get_operations_resp
    ):
        if get_operations_resp.operationState == ttypes.TOperationState.ERROR_STATE:
            if get_operations_resp.displayMessage:
                raise ServerOperationError(
                    get_operations_resp.displayMessage,
                    {
                        "operation-id": op_handle and op_handle.operationId.guid,
                        "diagnostic-info": get_operations_resp.diagnosticInfo,
                    },
                )
            else:
                raise ServerOperationError(
                    get_operations_resp.errorMessage,
                    {
                        "operation-id": op_handle and op_handle.operationId.guid,
                        "diagnostic-info": None,
                    },
                )
        elif get_operations_resp.operationState == ttypes.TOperationState.CLOSED_STATE:
            raise DatabaseError(
                "Command {} unexpectedly closed server side".format(
                    op_handle and op_handle.operationId.guid
                ),
                {"operation-id": op_handle and op_handle.operationId.guid},
            )

    def _poll_for_status(self, op_handle):
        req = ttypes.TGetOperationStatusReq(
            operationHandle=op_handle,
            getProgressUpdate=False,
        )
        return self.make_request(self._client.GetOperationStatus, req)

    def _create_arrow_table(self, t_row_set, schema_bytes, description):
        if t_row_set.columns is not None:
            (
                arrow_table,
                num_rows,
            ) = ThriftBackend._convert_column_based_set_to_arrow_table(
                t_row_set.columns, description
            )
        elif t_row_set.arrowBatches is not None:
            (
                arrow_table,
                num_rows,
            ) = ThriftBackend._convert_arrow_based_set_to_arrow_table(
                t_row_set.arrowBatches, schema_bytes
            )
        else:
            raise OperationalError("Unsupported TRowSet instance {}".format(t_row_set))
        return self._convert_decimals_in_arrow_table(arrow_table, description), num_rows

    @staticmethod
    def _convert_decimals_in_arrow_table(table, description):
        for (i, col) in enumerate(table.itercolumns()):
            if description[i][1] == "decimal":
                decimal_col = col.to_pandas().apply(
                    lambda v: v if v is None else Decimal(v)
                )
                precision, scale = description[i][4], description[i][5]
                assert scale is not None
                assert precision is not None
                # Spark limits decimal to a maximum scale of 38,
                # so 128 is guaranteed to be big enough
                dtype = pyarrow.decimal128(precision, scale)
                col_data = pyarrow.array(decimal_col, type=dtype)
                field = table.field(i).with_type(dtype)
                table = table.set_column(i, field, col_data)
        return table

    @staticmethod
    def _convert_arrow_based_set_to_arrow_table(arrow_batches, schema_bytes):
        ba = bytearray()
        ba += schema_bytes
        n_rows = 0
        for arrow_batch in arrow_batches:
            n_rows += arrow_batch.rowCount
            ba += arrow_batch.batch
        arrow_table = pyarrow.ipc.open_stream(ba).read_all()
        return arrow_table, n_rows

    @staticmethod
    def _convert_column_based_set_to_arrow_table(columns, description):
        arrow_table = pyarrow.Table.from_arrays(
            [ThriftBackend._convert_column_to_arrow_array(c) for c in columns],
            # Only use the column names from the schema, the types are determined by the
            # physical types used in column based set, as they can differ from the
            # mapping used in _hive_schema_to_arrow_schema.
            names=[c[0] for c in description],
        )
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
            "binaryVal": pyarrow.binary(),
        }
        for field in field_name_to_arrow_type.keys():
            wrapper = getattr(t_col, field)
            if wrapper:
                return ThriftBackend._create_arrow_array(
                    wrapper, field_name_to_arrow_type[field]
                )

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
                    ttypes.TTypeId.TIMESTAMP_TYPE: pyarrow.timestamp("us", None),
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
                    ttypes.TTypeId.INTERVAL_DAY_TIME_TYPE: pyarrow.string(),
                }[t_type_entry.primitiveEntry.type]
            else:
                # Current thriftserver implementation should always return a primitiveEntry,
                # even for complex types
                raise OperationalError(
                    "Thrift protocol error: t_type_entry not a primitiveEntry"
                )

        def convert_col(t_column_desc):
            return pyarrow.field(
                t_column_desc.columnName, map_type(t_column_desc.typeDesc.types[0])
            )

        return pyarrow.schema([convert_col(col) for col in t_table_schema.columns])

    @staticmethod
    def _col_to_description(col):
        type_entry = col.typeDesc.types[0]

        if type_entry.primitiveEntry:
            name = ttypes.TTypeId._VALUES_TO_NAMES[type_entry.primitiveEntry.type]
            # Drop _TYPE suffix
            cleaned_type = (name[:-5] if name.endswith("_TYPE") else name).lower()
        else:
            raise OperationalError(
                "Thrift protocol error: t_type_entry not a primitiveEntry"
            )

        if type_entry.primitiveEntry.type == ttypes.TTypeId.DECIMAL_TYPE:
            qualifiers = type_entry.primitiveEntry.typeQualifiers.qualifiers
            if qualifiers and "precision" in qualifiers and "scale" in qualifiers:
                precision, scale = (
                    qualifiers["precision"].i32Value,
                    qualifiers["scale"].i32Value,
                )
            else:
                raise OperationalError(
                    "Decimal type did not provide typeQualifier precision, scale in "
                    "primitiveEntry {}".format(type_entry.primitiveEntry)
                )
        else:
            precision, scale = None, None

        return col.columnName, cleaned_type, None, None, precision, scale, None

    @staticmethod
    def _hive_schema_to_description(t_table_schema):
        return [
            ThriftBackend._col_to_description(col) for col in t_table_schema.columns
        ]

    def _results_message_to_execute_response(self, resp, operation_state):
        if resp.directResults and resp.directResults.resultSetMetadata:
            t_result_set_metadata_resp = resp.directResults.resultSetMetadata
        else:
            t_result_set_metadata_resp = self._get_metadata_resp(resp.operationHandle)

        if t_result_set_metadata_resp.resultFormat not in [
            ttypes.TSparkRowSetType.ARROW_BASED_SET,
            ttypes.TSparkRowSetType.COLUMN_BASED_SET,
        ]:
            raise OperationalError(
                "Expected results to be in Arrow or column based format, "
                "instead they are: {}".format(
                    ttypes.TSparkRowSetType._VALUES_TO_NAMES[
                        t_result_set_metadata_resp.resultFormat
                    ]
                )
            )

        direct_results = resp.directResults
        has_been_closed_server_side = direct_results and direct_results.closeOperation
        has_more_rows = (
            (not direct_results)
            or (not direct_results.resultSet)
            or direct_results.resultSet.hasMoreRows
        )
        description = self._hive_schema_to_description(
            t_result_set_metadata_resp.schema
        )
        schema_bytes = (
            t_result_set_metadata_resp.arrowSchema
            or self._hive_schema_to_arrow_schema(t_result_set_metadata_resp.schema)
            .serialize()
            .to_pybytes()
        )

        if direct_results and direct_results.resultSet:
            assert direct_results.resultSet.results.startRowOffset == 0
            assert direct_results.resultSetMetadata
            arrow_results, n_rows = self._create_arrow_table(
                direct_results.resultSet.results, schema_bytes, description
            )
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
            arrow_schema_bytes=schema_bytes,
        )

    def _wait_until_command_done(self, op_handle, initial_operation_status_resp):
        if initial_operation_status_resp:
            self._check_command_not_in_error_or_closed_state(
                op_handle, initial_operation_status_resp
            )
        operation_state = (
            initial_operation_status_resp
            and initial_operation_status_resp.operationState
        )
        while not operation_state or operation_state in [
            ttypes.TOperationState.RUNNING_STATE,
            ttypes.TOperationState.PENDING_STATE,
        ]:
            poll_resp = self._poll_for_status(op_handle)
            operation_state = poll_resp.operationState
            self._check_command_not_in_error_or_closed_state(op_handle, poll_resp)
        return operation_state

    @staticmethod
    def _check_direct_results_for_error(t_spark_direct_results):
        if t_spark_direct_results:
            if t_spark_direct_results.operationStatus:
                ThriftBackend._check_response_for_error(
                    t_spark_direct_results.operationStatus
                )
            if t_spark_direct_results.resultSetMetadata:
                ThriftBackend._check_response_for_error(
                    t_spark_direct_results.resultSetMetadata
                )
            if t_spark_direct_results.resultSet:
                ThriftBackend._check_response_for_error(
                    t_spark_direct_results.resultSet
                )
            if t_spark_direct_results.closeOperation:
                ThriftBackend._check_response_for_error(
                    t_spark_direct_results.closeOperation
                )

    def execute_command(self, operation, session_handle, max_rows, max_bytes, cursor):
        assert session_handle is not None

        spark_arrow_types = ttypes.TSparkArrowTypes(
            timestampAsArrow=self._use_arrow_native_timestamps,
            decimalAsArrow=self._use_arrow_native_decimals,
            complexTypesAsArrow=self._use_arrow_native_complex_types,
            # TODO: The current Arrow type used for intervals can not be deserialised in PyArrow
            # DBR should be changed to use month_day_nano_interval
            intervalTypesAsArrow=False,
        )
        req = ttypes.TExecuteStatementReq(
            sessionHandle=session_handle,
            statement=operation,
            runAsync=True,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            canReadArrowResult=True,
            canDecompressLZ4Result=False,
            canDownloadResult=False,
            confOverlay={
                # We want to receive proper Timestamp arrow types.
                "spark.thriftserver.arrowBasedRowSet.timestampAsString": "false"
            },
            useArrowNativeTypes=spark_arrow_types,
        )
        resp = self.make_request(self._client.ExecuteStatement, req)
        return self._handle_execute_response(resp, cursor)

    def get_catalogs(self, session_handle, max_rows, max_bytes, cursor):
        assert session_handle is not None

        req = ttypes.TGetCatalogsReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
        )
        resp = self.make_request(self._client.GetCatalogs, req)
        return self._handle_execute_response(resp, cursor)

    def get_schemas(
        self,
        session_handle,
        max_rows,
        max_bytes,
        cursor,
        catalog_name=None,
        schema_name=None,
    ):
        assert session_handle is not None

        req = ttypes.TGetSchemasReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            catalogName=catalog_name,
            schemaName=schema_name,
        )
        resp = self.make_request(self._client.GetSchemas, req)
        return self._handle_execute_response(resp, cursor)

    def get_tables(
        self,
        session_handle,
        max_rows,
        max_bytes,
        cursor,
        catalog_name=None,
        schema_name=None,
        table_name=None,
        table_types=None,
    ):
        assert session_handle is not None

        req = ttypes.TGetTablesReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            tableTypes=table_types,
        )
        resp = self.make_request(self._client.GetTables, req)
        return self._handle_execute_response(resp, cursor)

    def get_columns(
        self,
        session_handle,
        max_rows,
        max_bytes,
        cursor,
        catalog_name=None,
        schema_name=None,
        table_name=None,
        column_name=None,
    ):
        assert session_handle is not None

        req = ttypes.TGetColumnsReq(
            sessionHandle=session_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            columnName=column_name,
        )
        resp = self.make_request(self._client.GetColumns, req)
        return self._handle_execute_response(resp, cursor)

    def _handle_execute_response(self, resp, cursor):
        cursor.active_op_handle = resp.operationHandle
        self._check_direct_results_for_error(resp.directResults)

        final_operation_state = self._wait_until_command_done(
            resp.operationHandle,
            resp.directResults and resp.directResults.operationStatus,
        )

        return self._results_message_to_execute_response(resp, final_operation_state)

    def fetch_results(
        self,
        op_handle,
        max_rows,
        max_bytes,
        expected_row_start_offset,
        arrow_schema_bytes,
        description,
    ):
        assert op_handle is not None

        req = ttypes.TFetchResultsReq(
            operationHandle=ttypes.TOperationHandle(
                op_handle.operationId,
                op_handle.operationType,
                False,
                op_handle.modifiedRowCount,
            ),
            maxRows=max_rows,
            maxBytes=max_bytes,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
        )

        resp = self.make_request(self._client.FetchResults, req)
        if resp.results.startRowOffset > expected_row_start_offset:
            logger.warning(
                "Expected results to start from {} but they instead start at {}".format(
                    expected_row_start_offset, resp.results.startRowOffset
                )
            )
        arrow_results, n_rows = self._create_arrow_table(
            resp.results, arrow_schema_bytes, description
        )
        arrow_queue = ArrowQueue(arrow_results, n_rows)

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
