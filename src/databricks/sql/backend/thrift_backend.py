from __future__ import annotations

import errno
import logging
import math
import time
import threading
from typing import List, Optional, Union, Any, TYPE_CHECKING
from uuid import UUID

from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.result_set import ThriftResultSet
from databricks.sql.telemetry.models.event import StatementType


if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

from databricks.sql.backend.types import (
    CommandState,
    SessionId,
    CommandId,
    ExecuteResponse,
)
from databricks.sql.backend.utils import guid_to_hex_id

try:
    import pyarrow
except ImportError:
    pyarrow = None
import thrift.transport.THttpClient
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

import urllib3.exceptions

import databricks.sql.auth.thrift_http_client
from databricks.sql.auth.thrift_http_client import CommandType
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.thrift_api.TCLIService import TCLIService, ttypes
from databricks.sql import *
from databricks.sql.thrift_api.TCLIService.TCLIService import (
    Client as TCLIServiceClient,
)

from databricks.sql.utils import (
    ThriftResultSetQueueFactory,
    _bound,
    RequestErrorInfo,
    NoRetryReason,
    convert_arrow_based_set_to_arrow_table,
    convert_decimals_in_arrow_table,
    convert_column_based_set_to_arrow_table,
)
from databricks.sql.types import SSLOptions
from databricks.sql.backend.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)

unsafe_logger = logging.getLogger("databricks.sql.unsafe")
unsafe_logger.setLevel(logging.DEBUG)

# To capture these logs in client code, add a non-NullHandler.
# See our e2e test suite for an example with logging.FileHandler
unsafe_logger.addHandler(logging.NullHandler())

# Disable propagation so that handlers for `databricks.sql` don't pick up these messages
unsafe_logger.propagate = False

THRIFT_ERROR_MESSAGE_HEADER = "x-thriftserver-error-message"
DATABRICKS_ERROR_OR_REDIRECT_HEADER = "x-databricks-error-or-redirect-message"
DATABRICKS_REASON_HEADER = "x-databricks-reason-phrase"

TIMESTAMP_AS_STRING_CONFIG = "spark.thriftserver.arrowBasedRowSet.timestampAsString"
DEFAULT_SOCKET_TIMEOUT = float(900)

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


class ThriftDatabricksClient(DatabricksClient):
    CLOSED_OP_STATE = CommandState.CLOSED
    ERROR_OP_STATE = CommandState.FAILED

    _retry_delay_min: float
    _retry_delay_max: float
    _retry_stop_after_attempts_count: int
    _retry_stop_after_attempts_duration: float
    _retry_delay_default: float

    def __init__(
        self,
        server_hostname: str,
        port,
        http_path: str,
        http_headers,
        auth_provider: AuthProvider,
        ssl_options: SSLOptions,
        http_client: UnifiedHttpClient,
        **kwargs,
    ):
        # Internal arguments in **kwargs:
        # _username, _password
        #   Username and password Basic authentication (no official support)
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
        # _retry_dangerous_codes
        #  An iterable of integer HTTP status codes. ExecuteStatement commands will be retried if these codes are received.
        #  (defaults to [])
        # _socket_timeout
        #  The timeout in seconds for socket send, recv and connect operations. Should be a positive float or integer.
        #  (defaults to 900)
        # _enable_v3_retries
        # Whether to use the DatabricksRetryPolicy implemented in urllib3
        # (defaults to True)
        # _retry_max_redirects
        #  An integer representing the maximum number of redirects to follow for a request.
        #  This number must be <= _retry_stop_after_attempts_count.
        #  (defaults to None)
        # max_download_threads
        #  Number of threads for handling cloud fetch downloads. Defaults to 10

        logger.debug(
            "ThriftBackend.__init__(server_hostname=%s, port=%s, http_path=%s)"
            % (server_hostname, port, http_path)
        )

        port = port or 443
        if kwargs.get("_connection_uri"):
            uri = kwargs.get("_connection_uri")
        elif server_hostname and http_path:
            uri = "{host}:{port}/{path}".format(
                host=server_hostname.rstrip("/"), port=port, path=http_path.lstrip("/")
            )
            if not uri.startswith("https://"):
                uri = "https://" + uri
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

        # Cloud fetch
        self._max_download_threads = kwargs.get("max_download_threads", 10)

        self._ssl_options = ssl_options
        self._auth_provider = auth_provider
        self._http_client = http_client

        # Connector version 3 retry approach
        self.enable_v3_retries = kwargs.get("_enable_v3_retries", True)

        if not self.enable_v3_retries:
            logger.warning(
                "Legacy retry behavior is enabled for this connection."
                " This behaviour is deprecated and will be removed in a future release."
            )
        self.force_dangerous_codes = kwargs.get("_retry_dangerous_codes", [])

        additional_transport_args = {}

        # Add proxy authentication method if specified
        proxy_auth_method = kwargs.get("_proxy_auth_method")
        if proxy_auth_method:
            additional_transport_args["_proxy_auth_method"] = proxy_auth_method

        _max_redirects: Union[None, int] = kwargs.get("_retry_max_redirects")

        if _max_redirects:
            if _max_redirects > self._retry_stop_after_attempts_count:
                logger.warning(
                    "_retry_max_redirects > _retry_stop_after_attempts_count so it will have no affect!"
                )
            urllib3_kwargs = {"redirect": _max_redirects}
        else:
            urllib3_kwargs = {}
        if self.enable_v3_retries:
            self.retry_policy = databricks.sql.auth.thrift_http_client.DatabricksRetryPolicy(
                delay_min=self._retry_delay_min,
                delay_max=self._retry_delay_max,
                stop_after_attempts_count=self._retry_stop_after_attempts_count,
                stop_after_attempts_duration=self._retry_stop_after_attempts_duration,
                delay_default=self._retry_delay_default,
                force_dangerous_codes=self.force_dangerous_codes,
                urllib3_kwargs=urllib3_kwargs,
            )

            additional_transport_args["retry_policy"] = self.retry_policy

        self._transport = databricks.sql.auth.thrift_http_client.THttpClient(
            auth_provider=self._auth_provider,
            uri_or_host=uri,
            ssl_options=self._ssl_options,
            **additional_transport_args,  # type: ignore
        )

        timeout = kwargs.get("_socket_timeout", DEFAULT_SOCKET_TIMEOUT)
        # setTimeout defaults to 15 minutes and is expected in ms
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
        self._session_id_hex = None

    @property
    def max_download_threads(self) -> int:
        return self._max_download_threads

    # TODO: Move this bounding logic into DatabricksRetryPolicy for v3 (PECO-918)
    def _initialize_retry_args(self, kwargs):
        # Configure retries & timing: use user-settings or defaults, and bound
        # by policy. Log.warn when given param gets restricted.
        for key, (type_, default, min, max) in _retry_policy.items():
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
    def _check_response_for_error(response, session_id_hex=None):
        if response.status and response.status.statusCode in [
            ttypes.TStatusCode.ERROR_STATUS,
            ttypes.TStatusCode.INVALID_HANDLE_STATUS,
        ]:
            raise DatabaseError(
                response.status.errorMessage,
                session_id_hex=session_id_hex,
            )

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

        if not err_msg:
            # if authentication token is invalid we need this branch
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
                user_friendly_error_message,
                full_error_info_context,
                self._session_id_hex,
                error_info.error,
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
    def make_request(self, method, request, retryable=True):
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
            retry_after = getattr(self._transport, "headers", {}).get("Retry-After", 1)
            if http_code in [429, 503]:
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
                this_method_name = getattr(method, "__name__")

                logger.debug("Sending request: {}(<REDACTED>)".format(this_method_name))
                unsafe_logger.debug("Sending request: {}".format(request))

                # These three lines are no-ops if the v3 retry policy is not in use
                if self.enable_v3_retries:
                    this_command_type = CommandType.get(this_method_name)
                    self._transport.set_retry_command_type(this_command_type)
                    self._transport.startRetryTimer()

                response = method(request)

                # We need to call type(response) here because thrift doesn't implement __name__ attributes for thrift responses
                logger.debug(
                    "Received response: {}(<REDACTED>)".format(type(response).__name__)
                )
                unsafe_logger.debug("Received response: {}".format(response))
                return response

            except urllib3.exceptions.HTTPError as err:
                # retry on timeout. Happens a lot in Azure and it is safe as data has not been sent to server yet

                # TODO: don't use exception handling for GOS polling...

                logger.error("ThriftBackend.attempt_request: HTTPError: %s", err)

                gos_name = TCLIServiceClient.GetOperationStatus.__name__
                if method.__name__ == gos_name:
                    delay_default = (
                        self.enable_v3_retries
                        and self.retry_policy.delay_default
                        or self._retry_delay_default
                    )
                    retry_delay = bound_retry_delay(attempt, delay_default)
                    logger.info(
                        f"GetOperationStatus failed with HTTP error and will be retried: {str(err)}"
                    )
                else:
                    raise err
            except OSError as err:
                error = err
                error_message = str(err)
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

                gos_name = TCLIServiceClient.GetOperationStatus.__name__
                # retry on timeout. Happens a lot in Azure and it is safe as data has not been sent to server yet
                if method.__name__ == gos_name or err.errno == errno.ETIMEDOUT:
                    retry_delay = bound_retry_delay(attempt, self._retry_delay_default)

                    # fmt: on
                    log_string = f"{gos_name} failed with code {err.errno} and will attempt to retry"
                    if err.errno in info_errs:
                        logger.info(log_string)
                    else:
                        logger.warning(log_string)
            except Exception as err:
                logger.error("ThriftBackend.attempt_request: Exception: %s", err)
                error = err
                retry_delay = extract_retry_delay(attempt)
                error_message = (
                    ThriftDatabricksClient._extract_error_message_from_headers(
                        getattr(self._transport, "headers", {})
                    )
                )
            finally:
                # Calling `close()` here releases the active HTTP connection back to the pool
                self._transport.close()

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
        max_attempts = self._retry_stop_after_attempts_count if retryable else 1

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
                ThriftDatabricksClient._check_response_for_error(
                    response, self._session_id_hex
                )
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
                "instead got: {}".format(protocol_version),
                session_id_hex=self._session_id_hex,
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
                "Please use a Databricks SQL endpoint or a cluster with DBR >= 9.0.",
                session_id_hex=self._session_id_hex,
            )

        if catalog:
            if not response.canUseMultipleCatalogs:
                raise InvalidServerResponseError(
                    "Unexpected response from server: Trying to set initial catalog to {}, "
                    + "but server does not support multiple catalogs.".format(catalog),  # type: ignore
                    session_id_hex=self._session_id_hex,
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
                ),
                session_id_hex=self._session_id_hex,
            )

    def open_session(self, session_configuration, catalog, schema) -> SessionId:
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
                client_protocol_i64=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                client_protocol=None,
                initialNamespace=initial_namespace,
                canUseMultipleCatalogs=True,
                configuration=session_configuration,
            )
            response = self.make_request(self._client.OpenSession, open_session_req)
            self._check_initial_namespace(catalog, schema, response)
            self._check_protocol_version(response)

            properties = (
                {"serverProtocolVersion": response.serverProtocolVersion}
                if response.serverProtocolVersion
                else {}
            )
            session_id = SessionId.from_thrift_handle(
                response.sessionHandle, properties
            )
            self._session_id_hex = session_id.hex_guid
            return session_id
        except:
            self._transport.close()
            raise

    def close_session(self, session_id: SessionId) -> None:
        thrift_handle = session_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift session ID")

        req = ttypes.TCloseSessionReq(sessionHandle=thrift_handle)
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
                        "operation-id": op_handle
                        and guid_to_hex_id(op_handle.operationId.guid),
                        "diagnostic-info": get_operations_resp.diagnosticInfo,
                    },
                    session_id_hex=self._session_id_hex,
                )
            else:
                raise ServerOperationError(
                    get_operations_resp.errorMessage,
                    {
                        "operation-id": op_handle
                        and guid_to_hex_id(op_handle.operationId.guid),
                        "diagnostic-info": None,
                    },
                    session_id_hex=self._session_id_hex,
                )
        elif get_operations_resp.operationState == ttypes.TOperationState.CLOSED_STATE:
            raise DatabaseError(
                "Command {} unexpectedly closed server side".format(
                    op_handle and guid_to_hex_id(op_handle.operationId.guid)
                ),
                {
                    "operation-id": op_handle
                    and guid_to_hex_id(op_handle.operationId.guid)
                },
                session_id_hex=self._session_id_hex,
            )

    def _poll_for_status(self, op_handle):
        req = ttypes.TGetOperationStatusReq(
            operationHandle=op_handle,
            getProgressUpdate=False,
        )
        return self.make_request(self._client.GetOperationStatus, req)

    def _create_arrow_table(self, t_row_set, lz4_compressed, schema_bytes, description):
        if t_row_set.columns is not None:
            (
                arrow_table,
                num_rows,
            ) = convert_column_based_set_to_arrow_table(t_row_set.columns, description)
        elif t_row_set.arrowBatches is not None:
            (arrow_table, num_rows,) = convert_arrow_based_set_to_arrow_table(
                t_row_set.arrowBatches, lz4_compressed, schema_bytes
            )
        else:
            raise OperationalError(
                "Unsupported TRowSet instance {}".format(t_row_set),
                session_id_hex=self._session_id_hex,
            )
        return convert_decimals_in_arrow_table(arrow_table, description), num_rows

    def _get_metadata_resp(self, op_handle):
        req = ttypes.TGetResultSetMetadataReq(operationHandle=op_handle)
        return self.make_request(self._client.GetResultSetMetadata, req)

    @staticmethod
    def _hive_schema_to_arrow_schema(t_table_schema, session_id_hex=None):
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
                    "Thrift protocol error: t_type_entry not a primitiveEntry",
                    session_id_hex=session_id_hex,
                )

        def convert_col(t_column_desc):
            return pyarrow.field(
                t_column_desc.columnName, map_type(t_column_desc.typeDesc.types[0])
            )

        return pyarrow.schema([convert_col(col) for col in t_table_schema.columns])

    @staticmethod
    def _col_to_description(col, session_id_hex=None):
        type_entry = col.typeDesc.types[0]

        if type_entry.primitiveEntry:
            name = ttypes.TTypeId._VALUES_TO_NAMES[type_entry.primitiveEntry.type]
            # Drop _TYPE suffix
            cleaned_type = (name[:-5] if name.endswith("_TYPE") else name).lower()
        else:
            raise OperationalError(
                "Thrift protocol error: t_type_entry not a primitiveEntry",
                session_id_hex=session_id_hex,
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
                    "primitiveEntry {}".format(type_entry.primitiveEntry),
                    session_id_hex=session_id_hex,
                )
        else:
            precision, scale = None, None

        return col.columnName, cleaned_type, None, None, precision, scale, None

    @staticmethod
    def _hive_schema_to_description(t_table_schema, session_id_hex=None):
        return [
            ThriftDatabricksClient._col_to_description(col, session_id_hex)
            for col in t_table_schema.columns
        ]

    def _results_message_to_execute_response(self, resp, operation_state):
        if resp.directResults and resp.directResults.resultSetMetadata:
            t_result_set_metadata_resp = resp.directResults.resultSetMetadata
        else:
            t_result_set_metadata_resp = self._get_metadata_resp(resp.operationHandle)

        if t_result_set_metadata_resp.resultFormat not in [
            ttypes.TSparkRowSetType.ARROW_BASED_SET,
            ttypes.TSparkRowSetType.COLUMN_BASED_SET,
            ttypes.TSparkRowSetType.URL_BASED_SET,
        ]:
            raise OperationalError(
                "Expected results to be in Arrow or column based format, "
                "instead they are: {}".format(
                    ttypes.TSparkRowSetType._VALUES_TO_NAMES[
                        t_result_set_metadata_resp.resultFormat
                    ]
                ),
                session_id_hex=self._session_id_hex,
            )
        direct_results = resp.directResults
        has_been_closed_server_side = direct_results and direct_results.closeOperation

        has_more_rows = (
            (not direct_results)
            or (not direct_results.resultSet)
            or direct_results.resultSet.hasMoreRows
        )

        description = self._hive_schema_to_description(
            t_result_set_metadata_resp.schema,
            self._session_id_hex,
        )

        if pyarrow:
            schema_bytes = (
                t_result_set_metadata_resp.arrowSchema
                or self._hive_schema_to_arrow_schema(
                    t_result_set_metadata_resp.schema, self._session_id_hex
                )
                .serialize()
                .to_pybytes()
            )
        else:
            schema_bytes = None

        lz4_compressed = t_result_set_metadata_resp.lz4Compressed
        command_id = CommandId.from_thrift_handle(resp.operationHandle)

        status = CommandState.from_thrift_state(operation_state)
        if status is None:
            raise ValueError(f"Unknown command state: {operation_state}")

        execute_response = ExecuteResponse(
            command_id=command_id,
            status=status,
            description=description,
            has_been_closed_server_side=has_been_closed_server_side,
            lz4_compressed=lz4_compressed,
            is_staging_operation=t_result_set_metadata_resp.isStagingOperation,
            arrow_schema_bytes=schema_bytes,
            result_format=t_result_set_metadata_resp.resultFormat,
        )

        return execute_response, has_more_rows

    def get_execution_result(
        self, command_id: CommandId, cursor: Cursor
    ) -> "ResultSet":
        thrift_handle = command_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift command ID")

        req = ttypes.TFetchResultsReq(
            operationHandle=ttypes.TOperationHandle(
                thrift_handle.operationId,
                thrift_handle.operationType,
                False,
                thrift_handle.modifiedRowCount,
            ),
            maxRows=cursor.arraysize,
            maxBytes=cursor.buffer_size_bytes,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
            includeResultSetMetadata=True,
        )

        resp = self.make_request(self._client.FetchResults, req)

        t_result_set_metadata_resp = resp.resultSetMetadata

        description = self._hive_schema_to_description(
            t_result_set_metadata_resp.schema,
            self._session_id_hex,
        )

        if pyarrow:
            schema_bytes = (
                t_result_set_metadata_resp.arrowSchema
                or self._hive_schema_to_arrow_schema(
                    t_result_set_metadata_resp.schema, self._session_id_hex
                )
                .serialize()
                .to_pybytes()
            )
        else:
            schema_bytes = None

        lz4_compressed = t_result_set_metadata_resp.lz4Compressed
        is_staging_operation = t_result_set_metadata_resp.isStagingOperation
        has_more_rows = resp.hasMoreRows

        status = CommandState.from_thrift_state(resp.status) or CommandState.RUNNING

        execute_response = ExecuteResponse(
            command_id=command_id,
            status=status,
            description=description,
            has_been_closed_server_side=False,
            lz4_compressed=lz4_compressed,
            is_staging_operation=is_staging_operation,
            arrow_schema_bytes=schema_bytes,
            result_format=t_result_set_metadata_resp.resultFormat,
        )

        return ThriftResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            thrift_client=self,
            buffer_size_bytes=cursor.buffer_size_bytes,
            arraysize=cursor.arraysize,
            use_cloud_fetch=cursor.connection.use_cloud_fetch,
            t_row_set=resp.results,
            max_download_threads=self.max_download_threads,
            ssl_options=self._ssl_options,
            has_more_rows=has_more_rows,
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

    def get_query_state(self, command_id: CommandId) -> CommandState:
        thrift_handle = command_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift command ID")

        poll_resp = self._poll_for_status(thrift_handle)
        operation_state = poll_resp.operationState
        self._check_command_not_in_error_or_closed_state(thrift_handle, poll_resp)
        state = CommandState.from_thrift_state(operation_state)
        if state is None:
            raise ValueError(f"Unknown command state: {operation_state}")
        return state

    @staticmethod
    def _check_direct_results_for_error(t_spark_direct_results, session_id_hex=None):
        if t_spark_direct_results:
            if t_spark_direct_results.operationStatus:
                ThriftDatabricksClient._check_response_for_error(
                    t_spark_direct_results.operationStatus,
                    session_id_hex,
                )
            if t_spark_direct_results.resultSetMetadata:
                ThriftDatabricksClient._check_response_for_error(
                    t_spark_direct_results.resultSetMetadata,
                    session_id_hex,
                )
            if t_spark_direct_results.resultSet:
                ThriftDatabricksClient._check_response_for_error(
                    t_spark_direct_results.resultSet,
                    session_id_hex,
                )
            if t_spark_direct_results.closeOperation:
                ThriftDatabricksClient._check_response_for_error(
                    t_spark_direct_results.closeOperation,
                    session_id_hex,
                )

    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: Cursor,
        use_cloud_fetch=True,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
        row_limit: Optional[int] = None,
    ) -> Union["ResultSet", None]:
        thrift_handle = session_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift session ID")

        logger.debug(
            "ThriftBackend.execute_command(operation=%s, session_handle=%s)",
            operation,
            thrift_handle,
        )

        spark_arrow_types = ttypes.TSparkArrowTypes(
            timestampAsArrow=self._use_arrow_native_timestamps,
            decimalAsArrow=self._use_arrow_native_decimals,
            complexTypesAsArrow=self._use_arrow_native_complex_types,
            # TODO: The current Arrow type used for intervals can not be deserialised in PyArrow
            # DBR should be changed to use month_day_nano_interval
            intervalTypesAsArrow=False,
        )
        req = ttypes.TExecuteStatementReq(
            sessionHandle=thrift_handle,
            statement=operation,
            runAsync=True,
            # For async operation we don't want the direct results
            getDirectResults=None
            if async_op
            else ttypes.TSparkGetDirectResults(
                maxRows=max_rows,
                maxBytes=max_bytes,
            ),
            canReadArrowResult=True if pyarrow else False,
            canDecompressLZ4Result=lz4_compression,
            canDownloadResult=use_cloud_fetch,
            confOverlay={
                # We want to receive proper Timestamp arrow types.
                "spark.thriftserver.arrowBasedRowSet.timestampAsString": "false"
            },
            useArrowNativeTypes=spark_arrow_types,
            parameters=parameters,
            enforceEmbeddedSchemaCorrectness=enforce_embedded_schema_correctness,
            resultRowLimit=row_limit,
        )
        resp = self.make_request(self._client.ExecuteStatement, req)

        if async_op:
            self._handle_execute_response_async(resp, cursor)
            return None
        else:
            execute_response, has_more_rows = self._handle_execute_response(
                resp, cursor
            )

            t_row_set = None
            if resp.directResults and resp.directResults.resultSet:
                t_row_set = resp.directResults.resultSet.results

            return ThriftResultSet(
                connection=cursor.connection,
                execute_response=execute_response,
                thrift_client=self,
                buffer_size_bytes=max_bytes,
                arraysize=max_rows,
                use_cloud_fetch=use_cloud_fetch,
                t_row_set=t_row_set,
                max_download_threads=self.max_download_threads,
                ssl_options=self._ssl_options,
                has_more_rows=has_more_rows,
            )

    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
    ) -> ResultSet:
        thrift_handle = session_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift session ID")

        req = ttypes.TGetCatalogsReq(
            sessionHandle=thrift_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
        )
        resp = self.make_request(self._client.GetCatalogs, req)

        execute_response, has_more_rows = self._handle_execute_response(resp, cursor)

        t_row_set = None
        if resp.directResults and resp.directResults.resultSet:
            t_row_set = resp.directResults.resultSet.results

        return ThriftResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            thrift_client=self,
            buffer_size_bytes=max_bytes,
            arraysize=max_rows,
            use_cloud_fetch=cursor.connection.use_cloud_fetch,
            t_row_set=t_row_set,
            max_download_threads=self.max_download_threads,
            ssl_options=self._ssl_options,
            has_more_rows=has_more_rows,
        )

    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name=None,
        schema_name=None,
    ) -> ResultSet:
        from databricks.sql.result_set import ThriftResultSet

        thrift_handle = session_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift session ID")

        req = ttypes.TGetSchemasReq(
            sessionHandle=thrift_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            catalogName=catalog_name,
            schemaName=schema_name,
        )
        resp = self.make_request(self._client.GetSchemas, req)

        execute_response, has_more_rows = self._handle_execute_response(resp, cursor)

        t_row_set = None
        if resp.directResults and resp.directResults.resultSet:
            t_row_set = resp.directResults.resultSet.results

        return ThriftResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            thrift_client=self,
            buffer_size_bytes=max_bytes,
            arraysize=max_rows,
            use_cloud_fetch=cursor.connection.use_cloud_fetch,
            t_row_set=t_row_set,
            max_download_threads=self.max_download_threads,
            ssl_options=self._ssl_options,
            has_more_rows=has_more_rows,
        )

    def get_tables(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name=None,
        schema_name=None,
        table_name=None,
        table_types=None,
    ) -> ResultSet:
        from databricks.sql.result_set import ThriftResultSet

        thrift_handle = session_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift session ID")

        req = ttypes.TGetTablesReq(
            sessionHandle=thrift_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            tableTypes=table_types,
        )
        resp = self.make_request(self._client.GetTables, req)

        execute_response, has_more_rows = self._handle_execute_response(resp, cursor)

        t_row_set = None
        if resp.directResults and resp.directResults.resultSet:
            t_row_set = resp.directResults.resultSet.results

        return ThriftResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            thrift_client=self,
            buffer_size_bytes=max_bytes,
            arraysize=max_rows,
            use_cloud_fetch=cursor.connection.use_cloud_fetch,
            t_row_set=t_row_set,
            max_download_threads=self.max_download_threads,
            ssl_options=self._ssl_options,
            has_more_rows=has_more_rows,
        )

    def get_columns(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name=None,
        schema_name=None,
        table_name=None,
        column_name=None,
    ) -> ResultSet:
        from databricks.sql.result_set import ThriftResultSet

        thrift_handle = session_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift session ID")

        req = ttypes.TGetColumnsReq(
            sessionHandle=thrift_handle,
            getDirectResults=ttypes.TSparkGetDirectResults(
                maxRows=max_rows, maxBytes=max_bytes
            ),
            catalogName=catalog_name,
            schemaName=schema_name,
            tableName=table_name,
            columnName=column_name,
        )
        resp = self.make_request(self._client.GetColumns, req)

        execute_response, has_more_rows = self._handle_execute_response(resp, cursor)

        t_row_set = None
        if resp.directResults and resp.directResults.resultSet:
            t_row_set = resp.directResults.resultSet.results

        return ThriftResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            thrift_client=self,
            buffer_size_bytes=max_bytes,
            arraysize=max_rows,
            use_cloud_fetch=cursor.connection.use_cloud_fetch,
            t_row_set=t_row_set,
            max_download_threads=self.max_download_threads,
            ssl_options=self._ssl_options,
            has_more_rows=has_more_rows,
        )

    def _handle_execute_response(self, resp, cursor):
        command_id = CommandId.from_thrift_handle(resp.operationHandle)
        if command_id is None:
            raise ValueError(f"Invalid Thrift handle: {resp.operationHandle}")

        cursor.active_command_id = command_id
        self._check_direct_results_for_error(resp.directResults, self._session_id_hex)

        final_operation_state = self._wait_until_command_done(
            resp.operationHandle,
            resp.directResults and resp.directResults.operationStatus,
        )

        return self._results_message_to_execute_response(resp, final_operation_state)

    def _handle_execute_response_async(self, resp, cursor):
        command_id = CommandId.from_thrift_handle(resp.operationHandle)
        if command_id is None:
            raise ValueError(f"Invalid Thrift handle: {resp.operationHandle}")

        cursor.active_command_id = command_id
        self._check_direct_results_for_error(resp.directResults, self._session_id_hex)

    def fetch_results(
        self,
        command_id: CommandId,
        max_rows: int,
        max_bytes: int,
        expected_row_start_offset: int,
        lz4_compressed: bool,
        arrow_schema_bytes,
        description,
        chunk_id: int,
        use_cloud_fetch=True,
    ):
        thrift_handle = command_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift command ID")

        req = ttypes.TFetchResultsReq(
            operationHandle=ttypes.TOperationHandle(
                thrift_handle.operationId,
                thrift_handle.operationType,
                False,
                thrift_handle.modifiedRowCount,
            ),
            maxRows=max_rows,
            maxBytes=max_bytes,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
            includeResultSetMetadata=True,
        )

        # Fetch results in Inline mode with FETCH_NEXT orientation are not idempotent and hence not retried
        resp = self.make_request(self._client.FetchResults, req, use_cloud_fetch)
        if resp.results.startRowOffset > expected_row_start_offset:
            raise DataError(
                "fetch_results failed due to inconsistency in the state between the client and the server. Expected results to start from {} but they instead start at {}, some result batches must have been skipped".format(
                    expected_row_start_offset, resp.results.startRowOffset
                ),
                session_id_hex=self._session_id_hex,
            )

        queue = ThriftResultSetQueueFactory.build_queue(
            row_set_type=resp.resultSetMetadata.resultFormat,
            t_row_set=resp.results,
            arrow_schema_bytes=arrow_schema_bytes,
            max_download_threads=self.max_download_threads,
            lz4_compressed=lz4_compressed,
            description=description,
            ssl_options=self._ssl_options,
            session_id_hex=self._session_id_hex,
            statement_id=command_id.to_hex_guid(),
            chunk_id=chunk_id,
            http_client=self._http_client,
        )

        return (
            queue,
            resp.hasMoreRows,
            len(resp.results.resultLinks) if resp.results.resultLinks else 0,
        )

    def cancel_command(self, command_id: CommandId) -> None:
        thrift_handle = command_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift command ID")

        logger.debug("Cancelling command %s", command_id.to_hex_guid())
        req = ttypes.TCancelOperationReq(thrift_handle)
        self.make_request(self._client.CancelOperation, req)

    def close_command(self, command_id: CommandId) -> None:
        thrift_handle = command_id.to_thrift_handle()
        if not thrift_handle:
            raise ValueError("Not a valid Thrift command ID")

        logger.debug("ThriftBackend.close_command(command_id=%s)", command_id)
        req = ttypes.TCloseOperationReq(operationHandle=thrift_handle)
        self.make_request(self._client.CloseOperation, req)
