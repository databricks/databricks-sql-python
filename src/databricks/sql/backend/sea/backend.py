import errno
import logging
import math
import threading
import uuid
import time
import re
from typing import Any, Dict, Tuple, List, Optional, Union, TYPE_CHECKING, Set

import urllib3

import databricks
from databricks.sql.auth.retry import CommandType
from databricks.sql.backend.sea.models.base import ExternalLink
from databricks.sql.backend.sea.utils.constants import (
    ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP,
    ResultFormat,
    ResultDisposition,
    ResultCompression,
    WaitTimeout,
    MetadataCommands,
)

from databricks.sql.backend.thrift_backend import (
    DATABRICKS_ERROR_OR_REDIRECT_HEADER,
    DATABRICKS_REASON_HEADER,
    THRIFT_ERROR_MESSAGE_HEADER,
    DEFAULT_SOCKET_TIMEOUT,
)
from databricks.sql.utils import NoRetryReason, RequestErrorInfo, _bound
from databricks.sql.thrift_api.TCLIService.TCLIService import (
    Client as TCLIServiceClient,
)

if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import (
    SessionId,
    CommandId,
    CommandState,
    BackendType,
    ExecuteResponse,
)
from databricks.sql.exc import DatabaseError, RequestError, ServerOperationError
from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.backend.sea.utils.http_client_adapter import SeaHttpClientAdapter
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions

from databricks.sql.backend.sea.models import (
    ExecuteStatementRequest,
    GetStatementRequest,
    CancelStatementRequest,
    CloseStatementRequest,
    CreateSessionRequest,
    DeleteSessionRequest,
    StatementParameter,
    ExecuteStatementResponse,
    GetStatementResponse,
    CreateSessionResponse,
)
from databricks.sql.backend.sea.models.responses import GetChunksResponse

logger = logging.getLogger(__name__)

unsafe_logger = logging.getLogger("databricks.sql.unsafe")
unsafe_logger.setLevel(logging.DEBUG)

# To capture these logs in client code, add a non-NullHandler.
# See our e2e test suite for an example with logging.FileHandler
unsafe_logger.addHandler(logging.NullHandler())

# Disable propagation so that handlers for `databricks.sql` don't pick up these messages
unsafe_logger.propagate = False

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


def _filter_session_configuration(
    session_configuration: Optional[Dict[str, str]]
) -> Optional[Dict[str, str]]:
    if not session_configuration:
        return None

    filtered_session_configuration = {}
    ignored_configs: Set[str] = set()

    for key, value in session_configuration.items():
        if key.upper() in ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP:
            filtered_session_configuration[key.lower()] = value
        else:
            ignored_configs.add(key)

    if ignored_configs:
        logger.warning(
            "Some session configurations were ignored because they are not supported: %s",
            ignored_configs,
        )
        logger.warning(
            "Supported session configurations are: %s",
            list(ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keys()),
        )

    return filtered_session_configuration


class SeaDatabricksClient(DatabricksClient):
    """
    Statement Execution API (SEA) implementation of the DatabricksClient interface.
    """

    # SEA API paths
    BASE_PATH = "/api/2.0/sql/"
    SESSION_PATH = BASE_PATH + "sessions"
    SESSION_PATH_WITH_ID = SESSION_PATH + "/{}"
    STATEMENT_PATH = BASE_PATH + "statements"
    STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "/{}"
    CANCEL_STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "/{}/cancel"
    CHUNK_PATH_WITH_ID_AND_INDEX = STATEMENT_PATH + "/{}/result/chunks/{}"

    _retry_delay_min: float
    _retry_delay_max: float
    _retry_stop_after_attempts_count: int
    _retry_stop_after_attempts_duration: float
    _retry_delay_default: float

    def __init__(
        self,
        server_hostname: str,
        port: int,
        http_path: str,
        http_headers: List[Tuple[str, str]],
        auth_provider,
        ssl_options: SSLOptions,
        **kwargs,
    ):
        """
        Initialize the SEA backend client.

        Args:
            server_hostname: Hostname of the Databricks server
            port: Port number for the connection
            http_path: HTTP path for the connection
            http_headers: List of HTTP headers to include in requests
            auth_provider: Authentication provider
            ssl_options: SSL configuration options
            **kwargs: Additional keyword arguments
        """

        logger.debug(
            "SeaDatabricksClient.__init__(server_hostname=%s, port=%s, http_path=%s)",
            server_hostname,
            port,
            http_path,
        )

        super().__init__(ssl_options=ssl_options, **kwargs)

        # Extract warehouse ID from http_path
        self.warehouse_id = self._extract_warehouse_id(http_path)

        self._ssl_options = ssl_options

        # Extract retry policy parameters
        self._initialize_retry_args(kwargs)
        self._auth_provider = auth_provider

        self.enable_v3_retries = kwargs.get("_enable_v3_retries", True)
        if not self.enable_v3_retries:
            logger.warning(
                "Legacy retry behavior is enabled for this connection."
                " This behaviour is deprecated and will be removed in a future release."
            )

        self.force_dangerous_codes = kwargs.get("_retry_dangerous_codes", [])

        additional_transport_args = {}
        _max_redirects: Union[None, int] = kwargs.get("_retry_max_redirects")
        if _max_redirects:
            if _max_redirects > self._retry_stop_after_attempts_count:
                logger.warn(
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

        # Initialize ThriftHttpClient
        self._transport = databricks.sql.auth.thrift_http_client.THttpClient(
            auth_provider=self._auth_provider,
            uri_or_host=f"https://{server_hostname}:{port}",
            ssl_options=self._ssl_options,
            **additional_transport_args,  # type: ignore
        )

        timeout = kwargs.get("_socket_timeout", DEFAULT_SOCKET_TIMEOUT)
        # setTimeout defaults to 15 minutes and is expected in ms
        self._transport.setTimeout(timeout and (float(timeout) * 1000.0))

        self._transport.setCustomHeaders(dict(http_headers))

        # protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(self._transport)
        # self._client = TCLIServiceClient(protocol)

        try:
            self._transport.open()
        except:
            self._transport.close()
            raise

        self._request_lock = threading.RLock()

        # Initialize HTTP client adapter
        self.http_client = SeaHttpClientAdapter(thrift_client=self._transport)

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
    def _check_response_for_error(response):
        pass  # TODO: implement

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
    def make_request(self, method_name, path, data, params, headers, retryable=True):
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
                logger.debug("Sending request: {}(<REDACTED>)".format(method_name))
                unsafe_logger.debug("Sending request: {}".format(path))

                # These three lines are no-ops if the v3 retry policy is not in use
                if self.enable_v3_retries:
                    command_type = self.http_client._determine_command_type(
                        path, method_name, data
                    )
                    self.http_client.thrift_client.set_retry_command_type(command_type)
                    self.http_client.thrift_client.startRetryTimer()

                if method_name == "GET":
                    response = self.http_client.get(path, params, headers)
                elif method_name == "POST":
                    response = self.http_client.post(path, data, params, headers)
                elif method_name == "DELETE":
                    response = self.http_client.delete(path, data, params, headers)
                else:
                    raise ValueError(f"Unsupported method: {method_name}")

                return response

            except urllib3.exceptions.HTTPError as err:
                # retry on timeout. Happens a lot in Azure and it is safe as data has not been sent to server yet

                # TODO: don't use exception handling for GOS polling...

                logger.error("ThriftBackend.attempt_request: HTTPError: %s", err)

                if command_type == CommandType.GET_OPERATION_STATUS:
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

                # retry on timeout. Happens a lot in Azure and it is safe as data has not been sent to server yet
                if command_type == CommandType.GET_OPERATION_STATUS or err.errno == errno.ETIMEDOUT:
                    retry_delay = bound_retry_delay(attempt, self._retry_delay_default)

                    # fmt: on
                    log_string = f"{command_type} failed with code {err.errno} and will attempt to retry"
                    if err.errno in info_errs:
                        logger.info(log_string)
                    else:
                        logger.warning(log_string)
            except Exception as err:
                logger.error("ThriftBackend.attempt_request: Exception: %s", err)
                error = err
                retry_delay = extract_retry_delay(attempt)
                error_message = SeaDatabricksClient._extract_error_message_from_headers(
                    getattr(self._transport, "headers", {})
                )
            finally:
                # Calling `close()` here releases the active HTTP connection back to the pool
                self._transport.close()

            return RequestErrorInfo(
                error=error,
                error_message=error_message,
                retry_delay=retry_delay,
                http_code=getattr(self._transport, "code", None),
                method=method_name,
                request=data,
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
                SeaDatabricksClient._check_response_for_error(response)
                return response

            error_info = response_or_error_info
            # The error handler will either sleep or throw an exception
            self._handle_request_error(error_info, attempt, elapsed)

    def _extract_warehouse_id(self, http_path: str) -> str:
        """
        Extract the warehouse ID from the HTTP path.

        Args:
            http_path: The HTTP path from which to extract the warehouse ID

        Returns:
            The extracted warehouse ID

        Raises:
            ValueError: If the warehouse ID cannot be extracted from the path
        """

        warehouse_pattern = re.compile(r".*/warehouses/(.+)")
        endpoint_pattern = re.compile(r".*/endpoints/(.+)")

        for pattern in [warehouse_pattern, endpoint_pattern]:
            match = pattern.match(http_path)
            if not match:
                continue
            warehouse_id = match.group(1)
            logger.debug(
                f"Extracted warehouse ID: {warehouse_id} from path: {http_path}"
            )
            return warehouse_id

        # If no match found, raise error
        error_message = (
            f"Could not extract warehouse ID from http_path: {http_path}. "
            f"Expected format: /path/to/warehouses/{{warehouse_id}} or "
            f"/path/to/endpoints/{{warehouse_id}}."
            f"Note: SEA only works for warehouses."
        )
        logger.error(error_message)
        raise ValueError(error_message)

    @property
    def max_download_threads(self) -> int:
        """Get the maximum number of download threads for cloud fetch operations."""
        return self._max_download_threads

    def open_session(
        self,
        session_configuration: Optional[Dict[str, str]],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> SessionId:
        """
        Opens a new session with the Databricks SQL service using SEA.

        Args:
            session_configuration: Optional dictionary of configuration parameters for the session.
                                   Only specific parameters are supported as documented at:
                                   https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters
            catalog: Optional catalog name to use as the initial catalog for the session
            schema: Optional schema name to use as the initial schema for the session

        Returns:
            SessionId: A session identifier object that can be used for subsequent operations

        Raises:
            Error: If the session configuration is invalid
            OperationalError: If there's an error establishing the session
        """

        logger.debug(
            "SeaDatabricksClient.open_session(session_configuration=%s, catalog=%s, schema=%s)",
            session_configuration,
            catalog,
            schema,
        )

        session_configuration = _filter_session_configuration(session_configuration)

        request_data = CreateSessionRequest(
            warehouse_id=self.warehouse_id,
            session_confs=session_configuration,
            catalog=catalog,
            schema=schema,
        )

        try:
            response = self.make_request(
                method_name="POST",
                path=self.SESSION_PATH,
                data=request_data.to_dict(),
                params=None,
                headers=None,
            )

            session_response = CreateSessionResponse.from_dict(response)
            session_id = session_response.session_id
            if not session_id:
                raise ServerOperationError(
                    "Failed to create session: No session ID returned",
                    {
                        "operation-id": None,
                        "diagnostic-info": None,
                    },
                )

            return SessionId.from_sea_session_id(session_id)
        except Exception as e:
            logger.error("SeaDatabricksClient.open_session: Exception: %s", e)
            raise

    def close_session(self, session_id: SessionId) -> None:
        """
        Closes an existing session with the Databricks SQL service.

        Args:
            session_id: The session identifier returned by open_session()

        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error closing the session
        """

        logger.debug("SeaDatabricksClient.close_session(session_id=%s)", session_id)

        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")
        sea_session_id = session_id.to_sea_session_id()

        request_data = DeleteSessionRequest(
            warehouse_id=self.warehouse_id,
            session_id=sea_session_id,
        )

        try:
            self.make_request(
                method_name="DELETE",
                path=self.SESSION_PATH_WITH_ID.format(sea_session_id),
                data=request_data.to_dict(),
                params=None,
                headers=None,
            )
        except Exception as e:
            logger.error("SeaDatabricksClient.close_session: Exception: %s", e)
            raise

    @staticmethod
    def get_default_session_configuration_value(name: str) -> Optional[str]:
        """
        Get the default value for a session configuration parameter.

        Args:
            name: The name of the session configuration parameter

        Returns:
            The default value if the parameter is supported, None otherwise
        """
        return ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.get(name.upper())

    @staticmethod
    def get_allowed_session_configurations() -> List[str]:
        """
        Get the list of allowed session configuration parameters.

        Returns:
            List of allowed session configuration parameter names
        """
        return list(ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keys())

    def _extract_description_from_manifest(
        self, manifest: ResultManifest
    ) -> Optional[List]:
        """
        Extract column description from a manifest object, in the format defined by
        the spec: https://peps.python.org/pep-0249/#description

        Args:
            manifest: The ResultManifest object containing schema information

        Returns:
            Optional[List]: A list of column tuples or None if no columns are found
        """

        schema_data = manifest.schema
        columns_data = schema_data.get("columns", [])

        if not columns_data:
            return None

        columns = []
        for col_data in columns_data:
            # Format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            columns.append(
                (
                    col_data.get("name", ""),  # name
                    col_data.get("type_name", ""),  # type_code
                    None,  # display_size (not provided by SEA)
                    None,  # internal_size (not provided by SEA)
                    col_data.get("precision"),  # precision
                    col_data.get("scale"),  # scale
                    col_data.get("nullable", True),  # null_ok
                )
            )

        return columns if columns else None

    def get_chunk_link(self, statement_id: str, chunk_index: int) -> ExternalLink:
        """
        Get links for chunks starting from the specified index.

        Args:
            statement_id: The statement ID
            chunk_index: The starting chunk index

        Returns:
            ExternalLink: External link for the chunk
        """

        response_data = self.http_client.get(
            path=self.CHUNK_PATH_WITH_ID_AND_INDEX.format(statement_id, chunk_index),
        )
        response = GetChunksResponse.from_dict(response_data)

        links = response.external_links
        link = next((l for l in links if l.chunk_index == chunk_index), None)
        if not link:
            raise ServerOperationError(
                f"No link found for chunk index {chunk_index}",
                {
                    "operation-id": statement_id,
                    "diagnostic-info": None,
                },
            )

        return link

    def _results_message_to_execute_response(self, sea_response, command_id):
        """
        Convert a SEA response to an ExecuteResponse and extract result data.

        Args:
            sea_response: The response from the SEA API
            command_id: The command ID

        Returns:
            ExecuteResponse: The normalized execute response
        """

        # Extract description from manifest schema
        description = self._extract_description_from_manifest(response.manifest)

        # Check for compression
        lz4_compressed = (
            response.manifest.result_compression == ResultCompression.LZ4_FRAME.value
        )

        execute_response = ExecuteResponse(
            command_id=CommandId.from_sea_statement_id(response.statement_id),
            status=response.status.state,
            description=description,
            has_been_closed_server_side=False,
            lz4_compressed=lz4_compressed,
            is_staging_operation=response.manifest.is_volume_operation,
            arrow_schema_bytes=None,
            result_format=response.manifest.format,
        )

        return execute_response

    def _check_command_not_in_failed_or_closed_state(
        self, state: CommandState, command_id: CommandId
    ) -> None:
        if state == CommandState.CLOSED:
            raise DatabaseError(
                "Command {} unexpectedly closed server side".format(command_id),
                {
                    "operation-id": command_id,
                },
            )
        if state == CommandState.FAILED:
            raise ServerOperationError(
                "Command {} failed".format(command_id),
                {
                    "operation-id": command_id,
                },
            )

    def _wait_until_command_done(
        self, response: ExecuteStatementResponse
    ) -> CommandState:
        """
        Wait until a command is done.
        """

        state = response.status.state
        command_id = CommandId.from_sea_statement_id(response.statement_id)

        while state in [CommandState.PENDING, CommandState.RUNNING]:
            time.sleep(self.POLL_INTERVAL_SECONDS)
            state = self.get_query_state(command_id)

        self._check_command_not_in_failed_or_closed_state(state, command_id)

        return state

    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: "Cursor",
        use_cloud_fetch: bool,
        parameters: List[Dict[str, Any]],
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
    ) -> Union["ResultSet", None]:
        """
        Execute a SQL command using the SEA backend.

        Args:
            operation: SQL command to execute
            session_id: Session identifier
            max_rows: Maximum number of rows to fetch
            max_bytes: Maximum number of bytes to fetch
            lz4_compression: Whether to use LZ4 compression
            cursor: Cursor executing the command
            use_cloud_fetch: Whether to use cloud fetch
            parameters: SQL parameters
            async_op: Whether to execute asynchronously
            enforce_embedded_schema_correctness: Whether to enforce schema correctness

        Returns:
            ResultSet: A SeaResultSet instance for the executed command
        """

        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")

        sea_session_id = session_id.to_sea_session_id()

        # Convert parameters to StatementParameter objects
        sea_parameters = []
        if parameters:
            for param in parameters:
                sea_parameters.append(
                    StatementParameter(
                        name=param["name"],
                        value=param["value"],
                        type=param["type"] if "type" in param else None,
                    )
                )

        format = (
            ResultFormat.ARROW_STREAM if use_cloud_fetch else ResultFormat.JSON_ARRAY
        ).value
        disposition = (
            ResultDisposition.EXTERNAL_LINKS
            if use_cloud_fetch
            else ResultDisposition.INLINE
        ).value
        result_compression = (
            ResultCompression.LZ4_FRAME if lz4_compression else ResultCompression.NONE
        ).value

        request = ExecuteStatementRequest(
            warehouse_id=self.warehouse_id,
            session_id=sea_session_id,
            statement=operation,
            disposition=disposition,
            format=format,
            wait_timeout=(WaitTimeout.ASYNC if async_op else WaitTimeout.SYNC).value,
            on_wait_timeout="CONTINUE",
            row_limit=max_rows,
            parameters=sea_parameters if sea_parameters else None,
            result_compression=result_compression,
        )

        try:
            response_data = self.make_request(
                method_name="POST",
                path=self.STATEMENT_PATH,
                data=request.to_dict(),
                params=None,
                headers=None,
            )
            response = ExecuteStatementResponse.from_dict(response_data)
            statement_id = response.statement_id
            if not statement_id:
                raise ServerOperationError(
                    "Failed to execute command: No statement ID returned",
                    {
                        "operation-id": None,
                        "diagnostic-info": None,
                    },
                )

            command_id = CommandId.from_sea_statement_id(statement_id)

            # Store the command ID in the cursor
            cursor.active_command_id = command_id

            # If async operation, return and let the client poll for results
            if async_op:
                return None

            # For synchronous operation, wait for the statement to complete
            status = response.status
            state = status.state

            # Keep polling until we reach a terminal state
            while state in [CommandState.PENDING, CommandState.RUNNING]:
                time.sleep(0.5)  # add a small delay to avoid excessive API calls
                state = self.get_query_state(command_id)

            if state in [CommandState.FAILED, CommandState.CLOSED]:
                raise DatabaseError(
                    f"Statement execution did not succeed: {status.error.message if status.error else 'Unknown error'}",
                    {
                        "operation-id": command_id.to_sea_statement_id(),
                        "diagnostic-info": None,
                    },
                )

            return self.get_execution_result(command_id, cursor)
        except Exception as e:
            logger.error("SeaDatabricksClient.execute_command: Exception: %s", e)
            raise

    def cancel_command(self, command_id: CommandId) -> None:
        """
        Cancel a running command.

        Args:
            command_id: Command identifier to cancel

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = CancelStatementRequest(statement_id=sea_statement_id)
        try:
            self.make_request(
                method_name="POST",
                path=self.CANCEL_STATEMENT_PATH_WITH_ID.format(sea_statement_id),
                data=request.to_dict(),
                params=None,
                headers=None,
            )
        except Exception as e:
            logger.error("SeaDatabricksClient.cancel_command: Exception: %s", e)
            raise

    def close_command(self, command_id: CommandId) -> None:
        """
        Close a command and release resources.

        Args:
            command_id: Command identifier to close

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = CloseStatementRequest(statement_id=sea_statement_id)
        try:
            self.make_request(
                method_name="DELETE",
                path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
                data=request.to_dict(),
                params=None,
                headers=None,
            )
        except Exception as e:
            logger.error("SeaDatabricksClient.close_command: Exception: %s", e)
            raise

    def get_query_state(self, command_id: CommandId) -> CommandState:
        """
        Get the state of a running query.

        Args:
            command_id: Command identifier

        Returns:
            CommandState: The current state of the command

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = GetStatementRequest(statement_id=sea_statement_id)
        try:
            response_data = self.make_request(
                method_name="GET",
                path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
                data=None,
                params=None,
                headers=None,
            )

            # Parse the response
            response = GetStatementResponse.from_dict(response_data)
            return response.status.state
        except Exception as e:
            logger.error("SeaDatabricksClient.get_query_state: Exception: %s", e)
            raise

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ) -> "ResultSet":
        """
        Get the result of a command execution.

        Args:
            command_id: Command identifier
            cursor: Cursor executing the command

        Returns:
            ResultSet: A SeaResultSet instance with the execution results

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        # Create the request model
        request = GetStatementRequest(statement_id=sea_statement_id)

        try:
            # Get the statement result
            response_data = self.make_request(
                method_name="GET",
                path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
                data=None,
                params=None,
                headers=None,
            )

            # Create and return a SeaResultSet
            from databricks.sql.result_set import SeaResultSet

            # Convert the response to an ExecuteResponse and extract result data
            (
                execute_response,
                result_data,
                manifest,
            ) = self._results_message_to_execute_response(response_data, command_id)

            return SeaResultSet(
                connection=cursor.connection,
                execute_response=execute_response,
                sea_client=self,
                buffer_size_bytes=cursor.buffer_size_bytes,
                arraysize=cursor.arraysize,
                result_data=result_data,
                manifest=manifest,
            )
        except Exception as e:
            logger.error("SeaDatabricksClient.get_execution_result: Exception: %s", e)
            raise

    def get_chunk_link(self, statement_id: str, chunk_index: int) -> ExternalLink:
        """
        Get links for chunks starting from the specified index.
        Args:
            statement_id: The statement ID
            chunk_index: The starting chunk index
        Returns:
            ExternalLink: External link for the chunk
        """

        response_data = self.http_client._make_request(
            method="GET",
            path=self.CHUNK_PATH_WITH_ID_AND_INDEX.format(statement_id, chunk_index),
        )
        response = GetChunksResponse.from_dict(response_data)

        links = response.external_links
        link = next((l for l in links if l.chunk_index == chunk_index), None)
        if not link:
            raise ServerOperationError(
                f"No link found for chunk index {chunk_index}",
                {
                    "operation-id": statement_id,
                    "diagnostic-info": None,
                },
            )

        return link

    # == Metadata Operations ==

    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
    ) -> "ResultSet":
        """Get available catalogs by executing 'SHOW CATALOGS'."""
        result = self.execute_command(
            operation=MetadataCommands.SHOW_CATALOGS.value,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result

    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> "ResultSet":
        """Get schemas by executing 'SHOW SCHEMAS IN catalog [LIKE pattern]'."""
        if not catalog_name:
            raise ValueError("Catalog name is required for get_schemas")

        operation = MetadataCommands.SHOW_SCHEMAS.value.format(catalog_name)

        if schema_name:
            operation += MetadataCommands.LIKE_PATTERN.value.format(schema_name)

        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result

    def get_tables(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ) -> "ResultSet":
        """Get tables by executing 'SHOW TABLES IN catalog [SCHEMA LIKE pattern] [LIKE pattern]'."""
        if not catalog_name:
            raise ValueError("Catalog name is required for get_tables")

        operation = (
            MetadataCommands.SHOW_TABLES_ALL_CATALOGS.value
            if catalog_name in [None, "*", "%"]
            else MetadataCommands.SHOW_TABLES.value.format(
                MetadataCommands.CATALOG_SPECIFIC.value.format(catalog_name)
            )
        )

        if schema_name:
            operation += MetadataCommands.SCHEMA_LIKE_PATTERN.value.format(schema_name)

        if table_name:
            operation += MetadataCommands.LIKE_PATTERN.value.format(table_name)

        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"

        # Apply client-side filtering by table_types
        from databricks.sql.backend.filters import ResultSetFilter

        result = ResultSetFilter.filter_tables_by_type(result, table_types)

        return result

    def get_columns(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> "ResultSet":
        """Get columns by executing 'SHOW COLUMNS IN CATALOG catalog [SCHEMA LIKE pattern] [TABLE LIKE pattern] [LIKE pattern]'."""
        if not catalog_name:
            raise ValueError("Catalog name is required for get_columns")

        operation = MetadataCommands.SHOW_COLUMNS.value.format(catalog_name)

        if schema_name:
            operation += MetadataCommands.SCHEMA_LIKE_PATTERN.value.format(schema_name)

        if table_name:
            operation += MetadataCommands.TABLE_LIKE_PATTERN.value.format(table_name)

        if column_name:
            operation += MetadataCommands.LIKE_PATTERN.value.format(column_name)

        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result
