import logging
import uuid
import time
import re
import errno
import math
import threading
from typing import Dict, Tuple, List, Optional, Any, Union, TYPE_CHECKING, Set

from databricks.sql.backend.sea.models.base import ExternalLink
from databricks.sql.backend.sea.utils.constants import (
    ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP,
    ResultFormat,
    ResultDisposition,
    ResultCompression,
    WaitTimeout,
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
from databricks.sql.exc import (
    ServerOperationError,
    RequestError,
    OperationalError,
    SessionAlreadyClosedError,
    CursorAlreadyClosedError,
)
from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.backend.sea.utils.http_client_adapter import SeaHttpClientAdapter
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.utils import (
    RequestErrorInfo,
    NoRetryReason,
    _bound,
)

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
    GetChunksResponse,
)
from databricks.sql.backend.sea.models.responses import (
    parse_status,
    parse_manifest,
    parse_result,
)

import urllib3.exceptions

logger = logging.getLogger(__name__)

# Same retry policy structure as Thrift backend
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

    # Retry parameters (similar to Thrift backend)
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
            **kwargs: Additional keyword arguments including retry parameters
        """

        logger.debug(
            "SeaDatabricksClient.__init__(server_hostname=%s, port=%s, http_path=%s)",
            server_hostname,
            port,
            http_path,
        )

        self._max_download_threads = kwargs.get("max_download_threads", 10)

        # Extract warehouse ID from http_path
        self.warehouse_id = self._extract_warehouse_id(http_path)

        # Initialize retry parameters (similar to Thrift backend)
        self._initialize_retry_args(kwargs)

        # Extract retry policy parameters
        retry_policy = kwargs.get("_retry_policy", None)
        retry_stop_after_attempts_count = kwargs.get(
            "_retry_stop_after_attempts_count", self._retry_stop_after_attempts_count
        )
        retry_stop_after_attempts_duration = kwargs.get(
            "_retry_stop_after_attempts_duration", self._retry_stop_after_attempts_duration
        )
        retry_delay_min = kwargs.get("_retry_delay_min", self._retry_delay_min)
        retry_delay_max = kwargs.get("_retry_delay_max", self._retry_delay_max)
        retry_delay_default = kwargs.get("_retry_delay_default", self._retry_delay_default)
        retry_dangerous_codes = kwargs.get("_retry_dangerous_codes", [])

        # Connector version 3 retry approach (similar to Thrift backend)
        self.enable_v3_retries = kwargs.get("_enable_v3_retries", True)

        if not self.enable_v3_retries:
            logger.warning(
                "Legacy retry behavior is enabled for this connection."
                " This behaviour is deprecated and will be removed in a future release."
            )
        self.force_dangerous_codes = retry_dangerous_codes

        # Create retry policy if not provided
        if not retry_policy:
            from databricks.sql.auth.retry import DatabricksRetryPolicy

            retry_policy = DatabricksRetryPolicy(
                delay_min=retry_delay_min,
                delay_max=retry_delay_max,
                stop_after_attempts_count=retry_stop_after_attempts_count,
                stop_after_attempts_duration=retry_stop_after_attempts_duration,
                delay_default=retry_delay_default,
                force_dangerous_codes=retry_dangerous_codes,
            )

        # Store retry policy for SEA-level retry logic
        self.retry_policy = retry_policy

        # Initialize ThriftHttpClient with no retries (retry_policy=0)
        # All retries will be handled at the SEA level in make_request()
        thrift_client = THttpClient(
            auth_provider=auth_provider,
            uri_or_host=f"https://{server_hostname}:{port}",
            path=http_path,
            ssl_options=ssl_options,
            max_connections=kwargs.get("max_connections", 1),
            retry_policy=0,  # Disable urllib3-level retries
        )

        # Set custom headers
        custom_headers = dict(http_headers)
        thrift_client.setCustomHeaders(custom_headers)

        # Initialize HTTP client adapter
        self.http_client = SeaHttpClientAdapter(thrift_client=thrift_client)

        # Add request lock for thread safety (similar to Thrift backend)
        self._request_lock = threading.RLock()

    def _initialize_retry_args(self, kwargs):
        """Initialize retry arguments with bounds checking (copied from Thrift backend)."""
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

    def _handle_request_error(self, error_info, attempt, elapsed):
        """Handle request errors with retry logic (copied from Thrift backend)."""
        # _retry_stop_after_attempts_count is the number of retries, so total attempts = retries + 1
        max_attempts = self._retry_stop_after_attempts_count + 1
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
            no_retry_reason, attempt, self._retry_stop_after_attempts_count, elapsed, max_duration_s
        )

        if no_retry_reason is not None:
            user_friendly_error_message = error_info.user_friendly_error_message(
                no_retry_reason, attempt, elapsed
            )
            
            # Raise specific exception types to match test expectations
            from databricks.sql.exc import MaxRetryDurationError, RequestError
            from urllib3.exceptions import MaxRetryError
            
            if no_retry_reason == NoRetryReason.OUT_OF_TIME:
                # Raise RequestError with MaxRetryDurationError as args[1] for timeout cases
                raise RequestError(user_friendly_error_message, None, MaxRetryDurationError(user_friendly_error_message))
            elif no_retry_reason == NoRetryReason.OUT_OF_ATTEMPTS:
                # Raise MaxRetryError directly for attempt exhaustion (matches test expectations)
                raise MaxRetryError(None, None, user_friendly_error_message)
            else:
                # For other non-retryable cases, raise RequestError
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

    def make_request(self, method_name, path, data=None, params=None, headers=None, retryable=True):
        """
        Execute given request, attempting retries similar to Thrift backend.
        
        Args:
            method_name: HTTP method name (GET, POST, DELETE)
            path: API endpoint path
            data: Request payload data
            params: Query parameters
            headers: Additional headers
            retryable: Whether this request should be retried on failure
        
        Returns:
            Response data parsed from JSON
            
        Raises:
            RequestError: If the request fails after all retries
        """
        
        t0 = time.time()

        def get_elapsed():
            return time.time() - t0

        def bound_retry_delay(attempt, proposed_delay, allow_exceed_max=False):
            """bound delay (seconds) by [min_delay*1.5^(attempt-1), max_delay]
            If allow_exceed_max is True, proposed_delay can exceed max_delay (for Retry-After headers)
            """
            delay = int(proposed_delay)
            delay = max(delay, self._retry_delay_min * math.pow(1.5, attempt - 1))
            if not allow_exceed_max:
                delay = min(delay, self._retry_delay_max)
            return delay

        def extract_retry_delay(attempt, error):
            # Let the DatabricksRetryPolicy handle the retry decision
            # This replicates the logic from the Thrift backend
            http_code = getattr(self.http_client.thrift_client, "code", None)
            retry_after = getattr(self.http_client.thrift_client, "headers", {}).get("Retry-After", 1)
            
            logger.debug(f"SEA extract_retry_delay called: attempt={attempt}, http_code={http_code}, path={path}, method={method_name}, enable_v3_retries={self.enable_v3_retries}")
            
            # If we have a retry policy and it's enabled, delegate to it
            if self.enable_v3_retries and hasattr(self, 'retry_policy'):
                from databricks.sql.auth.thrift_http_client import CommandType
                
                # Set the command type based on the request path
                if "statements" in path:
                    if method_name == "POST" and "cancel" not in path:
                        command_type = CommandType.EXECUTE_STATEMENT
                    elif method_name == "GET":
                        command_type = CommandType.GET_OPERATION_STATUS
                    elif method_name == "DELETE" or "cancel" in path:
                        command_type = CommandType.CLOSE_OPERATION
                    else:
                        command_type = CommandType.OTHER
                elif "sessions" in path and method_name == "DELETE":
                    command_type = CommandType.CLOSE_SESSION
                else:
                    command_type = CommandType.OTHER
                
                # Set the command type on the retry policy
                retry_policy = self.retry_policy
                retry_policy.command_type = command_type
                
                # Check if this request should be retried
                # Note: we pass the correct method name, not hardcoded "POST"
                try:
                    should_retry, reason = retry_policy.should_retry(method_name, http_code)
                    logger.debug(f"SEA retry decision: should_retry={should_retry}, reason='{reason}', command_type={command_type}, http_code={http_code}, method={method_name}")
                except Exception as retry_exception:
                    # DatabricksRetryPolicy may raise specific exceptions directly
                    # (e.g., SessionAlreadyClosedError, CursorAlreadyClosedError)
                    logger.debug(f"SEA retry policy raised exception: {retry_exception}")
                    raise retry_exception
                
                # Special handling for 404 on CLOSE_SESSION and CLOSE_OPERATION after first attempt
                # This replicates the logic from the retry policy but handles it at SEA level
                if (http_code == 404 and command_type == CommandType.CLOSE_SESSION and attempt > 1):
                    from databricks.sql.exc import SessionAlreadyClosedError
                    logger.debug(f"SEA raising SessionAlreadyClosedError for 404 on CLOSE_SESSION after attempt {attempt}")
                    raise SessionAlreadyClosedError("CloseSession received 404 code from Databricks. Session is already closed.")
                elif (http_code == 404 and command_type == CommandType.CLOSE_OPERATION and attempt > 1):
                    from databricks.sql.exc import CursorAlreadyClosedError
                    logger.debug(f"SEA raising CursorAlreadyClosedError for 404 on CLOSE_OPERATION after attempt {attempt}")
                    raise CursorAlreadyClosedError("CloseOperation received 404 code from Databricks. Cursor is already closed.")
                
                if not should_retry:
                    # Check specific error conditions to match Thrift backend behavior
                    from databricks.sql.exc import UnsafeToRetryError, NonRecoverableNetworkError
                    
                    logger.debug(f"SEA not retrying: {reason}")
                    if ("ExecuteStatement command can only be retried for codes 429 and 503" in reason):
                        # This is a dangerous code that shouldn't be retried for ExecuteStatement
                        logger.debug(f"SEA raising UnsafeToRetryError for dangerous code {http_code}")
                        raise UnsafeToRetryError(reason)
                    elif ("Non-recoverable network error" in reason or http_code == 501):
                        # Non-recoverable errors like 501 Not Implemented
                        raise NonRecoverableNetworkError(reason)
                    return None  # Not retryable for other reasons
                
                # If we should retry, return the delay
                if http_code in [429, 503]:
                    # Allow Retry-After headers to exceed max delay
                    delay = bound_retry_delay(attempt, int(retry_after), allow_exceed_max=True)
                    logger.debug(f"SEA returning retry delay {delay} for code {http_code} (429/503)")
                    return delay
                elif http_code in self.force_dangerous_codes:
                    # Dangerous code that user forced to be retryable
                    delay = bound_retry_delay(attempt, self._retry_delay_default)
                    logger.debug(f"SEA returning retry delay {delay} for dangerous code {http_code} in force_dangerous_codes")
                    return delay
                else:
                    # Default retry delay for other retryable codes
                    delay = bound_retry_delay(attempt, self._retry_delay_default)
                    logger.debug(f"SEA returning retry delay {delay} for other retryable code {http_code}")
                    return delay
            
            # Fallback to original logic for legacy retry behavior
            # This logic matches the Thrift backend exactly when v3 retries are disabled
            # or when v3 retries aren't available
            
            # For ExecuteStatement commands, check if this is a dangerous code
            if "statements" in path and method_name == "POST" and "cancel" not in path:
                # This is an ExecuteStatement command
                logger.debug(f"SEA ExecuteStatement dangerous code check: http_code={http_code}, force_dangerous_codes={self.force_dangerous_codes}")
                if http_code in [502, 504, 400] and http_code not in self.force_dangerous_codes:
                    # This is a dangerous code that should not be retried by default
                    from databricks.sql.exc import UnsafeToRetryError
                    logger.debug(f"SEA raising UnsafeToRetryError for dangerous code {http_code} on ExecuteStatement")
                    raise UnsafeToRetryError(f"ExecuteStatement command can only be retried for codes 429 and 503, received {http_code}")
                elif http_code in [502, 504, 400] and http_code in self.force_dangerous_codes:
                    # User explicitly forced dangerous codes to be retryable
                    logger.debug(f"SEA allowing retry for dangerous code {http_code} because it's in force_dangerous_codes")
                    return bound_retry_delay(attempt, self._retry_delay_default)
            
            # For other cases, use simple retry logic (matches Thrift backend)
            if http_code in [429, 503]:
                # Allow Retry-After headers to exceed max delay
                return bound_retry_delay(attempt, int(retry_after), allow_exceed_max=True)
            elif http_code in self.force_dangerous_codes:
                # User explicitly forced dangerous codes to be retryable
                return bound_retry_delay(attempt, self._retry_delay_default)
                
            return None

        def attempt_request(attempt):
            """
            splits out lockable attempt, from delay & retry loop
            returns tuple: (method_return, delay_fn(), error, error_message)
            - non-None method_return -> success, return and be done
            - non-None retry_delay -> sleep delay before retry
            - error, error_message always set when available
            """

            error, error_message, retry_delay = None, None, None
            try:
                logger.debug("Sending request: {}(<REDACTED>)".format(method_name))

                # These three lines are no-ops if the v3 retry policy is not in use
                if self.enable_v3_retries:
                    from databricks.sql.auth.thrift_http_client import CommandType
                    # Determine command type based on path and method
                    if "statements" in path:
                        if method_name == "POST" and "cancel" in path:
                            command_type = CommandType.CLOSE_OPERATION
                        elif method_name == "POST" and "cancel" not in path:
                            command_type = CommandType.EXECUTE_STATEMENT
                        elif method_name == "GET":
                            command_type = CommandType.GET_OPERATION_STATUS
                        elif method_name == "DELETE":
                            command_type = CommandType.CLOSE_OPERATION
                        else:
                            command_type = CommandType.OTHER
                    elif "sessions" in path:
                        if method_name == "DELETE":
                            command_type = CommandType.CLOSE_SESSION
                        else:
                            command_type = CommandType.OTHER
                    else:
                        command_type = CommandType.OTHER
                        
                    self.http_client.thrift_client.set_retry_command_type(command_type)
                    self.http_client.thrift_client.startRetryTimer()

                # Make the actual request
                if method_name == "GET":
                    response = self.http_client.thrift_client.make_rest_request(
                        "GET", path, params=params, headers=headers
                    )
                elif method_name == "POST":
                    response = self.http_client.thrift_client.make_rest_request(
                        "POST", path, data=data, params=params, headers=headers
                    )
                elif method_name == "DELETE":
                    response = self.http_client.thrift_client.make_rest_request(
                        "DELETE", path, data=data, params=params, headers=headers
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method_name}")

                logger.debug("Received response: (<REDACTED>)")
                return response

            except urllib3.exceptions.HTTPError as err:
                # retry on timeout. Happens a lot in Azure and it is safe as data has not been sent to server yet
                logger.error("SeaDatabricksClient.attempt_request: HTTPError: %s", err)

                # Special handling for GET requests (similar to GetOperationStatus in Thrift)
                if method_name == "GET":
                    delay_default = (
                        self.enable_v3_retries
                        and getattr(self.http_client.thrift_client, 'retry_policy', None)
                        and self.http_client.thrift_client.retry_policy.delay_default
                        or self._retry_delay_default
                    )
                    retry_delay = bound_retry_delay(attempt, delay_default)
                    logger.info(
                        f"GET request failed with HTTP error and will be retried: {str(err)}"
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
                                        # | Debian | Darwin |
                info_errs = [           # |--------|--------|
                    errno.ESHUTDOWN,    # |   32   |   32   |
                    errno.EAFNOSUPPORT, # |   97   |   47   |
                    errno.ECONNRESET,   # |   104  |   54   |
                    errno.ETIMEDOUT,    # |   110  |   60   |
                ]

                # retry on timeout. Happens a lot in Azure and it is safe as data has not been sent to server yet
                if method_name == "GET" or err.errno == errno.ETIMEDOUT:
                    retry_delay = bound_retry_delay(attempt, self._retry_delay_default)

                    # fmt: on
                    log_string = f"GET request failed with code {err.errno} and will attempt to retry"
                    if err.errno in info_errs:
                        logger.info(log_string)
                    else:
                        logger.warning(log_string)
            except Exception as err:
                logger.error("SeaDatabricksClient.attempt_request: Exception: %s", err)
                error = err
                try:
                    retry_delay = extract_retry_delay(attempt, err)
                except Exception as retry_err:
                    # If extract_retry_delay raises an exception (like UnsafeToRetryError), 
                    # we should raise it immediately rather than continuing with retry logic
                    from databricks.sql.exc import UnsafeToRetryError, NonRecoverableNetworkError
                    if isinstance(retry_err, (UnsafeToRetryError, NonRecoverableNetworkError)):
                        # These exceptions should be raised as RequestError with the specific exception as args[1]
                        request_error = RequestError(str(retry_err), None, retry_err)
                        logger.info(f"SEA raising RequestError with {type(retry_err).__name__}: {retry_err}")
                        raise request_error
                    else:
                        # For other exceptions, re-raise them directly
                        raise retry_err
                error_message = getattr(err, 'message', str(err))
            finally:
                # Similar to Thrift backend, we close the connection
                if hasattr(self.http_client.thrift_client, 'close'):
                    self.http_client.thrift_client.close()

            return RequestErrorInfo(
                error=error,
                error_message=error_message,
                retry_delay=retry_delay,
                http_code=getattr(self.http_client.thrift_client, "code", None),
                method=method_name,
                request=data or params,  # Store request data for context
            )

        # The real work:
        # - for each available attempt:
        #       lock-and-attempt
        #       return on success
        #       if available: bounded delay and retry
        #       if not: raise error
        # _retry_stop_after_attempts_count is the number of retries, so total attempts = retries + 1
        max_attempts = (self._retry_stop_after_attempts_count + 1) if retryable else 1

        # use index-1 counting for logging/human consistency
        for attempt in range(1, max_attempts + 1):
            # We have a lock here because .cancel can be called from a separate thread.
            # We do not want threads to be simultaneously sharing the HTTP client
            # because we use its state to determine retries
            with self._request_lock:
                response_or_error_info = attempt_request(attempt)
            elapsed = get_elapsed()

            # conditions: success, non-retry-able, no-attempts-left, no-time-left, delay+retry
            if not isinstance(response_or_error_info, RequestErrorInfo):
                # log nothing here, presume that main request logging covers
                response = response_or_error_info
                return response

            error_info = response_or_error_info
            # The error handler will either sleep or throw an exception
            self._handle_request_error(error_info, attempt, elapsed)

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
                "POST", self.SESSION_PATH, data=request_data.to_dict()
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
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import RequestError, OperationalError, MaxRetryDurationError
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error opening session: {str(e)}")

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
                "DELETE",
                self.SESSION_PATH_WITH_ID.format(sea_session_id),
                data=request_data.to_dict(),
            )
        except Exception as e:
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import (
                RequestError,
                OperationalError,
                SessionAlreadyClosedError,
                MaxRetryDurationError,
            )
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, SessionAlreadyClosedError):
                # Wrap SessionAlreadyClosedError in RequestError with it as args[1]
                request_error = RequestError(str(e), None, e)
                raise request_error
            elif isinstance(e, RequestError) and "404" in str(e):
                # Handle 404 by raising SessionAlreadyClosedError wrapped in RequestError
                session_error = SessionAlreadyClosedError("Session is already closed")
                request_error = RequestError(str(session_error), None, session_error)
                raise request_error
            elif isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error closing session: {str(e)}")

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

    def _extract_description_from_manifest(self, manifest_obj) -> Optional[List]:
        """
        Extract column description from a manifest object.

        Args:
            manifest_obj: The ResultManifest object containing schema information

        Returns:
            Optional[List]: A list of column tuples or None if no columns are found
        """

        schema_data = manifest_obj.schema
        columns_data = schema_data.get("columns", [])

        if not columns_data:
            return None

        columns = []
        for col_data in columns_data:
            if not isinstance(col_data, dict):
                continue

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

        response_data = self.make_request(
            "GET", self.CHUNK_PATH_WITH_ID_AND_INDEX.format(statement_id, chunk_index)
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
            tuple: (ExecuteResponse, ResultData, ResultManifest) - The normalized execute response,
                  result data object, and manifest object
        """

        # Parse the response
        status = parse_status(sea_response)
        manifest_obj = parse_manifest(sea_response)
        result_data_obj = parse_result(sea_response)

        # Extract description from manifest schema
        description = self._extract_description_from_manifest(manifest_obj)

        # Check for compression
        lz4_compressed = manifest_obj.result_compression == "LZ4_FRAME"

        execute_response = ExecuteResponse(
            command_id=command_id,
            status=status.state,
            description=description,
            has_been_closed_server_side=False,
            lz4_compressed=lz4_compressed,
            is_staging_operation=False,
            arrow_schema_bytes=None,
            result_format=manifest_obj.format,
        )

        return execute_response, result_data_obj, manifest_obj

    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: "Cursor",
        use_cloud_fetch: bool,
        parameters: List,
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
                        name=param.name,
                        value=param.value,
                        type=param.type if hasattr(param, "type") else None,
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
                "POST", self.STATEMENT_PATH, data=request.to_dict()
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

            if state != CommandState.SUCCEEDED:
                raise ServerOperationError(
                    f"Statement execution did not succeed: {status.error.message if status.error else 'Unknown error'}",
                    {
                        "operation-id": command_id.to_sea_statement_id(),
                        "diagnostic-info": None,
                    },
                )

            return self.get_execution_result(command_id, cursor)
        except Exception as e:
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import RequestError, OperationalError, MaxRetryDurationError
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error executing command: {str(e)}")

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
                "POST",
                self.CANCEL_STATEMENT_PATH_WITH_ID.format(sea_statement_id),
                data=request.to_dict(),
            )
        except Exception as e:
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import RequestError, OperationalError, MaxRetryDurationError
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, RequestError) and "404" in str(e):
                # Operation was already closed, so we can ignore this
                logger.warning(
                    f"Attempted to cancel a command that was already closed: {sea_statement_id}"
                )
                return
            elif isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error canceling command: {str(e)}")

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
                "DELETE",
                self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
                data=request.to_dict(),
            )
        except Exception as e:
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import (
                RequestError,
                OperationalError,
                CursorAlreadyClosedError,
                MaxRetryDurationError,
            )
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, CursorAlreadyClosedError):
                # Wrap CursorAlreadyClosedError in RequestError with it as args[1]
                request_error = RequestError(str(e), None, e)
                raise request_error
            elif isinstance(e, RequestError) and "404" in str(e):
                # Handle 404 by raising CursorAlreadyClosedError wrapped in RequestError
                cursor_error = CursorAlreadyClosedError("Cursor is already closed")
                request_error = RequestError(str(cursor_error), None, cursor_error)
                raise request_error
            elif isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error closing command: {str(e)}")

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
                "GET", self.STATEMENT_PATH_WITH_ID.format(sea_statement_id)
            )

            # Parse the response
            response = GetStatementResponse.from_dict(response_data)
            return response.status.state
        except Exception as e:
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import RequestError, OperationalError, MaxRetryDurationError
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, RequestError) and "404" in str(e):
                # If the operation is not found, it was likely already closed
                logger.warning(
                    f"Operation not found when checking state: {sea_statement_id}"
                )
                return CommandState.CANCELLED
            elif isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error getting query state: {str(e)}")

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
                "GET", self.STATEMENT_PATH_WITH_ID.format(sea_statement_id)
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
            # Map exceptions to match Thrift behavior
            from databricks.sql.exc import RequestError, OperationalError, MaxRetryDurationError
            from urllib3.exceptions import MaxRetryError

            if isinstance(e, (RequestError, ServerOperationError, MaxRetryDurationError, MaxRetryError)):
                raise
            else:
                raise OperationalError(f"Error getting execution result: {str(e)}")

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
            operation="SHOW CATALOGS",
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

        operation = f"SHOW SCHEMAS IN `{catalog_name}`"

        if schema_name:
            operation += f" LIKE '{schema_name}'"

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

        operation = "SHOW TABLES IN " + (
            "ALL CATALOGS"
            if catalog_name in [None, "*", "%"]
            else f"CATALOG `{catalog_name}`"
        )

        if schema_name:
            operation += f" SCHEMA LIKE '{schema_name}'"

        if table_name:
            operation += f" LIKE '{table_name}'"

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

        # Apply client-side filtering by table_types if specified
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

        operation = f"SHOW COLUMNS IN CATALOG `{catalog_name}`"

        if schema_name:
            operation += f" SCHEMA LIKE '{schema_name}'"

        if table_name:
            operation += f" TABLE LIKE '{table_name}'"

        if column_name:
            operation += f" LIKE '{column_name}'"

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
