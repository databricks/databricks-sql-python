import json
import logging

logger = logging.getLogger(__name__)

### PEP-249 Mandated ###
# https://peps.python.org/pep-0249/#exceptions
class Error(Exception):
    """Base class for DB-API2.0 exceptions.
    `message`: An optional user-friendly error message. It should be short, actionable and stable
    `context`: Optional extra context about the error. MUST be JSON serializable
    """

    def __init__(
        self,
        message=None,
        context=None,
        host_url=None,
        *args,
        session_id_hex=None,
        **kwargs,
    ):
        super().__init__(message, *args, **kwargs)
        self.message = message
        self.context = context or {}

        error_name = self.__class__.__name__
        if host_url:
            from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory

            telemetry_client = TelemetryClientFactory.get_telemetry_client(
                host_url=host_url
            )
            telemetry_client.export_failure_log(
                error_name, self.message, session_id=session_id_hex
            )

    def __str__(self):
        return self.message

    def message_with_context(self):
        return self.message + ": " + json.dumps(self.context, default=str)


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class TransactionError(DatabaseError):
    """
    Exception raised for transaction-specific errors.

    This exception is used when transaction control operations fail, such as:
    - Setting autocommit mode (AUTOCOMMIT_SET_DURING_ACTIVE_TRANSACTION)
    - Committing a transaction (MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION)
    - Rolling back a transaction
    - Setting transaction isolation level

    The exception includes context about which transaction operation failed
    and preserves the underlying cause via exception chaining.
    """

    pass


### Custom error classes ###
class InvalidServerResponseError(OperationalError):
    """Thrown if the server does not set the initial namespace correctly"""

    pass


class ServerOperationError(DatabaseError):
    """Thrown if the operation moved to an error state, if for example there was a syntax
    error.
    Its context will have the following keys:
    "diagnostic-info": The full Spark stack trace (if available)
    "operation-id": The Thrift ID of the operation
    """

    pass


class RequestError(OperationalError):
    """Thrown if there was a error during request to the server.
    Its context will have the following keys:
    "method": The RPC method name that failed
    "session-id": The Thrift session guid
    "query-id": The Thrift query guid (if available)
    "http-code": HTTP response code to RPC request (if available)
    "error-message": Error message from the HTTP headers (if available)
    "original-exception": The Python level original exception
    "no-retry-reason": Why the request wasn't retried (if available)
    "bounded-retry-delay": The maximum amount of time an error will be retried before giving up
    "attempt": current retry number / maximum number of retries
    "elapsed-seconds": time that has elapsed since first attempting the RPC request
    """

    pass


class MaxRetryDurationError(RequestError):
    """Thrown if the next HTTP request retry would exceed the configured
    stop_after_attempts_duration
    """


class NonRecoverableNetworkError(RequestError):
    """Thrown if an HTTP code 501 is received"""


class UnsafeToRetryError(RequestError):
    """Thrown if ExecuteStatement request receives a code other than 200, 429, or 503"""


class SessionAlreadyClosedError(RequestError):
    """Thrown if CloseSession receives a code 404. ThriftBackend should gracefully proceed as this is expected."""


class CursorAlreadyClosedError(RequestError):
    """Thrown if CancelOperation receives a code 404. ThriftBackend should gracefully proceed as this is expected."""


class TelemetryRateLimitError(Exception):
    """Raised when telemetry endpoint returns 429 or 503, indicating rate limiting or service unavailable.
    This exception is used exclusively by the circuit breaker to track telemetry rate limiting events."""


class TelemetryNonRateLimitError(Exception):
    """Wrapper for telemetry errors that should NOT trigger circuit breaker.

    This exception wraps non-rate-limiting errors (network errors, timeouts, server errors, etc.)
    and is excluded from circuit breaker failure counting. Only TelemetryRateLimitError should
    open the circuit breaker.

    Attributes:
        original_exception: The actual exception that occurred
    """

    def __init__(self, original_exception: Exception):
        self.original_exception = original_exception
        super().__init__(f"Non-rate-limit telemetry error: {original_exception}")
