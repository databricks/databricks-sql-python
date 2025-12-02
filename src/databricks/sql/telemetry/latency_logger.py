import time
import functools
from typing import Optional, Dict, Any
import logging
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import (
    SqlExecutionEvent,
)
from databricks.sql.telemetry.models.enums import ExecutionResultFormat, StatementType

logger = logging.getLogger(__name__)


def _extract_cursor_data(cursor) -> Dict[str, Any]:
    """
    Extract telemetry data directly from a Cursor object.

    OPTIMIZATION: Uses direct attribute access instead of wrapper objects.
    This eliminates object creation overhead and method call indirection.

    Args:
        cursor: The Cursor object to extract data from

    Returns:
        Dict with telemetry data (values may be None if extraction fails)
    """
    data = {}

    # Extract statement_id (query_id) - direct attribute access
    try:
        data["statement_id"] = cursor.query_id
    except (AttributeError, Exception):
        data["statement_id"] = None

    # Extract session_id_hex - direct method call
    try:
        data["session_id_hex"] = cursor.connection.get_session_id_hex()
    except (AttributeError, Exception):
        data["session_id_hex"] = None

    # Extract is_compressed - direct attribute access
    try:
        data["is_compressed"] = cursor.connection.lz4_compression
    except (AttributeError, Exception):
        data["is_compressed"] = False

    # Extract execution_result_format - inline logic
    try:
        if cursor.active_result_set is None:
            data["execution_result"] = ExecutionResultFormat.FORMAT_UNSPECIFIED
        else:
            from databricks.sql.utils import ColumnQueue, CloudFetchQueue, ArrowQueue

            results = cursor.active_result_set.results
            if isinstance(results, ColumnQueue):
                data["execution_result"] = ExecutionResultFormat.COLUMNAR_INLINE
            elif isinstance(results, CloudFetchQueue):
                data["execution_result"] = ExecutionResultFormat.EXTERNAL_LINKS
            elif isinstance(results, ArrowQueue):
                data["execution_result"] = ExecutionResultFormat.INLINE_ARROW
            else:
                data["execution_result"] = ExecutionResultFormat.FORMAT_UNSPECIFIED
    except (AttributeError, Exception):
        data["execution_result"] = ExecutionResultFormat.FORMAT_UNSPECIFIED

    # Extract retry_count - direct attribute access
    try:
        if hasattr(cursor.backend, "retry_policy") and cursor.backend.retry_policy:
            data["retry_count"] = len(cursor.backend.retry_policy.history)
        else:
            data["retry_count"] = 0
    except (AttributeError, Exception):
        data["retry_count"] = 0

    # chunk_id is always None for Cursor
    data["chunk_id"] = None

    return data


def _extract_result_set_handler_data(handler) -> Dict[str, Any]:
    """
    Extract telemetry data directly from a ResultSetDownloadHandler object.

    OPTIMIZATION: Uses direct attribute access instead of wrapper objects.

    Args:
        handler: The ResultSetDownloadHandler object to extract data from

    Returns:
        Dict with telemetry data (values may be None if extraction fails)
    """
    data = {}

    # Extract session_id_hex - direct attribute access
    try:
        data["session_id_hex"] = handler.session_id_hex
    except (AttributeError, Exception):
        data["session_id_hex"] = None

    # Extract statement_id - direct attribute access
    try:
        data["statement_id"] = handler.statement_id
    except (AttributeError, Exception):
        data["statement_id"] = None

    # Extract is_compressed - direct attribute access
    try:
        data["is_compressed"] = handler.settings.is_lz4_compressed
    except (AttributeError, Exception):
        data["is_compressed"] = False

    # execution_result is always EXTERNAL_LINKS for result set handlers
    data["execution_result"] = ExecutionResultFormat.EXTERNAL_LINKS

    # retry_count is not available for result set handlers
    data["retry_count"] = None

    # Extract chunk_id - direct attribute access
    try:
        data["chunk_id"] = handler.chunk_id
    except (AttributeError, Exception):
        data["chunk_id"] = None

    return data


def _extract_telemetry_data(obj) -> Optional[Dict[str, Any]]:
    """
    Extract telemetry data from an object based on its type.

    OPTIMIZATION: Returns a simple dict instead of creating wrapper objects.
    This dict will be used to create the SqlExecutionEvent in the background thread.

    Args:
        obj: The object to extract data from (Cursor, ResultSetDownloadHandler, etc.)

    Returns:
        Dict with telemetry data, or None if object type is not supported
    """
    obj_type = obj.__class__.__name__

    if obj_type == "Cursor":
        return _extract_cursor_data(obj)
    elif obj_type == "ResultSetDownloadHandler":
        return _extract_result_set_handler_data(obj)
    else:
        logger.debug("No telemetry extraction available for %s", obj_type)
        return None


def log_latency(statement_type: StatementType = StatementType.NONE):
    """
    Decorator for logging execution latency and telemetry information.

    This decorator measures the execution time of a method and sends telemetry
    data about the operation, including latency, statement information, and
    execution context.

    Args:
        statement_type (StatementType): The type of SQL statement being executed.

    Usage:
        @log_latency(StatementType.QUERY)
        def execute(self, query):
            # Method implementation
            pass

    Returns:
        function: A decorator that wraps methods to add latency logging.

    Note:
        The wrapped method's object (self) must be a Cursor or
        ResultSetDownloadHandler for telemetry data extraction.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.monotonic()
            try:
                return func(self, *args, **kwargs)
            finally:
                duration_ms = int((time.monotonic() - start_time) * 1000)

                # Always log for debugging
                logger.debug("%s completed in %dms", func.__name__, duration_ms)

                # Fast check: use cached telemetry_enabled flag from connection
                # Avoids dictionary lookup + instance check on every operation
                connection = getattr(self, "connection", None)
                if connection and getattr(connection, "telemetry_enabled", False):
                    session_id_hex = connection.get_session_id_hex()
                    if session_id_hex:
                        # Telemetry enabled - extract and send
                        telemetry_data = _extract_telemetry_data(self)
                        if telemetry_data:
                            sql_exec_event = SqlExecutionEvent(
                                statement_type=statement_type,
                                is_compressed=telemetry_data.get("is_compressed"),
                                execution_result=telemetry_data.get("execution_result"),
                                retry_count=telemetry_data.get("retry_count"),
                                chunk_id=telemetry_data.get("chunk_id"),
                            )

                            telemetry_client = (
                                TelemetryClientFactory.get_telemetry_client(
                                    host_url=connection.session.host
                                )
                            )
                            telemetry_client.export_latency_log(
                                latency_ms=duration_ms,
                                sql_execution_event=sql_exec_event,
                                sql_statement_id=telemetry_data.get("statement_id"),
                                session_id=session_id_hex,
                            )

        return wrapper

    return decorator
