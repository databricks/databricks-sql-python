import time
import functools
from typing import Optional
import logging
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import (
    SqlExecutionEvent,
)
from databricks.sql.telemetry.models.enums import ExecutionResultFormat, StatementType

logger = logging.getLogger(__name__)


class TelemetryExtractor:
    """
    Base class for extracting telemetry information from various object types.

    This class serves as a proxy that delegates attribute access to the wrapped object
    while providing a common interface for extracting telemetry-related data.
    """

    def __init__(self, obj):
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._obj, name)

    def get_session_id_hex(self):
        pass

    def get_statement_id(self):
        pass

    def get_is_compressed(self):
        pass

    def get_execution_result_format(self):
        pass

    def get_retry_count(self):
        pass

    def get_chunk_id(self):
        pass


class CursorExtractor(TelemetryExtractor):
    """
    Telemetry extractor specialized for Cursor objects.

    Extracts telemetry information from database cursor objects, including
    statement IDs, session information, compression settings, and result formats.
    """

    def get_statement_id(self) -> Optional[str]:
        return self.query_id

    def get_session_id_hex(self) -> Optional[str]:
        return self.connection.get_session_id_hex()

    def get_is_compressed(self) -> bool:
        return self.connection.lz4_compression

    def get_execution_result_format(self) -> ExecutionResultFormat:
        if self.active_result_set is None:
            return ExecutionResultFormat.FORMAT_UNSPECIFIED

        from databricks.sql.utils import ColumnQueue, CloudFetchQueue, ArrowQueue

        if isinstance(self.active_result_set.results, ColumnQueue):
            return ExecutionResultFormat.COLUMNAR_INLINE
        elif isinstance(self.active_result_set.results, CloudFetchQueue):
            return ExecutionResultFormat.EXTERNAL_LINKS
        elif isinstance(self.active_result_set.results, ArrowQueue):
            return ExecutionResultFormat.INLINE_ARROW
        return ExecutionResultFormat.FORMAT_UNSPECIFIED

    def get_retry_count(self) -> int:
        if hasattr(self.backend, "retry_policy") and self.backend.retry_policy:
            return len(self.backend.retry_policy.history)
        return 0

    def get_chunk_id(self):
        return None


class ResultSetDownloadHandlerExtractor(TelemetryExtractor):
    """
    Telemetry extractor specialized for ResultSetDownloadHandler objects.
    """

    def get_session_id_hex(self) -> Optional[str]:
        return self._obj.session_id_hex

    def get_statement_id(self) -> Optional[str]:
        return self._obj.statement_id

    def get_is_compressed(self) -> bool:
        return self._obj.settings.is_lz4_compressed

    def get_execution_result_format(self) -> ExecutionResultFormat:
        return ExecutionResultFormat.EXTERNAL_LINKS

    def get_retry_count(self) -> Optional[int]:
        # standard requests and urllib3 libraries don't expose retry count
        return None

    def get_chunk_id(self) -> Optional[int]:
        return self._obj.chunk_id


def get_extractor(obj):
    """
    Factory function to create the appropriate telemetry extractor for an object.

    Determines the object type and returns the corresponding specialized extractor
    that can extract telemetry information from that object type.

    Args:
        obj: The object to create an extractor for. Can be a Cursor,
             ResultSetDownloadHandler, or any other object.

    Returns:
        TelemetryExtractor: A specialized extractor instance:
            - CursorExtractor for Cursor objects
            - ResultSetDownloadHandlerExtractor for ResultSetDownloadHandler objects
            - None for all other objects
    """
    if obj.__class__.__name__ == "Cursor":
        return CursorExtractor(obj)
    elif obj.__class__.__name__ == "ResultSetDownloadHandler":
        return ResultSetDownloadHandlerExtractor(obj)
    else:
        logger.debug("No extractor found for %s", obj.__class__.__name__)
        return None


def log_latency(statement_type: StatementType = StatementType.NONE):
    """
    Decorator for logging execution latency and telemetry information.

    This decorator measures the execution time of a method and sends telemetry
    data about the operation, including latency, statement information, and
    execution context.

    The decorator automatically:
    - Measures execution time using high-precision performance counters
    - Extracts telemetry information from the method's object (self)
    - Creates a SqlExecutionEvent with execution details
    - Sends the telemetry data asynchronously via TelemetryClient

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
        The wrapped method's object (self) must be compatible with the
        telemetry extractor system (e.g., Cursor or ResultSet objects).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.perf_counter()
            result = None
            try:
                result = func(self, *args, **kwargs)
                return result
            finally:

                def _safe_call(func_to_call):
                    """Calls a function and returns a default value on any exception."""
                    try:
                        return func_to_call()
                    except Exception:
                        return None

                end_time = time.perf_counter()
                duration_ms = int((end_time - start_time) * 1000)

                extractor = get_extractor(self)

                if extractor is not None:
                    session_id_hex = _safe_call(extractor.get_session_id_hex)
                    statement_id = _safe_call(extractor.get_statement_id)

                    sql_exec_event = SqlExecutionEvent(
                        statement_type=statement_type,
                        is_compressed=_safe_call(extractor.get_is_compressed),
                        execution_result=_safe_call(
                            extractor.get_execution_result_format
                        ),
                        retry_count=_safe_call(extractor.get_retry_count),
                        chunk_id=_safe_call(extractor.get_chunk_id),
                    )

                    telemetry_client = TelemetryClientFactory.get_telemetry_client(
                        session_id_hex
                    )
                    telemetry_client.export_latency_log(
                        latency_ms=duration_ms,
                        sql_execution_event=sql_exec_event,
                        sql_statement_id=statement_id,
                    )

        return wrapper

    return decorator
