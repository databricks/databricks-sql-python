import time
import functools
from typing import Optional
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import (
    SqlExecutionEvent,
)
from databricks.sql.telemetry.models.enums import ExecutionResultFormat, StatementType
from databricks.sql.utils import ColumnQueue, CloudFetchQueue, ArrowQueue
from uuid import UUID


class TelemetryExtractor:
    """
    Base class for extracting telemetry information from various object types.

    This class serves as a proxy that delegates attribute access to the wrapped object
    while providing a common interface for extracting telemetry-related data.
    """

    def __init__(self, obj):
        """
        Initialize the extractor with an object to wrap.

        Args:
            obj: The object to extract telemetry information from.
        """
        self._obj = obj

    def __getattr__(self, name):
        """
        Delegate attribute access to the wrapped object.

        Args:
            name (str): The name of the attribute to access.

        Returns:
            The attribute value from the wrapped object.
        """
        return getattr(self._obj, name)

    def get_session_id_hex(self):
        pass

    def get_statement_id(self):
        pass

    def get_is_compressed(self):
        pass

    def get_execution_result(self):
        pass

    def get_retry_count(self):
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

    def get_execution_result(self) -> ExecutionResultFormat:
        if self.active_result_set is None:
            return ExecutionResultFormat.FORMAT_UNSPECIFIED

        if isinstance(self.active_result_set.results, ColumnQueue):
            return ExecutionResultFormat.COLUMNAR_INLINE
        elif isinstance(self.active_result_set.results, CloudFetchQueue):
            return ExecutionResultFormat.EXTERNAL_LINKS
        elif isinstance(self.active_result_set.results, ArrowQueue):
            return ExecutionResultFormat.INLINE_ARROW
        return ExecutionResultFormat.FORMAT_UNSPECIFIED

    def get_retry_count(self) -> int:
        if (
            hasattr(self.thrift_backend, "retry_policy")
            and self.thrift_backend.retry_policy
        ):
            return len(self.thrift_backend.retry_policy.history)
        return 0


class ResultSetExtractor(TelemetryExtractor):
    """
    Telemetry extractor specialized for ResultSet objects.

    Extracts telemetry information from database result set objects, including
    operation IDs, session information, compression settings, and result formats.
    """

    def get_statement_id(self) -> Optional[str]:
        if self.command_id:
            return str(UUID(bytes=self.command_id.operationId.guid))
        return None

    def get_session_id_hex(self) -> Optional[str]:
        return self.connection.get_session_id_hex()

    def get_is_compressed(self) -> bool:
        return self.lz4_compressed

    def get_execution_result(self) -> ExecutionResultFormat:
        if isinstance(self.results, ColumnQueue):
            return ExecutionResultFormat.COLUMNAR_INLINE
        elif isinstance(self.results, CloudFetchQueue):
            return ExecutionResultFormat.EXTERNAL_LINKS
        elif isinstance(self.results, ArrowQueue):
            return ExecutionResultFormat.INLINE_ARROW
        return ExecutionResultFormat.FORMAT_UNSPECIFIED

    def get_retry_count(self) -> int:
        if (
            hasattr(self.thrift_backend, "retry_policy")
            and self.thrift_backend.retry_policy
        ):
            return len(self.thrift_backend.retry_policy.history)
        return 0


def get_extractor(obj):
    """
    Factory function to create the appropriate telemetry extractor for an object.

    Determines the object type and returns the corresponding specialized extractor
    that can extract telemetry information from that object type.

    Args:
        obj: The object to create an extractor for. Can be a Cursor, ResultSet,
             or any other object.

    Returns:
        TelemetryExtractor: A specialized extractor instance:
            - CursorExtractor for Cursor objects
            - ResultSetExtractor for ResultSet objects
            - TelemetryExtractor (base) for all other objects
    """
    if obj.__class__.__name__ == "Cursor":
        return CursorExtractor(obj)
    elif obj.__class__.__name__ == "ResultSet":
        return ResultSetExtractor(obj)
    else:
        raise NotImplementedError(f"No extractor found for {obj.__class__.__name__}")


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
        @log_latency(StatementType.SQL)
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
                end_time = time.perf_counter()
                duration_ms = int((end_time - start_time) * 1000)

                extractor = get_extractor(self)
                session_id_hex = extractor.get_session_id_hex()
                statement_id = extractor.get_statement_id()

                sql_exec_event = SqlExecutionEvent(
                    statement_type=statement_type,
                    is_compressed=extractor.get_is_compressed(),
                    execution_result=extractor.get_execution_result(),
                    retry_count=extractor.get_retry_count(),
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
