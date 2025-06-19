import time
import functools
from typing import Optional
from uuid import UUID
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import (
    SqlExecutionEvent,
)
from databricks.sql.telemetry.models.enums import ExecutionResultFormat, StatementType
from databricks.sql.utils import ColumnQueue

# Helper to get statement_id/query_id from instance if available
def _get_statement_id(instance) -> Optional[str]:
    """
    Get statement ID from an instance using various methods:
    1. For Cursor: Use query_id property which returns UUID from active_op_handle
    2. For ResultSet: Use command_id which contains operationId

    Note: ThriftBackend itself doesn't have a statement ID since one backend
    can handle multiple concurrent operations/cursors.
    """
    if hasattr(instance, "query_id"):
        return instance.query_id

    if hasattr(instance, "command_id") and instance.command_id:
        return str(UUID(bytes=instance.command_id.operationId.guid))

    return None


def _get_session_id_hex(instance) -> Optional[str]:
    if hasattr(instance, "connection") and instance.connection:
        return instance.connection.get_session_id_hex()
    if hasattr(instance, "get_session_id_hex"):
        return instance.get_session_id_hex()
    return None


def _get_statement_type(func_name: str) -> StatementType:  # TODO: implement this
    return StatementType.SQL


def _get_is_compressed(instance) -> bool:
    """
    Get compression status from instance:
    1. Direct lz4_compression attribute (Connection)
    2. Through connection attribute (Cursor/ResultSet)
    3. Through thrift_backend attribute (Cursor)
    """
    if hasattr(instance, "lz4_compression"):
        return instance.lz4_compression
    if hasattr(instance, "connection") and instance.connection:
        return instance.connection.lz4_compression
    if hasattr(instance, "thrift_backend") and instance.thrift_backend:
        return instance.thrift_backend.lz4_compressed
    return False


def _get_execution_result(instance) -> ExecutionResultFormat:
    """
    Get execution result format from instance:
    1. For ResultSet: Check if using cloud fetch (external_links) or arrow/columnar format
    2. For Cursor: Check through active_result_set
    3. For ThriftBackend: Check result format from server
    """
    if hasattr(instance, "_use_cloud_fetch") and instance._use_cloud_fetch:
        return ExecutionResultFormat.EXTERNAL_LINKS

    if hasattr(instance, "active_result_set") and instance.active_result_set:
        if isinstance(instance.active_result_set.results, ColumnQueue):
            return ExecutionResultFormat.COLUMNAR_INLINE
        return ExecutionResultFormat.INLINE_ARROW

    if hasattr(instance, "thrift_backend") and instance.thrift_backend:
        if hasattr(instance.thrift_backend, "_use_arrow_native_complex_types"):
            return ExecutionResultFormat.INLINE_ARROW

    return ExecutionResultFormat.FORMAT_UNSPECIFIED


def _get_retry_count(instance) -> int:
    """
    Get retry count from instance by checking retry_policy.history length.
    The retry_policy is only accessible through thrift_backend.
    """
    # TODO: implement this

    return 0


def log_latency():
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

                session_id_hex = _get_session_id_hex(self)

                if session_id_hex:
                    statement_id = _get_statement_id(self)
                    statement_type = _get_statement_type(func.__name__)
                    is_compressed = _get_is_compressed(self)
                    execution_result = _get_execution_result(self)
                    retry_count = _get_retry_count(self)

                    sql_exec_event = SqlExecutionEvent(
                        statement_type=statement_type,
                        is_compressed=is_compressed,
                        execution_result=execution_result,
                        retry_count=retry_count,
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
