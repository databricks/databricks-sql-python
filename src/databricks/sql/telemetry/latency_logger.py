import time
import functools
from typing import Optional

from databricks.sql.telemetry.telemetry_client import telemetry_client
from databricks.sql.telemetry.models.event import (
    SqlExecutionEvent,
    DriverVolumeOperation,
)
from databricks.sql.telemetry.models.enums import ExecutionResultFormat, StatementType
from databricks.sql.utils import ColumnQueue

# Helper to get statement_id/query_id from instance if available
def _get_statement_id_from_instance(instance) -> Optional[str]:
    """
    Get statement ID from an instance using various methods:
    1. For Cursor: Use query_id property which returns UUID from active_op_handle
    2. For ResultSet: Use command_id which contains operationId
    3. For objects with active_op_handle: Convert operationId to UUID string
    4. For ThriftBackend: Get operation ID from session_handle if available
    """
    # Case 1: Direct query_id property (Cursor class)
    if hasattr(instance, "query_id"):
        return instance.query_id

    # Case 2: Direct command_id (ResultSet class)
    if hasattr(instance, "command_id"):
        return instance.guid_to_hex_id(instance.command_id.operationId.guid)

    # Case 3: Direct active_op_handle (Cursor class)
    if hasattr(instance, "active_op_handle"):
        return instance.guid_to_hex_id(instance.active_op_handle.operationId.guid)

    # Case 4: For ThriftBackend, get operation ID from session_handle
    if instance.__class__.__name__ == "ThriftBackend" and hasattr(
        instance, "_session_handle"
    ):
        return instance.handle_to_hex_id(instance._session_handle)

    return None


def _get_connection_uuid_from_instance(instance) -> Optional[str]:
    if hasattr(instance, "connection") and instance.connection:
        return instance.connection.get_session_id_hex()
    if hasattr(instance, "get_session_id_hex"):
        return instance.get_session_id_hex()
    if hasattr(instance, "get_connection_uuid"):
        return instance.get_connection_uuid()
    return None


def _get_statement_type(func_name: str) -> StatementType:
    """
    Map method names to statement types:
    - execute* methods -> SQL
    - catalogs/schemas/tables/columns -> METADATA
    - fetch* methods -> QUERY
    - Others -> NONE
    """
    if func_name.startswith("execute"):
        return StatementType.SQL
    elif func_name in ["catalogs", "schemas", "tables", "columns"]:
        return StatementType.METADATA
    elif func_name.startswith("fetch"):
        return StatementType.QUERY
    return StatementType.NONE


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
    # Check if using cloud fetch
    if hasattr(instance, "_use_cloud_fetch") and instance._use_cloud_fetch:
        return ExecutionResultFormat.EXTERNAL_LINKS

    # Check result format from ResultSet
    if hasattr(instance, "active_result_set") and instance.active_result_set:
        if isinstance(instance.active_result_set.results, ColumnQueue):
            return ExecutionResultFormat.COLUMNAR_INLINE
        return ExecutionResultFormat.INLINE_ARROW

    # Check result format from ThriftBackend
    if hasattr(instance, "thrift_backend") and instance.thrift_backend:
        if hasattr(instance.thrift_backend, "_use_arrow_native_complex_types"):
            return ExecutionResultFormat.INLINE_ARROW

    return ExecutionResultFormat.FORMAT_UNSPECIFIED


def _get_retry_count(instance) -> int:
    """
    Get retry count from instance:
    1. Direct retry_policy attribute (ThriftBackend)
    2. Through thrift_backend attribute (Cursor/ResultSet)
    3. Through connection attribute (Cursor/ResultSet)
    """
    # Case 1: Direct retry_policy (ThriftBackend)
    if hasattr(instance, "retry_policy") and instance.retry_policy:
        # Get attempts from history length
        return (
            len(instance.retry_policy.history) if instance.retry_policy.history else 0
        )

    # Case 2: Through thrift_backend (Cursor/ResultSet)
    if hasattr(instance, "thrift_backend") and instance.thrift_backend:
        if (
            hasattr(instance.thrift_backend, "retry_policy")
            and instance.thrift_backend.retry_policy
        ):
            return (
                len(instance.thrift_backend.retry_policy.history)
                if instance.thrift_backend.retry_policy.history
                else 0
            )

    # Case 3: Through connection (Cursor/ResultSet)
    if hasattr(instance, "connection") and instance.connection:
        if (
            hasattr(instance.connection, "thrift_backend")
            and instance.connection.thrift_backend
        ):
            if (
                hasattr(instance.connection.thrift_backend, "retry_policy")
                and instance.connection.thrift_backend.retry_policy
            ):
                return (
                    len(instance.connection.thrift_backend.retry_policy.history)
                    if instance.connection.thrift_backend.retry_policy.history
                    else 0
                )

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

                connection_uuid = _get_connection_uuid_from_instance(self)

                if connection_uuid:
                    # Check if this is a volume operation
                    if hasattr(self, "volume_operation_type") and hasattr(
                        self, "volume_path"
                    ):
                        volume_operation = DriverVolumeOperation(
                            volume_operation_type=self.volume_operation_type,
                            volume_path=self.volume_path,
                        )
                        telemetry_client.export_volume_latency_log(
                            latency_ms=duration_ms,
                            volume_operation=volume_operation,
                            connection_uuid=connection_uuid,
                        )
                    else:
                        # Regular SQL execution
                        statement_id = _get_statement_id_from_instance(self)
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

                        # Only append class and method names if we have a statement_id
                        if statement_id:
                            statement_id = (
                                statement_id
                                + "_"
                                + self.__class__.__name__
                                + "_"
                                + func.__name__
                            )
                        else:
                            statement_id = self.__class__.__name__ + "_" + func.__name__

                        telemetry_client.export_sql_latency_log(
                            latency_ms=duration_ms,
                            sql_execution_event=sql_exec_event,
                            sql_statement_id=statement_id,
                            connection_uuid=connection_uuid,
                        )

        return wrapper

    return decorator
