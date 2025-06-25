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
    def __init__(self, obj):
        self._obj = obj

    def __getattr__(self, name):
        return getattr(self._obj, name)

    def get_session_id_hex(self):
        pass

    def get_statement_id(self):
        pass

    def get_statement_type(self):
        pass

    def get_is_compressed(self):
        pass

    def get_execution_result(self):
        pass

    def get_retry_count(self):
        pass


class CursorExtractor(TelemetryExtractor):
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

    def get_statement_type(self) -> StatementType:
        # TODO: Implement this
        return StatementType.SQL


class ResultSetExtractor(TelemetryExtractor):
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

    def get_statement_type(self) -> StatementType:
        # TODO: Implement this
        return StatementType.SQL

    def get_retry_count(self) -> int:
        if (
            hasattr(self.thrift_backend, "retry_policy")
            and self.thrift_backend.retry_policy
        ):
            return len(self.thrift_backend.retry_policy.history)
        return 0


def get_extractor(obj):
    if obj.__class__.__name__ == "Cursor":
        return CursorExtractor(obj)
    elif obj.__class__.__name__ == "ResultSet":
        return ResultSetExtractor(obj)
    else:
        return TelemetryExtractor(obj)


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

                extractor = get_extractor(self)
                session_id_hex = extractor.get_session_id_hex()
                statement_id = extractor.get_statement_id()

                sql_exec_event = SqlExecutionEvent(
                    statement_type=extractor.get_statement_type(),
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
