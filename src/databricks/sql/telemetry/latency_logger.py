import time
import functools
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory
from databricks.sql.telemetry.models.event import (
    SqlExecutionEvent,
)


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

                session_id_hex = self.get_session_id_hex()
                statement_id = self.get_statement_id()

                sql_exec_event = SqlExecutionEvent(
                    statement_type=self.get_statement_type(func.__name__),
                    is_compressed=self.get_is_compressed(),
                    execution_result=self.get_execution_result(),
                    retry_count=self.get_retry_count(),
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
