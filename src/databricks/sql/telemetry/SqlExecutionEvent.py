import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.enums.StatementType import StatementType
from databricks.sql.telemetry.enums.ExecutionResultFormat import ExecutionResultFormat


@dataclass
class SqlExecutionEvent:
    statement_type: StatementType
    is_compressed: bool
    execution_result: ExecutionResultFormat
    retry_count: int

    def to_json(self):
        return json.dumps(asdict(self))

# Part of TelemetryEvent
# SqlExecutionEvent sqlExecutionEvent = new SqlExecutionEvent(
#     statementType = "QUERY",
#     isCompressed = true,
#     executionResult = "INLINE_ARROW",
#     retryCount = 0
# ) 