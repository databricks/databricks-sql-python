import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.DriverSystemConfiguration import DriverSystemConfiguration
from databricks.sql.telemetry.DriverConnectionParameters import (
    DriverConnectionParameters,
)
from databricks.sql.telemetry.DriverVolumeOperation import DriverVolumeOperation
from databricks.sql.telemetry.SqlExecutionEvent import SqlExecutionEvent
from databricks.sql.telemetry.DriverErrorInfo import DriverErrorInfo


@dataclass
class TelemetryEvent:
    session_id: str
    sql_statement_id: str
    system_configuration: DriverSystemConfiguration
    driver_connection_params: DriverConnectionParameters
    auth_type: str
    vol_operation: DriverVolumeOperation
    sql_operation: SqlExecutionEvent
    error_info: DriverErrorInfo
    latency: int

    def to_json(self):
        return json.dumps(asdict(self))
