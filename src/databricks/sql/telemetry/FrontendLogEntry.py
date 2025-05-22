import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.TelemetryEvent import TelemetryEvent


@dataclass
class FrontendLogEntry:
    sql_driver_log: TelemetryEvent

    def to_json(self):
        return json.dumps(asdict(self))
