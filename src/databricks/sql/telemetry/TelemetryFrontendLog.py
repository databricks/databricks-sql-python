import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.FrontendLogContext import FrontendLogContext
from databricks.sql.telemetry.FrontendLogEntry import FrontendLogEntry


@dataclass
class TelemetryFrontendLog:
    workspace_id: int
    frontend_log_event_id: str
    context: FrontendLogContext
    entry: FrontendLogEntry

    def to_json(self):
        return json.dumps(asdict(self))
