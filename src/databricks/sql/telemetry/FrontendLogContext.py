import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.TelemetryClientContext import TelemetryClientContext


@dataclass
class FrontendLogContext:
    client_context: TelemetryClientContext

    def to_json(self):
        return json.dumps(asdict(self))
