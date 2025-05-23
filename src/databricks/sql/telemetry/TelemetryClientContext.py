from dataclasses import dataclass, asdict
import json


@dataclass
class TelemetryClientContext:
    timestamp_millis: int
    user_agent: str

    def to_json(self):
        return json.dumps(asdict(self))


# used in FrontendLogContext
# TelemetryClientContext clientContext = new TelemetryClientContext(
#     timestampMillis = 1716489600000,
#     userAgent = "databricks-sql-python-test"
# )
