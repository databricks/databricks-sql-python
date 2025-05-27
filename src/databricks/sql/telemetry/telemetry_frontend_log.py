import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.telemetry_event import TelemetryEvent


@dataclass
class TelemetryClientContext:
    """
    Used in FrontendLogContext

    Example:
    TelemetryClientContext clientContext = new TelemetryClientContext(
        timestampMillis = 1716489600000,
        userAgent = "databricks-sql-python-test"
    )
    """

    timestamp_millis: int
    user_agent: str

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class FrontendLogContext:
    client_context: TelemetryClientContext

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class FrontendLogEntry:
    sql_driver_log: TelemetryEvent

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class TelemetryFrontendLog:
    workspace_id: int
    frontend_log_event_id: str
    context: FrontendLogContext
    entry: FrontendLogEntry

    def to_json(self):
        return json.dumps(asdict(self))
