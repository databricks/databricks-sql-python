import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.models.telemetry_event import TelemetryEvent
from typing import Optional


@dataclass
class TelemetryClientContext:
    """
    Contains client-side context information for telemetry events.
    This includes timestamp and user agent information for tracking when and how the client is being used.

    Attributes:
        timestamp_millis (int): Unix timestamp in milliseconds when the event occurred
        user_agent (str): Identifier for the client application making the request
    """

    timestamp_millis: int
    user_agent: str

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class FrontendLogContext:
    """
    Wrapper for client context information in frontend logs.
    Provides additional context about the client environment for telemetry events.

    Attributes:
        client_context (TelemetryClientContext): Client-specific context information
    """

    client_context: TelemetryClientContext

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class FrontendLogEntry:
    """
    Contains the actual telemetry event data in a frontend log.
    Wraps the SQL driver log information for frontend processing.

    Attributes:
        sql_driver_log (TelemetryEvent): The telemetry event containing SQL driver information
    """

    sql_driver_log: TelemetryEvent

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class TelemetryFrontendLog:
    """
    Main container for frontend telemetry data.
    Aggregates workspace information, event ID, context, and the actual log entry.
    Used for sending telemetry data to the server side.

    Attributes:
        workspace_id (int): Unique identifier for the Databricks workspace
        frontend_log_event_id (str): Unique identifier for this telemetry event
        context (FrontendLogContext): Context information about the client
        entry (FrontendLogEntry): The actual telemetry event data
    """

    frontend_log_event_id: str
    context: FrontendLogContext
    entry: FrontendLogEntry
    workspace_id: Optional[int] = None

    def to_json(self):
        return json.dumps(asdict(self))
