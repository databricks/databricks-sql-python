import json
from dataclasses import dataclass, asdict
from typing import List, Optional


@dataclass
class TelemetryRequest:
    """
    Represents a request to send telemetry data to the server side.
    Contains the telemetry items to be uploaded and optional protocol buffer logs.

    Attributes:
        uploadTime (int): Unix timestamp in milliseconds when the request is made
        items (List[str]): List of telemetry event items to be uploaded
        protoLogs (Optional[List[str]]): Optional list of protocol buffer formatted logs
    """

    uploadTime: int
    items: List[str]
    protoLogs: Optional[List[str]]

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class TelemetryResponse:
    """
    Represents the response from the telemetry backend after processing a request.
    Contains information about the success or failure of the telemetry upload.

    Attributes:
        errors (List[str]): List of error messages if any occurred during processing
        numSuccess (int): Number of successfully processed telemetry items
        numProtoSuccess (int): Number of successfully processed protocol buffer logs
    """

    errors: List[str]
    numSuccess: int
    numProtoSuccess: int

    def to_json(self):
        return json.dumps(asdict(self))
