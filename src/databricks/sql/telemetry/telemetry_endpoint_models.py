import json
from dataclasses import dataclass, asdict
from typing import List, Optional


@dataclass
class TelemetryRequest:
    uploadTime: int
    items: List[str]
    protoLogs: Optional[List[str]]

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class TelemetryResponse:
    errors: List[str]
    numSuccess: int
    numProtoSuccess: int

    def to_json(self):
        return json.dumps(asdict(self))
