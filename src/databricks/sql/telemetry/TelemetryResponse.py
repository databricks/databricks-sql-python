import json
from dataclasses import dataclass, asdict
from typing import List, Optional


@dataclass
class TelemetryResponse:
    errors: List[str]
    numSuccess: int
    numProtoSuccess: int

    def to_json(self):
        return json.dumps(asdict(self))
