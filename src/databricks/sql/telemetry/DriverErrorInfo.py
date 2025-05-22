import json
from dataclasses import dataclass, asdict


@dataclass
class DriverErrorInfo:
    error_name: str
    stack_trace: str

    def to_json(self):
        return json.dumps(asdict(self))
