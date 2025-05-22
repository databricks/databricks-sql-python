import json
from dataclasses import dataclass, asdict


@dataclass
class HostDetails:
    host_url: str
    port: int

    def to_json(self):
        return json.dumps(asdict(self))
