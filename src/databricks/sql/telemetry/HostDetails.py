import json
from dataclasses import dataclass, asdict


@dataclass
class HostDetails:
    host_url: str
    port: int

    def to_json(self):
        return json.dumps(asdict(self))

# Part of DriverConnectionParameters
# HostDetails hostDetails = new HostDetails(
#     hostUrl = "https://my-workspace.cloud.databricks.com",
#     port = 443
# )
