import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.enums.DriverVolumeOperationType import (
    DriverVolumeOperationType,
)


@dataclass
class DriverVolumeOperation:
    volume_operation_type: DriverVolumeOperationType
    volume_path: str

    def to_json(self):
        return json.dumps(asdict(self))


""" Part of TelemetryEvent
DriverVolumeOperation volumeOperation = new DriverVolumeOperation(
    volumeOperationType = "LIST",
    volumePath = "/path/to/volume"
)
"""
