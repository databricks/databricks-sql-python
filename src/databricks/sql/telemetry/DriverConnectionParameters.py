import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.HostDetails import HostDetails
from databricks.sql.telemetry.enums.AuthMech import AuthMech
from databricks.sql.telemetry.enums.AuthFlow import AuthFlow
from databricks.sql.telemetry.enums.DatabricksClientType import DatabricksClientType


@dataclass
class DriverConnectionParameters:
    http_path: str
    driver_mode: DatabricksClientType
    host_details: HostDetails
    auth_mech: AuthMech
    auth_flow: AuthFlow
    auth_scope: str
    discovery_url: str
    allowed_volume_ingestion_paths: str
    azure_tenant_id: str
    socket_timeout: int

    def to_json(self):
        return json.dumps(asdict(self))


# Part of TelemetryEvent
# DriverConnectionParameters connectionParams = new DriverConnectionParameters(
#     httpPath = " /sql/1.0/endpoints/1234567890abcdef",
#     driverMode = "THRIFT",
#     hostDetails = new HostDetails(
#         hostUrl = "https://my-workspace.cloud.databricks.com",
#         port = 443
#     ),
#     authMech = "OAUTH",
#     authFlow = "AZURE_MANAGED_IDENTITIES",
#     authScope = "sql",
#     discoveryUrl = "https://example-url",
#     allowedVolumeIngestionPaths = "[]",
#     azureTenantId = "1234567890abcdef",
#     socketTimeout = 10000
# )
