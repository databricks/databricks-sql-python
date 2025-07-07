from enum import Enum
import logging
from typing import Optional, List
from urllib.parse import urlparse
from databricks.sql.common.http import DatabricksHttpClient, HttpMethod

logger = logging.getLogger(__name__)


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    AZURE_OAUTH = "azure-oauth"
    AZURE_SP_M2M = "azure-sp-m2m"


class AzureAppId(Enum):
    DEV = (".dev.azuredatabricks.net", "62a912ac-b58e-4c1d-89ea-b2dbfc7358fc")
    STAGING = (".staging.azuredatabricks.net", "4a67d088-db5c-48f1-9ff2-0aace800ae68")
    PROD = (".azuredatabricks.net", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d")


class ClientContext:
    def __init__(
        self,
        hostname: str,
        access_token: Optional[str] = None,
        auth_type: Optional[str] = None,
        oauth_scopes: Optional[List[str]] = None,
        oauth_client_id: Optional[str] = None,
        azure_client_id: Optional[str] = None,
        azure_client_secret: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
        azure_workspace_resource_id: Optional[str] = None,
        oauth_redirect_port_range: Optional[List[int]] = None,
        use_cert_as_auth: Optional[str] = None,
        tls_client_cert_file: Optional[str] = None,
        oauth_persistence=None,
        credentials_provider=None,
    ):
        self.hostname = hostname
        self.access_token = access_token
        self.auth_type = auth_type
        self.oauth_scopes = oauth_scopes
        self.oauth_client_id = oauth_client_id
        self.azure_client_id = azure_client_id
        self.azure_client_secret = azure_client_secret
        self.azure_tenant_id = azure_tenant_id
        self.azure_workspace_resource_id = azure_workspace_resource_id
        self.oauth_redirect_port_range = oauth_redirect_port_range
        self.use_cert_as_auth = use_cert_as_auth
        self.tls_client_cert_file = tls_client_cert_file
        self.oauth_persistence = oauth_persistence
        self.credentials_provider = credentials_provider


def get_effective_azure_login_app_id(hostname) -> str:
    """
    Get the effective Azure login app ID for a given hostname.
    This function determines the appropriate Azure login app ID based on the hostname.
    If the hostname does not match any of these domains, it returns the default Databricks resource ID.

    """
    for azure_app_id in AzureAppId:
        domain, app_id = azure_app_id.value
        if domain in hostname:
            return app_id

    # default databricks resource id
    return AzureAppId.PROD.value[1]


def get_azure_tenant_id_from_host(host: str, http_client=None) -> str:
    """
    Load the Azure tenant ID from the Azure Databricks login page.
    """

    if http_client is None:
        http_client = DatabricksHttpClient.get_instance()

    login_url = f"{host}/aad/auth"
    logger.debug("Loading tenant ID from %s", login_url)
    with http_client.execute(HttpMethod.GET, login_url, allow_redirects=False) as resp:
        if resp.status_code // 100 != 3:
            raise ValueError(
                f"Failed to get tenant ID from {login_url}: expected status code 3xx, got {resp.status_code}"
            )
        entra_id_endpoint = resp.headers.get("Location")
        if entra_id_endpoint is None:
            raise ValueError(f"No Location header in response from {login_url}")
        # The Location header has the following form: https://login.microsoftonline.com/<tenant-id>/oauth2/authorize?...
        # The domain may change depending on the Azure cloud (e.g. login.microsoftonline.us for US Government cloud).
        url = urlparse(entra_id_endpoint)
        path_segments = url.path.split("/")
        if len(path_segments) < 2:
            raise ValueError(f"Invalid path in Location header: {url.path}")
        return path_segments[1]
