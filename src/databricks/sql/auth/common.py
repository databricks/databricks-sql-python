from enum import Enum
import logging
from typing import Optional, List
from urllib.parse import urlparse
from databricks.sql.auth.retry import DatabricksRetryPolicy
from databricks.sql.common.http import HttpMethod

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
        # HTTP client configuration parameters
        ssl_options=None,  # SSLOptions type
        socket_timeout: Optional[float] = None,
        retry_stop_after_attempts_count: int = 5,
        retry_delay_min: float = 1.0,
        retry_delay_max: float = 60.0,
        retry_stop_after_attempts_duration: float = 900.0,
        retry_delay_default: float = 5.0,
        retry_dangerous_codes: Optional[List[int]] = None,
        http_proxy: Optional[str] = None,
        proxy_username: Optional[str] = None,
        proxy_password: Optional[str] = None,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
        user_agent: Optional[str] = None,
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

        # HTTP client configuration
        self.ssl_options = ssl_options
        self.socket_timeout = socket_timeout
        self.retry_stop_after_attempts_count = retry_stop_after_attempts_count
        self.retry_delay_min = retry_delay_min
        self.retry_delay_max = retry_delay_max
        self.retry_stop_after_attempts_duration = retry_stop_after_attempts_duration
        self.retry_delay_default = retry_delay_default
        self.retry_dangerous_codes = retry_dangerous_codes or []
        self.http_proxy = http_proxy
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.user_agent = user_agent


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


def get_azure_tenant_id_from_host(host: str, http_client) -> str:
    """
    Load the Azure tenant ID from the Azure Databricks login page.

    This function retrieves the Azure tenant ID by making a request to the Databricks
    Azure Active Directory (AAD) authentication endpoint. The endpoint redirects to
    the Azure login page, and the tenant ID is extracted from the redirect URL.
    """

    login_url = f"{host}/aad/auth"
    logger.debug("Loading tenant ID from %s", login_url)

    with http_client.request_context(
        HttpMethod.GET, login_url, allow_redirects=False
    ) as resp:
        if resp.status // 100 != 3:
            raise ValueError(
                f"Failed to get tenant ID from {login_url}: expected status code 3xx, got {resp.status}"
            )
        entra_id_endpoint = dict(resp.headers).get("Location")
        if entra_id_endpoint is None:
            raise ValueError(f"No Location header in response from {login_url}")

    # The Location header has the following form: https://login.microsoftonline.com/<tenant-id>/oauth2/authorize?...
    # The domain may change depending on the Azure cloud (e.g. login.microsoftonline.us for US Government cloud).
    url = urlparse(entra_id_endpoint)
    path_segments = url.path.split("/")
    if len(path_segments) < 2:
        raise ValueError(f"Invalid path in Location header: {url.path}")
    return path_segments[1]
