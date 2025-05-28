from enum import Enum
from typing import Optional, List

from databricks.sql.auth.authenticators import (
    AuthProvider,
    AccessTokenAuthProvider,
    ExternalAuthProvider,
    DatabricksOAuthProvider,
    ClientCredentialsProvider,
)


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    AZURE_OAUTH = "azure-oauth"
    CLIENT_CREDENTIALS = "client-credentials"

class ClientContext:
    def __init__(
        self,
        hostname: str,
        access_token: Optional[str] = None,
        auth_type: Optional[str] = None,
        oauth_scopes: Optional[List[str]] = None,
        oauth_client_id: Optional[str] = None,
        oauth_redirect_port_range: Optional[List[int]] = None,
        use_cert_as_auth: Optional[str] = None,
        tls_client_cert_file: Optional[str] = None,
        oauth_persistence=None,
        credentials_provider=None,
        oauth_client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        use_token_federation: bool = False,
        identity_federation_client_id: Optional[str] = None,
    ):
        self.hostname = hostname
        self.access_token = access_token
        self.auth_type = auth_type
        self.oauth_scopes = oauth_scopes
        self.oauth_client_id = oauth_client_id
        self.oauth_redirect_port_range = oauth_redirect_port_range
        self.use_cert_as_auth = use_cert_as_auth
        self.tls_client_cert_file = tls_client_cert_file
        self.oauth_persistence = oauth_persistence
        self.credentials_provider = credentials_provider
        self.identity_federation_client_id = identity_federation_client_id
        self.use_token_federation = use_token_federation
        self.oauth_client_secret = oauth_client_secret
        self.tenant_id = tenant_id

def _create_azure_client_credentials_provider(cfg: ClientContext) -> ClientCredentialsProvider:
    """Create an Azure client credentials provider."""
    if not cfg.oauth_client_id or not cfg.oauth_client_secret or not cfg.tenant_id:
        raise ValueError("Azure client credentials flow requires oauth_client_id, oauth_client_secret, and tenant_id")
    
    token_endpoint = "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(cfg.tenant_id)
    return ClientCredentialsProvider(
        client_id=cfg.oauth_client_id,
        client_secret=cfg.oauth_client_secret,
        token_endpoint=token_endpoint,
        auth_type_value="azure-client-credentials"
    )


def _create_databricks_client_credentials_provider(cfg: ClientContext) -> ClientCredentialsProvider:
    """Create a Databricks client credentials provider for service principals."""
    if not cfg.oauth_client_id or not cfg.oauth_client_secret:
        raise ValueError("Databricks client credentials flow requires oauth_client_id and oauth_client_secret")
    
    token_endpoint = "{}oidc/v1/token".format(cfg.hostname)
    return ClientCredentialsProvider(
        client_id=cfg.oauth_client_id,
        client_secret=cfg.oauth_client_secret,
        token_endpoint=token_endpoint,
        auth_type_value="client-credentials"
    )


def get_auth_provider(cfg: ClientContext):
    """
    Get an appropriate auth provider based on the provided configuration.

    OAuth Flow Support:
    This function supports multiple OAuth flows:
    1. Interactive OAuth (databricks-oauth, azure-oauth) - for user authentication
    2. Client Credentials (client-credentials) - for machine-to-machine authentication
    3. Token Federation - implemented as a feature flag that wraps any auth type

    Token Federation Support:
    -----------------------
    Token federation is implemented as a feature flag (`use_token_federation=True`) that
    can be combined with any auth type. When enabled, it wraps the base auth provider
    in a DatabricksTokenFederationProvider for token exchange functionality.

    Args:
        cfg: The client context containing configuration parameters

    Returns:
        An appropriate AuthProvider instance

    Raises:
        RuntimeError: If no valid authentication settings are provided
    """
    from databricks.sql.auth.token_federation import DatabricksTokenFederationProvider
    
    base_provider = None
    
    if cfg.credentials_provider:
        base_provider = ExternalAuthProvider(cfg.credentials_provider)
    elif cfg.access_token is not None:
        base_provider = AccessTokenAuthProvider(cfg.access_token)
    elif cfg.auth_type == AuthType.CLIENT_CREDENTIALS.value:
        if cfg.tenant_id:
            # Azure client credentials flow
            base_provider = _create_azure_client_credentials_provider(cfg)
        else:
            # Databricks service principal client credentials flow
            base_provider = _create_databricks_client_credentials_provider(cfg)
    elif cfg.auth_type in [AuthType.DATABRICKS_OAUTH.value, AuthType.AZURE_OAUTH.value]:
        assert cfg.oauth_redirect_port_range is not None
        assert cfg.oauth_client_id is not None
        assert cfg.oauth_scopes is not None
        base_provider = DatabricksOAuthProvider(
            hostname=cfg.hostname,
            oauth_persistence=cfg.oauth_persistence,
            redirect_port_range=cfg.oauth_redirect_port_range,
            client_id=cfg.oauth_client_id,
            scopes=cfg.oauth_scopes,
            auth_type=cfg.auth_type,
        )
    elif cfg.use_cert_as_auth and cfg.tls_client_cert_file:
        base_provider = AuthProvider()
    else:
        if (
            cfg.oauth_redirect_port_range is not None
            and cfg.oauth_client_id is not None
            and cfg.oauth_scopes is not None
        ):
            base_provider = DatabricksOAuthProvider(
                hostname=cfg.hostname,
                oauth_persistence=cfg.oauth_persistence,
                redirect_port_range=cfg.oauth_redirect_port_range,
                client_id=cfg.oauth_client_id,
                scopes=cfg.oauth_scopes,
            )
        else:
            raise RuntimeError("No valid authentication settings!")

    if getattr(cfg, "use_token_federation", False):
        base_provider = DatabricksTokenFederationProvider(
            base_provider, cfg.hostname, cfg.identity_federation_client_id
        )

    return base_provider


PYSQL_OAUTH_SCOPES = ["sql", "offline_access"]
PYSQL_OAUTH_CLIENT_ID = "databricks-sql-python"
PYSQL_OAUTH_AZURE_CLIENT_ID = "96eecda7-19ea-49cc-abb5-240097d554f5"
PYSQL_OAUTH_REDIRECT_PORT_RANGE = list(range(8020, 8025))
PYSQL_OAUTH_AZURE_REDIRECT_PORT_RANGE = [8030]


def normalize_host_name(hostname: str):
    maybe_scheme = "https://" if not hostname.startswith("https://") else ""
    maybe_trailing_slash = "/" if not hostname.endswith("/") else ""
    return "{}{}{}".format(maybe_scheme, hostname, maybe_trailing_slash)


def get_client_id_and_redirect_port(use_azure_auth: bool):
    return (
        (PYSQL_OAUTH_CLIENT_ID, PYSQL_OAUTH_REDIRECT_PORT_RANGE)
        if not use_azure_auth
        else (PYSQL_OAUTH_AZURE_CLIENT_ID, PYSQL_OAUTH_AZURE_REDIRECT_PORT_RANGE)
    )


def get_python_sql_connector_auth_provider(hostname: str, **kwargs):
    """
    Get an auth provider for the Python SQL connector.

    This function is the main entry point for authentication in the SQL connector.
    It processes the parameters and creates an appropriate auth provider.

    Supported Authentication Methods:
    --------------------------------
    1. Access Token: Provide 'access_token' parameter
    2. Interactive OAuth: Set 'auth_type' to 'databricks-oauth' or 'azure-oauth'
    3. Client Credentials: Set 'auth_type' to 'client-credentials' with client_id, client_secret, tenant_id
    4. External Provider: Provide 'credentials_provider' parameter
    5. Token Federation: Set 'use_token_federation=True' with any of the above

    Args:
        hostname: The Databricks server hostname
        **kwargs: Additional configuration parameters including:
            - auth_type: Authentication type
            - access_token: Static access token
            - oauth_client_id: OAuth client ID
            - oauth_client_secret: OAuth client secret
            - tenant_id: Azure AD tenant ID (for Azure flows)
            - credentials_provider: External credentials provider
            - use_token_federation: Enable token federation
            - identity_federation_client_id: Federation client ID

    Returns:
        An appropriate AuthProvider instance

    Raises:
        ValueError: If username/password authentication is attempted (no longer supported)
    """
    auth_type = kwargs.get("auth_type")
    (client_id, redirect_port_range) = get_client_id_and_redirect_port(
        auth_type == AuthType.AZURE_OAUTH.value
    )
    if kwargs.get("username") or kwargs.get("password"):
        raise ValueError(
            "Username/password authentication is no longer supported. "
            "Please use OAuth or access token instead."
        )

    cfg = ClientContext(
        hostname=normalize_host_name(hostname),
        auth_type=auth_type,
        access_token=kwargs.get("access_token"),
        use_cert_as_auth=kwargs.get("_use_cert_as_auth"),
        tls_client_cert_file=kwargs.get("_tls_client_cert_file"),
        oauth_scopes=PYSQL_OAUTH_SCOPES,
        oauth_client_id=kwargs.get("oauth_client_id") or client_id,
        oauth_redirect_port_range=[kwargs["oauth_redirect_port"]]
        if kwargs.get("oauth_client_id") and kwargs.get("oauth_redirect_port")
        else redirect_port_range,
        oauth_persistence=kwargs.get("experimental_oauth_persistence"),
        credentials_provider=kwargs.get("credentials_provider"),
        oauth_client_secret=kwargs.get("oauth_client_secret"),
        tenant_id=kwargs.get("tenant_id"),
        identity_federation_client_id=kwargs.get("identity_federation_client_id"),
        use_token_federation=kwargs.get("use_token_federation", False),
    )
    return get_auth_provider(cfg)
