from enum import Enum
from typing import Optional, List

from databricks.sql.auth.authenticators import (
    AuthProvider,
    AccessTokenAuthProvider,
    ExternalAuthProvider,
    CredentialsProvider,
    DatabricksOAuthProvider,
)


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    AZURE_OAUTH = "azure-oauth"
    # TODO: Token federation should be a feature that works with different auth types,
    # not an auth type itself. This will be refactored in a future change.
    # We will add a use_token_federation flag that can be used with any auth type.
    TOKEN_FEDERATION = "token-federation"
    # other supported types (access_token) can be inferred
    # we can add more types as needed later


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
        identity_federation_client_id: Optional[str] = None,
        use_token_federation: bool = False,
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


def get_auth_provider(cfg: ClientContext):
    """
    Get an appropriate auth provider based on the provided configuration.

    Token Federation Support:
    -----------------------
    Currently, token federation is implemented as a separate auth type, but the goal is to
    refactor it as a feature that can work with any auth type. The current implementation
    is maintained for backward compatibility while the refactoring is planned.

    Future refactoring will introduce a `use_token_federation` flag that can be combined
    with any auth type to enable token federation.

    Args:
        cfg: The client context containing configuration parameters

    Returns:
        An appropriate AuthProvider instance

    Raises:
        RuntimeError: If no valid authentication settings are provided
    """
    from databricks.sql.auth.token_federation import DatabricksTokenFederationProvider
    if cfg.credentials_provider:
        base_provider = ExternalAuthProvider(cfg.credentials_provider)
    elif cfg.access_token is not None:
        base_provider = AccessTokenAuthProvider(cfg.access_token)
    elif cfg.auth_type in [AuthType.DATABRICKS_OAUTH.value, AuthType.AZURE_OAUTH.value]:
        assert cfg.oauth_redirect_port_range is not None
        assert cfg.oauth_client_id is not None
        assert cfg.oauth_scopes is not None
        base_provider = DatabricksOAuthProvider(
            cfg.hostname,
            cfg.oauth_persistence,
            cfg.oauth_redirect_port_range,
            cfg.oauth_client_id,
            cfg.oauth_scopes,
            cfg.auth_type,
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
                cfg.hostname,
                cfg.oauth_persistence,
                cfg.oauth_redirect_port_range,
                cfg.oauth_client_id,
                cfg.oauth_scopes,
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
    return f"{maybe_scheme}{hostname}{maybe_trailing_slash}"


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

    TODO: Future refactoring needed:
    1. Add a use_token_federation flag that can be combined with any auth type
    2. Remove TOKEN_FEDERATION as an auth_type while maintaining backward compatibility
    3. Create a token federation wrapper that can wrap any existing auth provider

    Args:
        hostname: The Databricks server hostname
        **kwargs: Additional configuration parameters

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
        identity_federation_client_id=kwargs.get("identity_federation_client_id"),
        use_token_federation=kwargs.get("use_token_federation", False),
    )
    return get_auth_provider(cfg)
