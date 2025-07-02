from typing import Optional, List

from databricks.sql.auth.authenticators import (
    AuthProvider,
    AccessTokenAuthProvider,
    ExternalAuthProvider,
    DatabricksOAuthProvider,
    AzureServicePrincipalCredentialProvider,
)
from databricks.sql.common.auth import AuthType


class ClientContext:
    def __init__(
        self,
        hostname: str,
        access_token: Optional[str] = None,
        auth_type: Optional[str] = None,
        oauth_scopes: Optional[List[str]] = None,
        oauth_client_id: Optional[str] = None,
        oauth_client_secret: Optional[str] = None,
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
        self.oauth_client_secret = oauth_client_secret
        self.azure_tenant_id = azure_tenant_id
        self.azure_workspace_resource_id = azure_workspace_resource_id
        self.oauth_redirect_port_range = oauth_redirect_port_range
        self.use_cert_as_auth = use_cert_as_auth
        self.tls_client_cert_file = tls_client_cert_file
        self.oauth_persistence = oauth_persistence
        self.credentials_provider = credentials_provider


def get_auth_provider(cfg: ClientContext):
    if cfg.credentials_provider:
        return ExternalAuthProvider(cfg.credentials_provider)
    elif cfg.auth_type == AuthType.AZURE_SP_M2M.value:
        return ExternalAuthProvider(
            AzureServicePrincipalCredentialProvider(
                cfg.hostname,
                cfg.oauth_client_id,
                cfg.oauth_client_secret,
                cfg.azure_tenant_id,
                cfg.azure_workspace_resource_id,
            )
        )
    elif cfg.auth_type in [AuthType.DATABRICKS_OAUTH.value, AuthType.AZURE_OAUTH.value]:
        assert cfg.oauth_redirect_port_range is not None
        assert cfg.oauth_client_id is not None
        assert cfg.oauth_scopes is not None

        return DatabricksOAuthProvider(
            cfg.hostname,
            cfg.oauth_persistence,
            cfg.oauth_redirect_port_range,
            cfg.oauth_client_id,
            cfg.oauth_scopes,
            cfg.auth_type,
        )
    elif cfg.access_token is not None:
        return AccessTokenAuthProvider(cfg.access_token)
    elif cfg.use_cert_as_auth and cfg.tls_client_cert_file:
        # no op authenticator. authentication is performed using ssl certificate outside of headers
        return AuthProvider()
    else:
        if (
            cfg.oauth_redirect_port_range is not None
            and cfg.oauth_client_id is not None
            and cfg.oauth_scopes is not None
        ):
            return DatabricksOAuthProvider(
                cfg.hostname,
                cfg.oauth_persistence,
                cfg.oauth_redirect_port_range,
                cfg.oauth_client_id,
                cfg.oauth_scopes,
            )
        else:
            raise RuntimeError("No valid authentication settings!")


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
    auth_type = kwargs.get("auth_type")
    client_id = kwargs.get("oauth_client_id")
    redirect_port_range = kwargs.get("oauth_redirect_port_range")

    if auth_type == AuthType.AZURE_SP_M2M.value:
        pass
    else:
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
        oauth_client_id=client_id,
        oauth_client_secret=kwargs.get("oauth_client_secret"),
        azure_tenant_id=kwargs.get("azure_tenant_id"),
        azure_workspace_resource_id=kwargs.get("azure_workspace_resource_id"),
        oauth_redirect_port_range=[kwargs["oauth_redirect_port"]]
        if client_id and kwargs.get("oauth_redirect_port")
        else redirect_port_range,
        oauth_persistence=kwargs.get("experimental_oauth_persistence"),
        credentials_provider=kwargs.get("credentials_provider"),
    )
    return get_auth_provider(cfg)
