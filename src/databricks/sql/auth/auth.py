from typing import Optional, List

from databricks.sql.auth.authenticators import (
    AuthProvider,
    AccessTokenAuthProvider,
    ExternalAuthProvider,
    DatabricksOAuthProvider,
    AzureServicePrincipalCredentialProvider,
)
from databricks.sql.auth.common import AuthType, ClientContext
from databricks.sql.auth.token_federation import TokenFederationProvider


def get_auth_provider(cfg: ClientContext, http_client):
    # Determine the base auth provider
    base_provider: Optional[AuthProvider] = None

    if cfg.credentials_provider:
        base_provider = ExternalAuthProvider(cfg.credentials_provider)
    elif cfg.auth_type == AuthType.AZURE_SP_M2M.value:
        base_provider = ExternalAuthProvider(
            AzureServicePrincipalCredentialProvider(
                cfg.hostname,
                cfg.azure_client_id,
                cfg.azure_client_secret,
                http_client,
                cfg.azure_tenant_id,
                cfg.azure_workspace_resource_id,
            )
        )
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
            http_client,
            cfg.auth_type,
        )
    elif cfg.access_token is not None:
        base_provider = AccessTokenAuthProvider(cfg.access_token)
    elif cfg.use_cert_as_auth and cfg.tls_client_cert_file:
        # no op authenticator. authentication is performed using ssl certificate outside of headers
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
                http_client,
                cfg.auth_type or AuthType.DATABRICKS_OAUTH.value,
            )
        else:
            raise RuntimeError("No valid authentication settings!")

    # Always wrap with token federation (falls back gracefully if not needed)
    if base_provider:
        return TokenFederationProvider(
            hostname=cfg.hostname,
            external_provider=base_provider,
            http_client=http_client,
            identity_federation_client_id=cfg.identity_federation_client_id,
        )

    return base_provider


PYSQL_OAUTH_SCOPES = ["sql", "offline_access"]
PYSQL_OAUTH_CLIENT_ID = "databricks-sql-python"
PYSQL_OAUTH_AZURE_CLIENT_ID = "96eecda7-19ea-49cc-abb5-240097d554f5"
PYSQL_OAUTH_REDIRECT_PORT_RANGE = list(range(8020, 8025))
PYSQL_OAUTH_AZURE_REDIRECT_PORT_RANGE = [8030]
# Base (app-neutral) redirect port used when a caller supplies their OWN
# oauth_client_id but no redirect port: the driver must NOT pin its own
# app-specific default range in that case (see AUTH-013 / PECOBLR-4039).
PYSQL_OAUTH_BASE_REDIRECT_PORT_RANGE = [8030]


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


def get_python_sql_connector_auth_provider(hostname: str, http_client, **kwargs):
    # TODO : unify all the auth mechanisms with the Python SDK

    auth_type = kwargs.get("auth_type")
    default_client_id, default_redirect_port_range = get_client_id_and_redirect_port(
        auth_type == AuthType.AZURE_OAUTH.value
    )

    if kwargs.get("username") or kwargs.get("password"):
        raise ValueError(
            "Username/password authentication is no longer supported. "
            "Please use OAuth or access token instead."
        )

    # A caller who supplies their OWN oauth_client_id owns the rest of the U2M
    # bundle: the driver forwards their scopes and redirect port verbatim and must
    # NOT substitute its own app-specific defaults (see AUTH-013 / PECOBLR-4039).
    # Only when the caller relies on the driver's default client_id do the
    # driver's app-specific default scopes/port range apply.
    caller_client_id = kwargs.get("oauth_client_id")
    oauth_redirect_port = kwargs.get("oauth_redirect_port")
    oauth_scopes = kwargs.get("oauth_scopes")

    scopes = oauth_scopes or PYSQL_OAUTH_SCOPES
    if caller_client_id:
        client_id = caller_client_id
        # Foreign client_id with no explicit port falls through to the base
        # (app-neutral) default, NOT the driver's app-specific range.
        redirect_port_range = (
            [oauth_redirect_port]
            if oauth_redirect_port
            else PYSQL_OAUTH_BASE_REDIRECT_PORT_RANGE
        )
    else:
        client_id = default_client_id
        redirect_port_range = (
            [oauth_redirect_port] if oauth_redirect_port else default_redirect_port_range
        )

    cfg = ClientContext(
        hostname=normalize_host_name(hostname),
        auth_type=auth_type,
        access_token=kwargs.get("access_token"),
        use_cert_as_auth=kwargs.get("_use_cert_as_auth"),
        tls_client_cert_file=kwargs.get("_tls_client_cert_file"),
        oauth_scopes=scopes,
        oauth_client_id=client_id,
        azure_client_id=kwargs.get("azure_client_id"),
        azure_client_secret=kwargs.get("azure_client_secret"),
        azure_tenant_id=kwargs.get("azure_tenant_id"),
        azure_workspace_resource_id=kwargs.get("azure_workspace_resource_id"),
        oauth_redirect_port_range=redirect_port_range,
        oauth_persistence=kwargs.get("experimental_oauth_persistence"),
        credentials_provider=kwargs.get("credentials_provider"),
        identity_federation_client_id=kwargs.get("identity_federation_client_id"),
    )
    return get_auth_provider(cfg, http_client)
