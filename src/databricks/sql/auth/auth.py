from enum import Enum
from typing import List

from databricks.sql.auth.authenticators import (
    AuthProvider,
    AccessTokenAuthProvider,
    BasicAuthProvider,
    DatabricksOAuthProvider,
)
from databricks.sql.experimental.oauth_persistence import OAuthPersistence


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    # other supported types (access_token, user/pass) can be inferred
    # we can add more types as needed later


class ClientContext:
    def __init__(
        self,
        hostname: str,
        username: str = None,
        password: str = None,
        access_token: str = None,
        auth_type: str = None,
        oauth_scopes: List[str] = None,
        oauth_client_id: str = None,
        oauth_redirect_port_range: List[int] = None,
        use_cert_as_auth: str = None,
        tls_client_cert_file: str = None,
        oauth_persistence=None,
    ):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.access_token = access_token
        self.auth_type = auth_type
        self.oauth_scopes = oauth_scopes
        self.oauth_client_id = oauth_client_id
        self.oauth_redirect_port_range = oauth_redirect_port_range
        self.use_cert_as_auth = use_cert_as_auth
        self.tls_client_cert_file = tls_client_cert_file
        self.oauth_persistence = oauth_persistence


def get_auth_provider(cfg: ClientContext):
    if cfg.auth_type == AuthType.DATABRICKS_OAUTH.value:
        assert cfg.oauth_redirect_port_range is not None
        assert cfg.oauth_client_id is not None
        assert cfg.oauth_scopes is not None

        return DatabricksOAuthProvider(
            cfg.hostname,
            cfg.oauth_persistence,
            cfg.oauth_redirect_port_range,
            cfg.oauth_client_id,
            cfg.oauth_scopes,
        )
    elif cfg.access_token is not None:
        return AccessTokenAuthProvider(cfg.access_token)
    elif cfg.username is not None and cfg.password is not None:
        return BasicAuthProvider(cfg.username, cfg.password)
    elif cfg.use_cert_as_auth and cfg.tls_client_cert_file:
        # no op authenticator. authentication is performed using ssl certificate outside of headers
        return AuthProvider()
    else:
        raise RuntimeError("No valid authentication settings!")


PYSQL_OAUTH_SCOPES = ["sql", "offline_access"]
PYSQL_OAUTH_CLIENT_ID = "databricks-sql-python"
PYSQL_OAUTH_REDIRECT_PORT_RANGE = list(range(8020, 8025))


def normalize_host_name(hostname: str):
    maybe_scheme = "https://" if not hostname.startswith("https://") else ""
    maybe_trailing_slash = "/" if not hostname.endswith("/") else ""
    return f"{maybe_scheme}{hostname}{maybe_trailing_slash}"


def get_python_sql_connector_auth_provider(hostname: str, **kwargs):
    cfg = ClientContext(
        hostname=normalize_host_name(hostname),
        auth_type=kwargs.get("auth_type"),
        access_token=kwargs.get("access_token"),
        username=kwargs.get("_username"),
        password=kwargs.get("_password"),
        use_cert_as_auth=kwargs.get("_use_cert_as_auth"),
        tls_client_cert_file=kwargs.get("_tls_client_cert_file"),
        oauth_scopes=PYSQL_OAUTH_SCOPES,
        oauth_client_id=PYSQL_OAUTH_CLIENT_ID,
        oauth_redirect_port_range=PYSQL_OAUTH_REDIRECT_PORT_RANGE,
        oauth_persistence=kwargs.get("experimental_oauth_persistence"),
    )
    return get_auth_provider(cfg)
