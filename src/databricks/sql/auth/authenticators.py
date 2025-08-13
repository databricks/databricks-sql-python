import abc
import logging
from typing import Callable, Dict, List
from databricks.sql.common.http import HttpHeader
from databricks.sql.auth.oauth import (
    OAuthManager,
    RefreshableTokenSource,
    ClientCredentialsTokenSource,
)
from databricks.sql.auth.endpoint import get_oauth_endpoints
from databricks.sql.auth.common import (
    AuthType,
    get_effective_azure_login_app_id,
    get_azure_tenant_id_from_host,
)

# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
from databricks.sql.experimental.oauth_persistence import OAuthToken, OAuthPersistence


class AuthProvider:
    def add_headers(self, request_headers: Dict[str, str]):
        pass


HeaderFactory = Callable[[], Dict[str, str]]


# In order to keep compatibility with SDK
class CredentialsProvider(abc.ABC):
    """CredentialsProvider is the protocol (call-side interface)
    for authenticating requests to Databricks REST APIs"""

    @abc.abstractmethod
    def auth_type(self) -> str:
        ...

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> HeaderFactory:
        ...


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class AccessTokenAuthProvider(AuthProvider):
    def __init__(self, access_token: str):
        self.__authorization_header_value = "Bearer {}".format(access_token)

    def add_headers(self, request_headers: Dict[str, str]):
        request_headers["Authorization"] = self.__authorization_header_value


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class DatabricksOAuthProvider(AuthProvider):
    SCOPE_DELIM = " "

    def __init__(
        self,
        hostname: str,
        oauth_persistence: OAuthPersistence,
        redirect_port_range: List[int],
        client_id: str,
        scopes: List[str],
        http_client,
        auth_type: str = "databricks-oauth",
    ):
        try:
            idp_endpoint = get_oauth_endpoints(hostname, auth_type == "azure-oauth")
            if not idp_endpoint:
                raise NotImplementedError(
                    f"OAuth is not supported for host ${hostname}"
                )

            # Convert to the corresponding scopes in the corresponding IdP
            cloud_scopes = idp_endpoint.get_scopes_mapping(scopes)

            self.oauth_manager = OAuthManager(
                port_range=redirect_port_range,
                client_id=client_id,
                idp_endpoint=idp_endpoint,
                http_client=http_client,
            )
            self._hostname = hostname
            self._scopes_as_str = DatabricksOAuthProvider.SCOPE_DELIM.join(cloud_scopes)
            self._oauth_persistence = oauth_persistence
            self._client_id = client_id
            self._access_token = None
            self._refresh_token = None
            self._initial_get_token()
        except Exception as e:
            logging.error(f"unexpected error", e, exc_info=True)
            raise e

    def add_headers(self, request_headers: Dict[str, str]):
        self._update_token_if_expired()
        request_headers["Authorization"] = f"Bearer {self._access_token}"

    def _initial_get_token(self):
        try:
            if self._access_token is None or self._refresh_token is None:
                if self._oauth_persistence:
                    token = self._oauth_persistence.read(self._hostname)
                    if token:
                        self._access_token = token.access_token
                        self._refresh_token = token.refresh_token

            if self._access_token and self._refresh_token:
                self._update_token_if_expired()
            else:
                (access_token, refresh_token) = self.oauth_manager.get_tokens(
                    hostname=self._hostname, scope=self._scopes_as_str
                )
                self._access_token = access_token
                self._refresh_token = refresh_token

                if self._oauth_persistence:
                    self._oauth_persistence.persist(
                        self._hostname, OAuthToken(access_token, refresh_token)
                    )
        except Exception as e:
            logging.error(f"unexpected error in oauth initialization", e, exc_info=True)
            raise e

    def _update_token_if_expired(self):
        try:
            (
                fresh_access_token,
                fresh_refresh_token,
                is_refreshed,
            ) = self.oauth_manager.check_and_refresh_access_token(
                hostname=self._hostname,
                access_token=self._access_token,
                refresh_token=self._refresh_token,
            )
            if not is_refreshed:
                return
            else:
                self._access_token = fresh_access_token
                self._refresh_token = fresh_refresh_token

                if self._oauth_persistence:
                    token = OAuthToken(self._access_token, self._refresh_token)
                    self._oauth_persistence.persist(self._hostname, token)
        except Exception as e:
            logging.error(f"unexpected error in oauth token update", e, exc_info=True)
            raise e


class ExternalAuthProvider(AuthProvider):
    def __init__(self, credentials_provider: CredentialsProvider) -> None:
        self._header_factory = credentials_provider()

    def add_headers(self, request_headers: Dict[str, str]):
        headers = self._header_factory()
        for k, v in headers.items():
            request_headers[k] = v


class AzureServicePrincipalCredentialProvider(CredentialsProvider):
    """
    A credential provider for Azure Service Principal authentication with Databricks.

    This class implements the CredentialsProvider protocol to authenticate requests
    to Databricks REST APIs using Azure Active Directory (AAD) service principal
    credentials. It handles OAuth 2.0 client credentials flow to obtain access tokens
    from Azure AD and automatically refreshes them when they expire.

    Attributes:
        hostname (str): The Databricks workspace hostname.
        azure_client_id (str): The Azure service principal's client ID.
        azure_client_secret (str): The Azure service principal's client secret.
        azure_tenant_id (str): The Azure AD tenant ID.
        azure_workspace_resource_id (str, optional): The Azure workspace resource ID.
    """

    AZURE_AAD_ENDPOINT = "https://login.microsoftonline.com"
    AZURE_TOKEN_ENDPOINT = "oauth2/token"

    AZURE_MANAGED_RESOURCE = "https://management.core.windows.net/"

    DATABRICKS_AZURE_SP_TOKEN_HEADER = "X-Databricks-Azure-SP-Management-Token"
    DATABRICKS_AZURE_WORKSPACE_RESOURCE_ID_HEADER = (
        "X-Databricks-Azure-Workspace-Resource-Id"
    )

    def __init__(
        self,
        hostname,
        azure_client_id,
        azure_client_secret,
        http_client,
        azure_tenant_id=None,
        azure_workspace_resource_id=None,
    ):
        self.hostname = hostname
        self.azure_client_id = azure_client_id
        self.azure_client_secret = azure_client_secret
        self.azure_workspace_resource_id = azure_workspace_resource_id
        self.azure_tenant_id = azure_tenant_id or get_azure_tenant_id_from_host(
            hostname, http_client
        )
        self._http_client = http_client

    def auth_type(self) -> str:
        return AuthType.AZURE_SP_M2M.value

    def get_token_source(self, resource: str) -> RefreshableTokenSource:
        return ClientCredentialsTokenSource(
            token_url=f"{self.AZURE_AAD_ENDPOINT}/{self.azure_tenant_id}/{self.AZURE_TOKEN_ENDPOINT}",
            client_id=self.azure_client_id,
            client_secret=self.azure_client_secret,
            http_client=self._http_client,
            extra_params={"resource": resource},
        )

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        inner = self.get_token_source(
            resource=get_effective_azure_login_app_id(self.hostname)
        )
        cloud = self.get_token_source(resource=self.AZURE_MANAGED_RESOURCE)

        def header_factory() -> Dict[str, str]:
            inner_token = inner.get_token()
            cloud_token = cloud.get_token()

            headers = {
                HttpHeader.AUTHORIZATION.value: f"{inner_token.token_type} {inner_token.access_token}",
                self.DATABRICKS_AZURE_SP_TOKEN_HEADER: cloud_token.access_token,
            }

            if self.azure_workspace_resource_id:
                headers[
                    self.DATABRICKS_AZURE_WORKSPACE_RESOURCE_ID_HEADER
                ] = self.azure_workspace_resource_id

            return headers

        return header_factory
