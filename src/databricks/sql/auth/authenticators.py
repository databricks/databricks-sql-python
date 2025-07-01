import abc
import jwt
import logging
import time
from typing import Callable, Dict, List
from databricks.sql.common.http import HttpMethod, DatabricksHttpClient, HttpHeader
from databricks.sql.auth.oauth import OAuthManager
from databricks.sql.auth.endpoint import get_oauth_endpoints
from databricks.sql.common.http import DatabricksHttpClient, OAuthResponse
from urllib.parse import urlencode

# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
from databricks.sql.experimental.oauth_persistence import OAuthToken, OAuthPersistence

logger = logging.getLogger(__name__)


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


class Token:
    """
    A class to represent a token.

    Attributes:
        access_token (str): The access token string.
        token_type (str): The type of token (e.g., "Bearer").
        refresh_token (str): The refresh token string.
    """

    def __init__(self, access_token: str, token_type: str, refresh_token: str):
        self.access_token = access_token
        self.token_type = token_type
        self.refresh_token = refresh_token

    def is_expired(self):
        try:
            decoded_token = jwt.decode(
                self.access_token, options={"verify_signature": False}
            )
            exp_time = decoded_token.get("exp")
            current_time = time.time()
            buffer_time = 30  # 30 seconds buffer
            return exp_time and (exp_time - buffer_time) <= current_time
        except Exception as e:
            logger.error("Failed to decode token: %s", e)
            return e


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
        client_id (str): The Azure service principal's client ID.
        client_secret (str): The Azure service principal's client secret.
        tenant_id (str): The Azure AD tenant ID.
    """

    AZURE_AAD_ENDPOINT = "https://login.microsoftonline.com"
    AZURE_TOKEN_ENDPOINT = "oauth2/token"

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._token: Token = None
        self._http_client = DatabricksHttpClient.get_instance()

    def auth_type(self) -> str:
        return "azure-service-principal"

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        def header_factory() -> Dict[str, str]:
            self._refresh()
            return {
                HttpHeader.AUTHORIZATION.value: f"{self._token.token_type} {self._token.access_token}",
            }

        return header_factory

    def _refresh(self) -> None:
        if self._token is None or self._token.is_expired():
            self._token = self._get_token()

    def _get_token(self) -> Token:
        request_url = (
            f"{self.AZURE_AAD_ENDPOINT}/{self.tenant_id}/{self.AZURE_TOKEN_ENDPOINT}"
        )
        headers = {
            HttpHeader.CONTENT_TYPE.value: "application/x-www-form-urlencoded",
        }
        data = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
        )

        response = self._http_client.execute(
            method=HttpMethod.POST, url=request_url, headers=headers, data=data
        )

        if response.status_code == 200:
            oauth_response = OAuthResponse(**response.json())
            return Token(
                oauth_response.access_token,
                oauth_response.token_type,
                oauth_response.refresh_token,
            )
        else:
            raise Exception(
                f"Failed to get token: {response.status_code} {response.text}"
            )
