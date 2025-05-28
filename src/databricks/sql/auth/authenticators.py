import abc
import base64
import logging
import time
from typing import Callable, Dict, List, Optional
import requests

from databricks.sql.auth.oauth import OAuthManager
from databricks.sql.auth.endpoint import get_oauth_endpoints, infer_cloud_from_host

# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
from databricks.sql.experimental.oauth_persistence import OAuthToken, OAuthPersistence
from databricks.sql.auth.endpoint import AzureOAuthEndpointCollection, InHouseOAuthEndpointCollection

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
        """
        Returns the authentication type for this provider
        """
        ...

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> HeaderFactory:
        """
        Configure and return a HeaderFactory that provides authentication headers
        """
        ...


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class AccessTokenAuthProvider(AuthProvider, CredentialsProvider):
    def __init__(self, access_token: str):
        self.__authorization_header_value = "Bearer {}".format(access_token)

    def add_headers(self, request_headers: Dict[str, str]):
        request_headers["Authorization"] = self.__authorization_header_value

    def auth_type(self) -> str:
        return "access-token"

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        def get_headers():
            return {"Authorization": self.__authorization_header_value}
        return get_headers

# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class DatabricksOAuthProvider(AuthProvider, CredentialsProvider):
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
        self._hostname = hostname
        self._oauth_persistence = oauth_persistence
        self._client_id = client_id
        self._auth_type = auth_type
        self._access_token = None
        self._refresh_token = None

        idp_endpoint = get_oauth_endpoints(hostname, auth_type == "azure-oauth")
        if not idp_endpoint:
            raise NotImplementedError(
                    f"OAuth is not supported for host ${hostname}"
                )

        
        cloud_scopes = idp_endpoint.get_scopes_mapping(scopes)
        self._scopes_as_str = self.SCOPE_DELIM.join(cloud_scopes)

        self.oauth_manager = OAuthManager(
            idp_endpoint=idp_endpoint,
            client_id=client_id,
            port_range=redirect_port_range,
        )
        self._initial_get_token()

    def add_headers(self, request_headers: Dict[str, str]):
        self._update_token_if_expired()
        request_headers["Authorization"] = "Bearer {}".format(self._access_token)

    def auth_type(self) -> str:
        return self._auth_type

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        def get_headers():
            self._update_token_if_expired()
            return {"Authorization": "Bearer {}".format(self._access_token)}
        return get_headers

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


class ClientCredentialsProvider(CredentialsProvider, AuthProvider):
    """Provider for OAuth client credentials flow (machine-to-machine authentication)."""

    AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_endpoint: str,
        auth_type_value: str = "client-credentials"
    ):
        """
        Initialize a ClientCredentialsProvider.
        
        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret  
            token_endpoint: OAuth token endpoint URL
            auth_type_value: Auth type identifier
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint = token_endpoint
        self.auth_type_value = auth_type_value
        
        self._cached_token = None
        self._token_expires_at = None
        

    def auth_type(self) -> str:
        return self.auth_type_value

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        def get_headers() -> Dict[str, str]:
            token = self._get_access_token()
            return {"Authorization": "Bearer {}".format(token)}
        return get_headers
    
    def add_headers(self, request_headers: Dict[str, str]):
        token = self._get_access_token()
        request_headers["Authorization"] = "Bearer {}".format(token)

    def _get_access_token(self) -> str:
        """Get a valid access token using client credentials flow, with caching."""
        # Check if we have a valid cached token (with 40 second buffer since azure doesn't respect a token with less than 30s expiry)
        if (self._cached_token and self._token_expires_at and 
            time.time() < self._token_expires_at - 40):
            return self._cached_token
        
        # Get new token using client credentials flow
        token_data = self._request_token()
        
        self._cached_token = token_data['access_token']
        # expires_in is in seconds, convert to absolute time
        self._token_expires_at = time.time() + token_data.get('expires_in', 3600)
        
        return self._cached_token

    def _request_token(self) -> dict:
        """Request a new token using OAuth client credentials flow."""
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.AZURE_DATABRICKS_SCOPE,
        }
        
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            response = requests.post(self.token_endpoint, data=data, headers=headers)
            response.raise_for_status()
            
            token_data = response.json()
            
            if 'access_token' not in token_data:
                raise ValueError("No access_token in response: {}".format(token_data))
                
            return token_data
            
        except requests.exceptions.RequestException as e:
            raise RuntimeError("Token request failed: {}".format(e)) from e
        except ValueError as e:
            raise RuntimeError("Invalid token response: {}".format(e)) from e


class ExternalAuthProvider(AuthProvider, CredentialsProvider):
    def __init__(self, credentials_provider: CredentialsProvider) -> None:
        self._credentials_provider = credentials_provider
        self._header_factory = credentials_provider()

    def add_headers(self, request_headers: Dict[str, str]):
        headers = self._header_factory()
        for k, v in headers.items():
            request_headers[k] = v

    def auth_type(self) -> str:
        return "external-auth"

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        return self._header_factory
