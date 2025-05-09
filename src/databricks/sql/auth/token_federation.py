import base64
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, Tuple
from urllib.parse import urlparse

import requests
from requests.exceptions import RequestException

from databricks.sql.auth.authenticators import CredentialsProvider, HeaderFactory
from databricks.sql.auth.endpoint import (
    get_oauth_endpoints,
    infer_cloud_from_host,
)

logger = logging.getLogger(__name__)

TOKEN_EXCHANGE_PARAMS = {
    "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
    "scope": "sql",
    "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
    "return_original_token_if_authenticated": "true",
}

TOKEN_REFRESH_BUFFER_SECONDS = 10


class Token:
    """Represents an OAuth token with expiry information."""

    def __init__(
        self,
        access_token: str,
        token_type: str,
        refresh_token: str = "",
        expiry: Optional[datetime] = None,
    ):
        self.access_token = access_token
        self.token_type = token_type
        self.refresh_token = refresh_token
        self.expiry = expiry or datetime.now(tz=timezone.utc)

    def is_expired(self) -> bool:
        """Check if the token is expired."""
        return datetime.now(tz=timezone.utc) >= self.expiry

    def needs_refresh(self) -> bool:
        """Check if the token needs to be refreshed soon."""
        buffer_time = timedelta(seconds=TOKEN_REFRESH_BUFFER_SECONDS)
        return datetime.now(tz=timezone.utc) >= (self.expiry - buffer_time)

    def __str__(self) -> str:
        return f"{self.token_type} {self.access_token}"


class DatabricksTokenFederationProvider(CredentialsProvider):
    """
    Implementation of the Credential Provider that exchanges the third party access token
    for a Databricks InHouse Token. This class exchanges the access token if the issued token
    is not from the same host as the Databricks host.
    """

    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        hostname: str,
        identity_federation_client_id: Optional[str] = None,
    ):
        """
        Initialize the token federation provider.

        Args:
            credentials_provider: The underlying credentials provider
            hostname: The Databricks hostname
            identity_federation_client_id: Optional client ID for identity federation
        """
        self.credentials_provider = credentials_provider
        self.hostname = hostname
        self.identity_federation_client_id = identity_federation_client_id
        self.external_provider_headers: Dict[str, str] = {}
        self.token_endpoint: Optional[str] = None
        self.idp_endpoints = None
        self.openid_config = None
        self.last_exchanged_token: Optional[Token] = None
        self.last_external_token: Optional[str] = None

    def auth_type(self) -> str:
        """Return the auth type from the underlying credentials provider."""
        return self.credentials_provider.auth_type()

    @property
    def host(self) -> str:
        """
        Alias for hostname to maintain compatibility with code expecting a host attribute.

        Returns:
            str: The hostname value
        """
        return self.hostname

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        """
        Configure and return a HeaderFactory that provides authentication headers.

        This is called by the ExternalAuthProvider to get headers for authentication.
        """
        # First call the underlying credentials provider to get its headers
        header_factory = self.credentials_provider(*args, **kwargs)

        # Initialize OIDC discovery
        self._init_oidc_discovery()

        def get_headers() -> Dict[str, str]:
            # Get headers from the underlying provider
            self.external_provider_headers = header_factory()

            # Extract the token from the headers
            token_info = self._extract_token_info_from_header(
                self.external_provider_headers
            )
            token_type, access_token = token_info

            try:
                # Check if we need to refresh the token
                if (
                    self.last_exchanged_token
                    and self.last_external_token == access_token
                    and self.last_exchanged_token.needs_refresh()
                ):
                    # The token is approaching expiry, try to refresh
                    logger.debug("Exchanged token approaching expiry, refreshing...")
                    return self._refresh_token(access_token, token_type)

                # Parse the JWT to get claims
                token_claims = self._parse_jwt_claims(access_token)

                # Check if token needs to be exchanged
                if self._is_same_host(token_claims.get("iss", ""), self.hostname):
                    # Token is from the same host, no need to exchange
                    return self.external_provider_headers
                else:
                    # Token is from a different host, need to exchange
                    return self._try_token_exchange_or_fallback(
                        access_token, token_type
                    )
            except Exception as e:
                logger.error(f"Failed to process token: {str(e)}")
                # Fall back to original headers in case of error
                return self.external_provider_headers

        return get_headers

    def _init_oidc_discovery(self):
        """Initialize OIDC discovery to find token endpoint."""
        if self.token_endpoint is not None:
            return

        try:
            # Use the existing OIDC discovery mechanism
            use_azure_auth = infer_cloud_from_host(self.hostname) == "azure"
            self.idp_endpoints = get_oauth_endpoints(self.hostname, use_azure_auth)

            if self.idp_endpoints:
                # Get the OpenID configuration URL
                openid_config_url = self.idp_endpoints.get_openid_config_url(
                    self.hostname
                )

                # Fetch the OpenID configuration
                response = requests.get(openid_config_url)
                if response.status_code == 200:
                    self.openid_config = response.json()
                    # Extract token endpoint from OpenID config
                    self.token_endpoint = self.openid_config.get("token_endpoint")
                    logger.info(f"Discovered token endpoint: {self.token_endpoint}")
                else:
                    logger.warning(
                        f"Failed to fetch OpenID configuration from {openid_config_url}: {response.status_code}"
                    )

            # Fallback to default token endpoint if discovery fails
            if not self.token_endpoint:
                hostname = self._format_hostname(self.hostname)
                self.token_endpoint = f"{hostname}oidc/v1/token"
                logger.info(f"Using default token endpoint: {self.token_endpoint}")
        except Exception as e:
            logger.warning(
                f"OIDC discovery failed: {str(e)}. Using default token endpoint."
            )
            hostname = self._format_hostname(self.hostname)
            self.token_endpoint = f"{hostname}oidc/v1/token"
            logger.info(
                f"Using default token endpoint after error: {self.token_endpoint}"
            )

    def _format_hostname(self, hostname: str) -> str:
        """Format hostname to ensure it has proper https:// prefix and trailing slash."""
        if not hostname.startswith("https://"):
            hostname = f"https://{hostname}"
        if not hostname.endswith("/"):
            hostname = f"{hostname}/"
        return hostname

    def _extract_token_info_from_header(
        self, headers: Dict[str, str]
    ) -> Tuple[str, str]:
        """Extract token type and token value from authorization header."""
        auth_header = headers.get("Authorization")
        if not auth_header:
            raise ValueError("No Authorization header found")

        parts = auth_header.split(" ", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid Authorization header format: {auth_header}")

        return parts[0], parts[1]

    def _parse_jwt_claims(self, token: str) -> Dict[str, Any]:
        """Parse JWT token claims without validation."""
        try:
            # Split the token
            parts = token.split(".")
            if len(parts) != 3:
                raise ValueError("Invalid JWT format")

            # Get the payload part (second part)
            payload = parts[1]

            # Add padding if needed
            padding = "=" * (4 - len(payload) % 4)
            payload += padding

            # Decode and parse JSON
            decoded = base64.b64decode(payload)
            return json.loads(decoded)
        except Exception as e:
            logger.error(f"Failed to parse JWT: {str(e)}")
            raise

    def _detect_idp_from_claims(self, token_claims: Dict[str, Any]) -> str:
        """
        Detect the identity provider type from token claims.

        This can be used to adjust token exchange parameters based on the IdP.
        """
        issuer = token_claims.get("iss", "")

        if "login.microsoftonline.com" in issuer or "sts.windows.net" in issuer:
            return "azure"
        elif "token.actions.githubusercontent.com" in issuer:
            return "github"
        elif "accounts.google.com" in issuer:
            return "google"
        elif "cognito-idp" in issuer and "amazonaws.com" in issuer:
            return "aws"
        else:
            return "unknown"

    def _is_same_host(self, url1: str, url2: str) -> bool:
        """Check if two URLs have the same host."""
        try:
            host1 = urlparse(url1).netloc
            host2 = urlparse(url2).netloc
            # If host1 is empty, it's not a valid URL, so we return False
            if not host1:
                return False
            return host1 == host2
        except Exception as e:
            logger.error(f"Failed to parse URLs: {str(e)}")
            return False

    def _refresh_token(self, access_token: str, token_type: str) -> Dict[str, str]:
        """
        Attempt to refresh an expired token by first getting a fresh external token
        and then exchanging it for a new Databricks token.

        Args:
            access_token: The original external access token (will be replaced)
            token_type: The token type (Bearer, etc.)

        Returns:
            The headers with the fresh token
        """
        try:
            logger.info("Refreshing expired token by getting a new external token")

            # Get a fresh token from the underlying credentials provider
            # instead of reusing the same access_token
            fresh_headers = self.credentials_provider()()

            # Extract the fresh token from the headers
            auth_header = fresh_headers.get("Authorization", "")
            if not auth_header:
                logger.error("No Authorization header in fresh headers")
                return self.external_provider_headers

            parts = auth_header.split(" ", 1)
            if len(parts) != 2:
                logger.error(f"Invalid Authorization header format: {auth_header}")
                return self.external_provider_headers

            fresh_token_type = parts[0]
            fresh_access_token = parts[1]

            logger.debug("Got fresh external token")

            # Now process the fresh token
            token_claims = self._parse_jwt_claims(fresh_access_token)
            idp_type = self._detect_idp_from_claims(token_claims)

            # Perform a new token exchange with the fresh token
            refreshed_token = self._exchange_token(fresh_access_token, idp_type)

            # Update the stored token
            self.last_exchanged_token = refreshed_token
            self.last_external_token = fresh_access_token

            # Create new headers with the refreshed token
            headers = dict(fresh_headers)  # Use the fresh headers as base
            headers[
                "Authorization"
            ] = f"{refreshed_token.token_type} {refreshed_token.access_token}"
            return headers
        except Exception as e:
            logger.error(
                f"Token refresh failed, falling back to original token: {str(e)}"
            )
            # If refresh fails, fall back to the original headers
            return self.external_provider_headers

    def _try_token_exchange_or_fallback(
        self, access_token: str, token_type: str
    ) -> Dict[str, str]:
        """Try to exchange the token or fall back to the original token."""
        try:
            # Parse the token to get claims for IdP-specific adjustments
            token_claims = self._parse_jwt_claims(access_token)
            idp_type = self._detect_idp_from_claims(token_claims)

            # Exchange the token
            exchanged_token = self._exchange_token(access_token, idp_type)

            # Store the exchanged token for potential refresh later
            self.last_exchanged_token = exchanged_token
            self.last_external_token = access_token

            # Create new headers with the exchanged token
            headers = dict(self.external_provider_headers)
            headers[
                "Authorization"
            ] = f"{exchanged_token.token_type} {exchanged_token.access_token}"
            return headers
        except Exception as e:
            logger.error(
                f"Token exchange failed, falling back to using external token: {str(e)}"
            )
            # Fall back to original headers
            return self.external_provider_headers

    def _exchange_token(self, access_token: str, idp_type: str = "unknown") -> Token:
        """
        Exchange an external token for a Databricks token.

        Args:
            access_token: The external token to exchange
            idp_type: The detected identity provider type (azure, github, etc.)

        Returns:
            A Token object containing the exchanged token
        """
        if not self.token_endpoint:
            self._init_oidc_discovery()

        # Ensure token_endpoint is set
        if not self.token_endpoint:
            raise ValueError("Token endpoint could not be determined")

        # Create request parameters
        params = dict(TOKEN_EXCHANGE_PARAMS)
        params["subject_token"] = access_token

        # Add client ID if available
        if self.identity_federation_client_id:
            params["client_id"] = self.identity_federation_client_id

        # Make IdP-specific adjustments
        if idp_type == "azure":
            # For Azure AD, add special handling if needed
            pass
        elif idp_type == "github":
            # For GitHub Actions, add special handling if needed
            pass

        # Set up headers
        headers = {"Accept": "*/*", "Content-Type": "application/x-www-form-urlencoded"}

        try:
            # Make the token exchange request
            response = requests.post(self.token_endpoint, data=params, headers=headers)
            response.raise_for_status()

            # Parse the response
            resp_data = response.json()

            # Create a token from the response
            token = Token(
                access_token=resp_data.get("access_token"),
                token_type=resp_data.get("token_type", "Bearer"),
                refresh_token=resp_data.get("refresh_token", ""),
            )

            # Set expiry time from the response's expires_in field if available
            # This is the standard OAuth approach
            if "expires_in" in resp_data and resp_data["expires_in"]:
                try:
                    # Calculate expiry by adding expires_in seconds to current time
                    expires_in_seconds = int(resp_data["expires_in"])
                    token.expiry = datetime.now(tz=timezone.utc) + timedelta(
                        seconds=expires_in_seconds
                    )
                    logger.debug(f"Token expiry set from expires_in: {token.expiry}")
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Could not parse expires_in from response: {str(e)}"
                    )

            # If expires_in wasn't available, try to parse expiry from the token JWT
            if token.expiry == datetime.now(tz=timezone.utc):
                try:
                    token_claims = self._parse_jwt_claims(token.access_token)
                    exp_time = token_claims.get("exp")
                    if exp_time:
                        token.expiry = datetime.fromtimestamp(exp_time, tz=timezone.utc)
                        logger.debug(
                            f"Token expiry set from JWT exp claim: {token.expiry}"
                        )
                except Exception as e:
                    logger.warning(f"Could not parse expiry from token: {str(e)}")

            return token
        except RequestException as e:
            logger.error(f"Failed to perform token exchange: {str(e)}")
            raise


class SimpleCredentialsProvider(CredentialsProvider):
    """A simple credentials provider that returns fixed headers."""

    def __init__(
        self, token: str, token_type: str = "Bearer", auth_type_value: str = "token"
    ):
        self.token = token
        self.token_type = token_type
        self._auth_type = auth_type_value

    def auth_type(self) -> str:
        return self._auth_type

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        def get_headers() -> Dict[str, str]:
            return {"Authorization": f"{self.token_type} {self.token}"}

        return get_headers


def create_token_federation_provider(
    token: str,
    hostname: str,
    identity_federation_client_id: Optional[str] = None,
    token_type: str = "Bearer",
) -> DatabricksTokenFederationProvider:
    """
    Create a token federation provider using a simple token.

    Args:
        token: The token to use
        hostname: The Databricks hostname
        identity_federation_client_id: Optional client ID for identity federation
        token_type: The token type (default: "Bearer")

    Returns:
        A DatabricksTokenFederationProvider
    """
    provider = SimpleCredentialsProvider(token, token_type)
    return DatabricksTokenFederationProvider(
        provider, hostname, identity_federation_client_id
    )
