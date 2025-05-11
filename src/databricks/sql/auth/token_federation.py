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

        # Ensure expiry is timezone-aware
        if expiry is None:
            self.expiry = datetime.now(tz=timezone.utc)
        elif expiry.tzinfo is None:
            # Convert naive datetime to aware datetime
            self.expiry = expiry.replace(tzinfo=timezone.utc)
        else:
            self.expiry = expiry

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
    Implementation of the Credential Provider that exchanges a third party access token
    for a Databricks token. It exchanges the token only if the issued token
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
            try:
                # Get headers from the underlying provider
                self.external_provider_headers = header_factory()

                # Extract the token from the headers
                token_type, access_token = self._extract_token_info_from_header(
                    self.external_provider_headers
                )

                # Check if we need to refresh the token
                if (
                    self.last_exchanged_token
                    and self.last_external_token == access_token
                    and self.last_exchanged_token.needs_refresh()
                ):
                    # The token is approaching expiry, try to refresh
                    logger.info(
                        "Exchanged token approaching expiry, refreshing with fresh external token..."
                    )
                    return self._refresh_token(access_token, token_type)

                # Parse the JWT to get claims
                token_claims = self._parse_jwt_claims(access_token)

                # Check if token needs to be exchanged
                if self._is_same_host(token_claims.get("iss", ""), self.hostname):
                    # Token is from the same host, no need to exchange
                    logger.debug("Token from same host, no exchange needed")
                    return self.external_provider_headers
                else:
                    # Token is from a different host, need to exchange
                    logger.debug("Token from different host, attempting exchange")
                    return self._try_token_exchange_or_fallback(
                        access_token, token_type
                    )
            except Exception as e:
                logger.error(f"Error processing token: {str(e)}")
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
            idp_endpoints = get_oauth_endpoints(self.hostname, use_azure_auth)

            if idp_endpoints:
                # Get the OpenID configuration URL
                openid_config_url = idp_endpoints.get_openid_config_url(self.hostname)

                # Fetch the OpenID configuration
                response = requests.get(openid_config_url)
                if response.status_code == 200:
                    openid_config = response.json()
                    # Extract token endpoint from OpenID config
                    self.token_endpoint = openid_config.get("token_endpoint")
                    logger.info(f"Discovered token endpoint: {self.token_endpoint}")
                else:
                    logger.warning(
                        f"Failed to fetch OpenID configuration from {openid_config_url}: "
                        f"{response.status_code}"
                    )
        except Exception as e:
            logger.warning(
                f"OIDC discovery failed: {str(e)}. Using default token endpoint."
            )

        # Fallback to default token endpoint if discovery fails
        if not self.token_endpoint:
            hostname = self._format_hostname(self.hostname)
            self.token_endpoint = f"{hostname}oidc/v1/token"
            logger.info(f"Using default token endpoint: {self.token_endpoint}")

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
            return {}

    def _is_same_host(self, url1: str, url2: str) -> bool:
        """
        Check if two URLs have the same host.

        Args:
            url1: First URL
            url2: Second URL

        Returns:
            bool: True if the hosts match, False otherwise
        """
        try:
            # Parse the URLs
            parsed1 = urlparse(url1)
            parsed2 = urlparse(url2)

            # Compare the hostnames
            return parsed1.netloc.lower() == parsed2.netloc.lower()
        except Exception as e:
            logger.warning(f"Error comparing hosts: {str(e)}")
            return False

    def _refresh_token(self, access_token: str, token_type: str) -> Dict[str, str]:
        """
        Refresh the exchanged token by getting a fresh external token.

        Args:
            access_token: The external access token
            token_type: The token type (usually "Bearer")

        Returns:
            Dict[str, str]: Headers with the refreshed token
        """
        try:
            # Get a fresh token from the underlying provider
            fresh_headers = self.credentials_provider()()

            # Extract the fresh token from the headers
            fresh_token_type, fresh_access_token = self._extract_token_info_from_header(
                fresh_headers
            )

            # Exchange the fresh token for a new Databricks token
            exchanged_token = self._exchange_token(fresh_access_token)
            self.last_exchanged_token = exchanged_token
            self.last_external_token = fresh_access_token

            # Update the headers with the new token
            return {
                "Authorization": (
                    f"{exchanged_token.token_type} {exchanged_token.access_token}"
                )
            }
        except Exception as e:
            logger.error(
                f"Token refresh failed: {str(e)}, falling back to original token"
            )
            return self.external_provider_headers

    def _try_token_exchange_or_fallback(
        self, access_token: str, token_type: str
    ) -> Dict[str, str]:
        """
        Attempt to exchange the token or fall back to the original token if exchange fails.

        Args:
            access_token: The external access token
            token_type: The token type (usually "Bearer")

        Returns:
            Dict[str, str]: Headers with either the exchanged token or the original token
        """
        try:
            exchanged_token = self._exchange_token(access_token)
            self.last_exchanged_token = exchanged_token
            self.last_external_token = access_token

            return {
                "Authorization": (
                    f"{exchanged_token.token_type} {exchanged_token.access_token}"
                )
            }
        except Exception as e:
            logger.warning(
                f"Token exchange failed: {str(e)}, falling back to original token"
            )
            return self.external_provider_headers

    def _send_token_exchange_request(
        self, token_exchange_data: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Send the token exchange request to the token endpoint.

        Args:
            token_exchange_data: The data to send in the request

        Returns:
            Dict[str, Any]: The parsed JSON response

        Raises:
            Exception: If the request fails
        """
        if not self.token_endpoint:
            raise ValueError("Token endpoint not initialized")

        headers = {"Accept": "*/*", "Content-Type": "application/x-www-form-urlencoded"}

        response = requests.post(
            self.token_endpoint, data=token_exchange_data, headers=headers
        )

        if response.status_code != 200:
            raise ValueError(
                f"Token exchange failed with status code {response.status_code}: "
                f"{response.text}"
            )

        return response.json()

    def _exchange_token(self, access_token: str) -> Token:
        """
        Exchange an external token for a Databricks token.

        Args:
            access_token: The external token to exchange

        Returns:
            Token: The exchanged token with expiry information

        Raises:
            Exception: If token exchange fails
        """
        # Prepare the request data
        token_exchange_data = dict(TOKEN_EXCHANGE_PARAMS)
        token_exchange_data["subject_token"] = access_token

        # Add client_id if provided
        if self.identity_federation_client_id:
            token_exchange_data["client_id"] = self.identity_federation_client_id

        try:
            # Send the token exchange request
            resp_data = self._send_token_exchange_request(token_exchange_data)

            # Extract token information
            new_access_token = resp_data.get("access_token")
            if not new_access_token:
                raise ValueError("No access token in exchange response")

            token_type = resp_data.get("token_type", "Bearer")
            refresh_token = resp_data.get("refresh_token", "")

            # Parse expiry time from token claims if possible
            expiry = datetime.now(tz=timezone.utc)

            # First try to get expiry from the response's expires_in field
            if "expires_in" in resp_data and resp_data["expires_in"]:
                try:
                    expires_in = int(resp_data["expires_in"])
                    expiry = datetime.now(tz=timezone.utc) + timedelta(
                        seconds=expires_in
                    )
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid expires_in value: {str(e)}")

            # If that didn't work, try to parse JWT claims for expiry
            if expiry == datetime.now(tz=timezone.utc):
                token_claims = self._parse_jwt_claims(new_access_token)
                if "exp" in token_claims:
                    try:
                        exp_timestamp = int(token_claims["exp"])
                        expiry = datetime.fromtimestamp(exp_timestamp, tz=timezone.utc)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid exp claim in token: {str(e)}")

            return Token(new_access_token, token_type, refresh_token, expiry)

        except Exception as e:
            logger.error(f"Token exchange failed: {str(e)}")
            raise


class SimpleCredentialsProvider(CredentialsProvider):
    """A simple credentials provider that returns a fixed token."""

    def __init__(
        self, token: str, token_type: str = "Bearer", auth_type_value: str = "token"
    ):
        self.token = token
        self.token_type = token_type
        self.auth_type_value = auth_type_value

    def auth_type(self) -> str:
        return self.auth_type_value

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        def get_headers() -> Dict[str, str]:
            return {"Authorization": f"{self.token_type} {self.token}"}

        return get_headers
