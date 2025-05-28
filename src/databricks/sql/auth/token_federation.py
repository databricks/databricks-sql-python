import base64
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, Tuple
from urllib.parse import urlparse

import requests
import jwt
from requests.exceptions import RequestException

from databricks.sql.auth.authenticators import CredentialsProvider, HeaderFactory
from databricks.sql.auth.oidc_utils import OIDCDiscoveryUtil, is_same_host
from databricks.sql.auth.token import Token

logger = logging.getLogger(__name__)


class DatabricksTokenFederationProvider(CredentialsProvider):
    """
    Token federation provider that exchanges external tokens for Databricks tokens.

    This implementation follows the JDBC pattern:
    1. Try token exchange without HTTP Basic authentication (per RFC 8693)
    2. Fall back to using external token directly if exchange fails
    3. Compare token issuer with Databricks host to determine if exchange is needed
    """

    # HTTP request configuration (no authentication)
    EXCHANGE_HEADERS = {
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Token exchange parameters following RFC 8693
    TOKEN_EXCHANGE_PARAMS = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "scope": "sql",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "return_original_token_if_authenticated": "true",
    }

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
        self.token_endpoint: Optional[str] = None

        # Store the current token information
        self.current_token: Optional[Token] = None
        self.external_headers: Optional[Dict[str, str]] = None

    def auth_type(self) -> str:
        """Return the auth type from the underlying credentials provider."""
        return self.credentials_provider.auth_type()

    @property
    def host(self) -> str:
        """
        Alias for hostname to maintain compatibility with code expecting a host attribute.
        """
        return self.hostname

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        """
        Configure and return a HeaderFactory that provides authentication headers.
        This is called by the ExternalAuthProvider to get headers for authentication.
        """
        # Return a function that will get authentication headers
        return self.get_auth_headers

    def _extract_token_info_from_header(
        self, headers: Dict[str, str]
    ) -> Tuple[str, str]:
        """
        Extract token type and token value from authorization header.

        Args:
            headers: Headers dictionary

        Returns:
            Tuple[str, str]: Token type and token value

        Raises:
            ValueError: If no authorization header is found or it has invalid format
        """
        auth_header = headers.get("Authorization")
        if not auth_header:
            raise ValueError("No Authorization header found")

        parts = auth_header.split(" ", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid Authorization header format: {auth_header}")

        return parts[0], parts[1]

    def _parse_jwt_claims(self, token: str) -> Dict[str, Any]:
        """
        Parse JWT token claims without validation.

        Args:
            token: JWT token string

        Returns:
            Dict[str, Any]: Parsed JWT claims
        """
        try:
            return jwt.decode(
                token, options={"verify_signature": False, "verify_aud": False}
            )
        except Exception as e:
            logger.debug("Failed to parse JWT: %s", str(e))
            return {}

    def _get_expiry_from_jwt(self, token: str) -> Optional[datetime]:
        """
        Extract expiry datetime from JWT token.

        Args:
            token: JWT token string

        Returns:
            Optional[datetime]: Expiry datetime if found in token, None otherwise
        """
        claims = self._parse_jwt_claims(token)

        # Look for standard JWT expiry claim ("exp")
        if "exp" in claims:
            try:
                expiry_timestamp = int(claims["exp"])
                return datetime.fromtimestamp(expiry_timestamp, tz=timezone.utc)
            except (ValueError, TypeError) as e:
                logger.warning("Invalid JWT expiry value: %s", e)

    def refresh_token(self) -> Token:
        """
        Refresh the token and return the new Token object.

        This method gets a fresh token from the credentials provider,
        exchanges it if necessary, and returns the new Token object.

        Returns:
            Token: The new refreshed token

        Raises:
            ValueError: If token refresh fails
        """
        # Get fresh headers from the credentials provider
        header_factory = self.credentials_provider()
        self.external_headers = header_factory()

        # Extract the new token info
        token_type, access_token = self._extract_token_info_from_header(
            self.external_headers
        )

        # Check if we need to exchange the token
        token_claims = self._parse_jwt_claims(access_token)

        # Create new token based on whether it's from the same host or not
        if is_same_host(token_claims.get("iss", ""), self.hostname):
            # Token is from the same host, no need to exchange
            logger.debug("Token from same host, creating token without exchange")
            expiry = self._get_expiry_from_jwt(access_token)
            new_token = Token(access_token, token_type, "", expiry)
            self.current_token = new_token
            return new_token
        else:
            logger.debug("Token from different host, attempting token exchange")
            try:
                new_token = self._exchange_token(access_token)
                self.current_token = new_token
                return new_token
            except Exception as e:
                logger.debug(
                    "Token exchange failed: %s. Using external token as fallback.", e
                )
                expiry = self._get_expiry_from_jwt(access_token)
                fallback_token = Token(access_token, token_type, "", expiry)
                self.current_token = fallback_token
                return fallback_token

    def get_current_token(self) -> Token:
        """
        Get the current token, refreshing if necessary.

        This method checks if the current token is valid and not expired.
        If it is valid, it returns the current token.
        If it is expired or doesn't exist, it refreshes the token.

        Returns:
            Token: The current valid token

        Raises:
            ValueError: If unable to get a valid token
        """
        # Return current token if it exists and is valid
        if self.current_token is not None and self.current_token.is_valid():
            return self.current_token

        # Token doesn't exist or is expired, get a fresh one
        return self.refresh_token()

    def get_auth_headers(self) -> Dict[str, str]:
        """
        Get authorization headers using the current token.

        Returns:
            Dict[str, str]: Authorization headers (may include extra headers from provider)
        """
        try:
            token = self.get_current_token()
            # Always get the latest headers from the credentials provider
            header_factory = self.credentials_provider()
            headers = dict(header_factory()) if header_factory else {}
            headers["Authorization"] = "{} {}".format(
                token.token_type, token.access_token
            )
            return headers
        except Exception as e:
            return dict(self.external_headers) if self.external_headers else {}

    def _send_token_exchange_request(
        self, token_exchange_data: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Send the token exchange request to the token endpoint.

        For M2M flows, this should include HTTP Basic authentication using client credentials.
        For U2M flows, token exchange is validated purely based on the JWT token and federation policies.

        Args:
            token_exchange_data: Token exchange request data

        Returns:
            Dict[str, Any]: Token exchange response

        Raises:
            requests.HTTPError: If token exchange fails
        """
        if not self.token_endpoint:
            raise ValueError("Token endpoint not initialized")

        auth = None
        if hasattr(self.credentials_provider, "client_id") and hasattr(
            self.credentials_provider, "client_secret"
        ):
            client_id = self.credentials_provider.client_id
            client_secret = self.credentials_provider.client_secret
            auth = (client_id, client_secret)
        else:
            logger.debug(
                "No client credentials available, sending request without authentication"
            )

        response = requests.post(
            self.token_endpoint,
            data=token_exchange_data,
            headers=self.EXCHANGE_HEADERS,
            auth=auth,
        )

        if response.status_code != 200:
            raise requests.HTTPError(
                "Token exchange failed with status code {}: {}".format(
                    response.status_code, response.text
                ),
                response=response,
            )

        return response.json()

    def _exchange_token(self, access_token: str) -> Token:
        """
        Exchange an external token for a Databricks token.

        Args:
            access_token: External token to exchange

        Returns:
            Token: Exchanged token

        Raises:
            ValueError: If token exchange fails
        """
        if self.token_endpoint is None:
            self.token_endpoint = OIDCDiscoveryUtil.discover_token_endpoint(
                self.hostname
            )

        # Prepare the request data according to RFC 8693
        token_exchange_data = dict(self.TOKEN_EXCHANGE_PARAMS)
        token_exchange_data["subject_token"] = access_token

        # Add client_id if provided for federation policy identification
        if self.identity_federation_client_id:
            token_exchange_data["client_id"] = self.identity_federation_client_id

        resp_data = self._send_token_exchange_request(token_exchange_data)

        # Extract token information
        new_access_token = resp_data.get("access_token")
        if not new_access_token:
            raise ValueError("No access token in exchange response")

        token_type = resp_data.get("token_type", "Bearer")
        refresh_token = resp_data.get("refresh_token", "")

        # Extract expiry from JWT claims
        expiry = self._get_expiry_from_jwt(new_access_token)

        return Token(new_access_token, token_type, refresh_token, expiry)

    def add_headers(self, request_headers: Dict[str, str]):
        """
        Add authentication headers to the request.
        """
        headers = self.get_auth_headers()
        for k, v in headers.items():
            request_headers[k] = v
