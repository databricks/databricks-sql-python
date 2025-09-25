import logging
import json
from typing import Optional, Dict, Tuple
from urllib.parse import urlencode

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.auth.auth_utils import (
    Token,
    parse_hostname,
    decode_token,
    is_same_host,
)
from databricks.sql.common.http import HttpMethod

logger = logging.getLogger(__name__)


class TokenFederationProvider(AuthProvider):
    """
    Implementation of Token Federation for Databricks SQL Python driver.

    This provider exchanges third-party access tokens for Databricks in-house tokens
    when the token issuer is different from the Databricks host.
    """

    TOKEN_EXCHANGE_ENDPOINT = "/oidc/v1/token"
    TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
    TOKEN_EXCHANGE_SUBJECT_TYPE = "urn:ietf:params:oauth:token-type:jwt"

    def __init__(
        self,
        hostname: str,
        external_provider: AuthProvider,
        http_client,
        identity_federation_client_id: Optional[str] = None,
    ):
        """
        Initialize the Token Federation Provider.

        Args:
            hostname: The Databricks workspace hostname
            external_provider: The external authentication provider
            http_client: HTTP client for making requests (required)
            identity_federation_client_id: Optional client ID for token federation
        """
        if not http_client:
            raise ValueError("http_client is required for TokenFederationProvider")

        self.hostname = parse_hostname(hostname)
        self.external_provider = external_provider
        self.http_client = http_client
        self.identity_federation_client_id = identity_federation_client_id

        self._cached_token: Optional[Token] = None
        self._external_headers = {}

    def add_headers(self, request_headers: Dict[str, str]):
        """Add authentication headers to the request."""
        token = self._get_token()
        request_headers["Authorization"] = f"{token.token_type} {token.access_token}"

    def _get_token(self) -> Token:
        """Get or refresh the authentication token."""
        # Check if cached token is still valid
        if self._cached_token and not self._cached_token.is_expired():
            return self._cached_token

        # Get the external token
        self._external_headers = {}
        self.external_provider.add_headers(self._external_headers)

        # Extract token from Authorization header
        auth_header = self._external_headers.get("Authorization", "")
        token_type, access_token = self._extract_token_from_header(auth_header)

        # Check if token exchange is needed
        if self._should_exchange_token(access_token):
            try:
                token = self._exchange_token(access_token)
                self._cached_token = token
                return token
            except Exception as e:
                logger.warning("Token exchange failed, using external token: %s", e)

        # Use external token directly
        token = Token(access_token, token_type)
        self._cached_token = token
        return token

    def _should_exchange_token(self, access_token: str) -> bool:
        """Check if the token should be exchanged based on issuer."""
        decoded = decode_token(access_token)
        if not decoded:
            return False

        issuer = decoded.get("iss", "")
        # Check if issuer host is different from Databricks host
        return not is_same_host(issuer, self.hostname)

    def _exchange_token(self, access_token: str) -> Token:
        """Exchange the external token for a Databricks token."""
        token_url = f"{self.hostname.rstrip('/')}{self.TOKEN_EXCHANGE_ENDPOINT}"

        data = {
            "grant_type": self.TOKEN_EXCHANGE_GRANT_TYPE,
            "subject_token": access_token,
            "subject_token_type": self.TOKEN_EXCHANGE_SUBJECT_TYPE,
            "scope": "sql",
            "return_original_token_if_authenticated": "true",
        }

        if self.identity_federation_client_id:
            data["client_id"] = self.identity_federation_client_id

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
        }

        body = urlencode(data)

        response = self.http_client.request(
            HttpMethod.POST, url=token_url, body=body, headers=headers
        )

        token_response = json.loads(response.data.decode())

        return Token(
            token_response["access_token"], token_response.get("token_type", "Bearer")
        )

    def _extract_token_from_header(self, auth_header: str) -> Tuple[str, str]:
        """Extract token type and access token from Authorization header."""
        if not auth_header:
            raise ValueError("Authorization header is missing")

        parts = auth_header.split(" ", 1)
        if len(parts) != 2:
            raise ValueError("Invalid Authorization header format")

        return parts[0], parts[1]
