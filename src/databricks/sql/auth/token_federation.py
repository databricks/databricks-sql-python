import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from urllib.parse import urlparse, urlencode
import jwt
import requests

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.auth.common import AuthType
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
        http_client=None,
        identity_federation_client_id: Optional[str] = None,
    ):
        """
        Initialize the Token Federation Provider.
        
        Args:
            hostname: The Databricks workspace hostname
            external_provider: The external authentication provider
            http_client: HTTP client for making requests
            identity_federation_client_id: Optional client ID for token federation
        """
        self.hostname = self._normalize_hostname(hostname)
        self.external_provider = external_provider
        self.http_client = http_client or requests.Session()
        self.identity_federation_client_id = identity_federation_client_id
        
        self._cached_token = None
        self._cached_token_expiry = None
        self._external_headers = {}
        
    def add_headers(self, request_headers: Dict[str, str]):
        """Add authentication headers to the request."""
        token_info = self._get_token()
        request_headers["Authorization"] = f"{token_info['token_type']} {token_info['access_token']}"
        
    def _get_token(self) -> Dict[str, str]:
        """Get or refresh the authentication token."""
        # Check if cached token is still valid
        if self._is_token_valid():
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
                exchanged_token = self._exchange_token(access_token)
                self._cache_token(exchanged_token)
                return exchanged_token
            except Exception as e:
                logger.warning(f"Token exchange failed, using external token: {e}")
                # Fall back to using the external token
                
        # Use external token directly
        token_info = {
            "access_token": access_token,
            "token_type": token_type,
        }
        self._cache_token(token_info)
        return token_info
        
    def _should_exchange_token(self, access_token: str) -> bool:
        """Check if the token should be exchanged based on issuer."""
        try:
            # Decode JWT without verification to check issuer
            decoded = jwt.decode(access_token, options={"verify_signature": False})
            issuer = decoded.get("iss", "")
            
            # Check if issuer host is different from Databricks host
            return not self._is_same_host(issuer, self.hostname)
        except Exception as e:
            logger.debug(f"Failed to decode JWT token: {e}")
            return False
            
    def _exchange_token(self, access_token: str) -> Dict[str, str]:
        """Exchange the external token for a Databricks token."""
        token_url = f"{self.hostname.rstrip('/')}{self.TOKEN_EXCHANGE_ENDPOINT}"
        
        # Prepare the token exchange request
        data = {
            "grant_type": self.TOKEN_EXCHANGE_GRANT_TYPE,
            "subject_token": access_token,
            "subject_token_type": self.TOKEN_EXCHANGE_SUBJECT_TYPE,
            "scope": "sql",
            "return_original_token_if_authenticated": "true",
        }
        
        # Add client_id if provided
        if self.identity_federation_client_id:
            data["client_id"] = self.identity_federation_client_id
            
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
        }
        
        # Encode data as URL-encoded form
        body = urlencode(data)
        
        # Make the token exchange request using UnifiedHttpClient API
        response = self.http_client.request(
            HttpMethod.POST, url=token_url, body=body, headers=headers
        )
        
        # Parse the response
        token_response = json.loads(response.data.decode())
        
        return {
            "access_token": token_response["access_token"],
            "token_type": token_response.get("token_type", "Bearer"),
            "expires_in": token_response.get("expires_in"),
        }
        
    def _extract_token_from_header(self, auth_header: str) -> Tuple[str, str]:
        """Extract token type and access token from Authorization header."""
        if not auth_header:
            raise ValueError("Authorization header is missing")
            
        parts = auth_header.split(" ", 1)
        if len(parts) != 2:
            raise ValueError("Invalid Authorization header format")
            
        return parts[0], parts[1]
        
    def _is_same_host(self, url1: str, url2: str) -> bool:
        """Check if two URLs have the same host."""
        try:
            host1 = urlparse(url1).netloc
            host2 = urlparse(url2).netloc
            return host1 == host2
        except Exception as e:
            logger.debug(f"Failed to parse URLs: {e}")
            return False
            
    def _normalize_hostname(self, hostname: str) -> str:
        """Normalize the hostname to include scheme and trailing slash."""
        if not hostname.startswith("http://") and not hostname.startswith("https://"):
            hostname = f"https://{hostname}"
        if not hostname.endswith("/"):
            hostname = f"{hostname}/"
        return hostname
        
    def _cache_token(self, token_info: Dict[str, str]):
        """Cache the token with its expiry time."""
        self._cached_token = token_info
        
        # Calculate expiry time
        if "expires_in" in token_info:
            expires_in = int(token_info["expires_in"])
            # Set expiry with a 1-minute buffer
            self._cached_token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)
        else:
            # Try to get expiry from JWT
            try:
                decoded = jwt.decode(
                    token_info["access_token"], 
                    options={"verify_signature": False}
                )
                exp = decoded.get("exp")
                if exp:
                    self._cached_token_expiry = datetime.fromtimestamp(exp) - timedelta(minutes=1)
                else:
                    # Default to 1 hour if no expiry info
                    self._cached_token_expiry = datetime.now() + timedelta(hours=1)
            except:
                # Default to 1 hour if we can't decode
                self._cached_token_expiry = datetime.now() + timedelta(hours=1)
                
    def _is_token_valid(self) -> bool:
        """Check if the cached token is still valid."""
        if not self._cached_token or not self._cached_token_expiry:
            return False
        return datetime.now() < self._cached_token_expiry


class ExternalTokenProvider(AuthProvider):
    """
    A simple provider that wraps an external credentials provider for token federation.
    """
    
    def __init__(self, credentials_provider):
        """
        Initialize with an external credentials provider.
        
        Args:
            credentials_provider: A callable that returns authentication headers
        """
        self.credentials_provider = credentials_provider
        self._header_factory = None
        
    def add_headers(self, request_headers: Dict[str, str]):
        """Add headers from the external provider."""
        if self._header_factory is None:
            self._header_factory = self.credentials_provider()
            
        headers = self._header_factory()
        for key, value in headers.items():
            request_headers[key] = value