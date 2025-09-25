import logging
import jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def parse_hostname(hostname: str) -> str:
    """
    Normalize the hostname to include scheme and trailing slash.

    Args:
        hostname: The hostname to normalize

    Returns:
        Normalized hostname with scheme and trailing slash
    """
    if not hostname.startswith("http://") and not hostname.startswith("https://"):
        hostname = f"https://{hostname}"
    if not hostname.endswith("/"):
        hostname = f"{hostname}/"
    return hostname


def decode_token(access_token: str) -> Optional[Dict]:
    """
    Decode a JWT token without verification to extract claims.

    Args:
        access_token: The JWT access token to decode

    Returns:
        Decoded token claims or None if decoding fails
    """
    try:
        return jwt.decode(access_token, options={"verify_signature": False})
    except Exception as e:
        logger.debug("Failed to decode JWT token: %s", e)
        return None


def is_same_host(url1: str, url2: str) -> bool:
    """
    Check if two URLs have the same host.

    Args:
        url1: First URL
        url2: Second URL

    Returns:
        True if hosts are the same, False otherwise
    """
    try:
        host1 = urlparse(url1).netloc
        host2 = urlparse(url2).netloc
        # Handle port differences (e.g., example.com vs example.com:443)
        host1_without_port = host1.split(":")[0]
        host2_without_port = host2.split(":")[0]
        return host1_without_port == host2_without_port
    except Exception as e:
        logger.debug("Failed to parse URLs: %s", e)
        return False


class Token:
    """
    Represents an OAuth token with expiration management.
    """

    def __init__(self, access_token: str, token_type: str = "Bearer"):
        """
        Initialize a token.

        Args:
            access_token: The access token string
            token_type: The token type (default: Bearer)
        """
        self.access_token = access_token
        self.token_type = token_type
        self.expiry_time = self._calculate_expiry()

    def _calculate_expiry(self) -> datetime:
        """
        Calculate the token expiry time from JWT claims.

        Returns:
            The token expiry datetime
        """
        decoded = decode_token(self.access_token)
        if decoded and "exp" in decoded:
            # Use JWT exp claim with 1 minute buffer
            return datetime.fromtimestamp(decoded["exp"]) - timedelta(minutes=1)
        # Default to 1 hour if no expiry info
        return datetime.now() + timedelta(hours=1)

    def is_expired(self) -> bool:
        """
        Check if the token is expired.

        Returns:
            True if token is expired, False otherwise
        """
        return datetime.now() >= self.expiry_time

    def to_dict(self) -> Dict[str, str]:
        """
        Convert token to dictionary format.

        Returns:
            Dictionary with access_token and token_type
        """
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
        }
