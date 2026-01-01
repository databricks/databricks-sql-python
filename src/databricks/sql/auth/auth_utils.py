import logging
import jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


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
