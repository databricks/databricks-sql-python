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
        def _extract_host(url: str) -> str:
            parsed = urlparse(url)
            netloc = parsed.netloc
            if not netloc:
                # Bare hostname with no scheme: urlparse puts the host in the
                # path component and leaves netloc empty.  Add a dummy scheme so
                # the parser can identify the netloc correctly.
                netloc = urlparse(f"https://{url}").netloc
            # Strip port (e.g. example.com:443 -> example.com)
            return netloc.split(":")[0].lower()

        return _extract_host(url1) == _extract_host(url2)
    except Exception as e:
        logger.debug("Failed to parse URLs: %s", e)
        return False
