import ssl
import urllib.parse
import urllib.request
import logging
from typing import Dict, Any, Optional, Tuple, Union

from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, ProxyManager
from urllib3.util import make_headers

from databricks.sql.auth.retry import DatabricksRetryPolicy
from databricks.sql.types import SSLOptions

logger = logging.getLogger(__name__)


def detect_and_parse_proxy(
    scheme: str,
    host: Optional[str],
    skip_bypass: bool = False,
    proxy_auth_method: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    """
    Detect system proxy and return proxy URI and headers using standardized logic.

    Args:
        scheme: URL scheme (http/https)
        host: Target hostname (optional, only needed for bypass checking)
        skip_bypass: If True, skip proxy bypass checking and return proxy config if found
        proxy_auth_method: Authentication method ('basic', 'negotiate', or None)

    Returns:
        Tuple of (proxy_uri, proxy_headers) or (None, None) if no proxy
    """
    try:
        # returns a dictionary of scheme -> proxy server URL mappings.
        # https://docs.python.org/3/library/urllib.request.html#urllib.request.getproxies
        proxy = urllib.request.getproxies().get(scheme)
    except (KeyError, AttributeError):
        # No proxy found or getproxies() failed - disable proxy
        proxy = None
    else:
        # Proxy found, but check if this host should bypass proxy (unless skipped)
        if not skip_bypass and host and urllib.request.proxy_bypass(host):
            proxy = None  # Host bypasses proxy per system rules

    if not proxy:
        return None, None

    parsed_proxy = urllib.parse.urlparse(proxy)

    # Generate appropriate auth headers based on method
    if proxy_auth_method == "negotiate":
        proxy_headers = _generate_negotiate_headers(parsed_proxy.hostname)
    elif proxy_auth_method == "basic" or proxy_auth_method is None:
        # Default to basic if method not specified (backward compatibility)
        proxy_headers = create_basic_proxy_auth_headers(parsed_proxy)
    else:
        raise ValueError(f"Unsupported proxy_auth_method: {proxy_auth_method}")

    return proxy, proxy_headers


def _generate_negotiate_headers(
    proxy_hostname: Optional[str],
) -> Optional[Dict[str, str]]:
    """Generate Kerberos/SPNEGO authentication headers"""
    try:
        from requests_kerberos import HTTPKerberosAuth

        logger.debug(
            "Attempting to generate Kerberos SPNEGO token for proxy: %s", proxy_hostname
        )
        auth = HTTPKerberosAuth()
        negotiate_details = auth.generate_request_header(
            None, proxy_hostname, is_preemptive=True
        )
        if negotiate_details:
            return {"proxy-authorization": negotiate_details}
        else:
            logger.debug("Unable to generate kerberos proxy auth headers")
    except Exception as e:
        logger.error("Error generating Kerberos proxy auth headers: %s", e)

    return None


def create_basic_proxy_auth_headers(parsed_proxy) -> Optional[Dict[str, str]]:
    """
    Create basic auth headers for proxy if credentials are provided.

    Args:
        parsed_proxy: Parsed proxy URL from urllib.parse.urlparse()

    Returns:
        Dictionary of proxy auth headers or None if no credentials
    """
    if parsed_proxy is None or not parsed_proxy.username:
        return None
    ap = f"{urllib.parse.unquote(parsed_proxy.username)}:{urllib.parse.unquote(parsed_proxy.password)}"
    return make_headers(proxy_basic_auth=ap)
