import ssl
import urllib.parse
import urllib.request
from typing import Dict, Any, Optional, Tuple, Union

from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, ProxyManager
from urllib3.util import make_headers

from databricks.sql.auth.retry import DatabricksRetryPolicy
from databricks.sql.types import SSLOptions

def detect_and_parse_proxy(
    scheme: str, host: str
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    """
    Detect system proxy and return proxy URI and headers using standardized logic.

    Args:
        scheme: URL scheme (http/https)
        host: Target hostname

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
        # Proxy found, but check if this host should bypass proxy
        if host and urllib.request.proxy_bypass(host):
            proxy = None  # Host bypasses proxy per system rules

    if not proxy:
        return None, None

    parsed_proxy = urllib.parse.urlparse(proxy)
    proxy_headers = create_basic_proxy_auth_headers(parsed_proxy)
    return proxy, proxy_headers


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