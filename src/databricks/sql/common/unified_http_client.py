import logging
import ssl
import urllib.parse
import urllib.request
from contextlib import contextmanager
from typing import Dict, Any, Optional, Generator

import urllib3
from urllib3 import PoolManager, ProxyManager
from urllib3.util import make_headers
from urllib3.exceptions import MaxRetryError

# Compatibility import for different urllib3 versions
try:
    # If urllib3~=2.0 is installed
    from urllib3 import BaseHTTPResponse
except ImportError:
    # If urllib3~=1.0 is installed
    from urllib3 import HTTPResponse as BaseHTTPResponse

from databricks.sql.auth.retry import DatabricksRetryPolicy, CommandType
from databricks.sql.exc import RequestError
from databricks.sql.common.http import HttpMethod
from databricks.sql.common.http_utils import (
    detect_and_parse_proxy,
)

logger = logging.getLogger(__name__)


class UnifiedHttpClient:
    """
    Unified HTTP client for all Databricks SQL connector HTTP operations.

    This client uses urllib3 for robust HTTP communication with retry policies,
    connection pooling, SSL support, and proxy support. It replaces the various
    singleton HTTP clients and direct requests usage throughout the codebase.

    The client supports per-request proxy decisions, automatically routing requests
    through proxy or direct connections based on system proxy bypass rules and
    the target hostname of each request.
    """

    def __init__(self, client_context):
        """
        Initialize the unified HTTP client.

        Args:
            client_context: ClientContext instance containing HTTP configuration
        """
        self.config = client_context
        # Since the unified http client is used for all requests, we need to have proxy and direct pool managers
        # for per-request proxy decisions.
        self._direct_pool_manager = None
        self._proxy_pool_manager = None
        self._retry_policy = None
        self._proxy_uri = None
        self._proxy_auth = None
        self._setup_pool_managers()

    def _setup_pool_managers(self):
        """Set up both direct and proxy pool managers for per-request proxy decisions."""

        # SSL context setup
        ssl_context = None
        if self.config.ssl_options:
            ssl_context = ssl.create_default_context()

            # Configure SSL verification
            if not self.config.ssl_options.tls_verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            elif not self.config.ssl_options.tls_verify_hostname:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_REQUIRED

            # Load custom CA file if specified
            if self.config.ssl_options.tls_trusted_ca_file:
                ssl_context.load_verify_locations(
                    self.config.ssl_options.tls_trusted_ca_file
                )

            # Load client certificate if specified
            if (
                self.config.ssl_options.tls_client_cert_file
                and self.config.ssl_options.tls_client_cert_key_file
            ):
                ssl_context.load_cert_chain(
                    self.config.ssl_options.tls_client_cert_file,
                    self.config.ssl_options.tls_client_cert_key_file,
                    self.config.ssl_options.tls_client_cert_key_password,
                )

        # Create retry policy
        self._retry_policy = DatabricksRetryPolicy(
            delay_min=self.config.retry_delay_min,
            delay_max=self.config.retry_delay_max,
            stop_after_attempts_count=self.config.retry_stop_after_attempts_count,
            stop_after_attempts_duration=self.config.retry_stop_after_attempts_duration,
            delay_default=self.config.retry_delay_default,
            force_dangerous_codes=self.config.retry_dangerous_codes,
        )

        # Initialize the required attributes that DatabricksRetryPolicy expects
        # but doesn't initialize in its constructor
        self._retry_policy._command_type = None
        self._retry_policy._retry_start_time = None

        # Common pool manager kwargs
        pool_kwargs = {
            "num_pools": self.config.pool_connections,
            "maxsize": self.config.pool_maxsize,
            "retries": self._retry_policy,
            "timeout": urllib3.Timeout(
                connect=self.config.socket_timeout, read=self.config.socket_timeout
            )
            if self.config.socket_timeout
            else None,
            "ssl_context": ssl_context,
        }

        # Always create a direct pool manager
        self._direct_pool_manager = PoolManager(**pool_kwargs)

        # Detect system proxy configuration
        # We use 'https' as default scheme since most requests will be HTTPS
        parsed_url = urllib.parse.urlparse(self.config.hostname)
        self.scheme = parsed_url.scheme or "https"
        self.host = parsed_url.hostname

        # Check if system has proxy configured for our scheme
        try:
            # Use shared proxy detection logic, skipping bypass since we handle that per-request
            proxy_url, proxy_auth = detect_and_parse_proxy(
                self.scheme,
                self.host,
                skip_bypass=True,
                proxy_auth_method=self.config.proxy_auth_method,
            )

            if proxy_url:
                # Store proxy configuration for per-request decisions
                self._proxy_uri = proxy_url
                self._proxy_auth = proxy_auth

                # Create proxy pool manager
                self._proxy_pool_manager = ProxyManager(
                    proxy_url, proxy_headers=proxy_auth, **pool_kwargs
                )
                logger.debug("Initialized with proxy support: %s", proxy_url)
            else:
                self._proxy_pool_manager = None
                logger.debug("No system proxy detected, using direct connections only")

        except Exception as e:
            # If proxy detection fails, fall back to direct connections only
            logger.debug("Error detecting system proxy configuration: %s", e)
            self._proxy_pool_manager = None

    def _should_use_proxy(self, target_host: str) -> bool:
        """
        Determine if a request to the target host should use proxy.

        Args:
            target_host: The hostname of the target URL

        Returns:
            True if proxy should be used, False for direct connection
        """
        # If no proxy is configured, always use direct connection
        if not self._proxy_pool_manager or not self._proxy_uri:
            return False

        # Check system proxy bypass rules for this specific host
        try:
            # proxy_bypass returns True if the host should BYPASS the proxy
            # We want the opposite - True if we should USE the proxy
            return not urllib.request.proxy_bypass(target_host)
        except Exception as e:
            # If proxy_bypass fails, default to using proxy (safer choice)
            logger.debug("Error checking proxy bypass for host %s: %s", target_host, e)
            return True

    def _get_pool_manager_for_url(self, url: str) -> urllib3.PoolManager:
        """
        Get the appropriate pool manager for the given URL.

        Args:
            url: The target URL

        Returns:
            PoolManager instance (either direct or proxy)
        """
        parsed_url = urllib.parse.urlparse(url)
        target_host = parsed_url.hostname

        if target_host and self._should_use_proxy(target_host):
            logger.debug("Using proxy for request to %s", target_host)
            return self._proxy_pool_manager
        else:
            logger.debug("Using direct connection for request to %s", target_host)
            return self._direct_pool_manager

    def _prepare_headers(
        self, headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Prepare headers for the request, including User-Agent."""
        request_headers = {}

        if self.config.user_agent:
            request_headers["User-Agent"] = self.config.user_agent

        if headers:
            request_headers.update(headers)

        return request_headers

    def _prepare_retry_policy(self):
        """Set up the retry policy for the current request."""
        if isinstance(self._retry_policy, DatabricksRetryPolicy):
            # Set command type for HTTP requests to OTHER (not database commands)
            self._retry_policy.command_type = CommandType.OTHER
            # Start the retry timer for duration-based retry limits
            self._retry_policy.start_retry_timer()

    @contextmanager
    def request_context(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> Generator[BaseHTTPResponse, None, None]:
        """
        Context manager for making HTTP requests with proper resource cleanup.

        Args:
            method: HTTP method (HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE)
            url: URL to request
            headers: Optional headers dict
            **kwargs: Additional arguments passed to urllib3 request

        Yields:
            BaseHTTPResponse: The HTTP response object
        """
        logger.debug(
            "Making %s request to %s", method, urllib.parse.urlparse(url).netloc
        )

        request_headers = self._prepare_headers(headers)

        # Prepare retry policy for this request
        self._prepare_retry_policy()

        # Select appropriate pool manager based on target URL
        pool_manager = self._get_pool_manager_for_url(url)

        response = None

        try:
            response = pool_manager.request(
                method=method.value, url=url, headers=request_headers, **kwargs
            )
            yield response
        except MaxRetryError as e:
            logger.error("HTTP request failed after retries: %s", e)
            raise RequestError(f"HTTP request failed: {e}")
        except Exception as e:
            logger.error("HTTP request error: %s", e)
            raise RequestError(f"HTTP request error: {e}")
        finally:
            if response:
                response.close()

    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> BaseHTTPResponse:
        """
        Make an HTTP request.

        Args:
            method: HTTP method (HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, etc.)
            url: URL to request
            headers: Optional headers dict
            **kwargs: Additional arguments passed to urllib3 request

        Returns:
            BaseHTTPResponse: The HTTP response object with data and metadata pre-loaded
        """
        with self.request_context(method, url, headers=headers, **kwargs) as response:
            # Read the response data to ensure it's available after context exit
            # Note: status and headers remain accessible after close(); calling response.read() loads and caches the response data so it remains accessible after the response is closed.
            response.read()
            return response

    def using_proxy(self) -> bool:
        """Check if proxy support is available (not whether it's being used for a specific request)."""
        return self._proxy_pool_manager is not None

    def close(self):
        """Close the underlying connection pools."""
        if self._direct_pool_manager:
            self._direct_pool_manager.clear()
            self._direct_pool_manager = None
        if self._proxy_pool_manager:
            self._proxy_pool_manager.clear()
            self._proxy_pool_manager = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
