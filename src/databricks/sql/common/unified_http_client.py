import logging
import ssl
import urllib.parse
from contextlib import contextmanager
from typing import Dict, Any, Optional, Generator

import urllib3
from urllib3 import PoolManager, ProxyManager
from urllib3.util import make_headers
from urllib3.exceptions import MaxRetryError

from databricks.sql.auth.retry import DatabricksRetryPolicy, CommandType
from databricks.sql.exc import RequestError
from databricks.sql.common.http import HttpMethod

logger = logging.getLogger(__name__)


class UnifiedHttpClient:
    """
    Unified HTTP client for all Databricks SQL connector HTTP operations.

    This client uses urllib3 for robust HTTP communication with retry policies,
    connection pooling, SSL support, and proxy support. It replaces the various
    singleton HTTP clients and direct requests usage throughout the codebase.
    """

    def __init__(self, client_context):
        """
        Initialize the unified HTTP client.

        Args:
            client_context: ClientContext instance containing HTTP configuration
        """
        self.config = client_context
        self._pool_manager = None
        self._retry_policy = None
        self._setup_pool_manager()

    def _setup_pool_manager(self):
        """Set up the urllib3 PoolManager with configuration from ClientContext."""

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

        # Create proxy or regular pool manager
        if self.config.http_proxy:
            proxy_headers = None
            if self.config.proxy_username and self.config.proxy_password:
                proxy_headers = make_headers(
                    proxy_basic_auth=f"{self.config.proxy_username}:{self.config.proxy_password}"
                )

            self._pool_manager = ProxyManager(
                self.config.http_proxy, proxy_headers=proxy_headers, **pool_kwargs
            )
        else:
            self._pool_manager = PoolManager(**pool_kwargs)

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
    ) -> Generator[urllib3.HTTPResponse, None, None]:
        """
        Context manager for making HTTP requests with proper resource cleanup.

        Args:
            method: HTTP method (HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE)
            url: URL to request
            headers: Optional headers dict
            **kwargs: Additional arguments passed to urllib3 request

        Yields:
            urllib3.HTTPResponse: The HTTP response object
        """
        logger.debug("Making %s request to %s", method, url)

        request_headers = self._prepare_headers(headers)

        # Prepare retry policy for this request
        self._prepare_retry_policy()

        response = None

        try:
            response = self._pool_manager.request(
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
    ) -> urllib3.HTTPResponse:
        """
        Make an HTTP request.

        Args:
            method: HTTP method (HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, etc.)
            url: URL to request
            headers: Optional headers dict
            **kwargs: Additional arguments passed to urllib3 request

        Returns:
            urllib3.HTTPResponse: The HTTP response object with data and metadata pre-loaded
        """
        with self.request_context(method, url, headers=headers, **kwargs) as response:
            # Read the response data to ensure it's available after context exit
            # Note: status and headers remain accessible after close(), only data needs caching
            response._body = response.data
            return response

    def close(self):
        """Close the underlying connection pools."""
        if self._pool_manager:
            self._pool_manager.clear()
            self._pool_manager = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
