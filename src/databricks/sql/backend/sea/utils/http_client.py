import json
import logging
import ssl
import urllib.parse
import urllib.request
from typing import Dict, Any, Optional, List, Tuple, Union

from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, ProxyManager
from urllib3.util import make_headers
from urllib3.exceptions import MaxRetryError

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.auth.retry import CommandType, DatabricksRetryPolicy
from databricks.sql.types import SSLOptions
from databricks.sql.exc import (
    RequestError,
)
from databricks.sql.common.http_utils import (
    detect_and_parse_proxy,
)

logger = logging.getLogger(__name__)


class SeaHttpClient:
    """
    HTTP client for Statement Execution API (SEA).

    This client uses urllib3 for robust HTTP communication with retry policies
    and connection pooling.
    """

    retry_policy: Union[DatabricksRetryPolicy, int]
    _pool: Optional[Union[HTTPConnectionPool, HTTPSConnectionPool]]
    proxy_uri: Optional[str]
    proxy_auth: Optional[Dict[str, str]]
    realhost: Optional[str]
    realport: Optional[int]

    def __init__(
        self,
        server_hostname: str,
        port: int,
        http_path: str,
        http_headers: List[Tuple[str, str]],
        auth_provider: AuthProvider,
        ssl_options: SSLOptions,
        **kwargs,
    ):
        """
        Initialize the SEA HTTP client.

        Args:
            server_hostname: Hostname of the Databricks server
            port: Port number for the connection
            http_path: HTTP path for the connection
            http_headers: List of HTTP headers to include in requests
            auth_provider: Authentication provider
            ssl_options: SSL configuration options
            **kwargs: Additional keyword arguments including retry policy settings
        """

        self.server_hostname = server_hostname
        self.port = port or 443
        self.http_path = http_path
        self.auth_provider = auth_provider
        self.ssl_options = ssl_options

        # Build base URL
        self.base_url = f"https://{server_hostname}:{self.port}"

        # Parse URL for proxy handling
        parsed_url = urllib.parse.urlparse(self.base_url)
        self.scheme = parsed_url.scheme
        self.host = parsed_url.hostname
        self.port = parsed_url.port or (443 if self.scheme == "https" else 80)

        # Setup headers
        self.headers: Dict[str, str] = dict(http_headers)
        self.headers.update({"Content-Type": "application/json"})

        # Extract retry policy settings
        self._retry_delay_min = kwargs.get("_retry_delay_min", 1.0)
        self._retry_delay_max = kwargs.get("_retry_delay_max", 60.0)
        self._retry_stop_after_attempts_count = kwargs.get(
            "_retry_stop_after_attempts_count", 30
        )
        self._retry_stop_after_attempts_duration = kwargs.get(
            "_retry_stop_after_attempts_duration", 900.0
        )
        self._retry_delay_default = kwargs.get("_retry_delay_default", 5.0)
        self.force_dangerous_codes = kwargs.get("_retry_dangerous_codes", [])

        # Connection pooling settings
        self.max_connections = kwargs.get("max_connections", 10)

        # Setup retry policy
        self.enable_v3_retries = kwargs.get("_enable_v3_retries", True)

        if self.enable_v3_retries:
            urllib3_kwargs = {"allowed_methods": ["GET", "POST", "DELETE"]}
            _max_redirects = kwargs.get("_retry_max_redirects")
            if _max_redirects:
                if _max_redirects > self._retry_stop_after_attempts_count:
                    logger.warning(
                        "_retry_max_redirects > _retry_stop_after_attempts_count so it will have no affect!"
                    )
                urllib3_kwargs["redirect"] = _max_redirects

            self.retry_policy = DatabricksRetryPolicy(
                delay_min=self._retry_delay_min,
                delay_max=self._retry_delay_max,
                stop_after_attempts_count=self._retry_stop_after_attempts_count,
                stop_after_attempts_duration=self._retry_stop_after_attempts_duration,
                delay_default=self._retry_delay_default,
                force_dangerous_codes=self.force_dangerous_codes,
                urllib3_kwargs=urllib3_kwargs,
            )
        else:
            # Legacy behavior - no automatic retries
            logger.warning(
                "Legacy retry behavior is enabled for this connection."
                " This behaviour is not supported for the SEA backend."
            )
            self.retry_policy = 0

        # Handle proxy settings using shared utility
        proxy_auth_method = kwargs.get("_proxy_auth_method")
        proxy_uri, proxy_auth = detect_and_parse_proxy(
            self.scheme, self.host, proxy_auth_method=proxy_auth_method
        )

        if proxy_uri:
            parsed_proxy = urllib.parse.urlparse(proxy_uri)
            self.realhost = self.host
            self.realport = self.port
            self.proxy_uri = proxy_uri
            self.host = parsed_proxy.hostname
            self.port = parsed_proxy.port or (443 if self.scheme == "https" else 80)
            self.proxy_auth = proxy_auth
        else:
            self.realhost = self.realport = self.proxy_auth = self.proxy_uri = None

        # Initialize connection pool
        self._pool = None
        self._open()

    def _open(self):
        """Initialize the connection pool."""
        pool_kwargs = {"maxsize": self.max_connections}

        if self.scheme == "http":
            pool_class = HTTPConnectionPool
        else:  # https
            pool_class = HTTPSConnectionPool
            pool_kwargs.update(
                {
                    "cert_reqs": ssl.CERT_REQUIRED
                    if self.ssl_options.tls_verify
                    else ssl.CERT_NONE,
                    "ca_certs": self.ssl_options.tls_trusted_ca_file,
                    "cert_file": self.ssl_options.tls_client_cert_file,
                    "key_file": self.ssl_options.tls_client_cert_key_file,
                    "key_password": self.ssl_options.tls_client_cert_key_password,
                }
            )

        if self.using_proxy():
            proxy_manager = ProxyManager(
                self.proxy_uri,
                num_pools=1,
                proxy_headers=self.proxy_auth,
            )
            self._pool = proxy_manager.connection_from_host(
                host=self.realhost,
                port=self.realport,
                scheme=self.scheme,
                pool_kwargs=pool_kwargs,
            )
        else:
            self._pool = pool_class(self.host, self.port, **pool_kwargs)

    def close(self):
        """Close the connection pool."""
        if self._pool:
            self._pool.clear()

    def using_proxy(self) -> bool:
        """Check if proxy is being used."""
        return self.realhost is not None

    def set_retry_command_type(self, command_type: CommandType):
        """Set the command type for retry policy decision making."""
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.command_type = command_type

    def start_retry_timer(self):
        """Start the retry timer for duration-based retry limits."""
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.start_retry_timer()

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers from the auth provider."""
        headers: Dict[str, str] = {}
        self.auth_provider.add_headers(headers)
        return headers

    def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to the SEA endpoint.

        Args:
            method: HTTP method (GET, POST, DELETE)
            path: API endpoint path
            data: Request payload data

        Returns:
            Dict[str, Any]: Response data parsed from JSON

        Raises:
            RequestError: If the request fails after retries
        """

        # Prepare headers
        headers = {**self.headers, **self._get_auth_headers()}

        # Prepare request body
        body = json.dumps(data).encode("utf-8") if data else b""
        if body:
            headers["Content-Length"] = str(len(body))

        # Set command type for retry policy
        command_type = self._get_command_type_from_path(path, method)
        self.set_retry_command_type(command_type)
        self.start_retry_timer()

        logger.debug(f"Making {method} request to {path}")

        if self._pool is None:
            raise RequestError("Connection pool not initialized", None)

        try:
            with self._pool.request(
                method=method.upper(),
                url=path,
                body=body,
                headers=headers,
                preload_content=False,
                retries=self.retry_policy,
            ) as response:
                # Handle successful responses
                if 200 <= response.status < 300:
                    if response.data:
                        return json.loads(response.data.decode())
                    else:
                        return {}

                error_message = f"SEA HTTP request failed with status {response.status}"
                raise Exception(error_message)
        except MaxRetryError as e:
            logger.error(f"SEA HTTP request failed with MaxRetryError: {e}")
            raise
        except Exception as e:
            logger.error(f"SEA HTTP request failed with exception: {e}")
            error_message = f"Error during request to server. {e}"
            raise RequestError(error_message, None, None, e)

    def _get_command_type_from_path(self, path: str, method: str) -> CommandType:
        """
        Determine the command type based on the API path and method.

        This helps the retry policy make appropriate decisions for different
        types of SEA operations.
        """

        path = path.lower()
        method = method.upper()

        if "/statements" in path:
            if method == "POST" and path.endswith("/statements"):
                return CommandType.EXECUTE_STATEMENT
            elif "/cancel" in path:
                return CommandType.OTHER  # Cancel operation
            elif method == "DELETE":
                return CommandType.CLOSE_OPERATION
            elif method == "GET":
                return CommandType.GET_OPERATION_STATUS
        elif "/sessions" in path:
            if method == "DELETE":
                return CommandType.CLOSE_SESSION

        return CommandType.OTHER
