import logging
from typing import Dict, Any, Optional, List, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.auth.retry import CommandType, DatabricksRetryPolicy
from databricks.sql.types import SSLOptions
from databricks.sql.exc import (
    RequestError,
    MaxRetryDurationError,
    SessionAlreadyClosedError,
    CursorAlreadyClosedError,
)

logger = logging.getLogger(__name__)


class SeaHttpClient:
    """
    HTTP client for Statement Execution API (SEA).

    This client uses requests.Session for HTTP communication with retry policies
    and connection pooling.
    """

    retry_policy: Union[DatabricksRetryPolicy, int]
    session: requests.Session

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
        self.http_headers = http_headers
        self.auth_provider = auth_provider
        self.ssl_options = ssl_options

        # Build base URL
        self.base_url = f"https://{server_hostname}:{self.port}"

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
            self.retry_policy = 0

        # Create session and configure it
        self.session = requests.Session()
        self._configure_session()

    def _configure_session(self):
        """Configure the requests session with headers, SSL, and retry policy."""
        # Setup headers
        self.session.headers.update(dict(self.http_headers))
        self.session.headers.update({"Content-Type": "application/json"})

        # Configure SSL
        if not self.ssl_options.tls_verify:
            self.session.verify = False
        elif self.ssl_options.tls_trusted_ca_file:
            self.session.verify = self.ssl_options.tls_trusted_ca_file

        if self.ssl_options.tls_client_cert_file:
            if self.ssl_options.tls_client_cert_key_file:
                self.session.cert = (
                    self.ssl_options.tls_client_cert_file,
                    self.ssl_options.tls_client_cert_key_file,
                )
            else:
                self.session.cert = self.ssl_options.tls_client_cert_file

        # Configure retry adapter
        adapter = HTTPAdapter(
            pool_connections=self.max_connections,
            pool_maxsize=self.max_connections,
            max_retries=self.retry_policy,
        )

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def close(self):
        """Close the session."""
        self.session.close()

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

        # Set command type for retry policy
        command_type = self._get_command_type_from_path(path, method)
        self.set_retry_command_type(command_type)
        self.start_retry_timer()

        # Prepare headers
        headers = self._get_auth_headers()

        logger.debug(f"Making {method} request to {path}")

        response = None
        try:
            response = self.session.request(
                method=method.upper(),
                url=f"{self.base_url}{path}",
                json=data,  # requests handles JSON encoding automatically
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except MaxRetryDurationError as e:
            # MaxRetryDurationError is raised directly by DatabricksRetryPolicy
            # when duration limits are exceeded
            error_message = f"Request failed due to retry duration limit: {e}"
            raise RequestError(error_message, None, e)
        except (SessionAlreadyClosedError, CursorAlreadyClosedError) as e:
            # These exceptions are raised by DatabricksRetryPolicy when detecting
            # "already closed" scenarios (404 responses with retry history)
            error_message = f"Request failed: {e}"
            raise RequestError(error_message, None, e)
        except MaxRetryError as e:
            # urllib3 MaxRetryError should bubble up for redirect tests to catch
            logger.error(f"SEA HTTP request failed with MaxRetryError: {e}")
            raise
        except Exception as e:
            logger.error(f"SEA HTTP request failed with exception: {e}")
            error_message = f"Error during request to server. {e}"
            raise RequestError(error_message, None, e)
        finally:
            if response is not None:
                response.close()

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
