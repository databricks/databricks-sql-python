import json
import logging
import ssl
import urllib.request
from typing import Dict, Any, Optional, List, Tuple, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, HTTPError, ConnectionError
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


class SSLContextAdapter(HTTPAdapter):
    """
    An HTTP adapter that uses a custom SSLContext to handle advanced SSL settings,
    including client certificate key passwords.
    """

    def __init__(self, ssl_options: SSLOptions, **kwargs):
        self.ssl_context = self._create_ssl_context(ssl_options)
        super().__init__(**kwargs)

    def _create_ssl_context(self, ssl_options: SSLOptions) -> ssl.SSLContext:
        """
        Build a custom SSLContext based on the provided SSLOptions.
        """
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        if not ssl_options.tls_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        elif ssl_options.tls_trusted_ca_file:
            context.load_verify_locations(cafile=ssl_options.tls_trusted_ca_file)
        if ssl_options.tls_client_cert_file:
            context.load_cert_chain(
                certfile=ssl_options.tls_client_cert_file,
                keyfile=ssl_options.tls_client_cert_key_file,
                password=ssl_options.tls_client_cert_key_password,
            )
        return context

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)


class SeaHttpClient:
    """
    HTTP client for Statement Execution API (SEA), using the requests library.
    """

    retry_policy: Union[DatabricksRetryPolicy, int]
    _session: requests.Session

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
        self.server_hostname = server_hostname
        self.port = port or 443
        self.auth_provider = auth_provider
        self.ssl_options = ssl_options
        self.scheme = "https"
        self.base_url = f"{self.scheme}://{server_hostname}:{self.port}"
        self._session = requests.Session()
        self.headers: Dict[str, str] = dict(http_headers)
        self.headers.update({"Content-Type": "application/json"})
        self._session.headers.update(self.headers)
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
        self.max_connections = kwargs.get("max_connections", 10)
        self._configure_proxies()
        self.enable_v3_retries = kwargs.get("_enable_v3_retries", True)
        self._configure_retries_and_ssl(**kwargs)

    def _configure_proxies(self):
        try:
            proxy = urllib.request.getproxies().get(self.scheme)
        except (KeyError, AttributeError):
            proxy = None
        else:
            if self.server_hostname and urllib.request.proxy_bypass(
                self.server_hostname
            ):
                proxy = None
        if proxy:
            self._session.proxies = {"http": proxy, "https": proxy}

    def _configure_retries_and_ssl(self, **kwargs):
        if self.enable_v3_retries:
            urllib3_kwargs = {"allowed_methods": ["GET", "POST", "DELETE"]}
            _max_redirects = kwargs.get("_retry_max_redirects")
            if _max_redirects:
                if _max_redirects > self._retry_stop_after_attempts_count:
                    logger.warning(
                        "_retry_max_redirects > _retry_stop_after_attempts_count "
                        "so it will have no effect!"
                    )
                urllib3_kwargs["redirect"] = _max_redirects
                self._session.max_redirects = _max_redirects
            self.retry_policy = DatabricksRetryPolicy(
                delay_min=self._retry_delay_min,
                delay_max=self._retry_delay_max,
                stop_after_attempts_count=self._retry_stop_after_attempts_count,
                stop_after_attempts_duration=self._retry_stop_after_attempts_duration,
                delay_default=self._retry_delay_default,
                force_dangerous_codes=self.force_dangerous_codes,
                urllib3_kwargs=urllib3_kwargs,
            )
            retry_strategy = self.retry_policy
        else:
            logger.warning(
                "Legacy retry behavior is enabled for this connection."
                " This behaviour is not supported for the SEA backend."
            )
            self.retry_policy = 0
            retry_strategy = 0
        adapter = SSLContextAdapter(
            ssl_options=self.ssl_options,
            pool_connections=self.max_connections,
            max_retries=retry_strategy,
        )
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def close(self):
        self._session.close()

    def set_retry_command_type(self, command_type: CommandType):
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.command_type = command_type

    def start_retry_timer(self):
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.start_retry_timer()

    def _get_auth_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        self.auth_provider.add_headers(headers)
        return headers

    def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        full_url = urljoin(self.base_url, path)
        auth_headers = self._get_auth_headers()
        command_type = self._get_command_type_from_path(path, method)
        self.set_retry_command_type(command_type)
        self.start_retry_timer()
        logger.debug(f"Making {method} request to {full_url}")
        try:
            with self._session.request(
                method=method.upper(),
                url=full_url,
                json=data,
                headers=auth_headers,
            ) as response:
                logger.debug(f"Response status: {response.status_code}")
                response.raise_for_status()
                return response.json()
        except requests.exceptions.ConnectionError as e:
            # Check if the first argument of the ConnectionError is a MaxRetryError
            if e.args and isinstance(e.args[0], MaxRetryError):
                # We want to raise the original MaxRetryError, not the wrapper
                original_error = e.args[0]
                logger.error(
                    f"SEA HTTP request failed with MaxRetryError: {original_error}"
                )
                raise original_error
            else:
                logger.error(f"SEA HTTP request failed with ConnectionError: {e}")
                raise RequestError("Error during request to server.", None, None, e)
        except RequestException as e:
            error_message = f"Error during request to server: {e}"
            raise RequestError(error_message, None, None, e)

    def _get_command_type_from_path(self, path: str, method: str) -> CommandType:
        path = path.lower()
        method = method.upper()
        if "/statements" in path:
            if method == "POST" and path.endswith("/statements"):
                return CommandType.EXECUTE_STATEMENT
            elif "/cancel" in path:
                return CommandType.OTHER
            elif method == "DELETE":
                return CommandType.CLOSE_OPERATION
            elif method == "GET":
                return CommandType.GET_OPERATION_STATUS
        elif "/sessions" in path:
            if method == "DELETE":
                return CommandType.CLOSE_SESSION
        return CommandType.OTHER
