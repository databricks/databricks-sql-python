import base64
import json
import logging
import urllib.parse
from typing import Dict, Union, Optional, Any

import six
import thrift.transport.THttpClient

import ssl
import warnings
from http.client import HTTPResponse
from io import BytesIO

import urllib3
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, ProxyManager
from urllib3.util import make_headers
from databricks.sql.auth.retry import CommandType, DatabricksRetryPolicy
from databricks.sql.types import SSLOptions

logger = logging.getLogger(__name__)


class THttpClient(thrift.transport.THttpClient.THttpClient):
    def __init__(
        self,
        auth_provider,
        uri_or_host,
        port=None,
        path=None,
        ssl_options: Optional[SSLOptions] = None,
        max_connections: int = 1,
        retry_policy: Union[DatabricksRetryPolicy, int] = 0,
    ):
        self._ssl_options = ssl_options

        if port is not None:
            warnings.warn(
                "Please use the THttpClient('http{s}://host:port/path') constructor",
                DeprecationWarning,
                stacklevel=2,
            )
            self.host = uri_or_host
            self.port = port
            assert path
            self.path = path
            self.scheme = "http"
        else:
            parsed = urllib.parse.urlsplit(uri_or_host)
            self.scheme = parsed.scheme
            assert self.scheme in ("http", "https")
            if self.scheme == "https":
                if self._ssl_options is not None:
                    # TODO: Not sure if those options are used anywhere - need to double-check
                    self.certfile = self._ssl_options.tls_client_cert_file
                    self.keyfile = self._ssl_options.tls_client_cert_key_file
                    self.context = self._ssl_options.create_ssl_context()
            self.port = parsed.port
            self.host = parsed.hostname
            self.path = parsed.path
            if parsed.query:
                self.path += "?%s" % parsed.query
        try:
            proxy = urllib.request.getproxies()[self.scheme]
        except KeyError:
            proxy = None
        else:
            if urllib.request.proxy_bypass(self.host):
                proxy = None
        if proxy:
            parsed = urllib.parse.urlparse(proxy)

            # realhost and realport are the host and port of the actual request
            self.realhost = self.host
            self.realport = self.port

            # this is passed to ProxyManager
            self.proxy_uri: str = proxy
            self.host = parsed.hostname
            self.port = parsed.port
            self.proxy_auth = self.basic_proxy_auth_headers(parsed)
        else:
            self.realhost = self.realport = self.proxy_auth = None

        self.max_connections = max_connections

        # If retry_policy == 0 then urllib3 will not retry automatically
        # this falls back to the pre-v3 behaviour where thrift_backend.py handles retry logic
        self.retry_policy = retry_policy

        self.__wbuf = BytesIO()
        self.__resp: Union[None, HTTPResponse] = None
        self.__timeout = None
        self.__custom_headers = None

        self.__auth_provider = auth_provider

    def setCustomHeaders(self, headers: Dict[str, str]):
        self._headers = headers
        super().setCustomHeaders(headers)

    def startRetryTimer(self):
        """Notify DatabricksRetryPolicy of the request start time

        This is used to enforce the retry_stop_after_attempts_duration
        """
        self.retry_policy and self.retry_policy.start_retry_timer()

    def open(self):

        # self.__pool replaces the self.__http used by the original THttpClient
        _pool_kwargs = {"maxsize": self.max_connections}

        if self.scheme == "http":
            pool_class = HTTPConnectionPool
        elif self.scheme == "https":
            pool_class = HTTPSConnectionPool
            _pool_kwargs.update(
                {
                    "cert_reqs": ssl.CERT_REQUIRED
                    if self._ssl_options.tls_verify
                    else ssl.CERT_NONE,
                    "ca_certs": self._ssl_options.tls_trusted_ca_file,
                    "cert_file": self._ssl_options.tls_client_cert_file,
                    "key_file": self._ssl_options.tls_client_cert_key_file,
                    "key_password": self._ssl_options.tls_client_cert_key_password,
                }
            )

        if self.using_proxy():
            proxy_manager = ProxyManager(
                self.proxy_uri,
                num_pools=1,
                proxy_headers=self.proxy_auth,
            )
            self.__pool = proxy_manager.connection_from_host(
                host=self.realhost,
                port=self.realport,
                scheme=self.scheme,
                pool_kwargs=_pool_kwargs,
            )
        else:
            self.__pool = pool_class(self.host, self.port, **_pool_kwargs)

    def close(self):
        self.__resp and self.__resp.drain_conn()
        self.__resp and self.__resp.release_conn()
        self.__resp = None

    def read(self, sz):
        return self.__resp.read(sz)

    def isOpen(self):
        return self.__resp is not None

    def flush(self):

        # Pull data out of buffer that will be sent in this request
        data = self.__wbuf.getvalue()
        self.__wbuf = BytesIO()

        # Header handling

        headers = dict(self._headers)
        self.__auth_provider.add_headers(headers)
        self._headers = headers
        self.setCustomHeaders(self._headers)

        # Note: we don't set User-Agent explicitly in this class because PySQL
        # should always provide one. Unlike the original THttpClient class, our version
        # doesn't define a default User-Agent and so should raise an exception if one
        # isn't provided.
        assert self.__custom_headers and "User-Agent" in self.__custom_headers

        headers = {
            "Content-Type": "application/x-thrift",
            "Content-Length": str(len(data)),
        }

        if self.using_proxy() and self.scheme == "http" and self.proxy_auth is not None:
            headers.update(self.proxy_auth)

        if self.__custom_headers:
            custom_headers = {key: val for key, val in self.__custom_headers.items()}
            headers.update(**custom_headers)

        # HTTP request
        self.__resp = self.__pool.request(
            "POST",
            url=self.path,
            body=data,
            headers=headers,
            preload_content=False,
            timeout=self.__timeout,
            retries=self.retry_policy,
        )

        # Get reply to flush the request
        self.code = self.__resp.status
        self.message = self.__resp.reason
        self.headers = self.__resp.headers

        logger.info(
            "HTTP Response with status code {}, message: {}".format(
                self.code, self.message
            )
        )

    @staticmethod
    def basic_proxy_auth_headers(proxy):
        if proxy is None or not proxy.username:
            return None
        ap = "%s:%s" % (
            urllib.parse.unquote(proxy.username),
            urllib.parse.unquote(proxy.password),
        )
        return make_headers(proxy_basic_auth=ap)

    def set_retry_command_type(self, value: CommandType):
        """Pass the provided CommandType to the retry policy"""
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.command_type = value
        else:
            logger.warning(
                "DatabricksRetryPolicy is currently bypassed. The CommandType cannot be set."
            )

    def make_rest_request(
        self,
        method: str,
        endpoint_path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Make a REST API request using the existing connection pool.

        Args:
            method (str): HTTP method (GET, POST, DELETE, etc.)
            endpoint_path (str): API endpoint path (e.g., "sessions" or "statements/123")
            data (dict, optional): Request payload data
            params (dict, optional): Query parameters
            headers (dict, optional): Additional headers

        Returns:
            dict: Response data parsed from JSON

        Raises:
            RequestError: If the request fails
        """
        # Ensure the transport is open
        if not self.isOpen():
            self.open()

        # Prepare headers
        request_headers = {
            "Content-Type": "application/json",
        }

        # Add authentication headers
        auth_headers: Dict[str, str] = {}
        self.__auth_provider.add_headers(auth_headers)
        request_headers.update(auth_headers)

        # Add custom headers if provided
        if headers:
            request_headers.update(headers)

        # Prepare request body
        body = json.dumps(data).encode("utf-8") if data else None

        # Build query string for params
        query_string = ""
        if params:
            query_string = "?" + urllib.parse.urlencode(params)

        # Determine full path
        full_path = (
            self.path.rstrip("/") + "/" + endpoint_path.lstrip("/") + query_string
        )

        # Log request details (debug level)
        logger.debug(f"Making {method} request to {full_path}")

        try:
            # Make request using the connection pool
            logger.debug(f"making request to {full_path}")
            logger.debug(f"\trequest headers: {request_headers}")
            logger.debug(f"\trequest body: {body.decode('utf-8') if body else None}")
            logger.debug(f"\trequest params: {params}")
            logger.debug(f"\trequest full path: {full_path}")
            self.__resp = self.__pool.request(
                method,
                url=full_path,
                body=body,
                headers=request_headers,
                preload_content=False,
                timeout=self.__timeout,
                retries=self.retry_policy,
            )

            # Store response status and headers
            if self.__resp is not None:
                self.code = self.__resp.status
                self.message = self.__resp.reason
                self.headers = self.__resp.headers

                # Log response status
                logger.debug(f"Response status: {self.code}, message: {self.message}")

                # Read and parse response data
                # Note: urllib3's HTTPResponse has a data attribute, but it's not in the type stubs
                response_data = getattr(self.__resp, "data", None)

                # Check for HTTP errors
                self._check_rest_response_for_error(self.code, response_data)

                # Parse JSON response if there is content
                if response_data:
                    result = json.loads(response_data.decode("utf-8"))

                    # Log response content (truncated for large responses)
                    content_str = json.dumps(result)
                    logger.debug(f"Response content: {content_str}")

                    return result

                return {}
            else:
                raise ValueError("No response received from server")

        except urllib3.exceptions.HTTPError as e:
            error_message = f"REST HTTP request failed: {str(e)}"
            logger.error(error_message)
            from databricks.sql.exc import RequestError

            raise RequestError(error_message, e)

    def _check_rest_response_for_error(
        self, status_code: int, response_data: Optional[bytes]
    ) -> None:
        """
        Check if the REST response indicates an error and raise an appropriate exception.

        Args:
            status_code: HTTP status code
            response_data: Raw response data

        Raises:
            Various exceptions based on the error type
        """
        if status_code >= 400:
            error_message = f"REST HTTP request failed with status {status_code}"
            error_code = None
            
            # Try to extract error details from JSON response
            if response_data:
                try:
                    error_details = json.loads(response_data.decode("utf-8"))
                    if isinstance(error_details, dict):
                        if "message" in error_details:
                            error_message = f"{error_message}: {error_details['message']}"
                        if "error_code" in error_details:
                            error_code = error_details["error_code"]
                        elif "errorCode" in error_details:
                            error_code = error_details["errorCode"]
                    logger.error(
                        f"Request failed (status {status_code}): {error_details}"
                    )
                except (ValueError, KeyError):
                    # If we can't parse JSON, log raw content
                    content = response_data.decode("utf-8", errors="replace")
                    logger.error(f"Request failed (status {status_code}): {content}")
            else:
                logger.error(f"Request failed (status {status_code}): No response data")

            from databricks.sql.exc import (
                RequestError, 
                OperationalError, 
                DatabaseError,
                SessionAlreadyClosedError,
                CursorAlreadyClosedError,
                NonRecoverableNetworkError,
                UnsafeToRetryError
            )
            
            # Map HTTP status codes to appropriate exceptions
            if status_code == 429:
                # Rate limiting errors - similar to what ThriftDatabricksClient does
                retry_after = None
                if self.headers and "Retry-After" in self.headers:
                    retry_after = self.headers["Retry-After"]
                    
                rate_limit_msg = f"Maximum rate has been exceeded. Please reduce the rate of requests and try again"
                if retry_after:
                    rate_limit_msg += f" after {retry_after} seconds."
                raise RequestError(rate_limit_msg)
                
            elif status_code == 503:
                # Service unavailable errors
                raise OperationalError("TEMPORARILY_UNAVAILABLE: Service temporarily unavailable")
                
            elif status_code == 404:
                # Not found errors - could be session or operation already closed
                if error_message and "session" in error_message.lower():
                    raise SessionAlreadyClosedError("Session was closed by a prior request")
                elif error_message and ("operation" in error_message.lower() or "statement" in error_message.lower()):
                    raise CursorAlreadyClosedError("Operation was canceled by a prior request")
                else:
                    raise RequestError(error_message)
                    
            elif status_code == 401:
                # Authentication errors
                raise OperationalError("Authentication failed. Please check your credentials.")
                
            elif status_code == 403:
                # Permission errors
                raise OperationalError("Permission denied. You do not have access to this resource.")
                
            elif status_code == 400:
                # Bad request errors - often syntax errors
                if error_message and "syntax" in error_message.lower():
                    raise DatabaseError(f"Syntax error in SQL statement: {error_message}")
                else:
                    raise RequestError(error_message)
                    
            elif status_code == 501:
                # Not implemented errors
                raise NonRecoverableNetworkError(f"Not implemented: {error_message}")
                
            elif status_code == 502 or status_code == 504:
                # Bad gateway or gateway timeout errors
                # These are considered dangerous to retry for ExecuteStatement
                raise UnsafeToRetryError(f"Gateway error: {error_message}")
                
            else:
                # Generic errors
                raise RequestError(error_message)
