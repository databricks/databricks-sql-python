import base64
import logging
import urllib.parse
from typing import Dict, Union, Optional

import six
import thrift

import ssl
import warnings
from http.client import HTTPResponse
from io import BytesIO

from urllib3 import HTTPConnectionPool, HTTPSConnectionPool, ProxyManager
from urllib3.util import make_headers
from databricks.sql.auth.retry import CommandType, DatabricksRetryPolicy
from databricks.sql.types import SSLOptions
from databricks.sql.common.http_utils import (
    detect_and_parse_proxy,
    create_connection_pool,
)

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
        
        # Handle proxy settings using shared utility
        proxy_uri, proxy_auth = detect_and_parse_proxy(self.scheme, self.host)
        
        if proxy_uri:
            parsed_proxy = urllib.parse.urlparse(proxy_uri)
            # realhost and realport are the host and port of the actual request
            self.realhost = self.host
            self.realport = self.port
            # this is passed to ProxyManager
            self.proxy_uri: str = proxy_uri
            self.host = parsed_proxy.hostname
            self.port = parsed_proxy.port
            self.proxy_auth = proxy_auth
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
        """Initialize the connection pool using shared utility."""
        self.__pool = create_connection_pool(
            scheme=self.scheme,
            host=self.realhost if self.using_proxy() else self.host,
            port=self.realport if self.using_proxy() else self.port,
            ssl_options=self._ssl_options,
            proxy_uri=self.proxy_uri,
            proxy_headers=self.proxy_auth,
            retry_policy=self.retry_policy,
            max_connections=self.max_connections,
        )

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

    def using_proxy(self) -> bool:
        """Check if proxy is being used."""
        return self.realhost is not None

    def set_retry_command_type(self, value: CommandType):
        """Pass the provided CommandType to the retry policy"""
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.command_type = value
        else:
            logger.warning(
                "DatabricksRetryPolicy is currently bypassed. The CommandType cannot be set."
            )
