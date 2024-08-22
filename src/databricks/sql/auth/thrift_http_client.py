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
