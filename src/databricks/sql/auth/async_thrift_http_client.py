import base64
import logging
import urllib.parse
from typing import Dict, Union

import six
import thrift

logger = logging.getLogger(__name__)

import ssl
import warnings
from http.client import HTTPResponse
from io import BytesIO

import httpx

from databricks.sql.auth.retry import CommandType, DatabricksRetryPolicy


class THttpClient(thrift.transport.THttpClient.THttpClient):
    def __init__(
        self,
        auth_provider,
        uri_or_host,
        port=None,
        path=None,
        cafile=None,
        cert_file=None,
        key_file=None,
        ssl_context=None,
        max_connections: int = 1,
        retry_policy: Union[DatabricksRetryPolicy, int] = 0,
    ):
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
                self.certfile = cert_file
                self.keyfile = key_file
                self.context = (
                    ssl.create_default_context(cafile=cafile)
                    if (cafile and not ssl_context)
                    else ssl_context
                )
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
            self.proxy_auth = self.basic_proxy_auth_header(parsed)
        else:
            self.realhost = self.realport = self.proxy_auth = None

        self.max_connections = max_connections

        # If retry_policy == 0 then urllib3 will not retry automatically
        # this falls back to the pre-v3 behaviour where thrift_backend.py handles retry logic
        self.retry_policy = retry_policy

        self.__wbuf = BytesIO()
        self.__rbuf = None
        self.__resp: Union[None, httpx.Response] = None
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
        # pretty sure we need this to not default to 1 once I enable async because each request uses one connection
        limits = httpx.Limits(max_connections=self.max_connections)

        # httpx automatically handles http(s)
        # TODO: implement proxy handling (deferred as we don't have any e2e tests for it)
        # TODO: rename self._pool once POC works
        self.__pool = httpx.Client(limits=limits)

    def close(self):
        self.__resp and self.__resp.close()
        # must clear out buffer because thrift leaves stray bytes behind
        # equivalent to urllib3's .drain_conn() method
        self.__rbuf = None
        self.__resp = None

    def read(self, sz):
        return self.__rbuf.read(sz)

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

        if self.__custom_headers:
            custom_headers = {key: val for key, val in self.__custom_headers.items()}
            headers.update(**custom_headers)

        target_url_parts = (self.scheme, self.host, self.path, "", "")
        target_url = urllib.parse.urlunsplit(target_url_parts)
        # HTTP request
        self.__resp = self.__pool.request(
            "POST",
            url=target_url,
            content=data,
            headers=headers,
            timeout=self.__timeout,
        )

        self.__rbuf = BytesIO(self.__resp.read())

        # Get reply to flush the request
        self.code = self.__resp.status_code
        self.message = self.__resp.reason_phrase
        self.headers = self.__resp.headers

    @staticmethod
    def basic_proxy_auth_header(proxy):
        if proxy is None or not proxy.username:
            return None
        ap = "%s:%s" % (
            urllib.parse.unquote(proxy.username),
            urllib.parse.unquote(proxy.password),
        )
        cr = base64.b64encode(ap.encode()).strip()
        return "Basic " + six.ensure_str(cr)

    def set_retry_command_type(self, value: CommandType):
        """Pass the provided CommandType to the retry policy"""
        if isinstance(self.retry_policy, DatabricksRetryPolicy):
            self.retry_policy.command_type = value
        else:
            logger.warning(
                "DatabricksRetryPolicy is currently bypassed. The CommandType cannot be set."
            )
