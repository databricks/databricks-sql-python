import logging
from typing import Dict


import thrift

import urllib.parse, six, base64

logger = logging.getLogger(__name__)

import ssl
import warnings

from io import BytesIO


from urllib3 import HTTPConnectionPool, HTTPSConnectionPool


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
    ):
        if port is not None:
            warnings.warn(
                "Please use the THttpClient('http{s}://host:port/path') constructor",
                DeprecationWarning,
                stacklevel=2)
            self.host = uri_or_host
            self.port = port
            assert path
            self.path = path
            self.scheme = 'http'
        else:
            parsed = urllib.parse.urlsplit(uri_or_host)
            self.scheme = parsed.scheme
            assert self.scheme in ('http', 'https')
            if self.scheme == 'https':
                self.certfile = cert_file
                self.keyfile = key_file
                self.context = ssl.create_default_context(cafile=cafile) if (cafile and not ssl_context) else ssl_context
            self.port = parsed.port
            self.host = parsed.hostname
            self.path = parsed.path
            if parsed.query:
                self.path += '?%s' % parsed.query
        try:
            proxy = urllib.request.getproxies()[self.scheme]
        except KeyError:
            proxy = None
        else:
            if urllib.request.proxy_bypass(self.host):
                proxy = None
        if proxy:
            parsed = urllib.parse.urlparse(proxy)
            self.realhost = self.host
            self.realport = self.port
            self.host = parsed.hostname
            self.port = parsed.port
            self.proxy_auth = self.basic_proxy_auth_header(parsed)
        else:
            self.realhost = self.realport = self.proxy_auth = None
        self.__wbuf = BytesIO()

        self.__http_response = None
        self.__timeout = None
        self.__custom_headers = None

        self.__auth_provider = auth_provider

    def setCustomHeaders(self, headers: Dict[str, str]):
        self._headers = headers
        super().setCustomHeaders(headers)

    def flush(self):


        """
        The original implementation makes these headers:

        - Content-Type
        - Content-Length
        - User-Agent: a default is used by thrift. But we don't need that default because we always write a user-agent.
            I can assert that user agent must be provided
        - Proxy-Authorization

        And then any custom headers.
        
        """

        # Pull data out of buffer that will be sent in this request
        data = self.__wbuf.getvalue()
        self.__wbuf = BytesIO()

        # Header handling

        # NOTE: self._headers is only ever changed by .setCustomHeaders. .setCustomHeaders is never called by thrift lib code


        # Pretty sure this line never has any effect
        headers = dict(self._headers)
        self.__auth_provider.add_headers(headers)
        self._headers = headers
        self.setCustomHeaders(self._headers)
        

        # Note: we don't set User-Agent explicitly in this class because PySQL
        # should always provide one. Unlike the original THttpClient class, our version
        # doesn't define a default User-Agent and so should raise an exception if one
        # isn't provided. 
        assert self.__custom_headers and 'User-Agent' in self.__custom_headers

        headers = {
            "Content-Type": "application/x-thrift",
            "Content-Length": str(len(data))
        }

        if self.using_proxy() and self.scheme == "http" and self.proxy_auth is not None:
            headers["Proxy-Authorization": self.proxy_auth]

        if self.__custom_headers:
            # Don't need to use six.iteritems because PySQL only supports Python 3
            custom_headers = {key: val for key, val in self.__custom_headers.items()}
            headers.update(**custom_headers)


        # Write payload
        self.__http.send(data)

        # HTTP request
        self.__resp = r = self.__pool.urlopen("POST", self.path, data, headers, preload_content=False)

        # Get reply to flush the request
        self.__http_response = self.__http.getresponse()
        self.code = self.__http_response.status
        self.message = self.__http_response.reason
        self.headers = self.__http_response.msg




        # # HTTP request (replace this since __pool.urlopen() doesn't use a .putrequest() method)
        # if self.using_proxy() and self.scheme == "http":
        #     # need full URL of real host for HTTP proxy here (HTTPS uses CONNECT tunnel)
        #     raise Exception("This subclass of thrift http transport doesn't support proxies yet.")
            
        #     # As part of resuing tcp connections we replaced __http with __pool
            
        #     self.__http.putrequest('POST', "http://%s:%s%s" %
        #                            (self.realhost, self.realport, self.path))
            
        # # else:
        # #     self.__http.putrequest('POST', self.path)

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
