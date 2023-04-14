import logging
from typing import Dict


import thrift

import urllib.parse, six, base64

logger = logging.getLogger(__name__)


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
        super().__init__(
            uri_or_host, port, path, cafile, cert_file, key_file, ssl_context
        )
        self.__auth_provider = auth_provider

    def setCustomHeaders(self, headers: Dict[str, str]):
        self._headers = headers
        super().setCustomHeaders(headers)

    def flush(self):
        headers = dict(self._headers)
        self.__auth_provider.add_headers(headers)
        self._headers = headers
        self.setCustomHeaders(self._headers)
        super().flush()

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
