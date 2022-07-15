# Copyright 2022 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import thrift.transport.THttpClient

logger = logging.getLogger(__name__)


class THttpClient(thrift.transport.THttpClient.THttpClient):

    def __init__(self, auth_provider, uri_or_host, port=None, path=None, cafile=None, cert_file=None, key_file=None, ssl_context=None):
        super().__init__(uri_or_host, port, path, cafile, cert_file, key_file, ssl_context)
        self.__auth_provider = auth_provider

    def setCustomHeaders(self, headers):
        self._headers = headers
        super().setCustomHeaders(headers)

    def flush(self):
        # TODO retry behaviour
        headers = dict(self._headers)
        self.__auth_provider.add_headers(headers)
        self._headers = headers
        self.setCustomHeaders(self._headers)
        super().flush()