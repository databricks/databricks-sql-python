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


from databricks.sql.auth.oauth import get_tokens, check_and_refresh_access_token
import base64


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class CredentialsProvider:
    def add_auth_token(self, request_headers):
        pass


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class AccessTokenAuthProvider(CredentialsProvider):
    def __init__(self, access_token):
        self.__authorization_header_value = "Bearer {}".format(access_token)

    def add_auth_token(self, request_headers):
        request_headers['Authorization'] = self.__authorization_header_value


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class BasicAuthProvider(CredentialsProvider):
    def __init__(self, username, password):
        auth_credentials = "{username}:{password}".format(username, password).encode("UTF-8")
        auth_credentials_base64 = base64.standard_b64encode(auth_credentials).decode("UTF-8")

        self.__authorization_header_value = "Basic {}".format(auth_credentials_base64)

    def add_auth_token(self, request_headers):
        request_headers['Authorization'] = self.__authorization_header_value


# Private API: this is an evolving interface and it will change in the future.
# Please must not depend on it in your applications.
class DatabricksOAuthProvider(CredentialsProvider):
    # TODO: moderakh the refresh_token is only kept in memory. not saved on disk
    # hence if application restarts the user may need to re-authenticate
    # I will add support for this outside of the scope of current PR.
    def __init__(self, hostname, client_id, scopes):
        self._hostname = self._normalize_host_name(hostname=hostname)
        self._scopes_as_str = ''.join(scopes)
        access_token, refresh_token = get_tokens(hostname=self._hostname, client_id=client_id, scope=self._scopes_as_str)
        self._access_token = access_token
        self._refresh_token = refresh_token

    def add_auth_token(self, request_headers):
        check_and_refresh_access_token(hostname=self._hostname,
                                       access_token=self._access_token,
                                       refresh_token=self._refresh_token)
        request_headers['Authorization'] = "Bearer {}".format(self._access_token)

    @staticmethod
    def _normalize_host_name(hostname):
        maybe_scheme = "https://" if not hostname.startswith("https://") else ""
        maybe_trailing_slash = "/" if not hostname.endswith("/") else ""
        return "{scheme}{host}{trailing}".format(
            scheme=maybe_scheme, host=hostname, trailing=maybe_trailing_slash)



