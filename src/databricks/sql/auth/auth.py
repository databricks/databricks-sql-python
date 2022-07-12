# Databricks CLI
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

from enum import Enum
from databricks.sql.auth.authenticators import Authenticator, \
    AccessTokenAuthenticator, UserPassAuthenticator, OAuthAuthenticator


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    # other supported types (access_token, user/pass) can be inferred
    # we can add more types as needed later


class ClientContext:
    def __init__(self,
                 hostname: str,
                 username: str = None,
                 password: str = None,
                 token: str = None,
                 auth_type: str = None,
                 oauth_scopes: str = None,
                 oauth_client_id: str = None,
                 use_cert_as_auth: str = None,
                 tls_client_cert_file: str = None):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.token = token
        self.auth_type = auth_type
        self.oauth_scopes = oauth_scopes
        self.oauth_client_id = oauth_client_id
        self.use_cert_as_auth = use_cert_as_auth
        self.tls_client_cert_file = tls_client_cert_file


class SqlConnectorClientContext(ClientContext):
    def __init__(self,
                 hostname: str,
                 username: str = None,
                 password: str = None,
                 access_token: str = None,
                 auth_type: str = None,
                 use_cert_as_auth: str = None,
                 tls_client_cert_file: str = None):
        super().__init__(oauth_scopes="sql offline_access",
                         # to be changed once registered on the service side
                         oauth_client_id="databricks-cli",
                         hostname=hostname,
                         username=username,
                         password=password,
                         token=access_token,
                         auth_type=auth_type,
                         use_cert_as_auth=use_cert_as_auth,
                         tls_client_cert_file=tls_client_cert_file)


def get_authenticator(cfg: ClientContext):
    if cfg.auth_type == AuthType.DATABRICKS_OAUTH.value:
        return OAuthAuthenticator(cfg.hostname, cfg.oauth_client_id, cfg.oauth_scopes)
    elif cfg.token is not None:
        return AccessTokenAuthenticator(cfg.token)
    elif cfg.username is not None and cfg.password is not None:
        return UserPassAuthenticator(cfg.username, cfg.password)
    elif cfg.use_cert_as_auth and cfg.tls_client_cert_file:
        # no op authenticator. authentication is performed using ssl certificate outside of headers
        return Authenticator()
    else:
        raise RuntimeError("No valid authentication settings!")


def get_python_sql_connector_authenticator(hostname: str, **kwargs):

    cfg = SqlConnectorClientContext(hostname=hostname,
                                    auth_type=kwargs.get("auth_type"),
                                    access_token=kwargs.get("access_token"),
                                    username=kwargs.get("_username"),
                                    password=kwargs.get("_password"),
                                    use_cert_as_auth=kwargs.get("_use_cert_as_auth"),
                                    tls_client_cert_file=kwargs.get("_tls_client_cert_file"))
    return get_authenticator(cfg)


