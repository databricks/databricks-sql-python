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

from typing import List
from enum import Enum
from databricks.sql.auth.authenticators import CredentialsProvider, \
    AccessTokenAuthProvider, BasicAuthProvider, DatabricksOAuthProvider
from databricks.sql.experimental.oauth_persistence import OAuthPersistence


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    # other supported types (access_token, user/pass) can be inferred
    # we can add more types as needed later


class ClientContext:
    def __init__(self,
                 hostname: str,
                 username: str = None,
                 password: str = None,
                 access_token: str = None,
                 auth_type: str = None,
                 oauth_scopes: List[str] = None,
                 oauth_client_id: str = None,
                 use_cert_as_auth: str = None,
                 tls_client_cert_file: str = None,
                 oauth_persistence=None
                 ):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.access_token = access_token
        self.auth_type = auth_type
        self.oauth_scopes = oauth_scopes
        self.oauth_client_id = oauth_client_id
        self.use_cert_as_auth = use_cert_as_auth
        self.tls_client_cert_file = tls_client_cert_file
        self.oauth_persistence = oauth_persistence


def get_auth_provider(cfg: ClientContext):
    if cfg.auth_type == AuthType.DATABRICKS_OAUTH.value:
        return DatabricksOAuthProvider(cfg.hostname, cfg.oauth_persistence, cfg.oauth_client_id, cfg.oauth_scopes)
    elif cfg.access_token is not None:
        return AccessTokenAuthProvider(cfg.access_token)
    elif cfg.username is not None and cfg.password is not None:
        return BasicAuthProvider(cfg.username, cfg.password)
    elif cfg.use_cert_as_auth and cfg.tls_client_cert_file:
        # no op authenticator. authentication is performed using ssl certificate outside of headers
        return CredentialsProvider()
    else:
        raise RuntimeError("No valid authentication settings!")


OAUTH_SCOPES = ["sql", "offline_access"]
# TODO: moderakh to be changed once registered on the service side
OAUTH_CLIENT_ID = "databricks-cli"


def get_python_sql_connector_auth_provider(hostname: str, oauth_persistence: OAuthPersistence, **kwargs):
    cfg = ClientContext(hostname=hostname,
                        auth_type=kwargs.get("auth_type"),
                        access_token=kwargs.get("access_token"),
                        username=kwargs.get("_username"),
                        password=kwargs.get("_password"),
                        use_cert_as_auth=kwargs.get("_use_cert_as_auth"),
                        tls_client_cert_file=kwargs.get("_tls_client_cert_file"),
                        oauth_scopes=OAUTH_SCOPES,
                        oauth_client_id=OAUTH_CLIENT_ID,
                        oauth_persistence=oauth_persistence)
    return get_auth_provider(cfg)


