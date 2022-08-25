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

import unittest

from databricks.sql.auth.auth import AccessTokenAuthProvider, BasicAuthProvider, CredentialsProvider
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.experimental.oauth_persistence import DevOnlyFilePersistence, OAuthToken
import tempfile
import os


class OAuthPersistenceTests(unittest.TestCase):

    def test_DevOnlyFilePersistence_read_my_write(self):
        with tempfile.TemporaryDirectory() as tempdir:
            test_json_file_path = os.path.join(tempdir, 'test.json')
            persistence_manager = DevOnlyFilePersistence(test_json_file_path)
            access_token = "abc#$%%^&^*&*()()_=-/"
            refresh_token = "#$%%^^&**()+)_gter243]xyz"
            token = OAuthToken(access_token=access_token, refresh_token=refresh_token)
            persistence_manager.persist(token)
            new_token = persistence_manager.read()

            self.assertEqual(new_token.get_access_token(), access_token)
            self.assertEqual(new_token.get_refresh_token(), refresh_token)

    def test_DevOnlyFilePersistence_file_does_not_exist(self):
        with tempfile.TemporaryDirectory() as tempdir:
            test_json_file_path = os.path.join(tempdir, 'test.json')
            persistence_manager = DevOnlyFilePersistence(test_json_file_path)
            new_token = persistence_manager.read()

            self.assertEqual(new_token, None)

    # TODO moderakh add test for file with invalid format
