
import unittest

from databricks.sql.auth.auth import AccessTokenAuthProvider, BasicAuthProvider, AuthProvider
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
            persistence_manager.persist("https://randomserver", token)
            new_token = persistence_manager.read("https://randomserver")

            self.assertEqual(new_token.access_token, access_token)
            self.assertEqual(new_token.refresh_token, refresh_token)

    def test_DevOnlyFilePersistence_file_does_not_exist(self):
        with tempfile.TemporaryDirectory() as tempdir:
            test_json_file_path = os.path.join(tempdir, 'test.json')
            persistence_manager = DevOnlyFilePersistence(test_json_file_path)
            new_token = persistence_manager.read("https://randomserver")

            self.assertEqual(new_token, None)

    # TODO moderakh add test for file with invalid format (should return None)
