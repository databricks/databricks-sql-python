import unittest

from databricks.sql.auth.auth import AccessTokenAuthProvider, BasicAuthProvider, AuthProvider
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider


class Auth(unittest.TestCase):

    def test_access_token_provider(self):
        access_token = "aBc2"
        auth = AccessTokenAuthProvider(access_token=access_token)

        http_request = {'myKey': 'myVal'}
        auth.add_headers(http_request)
        self.assertEqual(http_request['Authorization'], 'Bearer aBc2')
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request['myKey'], 'myVal')

    def test_basic_auth_provider(self):
        username = "moderakh"
        password = "Elevate Databricks 123!!!"
        auth = BasicAuthProvider(username=username, password=password)

        http_request = {'myKey': 'myVal'}
        auth.add_headers(http_request)

        self.assertEqual(http_request['Authorization'], 'Basic bW9kZXJha2g6RWxldmF0ZSBEYXRhYnJpY2tzIDEyMyEhIQ==')
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request['myKey'], 'myVal')

    def test_noop_auth_provider(self):
        auth = AuthProvider()

        http_request = {'myKey': 'myVal'}
        auth.add_headers(http_request)

        self.assertEqual(len(http_request.keys()), 1)
        self.assertEqual(http_request['myKey'], 'myVal')

    def test_get_python_sql_connector_auth_provider_access_token(self):
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {'access_token': 'dpi123'}
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "AccessTokenAuthProvider")

        headers = {}
        auth_provider.add_headers(headers)
        self.assertEqual(headers['Authorization'], 'Bearer dpi123')

    def test_get_python_sql_connector_auth_provider_username_password(self):
        username = "moderakh"
        password = "Elevate Databricks 123!!!"
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {'_username': username, '_password': password}
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "BasicAuthProvider")

        headers = {}
        auth_provider.add_headers(headers)
        self.assertEqual(headers['Authorization'], 'Basic bW9kZXJha2g6RWxldmF0ZSBEYXRhYnJpY2tzIDEyMyEhIQ==')

    def test_get_python_sql_connector_auth_provider_noop(self):
        tls_client_cert_file = "fake.cert"
        use_cert_as_auth = "abc"
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {'_tls_client_cert_file': tls_client_cert_file, '_use_cert_as_auth': use_cert_as_auth}
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "CredentialProvider")
