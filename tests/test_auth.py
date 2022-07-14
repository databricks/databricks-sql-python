import unittest

from databricks.sql.auth.auth import AccessTokenAuthProvider, BasicAuthProvider, CredentialsProvider


class Auth(unittest.TestCase):

    def test_access_token_provider(self):
        access_token = "aBc2"
        auth = AccessTokenAuthProvider(access_token=access_token)

        http_request = {'myKey': 'myVal'}
        auth.add_auth_token(http_request)
        self.assertEqual(http_request['Authorization'], 'Bearer aBc2')
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request['myKey'], 'myVal')

    def test_basic_auth_provider(self):
        username = "moderakh"
        password = "Elevate Databricks 123!!!"
        auth = BasicAuthProvider(username=username, password=password)

        http_request = {'myKey': 'myVal'}
        auth.add_auth_token(http_request)

        self.assertEqual(http_request['Authorization'], 'Basic bW9kZXJha2g6RWxldmF0ZSBEYXRhYnJpY2tzIDEyMyEhIQ==')
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request['myKey'], 'myVal')

    def test_noop_auth_provider(self):
        auth = CredentialsProvider()

        http_request = {'myKey': 'myVal'}
        auth.add_auth_token(http_request)

        self.assertEqual(len(http_request.keys()), 1)
        self.assertEqual(http_request['myKey'], 'myVal')