import unittest
import pytest
from typing import Optional
from unittest.mock import patch

from databricks.sql.auth.auth import (
    AccessTokenAuthProvider,
    BasicAuthProvider,
    AuthProvider,
    ExternalAuthProvider,
    AuthType,
)
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.auth.oauth import OAuthManager
from databricks.sql.auth.authenticators import DatabricksOAuthProvider
from databricks.sql.auth.endpoint import (
    CloudType,
    InHouseOAuthEndpointCollection,
    AzureOAuthEndpointCollection,
)
from databricks.sql.auth.authenticators import CredentialsProvider, HeaderFactory
from databricks.sql.experimental.oauth_persistence import OAuthPersistenceCache


class Auth(unittest.TestCase):
    def test_access_token_provider(self):
        access_token = "aBc2"
        auth = AccessTokenAuthProvider(access_token=access_token)

        http_request = {"myKey": "myVal"}
        auth.add_headers(http_request)
        self.assertEqual(http_request["Authorization"], "Bearer aBc2")
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request["myKey"], "myVal")

    def test_basic_auth_provider(self):
        username = "moderakh"
        password = "Elevate Databricks 123!!!"
        auth = BasicAuthProvider(username=username, password=password)

        http_request = {"myKey": "myVal"}
        auth.add_headers(http_request)

        self.assertEqual(
            http_request["Authorization"],
            "Basic bW9kZXJha2g6RWxldmF0ZSBEYXRhYnJpY2tzIDEyMyEhIQ==",
        )
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request["myKey"], "myVal")

    def test_noop_auth_provider(self):
        auth = AuthProvider()

        http_request = {"myKey": "myVal"}
        auth.add_headers(http_request)

        self.assertEqual(len(http_request.keys()), 1)
        self.assertEqual(http_request["myKey"], "myVal")

    @patch.object(OAuthManager, "check_and_refresh_access_token")
    @patch.object(OAuthManager, "get_tokens")
    def test_oauth_auth_provider(self, mock_get_tokens, mock_check_and_refresh):
        client_id = "mock-id"
        scopes = ["offline_access", "sql"]
        access_token = "mock_token"
        refresh_token = "mock_refresh_token"
        mock_get_tokens.return_value = (access_token, refresh_token)
        mock_check_and_refresh.return_value = (access_token, refresh_token, False)

        params = [
            (
                CloudType.AWS,
                "foo.cloud.databricks.com",
                False,
                InHouseOAuthEndpointCollection,
                "offline_access sql",
            ),
            (
                CloudType.AZURE,
                "foo.1.azuredatabricks.net",
                True,
                AzureOAuthEndpointCollection,
                f"{AzureOAuthEndpointCollection.DATATRICKS_AZURE_APP}/user_impersonation offline_access",
            ),
            (
                CloudType.AZURE,
                "foo.1.azuredatabricks.net",
                False,
                InHouseOAuthEndpointCollection,
                "offline_access sql",
            ),
            (
                CloudType.GCP,
                "foo.gcp.databricks.com",
                False,
                InHouseOAuthEndpointCollection,
                "offline_access sql",
            ),
        ]

        for (
            cloud_type,
            host,
            use_azure_auth,
            expected_endpoint_type,
            expected_scopes,
        ) in params:
            with self.subTest(cloud_type.value):
                oauth_persistence = OAuthPersistenceCache()
                auth_provider = DatabricksOAuthProvider(
                    hostname=host,
                    oauth_persistence=oauth_persistence,
                    redirect_port_range=[8020],
                    client_id=client_id,
                    scopes=scopes,
                    auth_type=AuthType.AZURE_OAUTH.value
                    if use_azure_auth
                    else AuthType.DATABRICKS_OAUTH.value,
                )

                self.assertIsInstance(
                    auth_provider.oauth_manager.idp_endpoint, expected_endpoint_type
                )
                self.assertEqual(auth_provider.oauth_manager.port_range, [8020])
                self.assertEqual(auth_provider.oauth_manager.client_id, client_id)
                self.assertEqual(
                    oauth_persistence.read(host).refresh_token, refresh_token
                )
                mock_get_tokens.assert_called_with(hostname=host, scope=expected_scopes)

                headers = {}
                auth_provider.add_headers(headers)
                self.assertEqual(headers["Authorization"], f"Bearer {access_token}")

    def test_external_provider(self):
        class MyProvider(CredentialsProvider):
            def auth_type(self) -> str:
                return "mine"

            def __call__(self, *args, **kwargs) -> HeaderFactory:
                return lambda: {"foo": "bar"}

        auth = ExternalAuthProvider(MyProvider())

        http_request = {"myKey": "myVal"}
        auth.add_headers(http_request)
        self.assertEqual(http_request["foo"], "bar")
        self.assertEqual(len(http_request.keys()), 2)
        self.assertEqual(http_request["myKey"], "myVal")

    def test_get_python_sql_connector_auth_provider_access_token(self):
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {"access_token": "dpi123"}
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "AccessTokenAuthProvider")

        headers = {}
        auth_provider.add_headers(headers)
        self.assertEqual(headers["Authorization"], "Bearer dpi123")

    def test_get_python_sql_connector_auth_provider_external(self):
        class MyProvider(CredentialsProvider):
            def auth_type(self) -> str:
                return "mine"

            def __call__(self, *args, **kwargs) -> HeaderFactory:
                return lambda: {"foo": "bar"}

        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {"credentials_provider": MyProvider()}
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "ExternalAuthProvider")

        headers = {}
        auth_provider.add_headers(headers)
        self.assertEqual(headers["foo"], "bar")

    def test_get_python_sql_connector_auth_provider_username_password(self):
        username = "moderakh"
        password = "Elevate Databricks 123!!!"
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {"_username": username, "_password": password}
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "BasicAuthProvider")

        headers = {}
        auth_provider.add_headers(headers)
        self.assertEqual(
            headers["Authorization"],
            "Basic bW9kZXJha2g6RWxldmF0ZSBEYXRhYnJpY2tzIDEyMyEhIQ==",
        )

    def test_get_python_sql_connector_auth_provider_noop(self):
        tls_client_cert_file = "fake.cert"
        use_cert_as_auth = "abc"
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {
            "_tls_client_cert_file": tls_client_cert_file,
            "_use_cert_as_auth": use_cert_as_auth,
        }
        auth_provider = get_python_sql_connector_auth_provider(hostname, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "CredentialProvider")
