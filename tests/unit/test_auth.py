import unittest
import pytest
from unittest.mock import patch, MagicMock
import jwt
from databricks.sql.auth.auth import (
    AccessTokenAuthProvider,
    AuthProvider,
    ExternalAuthProvider,
    AuthType,
)
import time
from databricks.sql.auth.auth import (
    get_python_sql_connector_auth_provider,
    PYSQL_OAUTH_CLIENT_ID,
)
from databricks.sql.auth.oauth import OAuthManager, Token, ClientCredentialsTokenSource
from databricks.sql.auth.authenticators import (
    DatabricksOAuthProvider,
    AzureServicePrincipalCredentialProvider,
)
from databricks.sql.auth.endpoint import (
    CloudType,
    InHouseOAuthEndpointCollection,
    AzureOAuthEndpointCollection,
)
from databricks.sql.auth.authenticators import CredentialsProvider, HeaderFactory
from databricks.sql.experimental.oauth_persistence import OAuthPersistenceCache
import json


class Auth(unittest.TestCase):
    def test_access_token_provider(self):
        access_token = "aBc2"
        auth = AccessTokenAuthProvider(access_token=access_token)

        http_request = {"myKey": "myVal"}
        auth.add_headers(http_request)
        self.assertEqual(http_request["Authorization"], "Bearer aBc2")
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
                mock_http_client = MagicMock()
                auth_provider = DatabricksOAuthProvider(
                    hostname=host,
                    oauth_persistence=oauth_persistence,
                    redirect_port_range=[8020],
                    client_id=client_id,
                    scopes=scopes,
                    http_client=mock_http_client,
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
        mock_http_client = MagicMock()
        auth_provider = get_python_sql_connector_auth_provider(hostname, mock_http_client, **kwargs)
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
        mock_http_client = MagicMock()
        auth_provider = get_python_sql_connector_auth_provider(hostname, mock_http_client, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "ExternalAuthProvider")

        headers = {}
        auth_provider.add_headers(headers)
        self.assertEqual(headers["foo"], "bar")

    def test_get_python_sql_connector_auth_provider_noop(self):
        tls_client_cert_file = "fake.cert"
        use_cert_as_auth = "abc"
        hostname = "moderakh-test.cloud.databricks.com"
        kwargs = {
            "_tls_client_cert_file": tls_client_cert_file,
            "_use_cert_as_auth": use_cert_as_auth,
        }
        mock_http_client = MagicMock()
        auth_provider = get_python_sql_connector_auth_provider(hostname, mock_http_client, **kwargs)
        self.assertTrue(type(auth_provider).__name__, "CredentialProvider")

    def test_get_python_sql_connector_basic_auth(self):
        kwargs = {
            "username": "username",
            "password": "password",
        }
        mock_http_client = MagicMock()
        with self.assertRaises(ValueError) as e:
            get_python_sql_connector_auth_provider("foo.cloud.databricks.com", mock_http_client, **kwargs)
        self.assertIn(
            "Username/password authentication is no longer supported", str(e.exception)
        )

    @patch.object(DatabricksOAuthProvider, "_initial_get_token")
    def test_get_python_sql_connector_default_auth(self, mock__initial_get_token):
        hostname = "foo.cloud.databricks.com"
        mock_http_client = MagicMock()
        auth_provider = get_python_sql_connector_auth_provider(hostname, mock_http_client)
        self.assertTrue(type(auth_provider).__name__, "DatabricksOAuthProvider")
        self.assertTrue(auth_provider._client_id, PYSQL_OAUTH_CLIENT_ID)


class TestClientCredentialsTokenSource:
    @pytest.fixture
    def indefinite_token(self):
        secret_key = "mysecret"
        expires_in_100_years = int(time.time()) + (100 * 365 * 24 * 60 * 60)

        payload = {"sub": "user123", "role": "admin", "exp": expires_in_100_years}

        access_token = jwt.encode(payload, secret_key, algorithm="HS256")
        return Token(access_token, "Bearer", "refresh_token")

    @pytest.fixture
    def http_response(self):
        def status_response(response_status_code):
            mock_response = MagicMock()
            mock_response.status_code = response_status_code
            mock_response.json.return_value = {
                "access_token": "abc123",
                "token_type": "Bearer",
                "refresh_token": None,
            }
            return mock_response

        return status_response

    @pytest.fixture
    def token_source(self):
        mock_http_client = MagicMock()
        return ClientCredentialsTokenSource(
            token_url="https://token_url.com",
            client_id="client_id",
            client_secret="client_secret",
            http_client=mock_http_client,
        )

    def test_no_token_refresh__when_token_is_not_expired(
        self, token_source, indefinite_token
    ):
        with patch.object(token_source, "refresh") as mock_get_token:
            mock_get_token.return_value = indefinite_token

            # Mulitple calls for token
            token1 = token_source.get_token()
            token2 = token_source.get_token()
            token3 = token_source.get_token()

            assert token1 == token2 == token3
            assert token1.access_token == indefinite_token.access_token
            assert token1.token_type == indefinite_token.token_type
            assert token1.refresh_token == indefinite_token.refresh_token

            # should refresh only once as token is not expired
            assert mock_get_token.call_count == 1

    def test_get_token_success(self, token_source, http_response):
        mock_http_client = MagicMock()
        
        with patch.object(token_source, "_http_client", mock_http_client):
            # Create a mock response with the expected format
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.data.decode.return_value = '{"access_token": "abc123", "token_type": "Bearer", "refresh_token": null}'
            
            # Mock the request method to return the response directly
            mock_http_client.request.return_value = mock_response
            
            token = token_source.get_token()

            # Assert
            assert isinstance(token, Token)
            assert token.access_token == "abc123"
            assert token.token_type == "Bearer"
            assert token.refresh_token is None

    def test_get_token_failure(self, token_source, http_response):
        mock_http_client = MagicMock()
        
        with patch.object(token_source, "_http_client", mock_http_client):
            # Create a mock response with error
            mock_response = MagicMock()
            mock_response.status = 400
            mock_response.data.decode.return_value = "Bad Request"
            
            # Mock the request method to return the response directly
            mock_http_client.request.return_value = mock_response
            
            with pytest.raises(Exception) as e:
                token_source.get_token()
            assert "Failed to get token: 400" in str(e.value)


class TestAzureServicePrincipalCredentialProvider:
    @pytest.fixture
    def credential_provider(self):
        return AzureServicePrincipalCredentialProvider(
            hostname="hostname",
            azure_client_id="client_id",
            azure_client_secret="client_secret",
            http_client=MagicMock(),
            azure_tenant_id="tenant_id",
        )

    def test_provider_credentials(self, credential_provider):

        test_token = Token("access_token", "Bearer", "refresh_token")

        with patch.object(
            credential_provider, "get_token_source"
        ) as mock_get_token_source:
            mock_get_token_source.return_value = MagicMock()
            mock_get_token_source.return_value.get_token.return_value = test_token

            headers = credential_provider()()

            assert headers["Authorization"] == f"Bearer {test_token.access_token}"
            assert (
                headers["X-Databricks-Azure-SP-Management-Token"]
                == test_token.access_token
            )
