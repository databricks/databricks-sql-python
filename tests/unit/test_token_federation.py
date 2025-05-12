import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
import jwt

from databricks.sql.auth.token import Token
from databricks.sql.auth.token_federation import (
    DatabricksTokenFederationProvider,
    SimpleCredentialsProvider,
)
from databricks.sql.auth.oidc_utils import OIDCDiscoveryUtil


# Tests for Token class
class TestToken:
    """Tests for the Token class."""

    def test_token_initialization_and_properties(self):
        """Test Token initialization, properties and methods."""
        # Test with minimum required parameters plus expiry
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        token = Token("access_token_value", "Bearer", expiry=future)
        assert token.access_token == "access_token_value"
        assert token.token_type == "Bearer"
        assert token.refresh_token == ""
        assert token.expiry == future
        assert token.is_valid()

        # Test expired token
        past = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        expired_token = Token("expired", "Bearer", expiry=past)
        assert not expired_token.is_valid()

        # Test almost expired token (will expire within buffer)
        almost_expired = datetime.now(tz=timezone.utc) + timedelta(
            seconds=5
        )  # Less than MIN_VALIDITY_BUFFER
        almost_token = Token("almost", "Bearer", expiry=almost_expired)
        assert not almost_token.is_valid()  # Not valid due to buffer

        # Test string representation
        assert str(token) == "Bearer access_token_value"


# Tests for SimpleCredentialsProvider
class TestSimpleCredentialsProvider:
    """Tests for the SimpleCredentialsProvider class."""

    def test_provider_initialization(self):
        """Test initialization and methods of SimpleCredentialsProvider."""
        provider = SimpleCredentialsProvider("token1", "Bearer", "token")
        assert provider.auth_type() == "token"

        # Test header factory
        header_factory = provider()
        headers = header_factory()
        assert headers == {"Authorization": "Bearer token1"}


# Tests for OIDCDiscoveryUtil
class TestOIDCDiscoveryUtil:
    """Tests for the OIDCDiscoveryUtil class."""

    def test_discover_token_endpoint(self):
        """Test token endpoint creation for Databricks workspaces."""
        # Test with different hostname formats
        # Without protocol and without trailing slash
        token_endpoint = OIDCDiscoveryUtil.discover_token_endpoint("databricks.com")
        assert token_endpoint == "https://databricks.com/oidc/v1/token"

        # With protocol but without trailing slash
        token_endpoint = OIDCDiscoveryUtil.discover_token_endpoint(
            "https://databricks.com"
        )
        assert token_endpoint == "https://databricks.com/oidc/v1/token"

        # With protocol and trailing slash
        token_endpoint = OIDCDiscoveryUtil.discover_token_endpoint(
            "https://databricks.com/"
        )
        assert token_endpoint == "https://databricks.com/oidc/v1/token"

    def test_format_hostname(self):
        """Test hostname formatting."""
        # Without protocol and without trailing slash
        assert (
            OIDCDiscoveryUtil.format_hostname("databricks.com")
            == "https://databricks.com/"
        )

        # With protocol but without trailing slash
        assert (
            OIDCDiscoveryUtil.format_hostname("https://databricks.com")
            == "https://databricks.com/"
        )

        # With protocol and trailing slash
        assert (
            OIDCDiscoveryUtil.format_hostname("https://databricks.com/")
            == "https://databricks.com/"
        )


# Tests for DatabricksTokenFederationProvider
class TestDatabricksTokenFederationProvider:
    """Tests for the DatabricksTokenFederationProvider class."""

    @pytest.fixture
    def mock_credentials_provider(self):
        """Fixture for a mock credentials provider."""
        provider = MagicMock()
        provider.auth_type.return_value = "mock_auth_type"
        header_factory = MagicMock()
        header_factory.return_value = {"Authorization": "Bearer mock_token"}
        provider.return_value = header_factory
        return provider

    @pytest.fixture
    def federation_provider(self, mock_credentials_provider):
        """Fixture for a token federation provider."""
        return DatabricksTokenFederationProvider(
            mock_credentials_provider, "databricks.com", "client_id"
        )

    @pytest.fixture
    def mock_discover_token_endpoint(self):
        """Fixture for mocking OIDCDiscoveryUtil.discover_token_endpoint."""
        with patch(
            "databricks.sql.auth.oidc_utils.OIDCDiscoveryUtil.discover_token_endpoint"
        ) as mock:
            mock.return_value = "https://databricks.com/oidc/v1/token"
            yield mock

    @pytest.fixture
    def mock_parse_jwt_claims(self):
        """Fixture for mocking _parse_jwt_claims."""
        with patch(
            "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._parse_jwt_claims"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_exchange_token(self):
        """Fixture for mocking _exchange_token."""
        with patch(
            "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._exchange_token"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_is_same_host(self):
        """Fixture for mocking _is_same_host."""
        with patch(
            "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._is_same_host"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_request_post(self):
        """Fixture for mocking requests.post."""
        with patch("databricks.sql.auth.token_federation.requests.post") as mock:
            yield mock

    def test_host_and_auth_type(self, federation_provider):
        """Test the host property and auth_type of DatabricksTokenFederationProvider."""
        assert federation_provider.host == "databricks.com"
        assert federation_provider.hostname == "databricks.com"
        assert federation_provider.auth_type() == "mock_auth_type"

    def test_is_same_host(self, federation_provider):
        """Test the _is_same_host method with various URL combinations."""
        # Same host
        assert federation_provider._is_same_host(
            "https://databricks.com", "https://databricks.com"
        )
        # Different host
        assert not federation_provider._is_same_host(
            "https://databricks.com", "https://different.com"
        )
        # Same host with paths
        assert federation_provider._is_same_host(
            "https://databricks.com/path", "https://databricks.com/other"
        )
        # Missing protocol
        assert federation_provider._is_same_host(
            "databricks.com", "https://databricks.com"
        )

    def test_extract_token_info_from_header(self, federation_provider):
        """Test _extract_token_info_from_header with valid and invalid headers."""
        # Valid headers
        assert federation_provider._extract_token_info_from_header(
            {"Authorization": "Bearer token"}
        ) == ("Bearer", "token")

        assert federation_provider._extract_token_info_from_header(
            {"Authorization": "CustomType token"}
        ) == ("CustomType", "token")

        # Invalid headers
        with pytest.raises(ValueError):
            federation_provider._extract_token_info_from_header({})

        with pytest.raises(ValueError):
            federation_provider._extract_token_info_from_header({"Authorization": ""})

        with pytest.raises(ValueError):
            federation_provider._extract_token_info_from_header(
                {"Authorization": "Bearer"}
            )

    def test_token_reuse(
        self,
        federation_provider,
        mock_exchange_token,
    ):
        """Test token reuse when token is still valid."""
        # Set up the initial token
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        initial_token = Token("exchanged_token", "Bearer", expiry=future_time)
        federation_provider.current_token = initial_token
        federation_provider.external_headers = {
            "Authorization": "Bearer external_token"
        }

        # Get headers and verify the token is reused without calling exchange
        headers = federation_provider.get_auth_headers()
        assert headers["Authorization"] == "Bearer exchanged_token"
        # Verify exchange was not called
        mock_exchange_token.assert_not_called()

    def test_refresh_token_method(
        self,
        federation_provider,
        mock_parse_jwt_claims,
        mock_exchange_token,
        mock_is_same_host,
        mock_discover_token_endpoint,
    ):
        """Test the refactored refresh_token method for both exchange and non-exchange cases."""
        # CASE 1: Token from different host (needs exchange)
        # Set up mocks
        mock_parse_jwt_claims.return_value = {
            "iss": "https://login.microsoftonline.com/tenant"
        }
        mock_is_same_host.return_value = False

        # Set up headers that the credentials provider will return
        headers = {"Authorization": "Bearer test_token"}
        header_factory = MagicMock()
        header_factory.return_value = headers

        # Configure the credentials provider
        mock_creds_provider = MagicMock()
        mock_creds_provider.return_value = header_factory
        federation_provider.credentials_provider = mock_creds_provider

        # Configure the mock token exchange
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        mock_exchange_token.return_value = Token(
            "exchanged_token", "Bearer", expiry=future_time
        )

        # Call the refresh_token method
        token = federation_provider.refresh_token()

        # Verify the token was exchanged
        mock_exchange_token.assert_called_with("test_token")
        assert token.access_token == "exchanged_token"
        assert token == federation_provider.current_token

        # CASE 2: Token from same host (no exchange needed)
        mock_is_same_host.return_value = True
        mock_exchange_token.reset_mock()

        # Mock the JWT expiry extraction
        expiry_time = datetime.now(tz=timezone.utc) + timedelta(hours=2)
        with patch(
            "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._get_expiry_from_jwt",
            return_value=expiry_time,
        ):
            # Call refresh_token again
            token = federation_provider.refresh_token()

            # Verify no exchange was performed
            mock_exchange_token.assert_not_called()
            # Verify token was created directly
            assert token.access_token == "test_token"
            assert token.expiry == expiry_time

    def test_call_method_returns_auth_headers_directly(
        self,
        federation_provider,
        mock_discover_token_endpoint,
    ):
        """Test that __call__ directly returns the get_auth_headers method."""
        # Mock get_auth_headers to verify it's called directly
        with patch.object(
            federation_provider,
            "get_auth_headers",
            return_value={"Authorization": "Bearer test_auth"},
        ) as mock_get_auth:
            # Get the header factory from __call__
            result = federation_provider()

            # In our refactored implementation, __call__ returns get_auth_headers directly
            assert result is federation_provider.get_auth_headers

            # Now call the result and verify it returns what get_auth_headers returns
            headers = result()
            assert headers == {"Authorization": "Bearer test_auth"}
            mock_get_auth.assert_called_once()

    def test_get_expiry_from_jwt(self, federation_provider):
        """Test extracting expiry from JWT token."""
        # Create a JWT token with expiry
        expiry_timestamp = int(
            (datetime.now(tz=timezone.utc) + timedelta(hours=1)).timestamp()
        )
        payload = {
            "exp": expiry_timestamp,
            "iat": int(datetime.now(tz=timezone.utc).timestamp()),
            "sub": "test-subject",
        }

        # Create JWT token
        token = jwt.encode(payload, "secret", algorithm="HS256")

        # Test the method
        expiry = federation_provider._get_expiry_from_jwt(token)

        # Verify the expiry is extracted correctly
        assert expiry is not None
        assert isinstance(expiry, datetime)
        assert expiry.tzinfo is not None  # Should be timezone-aware
        assert (
            abs(
                (
                    expiry - datetime.fromtimestamp(expiry_timestamp, tz=timezone.utc)
                ).total_seconds()
            )
            < 1
        )  # Allow for small rounding differences

        # Test with invalid token
        expiry = federation_provider._get_expiry_from_jwt("invalid-token")
        assert expiry is None

        # Test with token missing expiry
        payload = {"sub": "test-subject"}
        token_without_exp = jwt.encode(payload, "secret", algorithm="HS256")
        expiry = federation_provider._get_expiry_from_jwt(token_without_exp)
        assert expiry is None

    def test_exchange_token(
        self, federation_provider, mock_request_post, mock_discover_token_endpoint
    ):
        """Test the _exchange_token method with success and failure cases."""
        # SUCCESS CASE
        # Mock the response data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_token",
            "token_type": "Bearer",
            "refresh_token": "refresh_value",
            "expires_in": 3600,
        }
        mock_request_post.return_value = mock_response

        # Set the token endpoint
        federation_provider.token_endpoint = "https://databricks.com/oidc/v1/token"

        # Call the method
        token = federation_provider._exchange_token("original_token")

        # Verify the token was created correctly
        assert token.access_token == "new_token"
        assert token.token_type == "Bearer"
        assert token.refresh_token == "refresh_value"
        # Expiry should be around 1 hour in the future
        assert token.expiry > datetime.now(tz=timezone.utc)
        assert token.expiry < datetime.now(tz=timezone.utc) + timedelta(seconds=3601)

        # FAILURE CASE
        # Mock the response data for failure
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_request_post.return_value = mock_response

        # Call the method and expect an exception
        with pytest.raises(
            ValueError, match="Token exchange failed with status code 401"
        ):
            federation_provider._exchange_token("original_token")
