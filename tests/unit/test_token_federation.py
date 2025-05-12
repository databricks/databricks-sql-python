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


@pytest.fixture
def future_time():
    """Fixture providing a future time for token expiry."""
    return datetime.now(tz=timezone.utc) + timedelta(hours=1)


@pytest.fixture
def valid_token(future_time):
    """Fixture providing a valid token."""
    return Token("access_token_value", "Bearer", expiry=future_time)


class TestToken:
    """Tests for the Token class."""

    def test_valid_token_properties(self, future_time):
        """Test that a valid token has the expected properties."""
        # Create token with future expiry
        token = Token("access_token_value", "Bearer", expiry=future_time)

        # Verify properties
        assert token.access_token == "access_token_value"
        assert token.token_type == "Bearer"
        assert token.refresh_token == ""
        assert token.expiry == future_time
        assert token.is_valid()
        assert str(token) == "Bearer access_token_value"

    def test_expired_token_is_invalid(self):
        """Test that an expired token is recognized as invalid."""
        past_time = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        token = Token("expired", "Bearer", expiry=past_time)

        assert not token.is_valid()

    def test_almost_expired_token_is_invalid(self):
        """Test that a token about to expire is recognized as invalid."""
        almost_expired = datetime.now(tz=timezone.utc) + timedelta(
            seconds=5
        )  # Less than MIN_VALIDITY_BUFFER
        token = Token("almost", "Bearer", expiry=almost_expired)

        assert not token.is_valid()


class TestSimpleCredentialsProvider:
    """Tests for the SimpleCredentialsProvider class."""

    def test_provider_initialization_and_headers(self):
        """Test SimpleCredentialsProvider initialization and header generation."""
        provider = SimpleCredentialsProvider("token1", "Bearer", "token")

        # Check auth type
        assert provider.auth_type() == "token"

        # Check header generation
        headers = provider()()
        assert headers == {"Authorization": "Bearer token1"}


class TestOIDCDiscoveryUtil:
    """Tests for the OIDCDiscoveryUtil class."""

    @pytest.mark.parametrize(
        "hostname,expected",
        [
            # Without protocol and without trailing slash
            ("databricks.com", "https://databricks.com/oidc/v1/token"),
            # With protocol but without trailing slash
            ("https://databricks.com", "https://databricks.com/oidc/v1/token"),
            # With protocol and trailing slash
            ("https://databricks.com/", "https://databricks.com/oidc/v1/token"),
        ],
    )
    def test_discover_token_endpoint(self, hostname, expected):
        """Test token endpoint creation for various hostname formats."""
        token_endpoint = OIDCDiscoveryUtil.discover_token_endpoint(hostname)
        assert token_endpoint == expected

    @pytest.mark.parametrize(
        "hostname,expected",
        [
            # Without protocol and without trailing slash
            ("databricks.com", "https://databricks.com/"),
            # With protocol but without trailing slash
            ("https://databricks.com", "https://databricks.com/"),
            # With protocol and trailing slash
            ("https://databricks.com/", "https://databricks.com/"),
        ],
    )
    def test_format_hostname(self, hostname, expected):
        """Test hostname formatting with various input formats."""
        formatted = OIDCDiscoveryUtil.format_hostname(hostname)
        assert formatted == expected


class TestDatabricksTokenFederationProvider:
    """Tests for the DatabricksTokenFederationProvider class."""

    # ==== Fixtures ====
    @pytest.fixture
    def mock_credentials_provider(self):
        """Fixture providing a mock credentials provider."""
        provider = MagicMock()
        provider.auth_type.return_value = "mock_auth_type"
        header_factory = MagicMock()
        header_factory.return_value = {"Authorization": "Bearer mock_token"}
        provider.return_value = header_factory
        return provider

    @pytest.fixture
    def federation_provider(self, mock_credentials_provider):
        """Fixture providing a token federation provider with mocked dependencies."""
        provider = DatabricksTokenFederationProvider(
            mock_credentials_provider, "databricks.com", "client_id"
        )
        # Initialize token endpoint to avoid discovery during tests
        provider.token_endpoint = "https://databricks.com/oidc/v1/token"
        return provider

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies of the federation provider."""
        with patch(
            "databricks.sql.auth.oidc_utils.OIDCDiscoveryUtil.discover_token_endpoint",
            return_value="https://databricks.com/oidc/v1/token",
        ) as mock_discover:
            with patch(
                "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._parse_jwt_claims"
            ) as mock_parse_jwt:
                with patch(
                    "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._exchange_token"
                ) as mock_exchange:
                    with patch(
                        "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._is_same_host"
                    ) as mock_is_same_host:
                        with patch(
                            "databricks.sql.auth.token_federation.requests.post"
                        ) as mock_post:
                            yield {
                                "discover": mock_discover,
                                "parse_jwt": mock_parse_jwt,
                                "exchange": mock_exchange,
                                "is_same_host": mock_is_same_host,
                                "post": mock_post,
                            }

    # ==== Basic functionality tests ====
    def test_provider_initialization(self, federation_provider):
        """Test basic provider initialization and properties."""
        assert federation_provider.host == "databricks.com"
        assert federation_provider.hostname == "databricks.com"
        assert federation_provider.auth_type() == "mock_auth_type"

    # ==== Utility method tests ====
    @pytest.mark.parametrize(
        "url1,url2,expected",
        [
            # Same host with same protocol
            ("https://databricks.com", "https://databricks.com", True),
            # Different hosts
            ("https://databricks.com", "https://different.com", False),
            # Same host with different paths
            ("https://databricks.com/path", "https://databricks.com/other", True),
            # Same host with missing protocol
            ("databricks.com", "https://databricks.com", True),
        ],
    )
    def test_is_same_host(self, federation_provider, url1, url2, expected):
        """Test host comparison logic with various URL formats."""
        assert federation_provider._is_same_host(url1, url2) is expected

    @pytest.mark.parametrize(
        "headers,expected_result,should_raise",
        [
            # Valid Bearer token
            ({"Authorization": "Bearer token"}, ("Bearer", "token"), False),
            # Valid custom token type
            ({"Authorization": "CustomType token"}, ("CustomType", "token"), False),
            # Missing Authorization header
            ({}, None, True),
            # Empty Authorization header
            ({"Authorization": ""}, None, True),
            # Malformed Authorization header
            ({"Authorization": "Bearer"}, None, True),
        ],
    )
    def test_extract_token_info(
        self, federation_provider, headers, expected_result, should_raise
    ):
        """Test token extraction from headers with various formats."""
        if should_raise:
            with pytest.raises(ValueError):
                federation_provider._extract_token_info_from_header(headers)
        else:
            result = federation_provider._extract_token_info_from_header(headers)
            assert result == expected_result

    def test_get_expiry_from_jwt(self, federation_provider):
        """Test JWT token expiry extraction."""
        # Create a valid JWT token with expiry
        expiry_timestamp = int(
            (datetime.now(tz=timezone.utc) + timedelta(hours=1)).timestamp()
        )
        valid_payload = {
            "exp": expiry_timestamp,
            "iat": int(datetime.now(tz=timezone.utc).timestamp()),
            "sub": "test-subject",
        }
        valid_token = jwt.encode(valid_payload, "secret", algorithm="HS256")

        # Test with valid token
        expiry = federation_provider._get_expiry_from_jwt(valid_token)
        assert expiry is not None
        assert isinstance(expiry, datetime)
        assert expiry.tzinfo is not None  # Should be timezone-aware
        # Allow for small rounding differences
        assert (
            abs(
                (
                    expiry - datetime.fromtimestamp(expiry_timestamp, tz=timezone.utc)
                ).total_seconds()
            )
            < 1
        )

        # Test with invalid token format
        assert federation_provider._get_expiry_from_jwt("invalid-token") is None

        # Test with token missing expiry claim
        token_without_exp = jwt.encode(
            {"sub": "test-subject"}, "secret", algorithm="HS256"
        )
        assert federation_provider._get_expiry_from_jwt(token_without_exp) is None

    # ==== Core functionality tests ====
    def test_token_reuse_when_valid(self, federation_provider, future_time):
        """Test that a valid token is reused without exchange."""
        # Prepare mock for exchange function
        with patch.object(federation_provider, "_exchange_token") as mock_exchange:
            # Set up a valid token
            federation_provider.current_token = Token(
                "existing_token", "Bearer", expiry=future_time
            )
            federation_provider.external_headers = {
                "Authorization": "Bearer external_token"
            }

            # Get headers
            headers = federation_provider.get_auth_headers()

            # Verify token was reused without exchange
            assert headers["Authorization"] == "Bearer existing_token"
            mock_exchange.assert_not_called()

    def test_token_exchange_from_different_host(
        self, federation_provider, mock_dependencies
    ):
        """Test token exchange when token is from a different host."""
        # Configure mocks for token from different host
        mock_dependencies["parse_jwt"].return_value = {
            "iss": "https://login.microsoftonline.com/tenant"
        }
        mock_dependencies["is_same_host"].return_value = False

        # Configure credentials provider
        headers = {"Authorization": "Bearer external_token"}
        header_factory = MagicMock(return_value=headers)
        mock_creds = MagicMock(return_value=header_factory)
        federation_provider.credentials_provider = mock_creds

        # Configure mock token exchange
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        exchanged_token = Token("databricks_token", "Bearer", expiry=future_time)
        mock_dependencies["exchange"].return_value = exchanged_token

        # Call refresh_token
        token = federation_provider.refresh_token()

        # Verify token was exchanged
        mock_dependencies["exchange"].assert_called_with("external_token")
        assert token.access_token == "databricks_token"
        assert federation_provider.current_token == token

    def test_token_from_same_host(self, federation_provider, mock_dependencies):
        """Test handling of token from the same host (no exchange needed)."""
        # Configure mocks for token from same host
        mock_dependencies["parse_jwt"].return_value = {"iss": "https://databricks.com"}
        mock_dependencies["is_same_host"].return_value = True

        # Configure credentials provider
        headers = {"Authorization": "Bearer databricks_token"}
        header_factory = MagicMock(return_value=headers)
        mock_creds = MagicMock(return_value=header_factory)
        federation_provider.credentials_provider = mock_creds

        # Mock JWT expiry extraction
        expiry_time = datetime.now(tz=timezone.utc) + timedelta(hours=2)
        with patch.object(
            federation_provider, "_get_expiry_from_jwt", return_value=expiry_time
        ):
            # Call refresh_token
            token = federation_provider.refresh_token()

            # Verify no exchange was performed
            mock_dependencies["exchange"].assert_not_called()
            assert token.access_token == "databricks_token"
            assert token.expiry == expiry_time

    def test_call_returns_auth_headers_function(
        self, federation_provider, mock_dependencies
    ):
        """Test that __call__ returns the get_auth_headers method directly."""
        with patch.object(
            federation_provider,
            "get_auth_headers",
            return_value={"Authorization": "Bearer test_token"},
        ) as mock_get_auth:
            # Get the header factory from __call__
            result = federation_provider()

            # Verify it's the get_auth_headers method
            assert result is federation_provider.get_auth_headers

            # Call the result and verify it returns headers
            headers = result()
            assert headers == {"Authorization": "Bearer test_token"}
            mock_get_auth.assert_called_once()

    def test_token_exchange_success(self, federation_provider):
        """Test successful token exchange."""
        # Mock successful response
        with patch("databricks.sql.auth.token_federation.requests.post") as mock_post:
            # Create a token with a valid expiry
            expiry_timestamp = int(
                (datetime.now(tz=timezone.utc) + timedelta(hours=1)).timestamp()
            )

            # Configure mock response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "new_token",
                "token_type": "Bearer",
                "refresh_token": "refresh_value",
            }
            mock_post.return_value = mock_response

            # Mock JWT expiry extraction to return a valid expiry
            with patch.object(
                federation_provider,
                "_get_expiry_from_jwt",
                return_value=datetime.fromtimestamp(expiry_timestamp, tz=timezone.utc),
            ):
                # Call the exchange method
                token = federation_provider._exchange_token("original_token")

                # Verify token properties
                assert token.access_token == "new_token"
                assert token.token_type == "Bearer"
                assert token.refresh_token == "refresh_value"

                # Verify expiry time is correctly set
                expiry_datetime = datetime.fromtimestamp(
                    expiry_timestamp, tz=timezone.utc
                )
                assert token.expiry == expiry_datetime

    def test_token_exchange_failure(self, federation_provider):
        """Test token exchange failure handling."""
        # Mock error response
        with patch("databricks.sql.auth.token_federation.requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 401
            mock_response.text = "Unauthorized"
            mock_post.return_value = mock_response

            # Call the method and expect an exception
            with pytest.raises(
                ValueError, match="Token exchange failed with status code 401"
            ):
                federation_provider._exchange_token("original_token")
