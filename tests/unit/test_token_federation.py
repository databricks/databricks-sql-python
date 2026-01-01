import pytest
from unittest.mock import Mock, patch
import json
import jwt
from datetime import datetime, timedelta

from databricks.sql.auth.token_federation import TokenFederationProvider, Token
from databricks.sql.auth.auth_utils import (
    decode_token,
    is_same_host,
)
from databricks.sql.common.url_utils import normalize_host_with_protocol
from databricks.sql.common.http import HttpMethod


@pytest.fixture
def mock_http_client():
    """Fixture for mock HTTP client."""
    return Mock()


@pytest.fixture
def mock_external_provider():
    """Fixture for mock external provider."""
    return Mock()


@pytest.fixture
def token_federation_provider(mock_http_client, mock_external_provider):
    """Fixture for TokenFederationProvider."""
    return TokenFederationProvider(
        hostname="https://test.databricks.com/",
        external_provider=mock_external_provider,
        http_client=mock_http_client,
        identity_federation_client_id="test-client-id",
    )


def create_mock_token_response(
    access_token="databricks-token-456", token_type="Bearer", expires_in=3600
):
    """Helper function to create mock token exchange response."""
    mock_response = Mock()
    mock_response.data = json.dumps(
        {
            "access_token": access_token,
            "token_type": token_type,
            "expires_in": expires_in,
        }
    ).encode("utf-8")
    mock_response.status = 200
    return mock_response


def create_jwt_token(issuer="https://test.databricks.com", exp_hours=1, **kwargs):
    """Helper function to create JWT tokens for testing."""
    payload = {
        "iss": issuer,
        "aud": "databricks",
        "exp": int((datetime.now() + timedelta(hours=exp_hours)).timestamp()),
        **kwargs,
    }
    return jwt.encode(payload, "secret", algorithm="HS256")


class TestTokenFederationProvider:
    """Test TokenFederationProvider functionality."""

    def test_init_requires_http_client(self, mock_external_provider):
        """Test that http_client is required."""
        with pytest.raises(ValueError, match="http_client is required"):
            TokenFederationProvider(
                hostname="test.databricks.com",
                external_provider=mock_external_provider,
                http_client=None,
            )

    @pytest.mark.parametrize(
        "input_hostname,expected",
        [
            ("test.databricks.com", "https://test.databricks.com"),
            ("https://test.databricks.com", "https://test.databricks.com"),
            ("https://test.databricks.com/", "https://test.databricks.com"),
            ("test.databricks.com/", "https://test.databricks.com"),
        ],
    )
    def test_hostname_normalization(
        self, input_hostname, expected, mock_http_client, mock_external_provider
    ):
        """Test hostname normalization during initialization."""
        provider = TokenFederationProvider(
            hostname=input_hostname,
            external_provider=mock_external_provider,
            http_client=mock_http_client,
        )
        assert provider.hostname == expected

    @pytest.mark.parametrize(
        "auth_header,expected_type,expected_token",
        [
            ("Bearer test-token-123", "Bearer", "test-token-123"),
            ("Basic dGVzdDp0ZXN0", "Basic", "dGVzdDp0ZXN0"),
        ],
    )
    def test_extract_token_from_valid_header(
        self, token_federation_provider, auth_header, expected_type, expected_token
    ):
        """Test extraction of token from valid Authorization header."""
        token_type, access_token = token_federation_provider._extract_token_from_header(
            auth_header
        )
        assert token_type == expected_type
        assert access_token == expected_token

    @pytest.mark.parametrize(
        "invalid_header",
        [
            "Bearer",  # Missing token
            "",  # Empty header
            "InvalidFormat",  # No space separator
        ],
    )
    def test_extract_token_from_invalid_header(
        self, token_federation_provider, invalid_header
    ):
        """Test extraction fails for invalid Authorization headers."""
        with pytest.raises(ValueError):
            token_federation_provider._extract_token_from_header(invalid_header)

    @pytest.mark.parametrize(
        "issuer,hostname,should_exchange",
        [
            (
                "https://login.microsoftonline.com/tenant-id/",
                "https://test.databricks.com/",
                True,
            ),
            ("https://test.databricks.com", "https://test.databricks.com/", False),
            ("https://test.databricks.com:443", "https://test.databricks.com/", False),
            ("https://accounts.google.com", "https://test.databricks.com/", True),
        ],
    )
    def test_should_exchange_token(
        self, token_federation_provider, issuer, hostname, should_exchange
    ):
        """Test token exchange decision based on issuer."""
        token_federation_provider.hostname = hostname
        jwt_token = create_jwt_token(issuer=issuer)

        result = token_federation_provider._should_exchange_token(jwt_token)
        assert result == should_exchange

    def test_should_exchange_token_invalid_jwt(self, token_federation_provider):
        """Test that invalid JWT returns False for exchange."""
        result = token_federation_provider._should_exchange_token("invalid-jwt-token")
        assert result is False

    def test_exchange_token_success(self, token_federation_provider, mock_http_client):
        """Test successful token exchange."""
        access_token = "external-token-123"
        mock_http_client.request.return_value = create_mock_token_response()

        result = token_federation_provider._exchange_token(access_token)

        # Verify result is a Token object
        assert isinstance(result, Token)
        assert result.access_token == "databricks-token-456"
        assert result.token_type == "Bearer"

        # Verify the request
        mock_http_client.request.assert_called_once()
        call_args = mock_http_client.request.call_args

        # Check method and URL
        assert call_args[0][0] == HttpMethod.POST
        assert call_args[1]["url"] == "https://test.databricks.com/oidc/v1/token"

        # Check body contains expected parameters
        from urllib.parse import parse_qs

        body = call_args[1]["body"]
        parsed_body = parse_qs(body)

        assert (
            parsed_body["grant_type"][0]
            == "urn:ietf:params:oauth:grant-type:token-exchange"
        )
        assert parsed_body["subject_token"][0] == access_token
        assert (
            parsed_body["subject_token_type"][0]
            == "urn:ietf:params:oauth:token-type:jwt"
        )
        assert parsed_body["scope"][0] == "sql"
        assert parsed_body["client_id"][0] == "test-client-id"

    def test_exchange_token_failure(self, token_federation_provider, mock_http_client):
        """Test token exchange failure handling."""
        mock_response = Mock()
        mock_response.data = b'{"error": "invalid_request"}'
        mock_response.status = 400
        mock_http_client.request.return_value = mock_response

        with pytest.raises(KeyError):  # Will raise KeyError due to missing access_token
            token_federation_provider._exchange_token("external-token-123")

    @pytest.mark.parametrize(
        "external_issuer,should_exchange",
        [
            ("https://login.microsoftonline.com/tenant-id/", True),
            ("https://test.databricks.com", False),
        ],
    )
    def test_add_headers_token_exchange(
        self,
        token_federation_provider,
        mock_external_provider,
        mock_http_client,
        external_issuer,
        should_exchange,
    ):
        """Test adding headers with and without token exchange."""
        # Setup external provider to return a token
        external_token = create_jwt_token(issuer=external_issuer)
        mock_external_provider.add_headers = Mock(
            side_effect=lambda headers: headers.update(
                {"Authorization": f"Bearer {external_token}"}
            )
        )

        if should_exchange:
            # Mock successful token exchange
            mock_http_client.request.return_value = create_mock_token_response()
            expected_token = "databricks-token-456"
        else:
            expected_token = external_token

        headers = {}
        token_federation_provider.add_headers(headers)

        assert headers["Authorization"] == f"Bearer {expected_token}"

    def test_token_caching(self, token_federation_provider, mock_external_provider):
        """Test that tokens are cached and reused."""
        external_token = create_jwt_token()
        mock_external_provider.add_headers = Mock(
            side_effect=lambda headers: headers.update(
                {"Authorization": f"Bearer {external_token}"}
            )
        )

        # First call
        headers1 = {}
        token_federation_provider.add_headers(headers1)

        # Second call - should use cached token
        headers2 = {}
        token_federation_provider.add_headers(headers2)

        # External provider should only be called once
        assert mock_external_provider.add_headers.call_count == 1

        # Both headers should be the same
        assert headers1["Authorization"] == headers2["Authorization"]

    def test_token_cache_expiry(
        self, token_federation_provider, mock_external_provider
    ):
        """Test that expired cached tokens are refreshed."""
        call_count = [0]

        def add_headers_side_effect(headers):
            call_count[0] += 1
            token = create_jwt_token(
                exp_hours=0.001 if call_count[0] == 1 else 1
            )  # First token expires quickly
            headers.update({"Authorization": f"Bearer {token}"})

        mock_external_provider.add_headers = Mock(side_effect=add_headers_side_effect)

        # First call
        headers1 = {}
        token_federation_provider.add_headers(headers1)
        first_token = headers1["Authorization"].split(" ")[1]

        # Force cache expiry
        token_federation_provider._cached_token = Token(first_token)
        token_federation_provider._cached_token.expiry_time = (
            datetime.now() - timedelta(seconds=1)
        )

        # Second call - should get new token
        headers2 = {}
        token_federation_provider.add_headers(headers2)
        second_token = headers2["Authorization"].split(" ")[1]

        # External provider should be called twice
        assert mock_external_provider.add_headers.call_count == 2
        # Tokens should be different
        assert first_token != second_token


class TestUtilityFunctions:
    """Test utility functions used by TokenFederationProvider."""

    @pytest.mark.parametrize(
        "input_hostname,expected",
        [
            ("test.databricks.com", "https://test.databricks.com"),
            ("https://test.databricks.com", "https://test.databricks.com"),
            ("https://test.databricks.com/", "https://test.databricks.com"),
            ("test.databricks.com/", "https://test.databricks.com"),
        ],
    )
    def test_normalize_hostname(self, input_hostname, expected):
        """Test hostname normalization."""
        assert normalize_host_with_protocol(input_hostname) == expected

    @pytest.mark.parametrize(
        "url1,url2,expected",
        [
            ("https://test.databricks.com", "https://test.databricks.com", True),
            ("https://test.databricks.com", "https://test.databricks.com:443", True),
            ("https://test1.databricks.com", "https://test2.databricks.com", False),
            ("https://login.microsoftonline.com", "https://test.databricks.com", False),
        ],
    )
    def test_is_same_host(self, url1, url2, expected):
        """Test host comparison."""
        assert is_same_host(url1, url2) == expected

    def test_decode_token_valid(self):
        """Test decoding a valid JWT token."""
        token = create_jwt_token()
        result = decode_token(token)
        assert result is not None
        assert "iss" in result
        assert "exp" in result

    def test_decode_token_invalid(self):
        """Test decoding an invalid token."""
        result = decode_token("invalid-token")
        assert result is None
