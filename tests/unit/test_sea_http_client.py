import json
import unittest
from unittest.mock import patch, Mock, MagicMock
import pytest

from databricks.sql.backend.sea.utils.http_client import SeaHttpClient
from databricks.sql.auth.retry import CommandType
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.types import SSLOptions
from databricks.sql.exc import RequestError


class TestSeaHttpClient:
    @pytest.fixture
    def mock_auth_provider(self):
        auth_provider = Mock(spec=AuthProvider)
        auth_provider.add_headers = Mock(return_value=None)
        return auth_provider

    @pytest.fixture
    def ssl_options(self):
        return SSLOptions(
            tls_verify=True,
            tls_trusted_ca_file=None,
            tls_client_cert_file=None,
            tls_client_cert_key_file=None,
            tls_client_cert_key_password=None,
        )

    @pytest.fixture
    def sea_http_client(self, mock_auth_provider, ssl_options):
        with patch(
            "databricks.sql.backend.sea.utils.http_client.HTTPSConnectionPool"
        ) as mock_pool:
            client = SeaHttpClient(
                server_hostname="test-server.databricks.com",
                port=443,
                http_path="/sql/1.0/warehouses/abc123",
                http_headers=[("User-Agent", "test-agent")],
                auth_provider=mock_auth_provider,
                ssl_options=ssl_options,
            )
            # Replace the real pool with a mock
            client._pool = Mock()
            return client

    @pytest.mark.parametrize(
        "server_hostname,port,expected_base_url",
        [
            # Basic hostname without protocol
            ("myserver.com", 443, "https://myserver.com:443"),
            # Hostname with trailing slash
            ("myserver.com/", 443, "https://myserver.com:443"),
            # Hostname with https:// protocol
            ("https://myserver.com", 443, "https://myserver.com:443"),
            # Hostname with http:// protocol (preserved as-is)
            ("http://myserver.com", 443, "http://myserver.com:443"),
            # Hostname with protocol and trailing slash
            ("https://myserver.com/", 443, "https://myserver.com:443"),
            # Custom port
            ("myserver.com", 8080, "https://myserver.com:8080"),
            # Protocol with custom port
            ("https://myserver.com", 8080, "https://myserver.com:8080"),
        ],
    )
    def test_base_url_construction(
        self, server_hostname, port, expected_base_url, mock_auth_provider, ssl_options
    ):
        """Test that base_url is constructed correctly from various hostname inputs."""
        with patch("databricks.sql.backend.sea.utils.http_client.HTTPSConnectionPool"):
            client = SeaHttpClient(
                server_hostname=server_hostname,
                port=port,
                http_path="/sql/1.0/warehouses/test",
                http_headers=[],
                auth_provider=mock_auth_provider,
                ssl_options=ssl_options,
            )
            assert client.base_url == expected_base_url

    def test_get_command_type_from_path(self, sea_http_client):
        """Test the _get_command_type_from_path method with various paths and methods."""
        # Test statement execution
        assert (
            sea_http_client._get_command_type_from_path("/statements", "POST")
            == CommandType.EXECUTE_STATEMENT
        )

        # Test statement cancellation
        assert (
            sea_http_client._get_command_type_from_path(
                "/statements/123/cancel", "POST"
            )
            == CommandType.OTHER
        )

        # Test statement deletion (close operation)
        assert (
            sea_http_client._get_command_type_from_path("/statements/123", "DELETE")
            == CommandType.CLOSE_OPERATION
        )

        # Test get statement status
        assert (
            sea_http_client._get_command_type_from_path("/statements/123", "GET")
            == CommandType.GET_OPERATION_STATUS
        )

        # Test session close
        assert (
            sea_http_client._get_command_type_from_path("/sessions/456", "DELETE")
            == CommandType.CLOSE_SESSION
        )

        # Test other paths
        assert (
            sea_http_client._get_command_type_from_path("/other/endpoint", "GET")
            == CommandType.OTHER
        )
        assert (
            sea_http_client._get_command_type_from_path("/other/endpoint", "POST")
            == CommandType.OTHER
        )

    @patch(
        "databricks.sql.backend.sea.utils.http_client.SeaHttpClient._get_auth_headers"
    )
    def test_make_request_success(self, mock_get_auth_headers, sea_http_client):
        """Test successful _make_request calls."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status = 200
        # Mock response.data.decode() to return a valid JSON string
        mock_response.data.decode.return_value = '{"result": "success"}'
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)

        # Setup mock auth headers
        mock_get_auth_headers.return_value = {"Authorization": "Bearer test-token"}

        # Configure the pool's request method to return our mock response
        sea_http_client._pool.request.return_value = mock_response

        # Test GET request without data
        result = sea_http_client._make_request("GET", "/test/path")

        # Verify the request was made correctly
        sea_http_client._pool.request.assert_called_with(
            method="GET",
            url="/test/path",
            body=b"",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "test-agent",
                "Authorization": "Bearer test-token",
            },
            preload_content=False,
            retries=sea_http_client.retry_policy,
        )

        # Check the result
        assert result == {"result": "success"}

        # Test POST request with data
        test_data = {"query": "SELECT * FROM test"}
        result = sea_http_client._make_request("POST", "/statements", test_data)

        # Verify the request was made with the correct body
        expected_body = json.dumps(test_data).encode("utf-8")
        sea_http_client._pool.request.assert_called_with(
            method="POST",
            url="/statements",
            body=expected_body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "test-agent",
                "Authorization": "Bearer test-token",
                "Content-Length": str(len(expected_body)),
            },
            preload_content=False,
            retries=sea_http_client.retry_policy,
        )

    @patch(
        "databricks.sql.backend.sea.utils.http_client.SeaHttpClient._get_auth_headers"
    )
    def test_make_request_error_response(self, mock_get_auth_headers, sea_http_client):
        """Test _make_request with error HTTP status."""
        # Setup mock response with error status
        mock_response = Mock()
        mock_response.status = 400
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)

        # Setup mock auth headers
        mock_get_auth_headers.return_value = {"Authorization": "Bearer test-token"}

        # Configure the pool's request method to return our mock response
        sea_http_client._pool.request.return_value = mock_response

        # Test request with error response
        with pytest.raises(Exception) as excinfo:
            sea_http_client._make_request("GET", "/test/path")

        assert "SEA HTTP request failed with status 400" in str(excinfo.value)

    @patch(
        "databricks.sql.backend.sea.utils.http_client.SeaHttpClient._get_auth_headers"
    )
    def test_make_request_connection_error(
        self, mock_get_auth_headers, sea_http_client
    ):
        """Test _make_request with connection error."""
        # Setup mock auth headers
        mock_get_auth_headers.return_value = {"Authorization": "Bearer test-token"}

        # Configure the pool's request to raise an exception
        sea_http_client._pool.request.side_effect = Exception("Connection error")

        # Test request with connection error
        with pytest.raises(RequestError) as excinfo:
            sea_http_client._make_request("GET", "/test/path")

        assert "Error during request to server" in str(excinfo.value)

    def test_make_request_no_pool(self, sea_http_client):
        """Test _make_request when pool is not initialized."""
        # Set pool to None to simulate uninitialized pool
        sea_http_client._pool = None

        # Test request with no pool
        with pytest.raises(RequestError) as excinfo:
            sea_http_client._make_request("GET", "/test/path")

        assert "Connection pool not initialized" in str(excinfo.value)
