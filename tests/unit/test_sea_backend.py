import pytest
from unittest.mock import patch, MagicMock

from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.types import SessionId, BackendType
from databricks.sql.types import SSLOptions
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.exc import Error


class TestSeaBackend:
    """Test suite for the SeaDatabricksClient class."""

    @pytest.fixture
    def mock_http_client(self):
        """Create a mock HTTP client."""
        with patch(
            "databricks.sql.backend.sea.backend.SeaHttpClient"
        ) as mock_client_class:
            mock_client = mock_client_class.return_value
            yield mock_client

    @pytest.fixture
    def sea_client(self, mock_http_client):
        """Create a SeaDatabricksClient instance with mocked dependencies."""
        server_hostname = "test-server.databricks.com"
        port = 443
        http_path = "/sql/warehouses/abc123"
        http_headers = [("header1", "value1"), ("header2", "value2")]
        auth_provider = AuthProvider()
        ssl_options = SSLOptions()

        client = SeaDatabricksClient(
            server_hostname=server_hostname,
            port=port,
            http_path=http_path,
            http_headers=http_headers,
            auth_provider=auth_provider,
            ssl_options=ssl_options,
        )

        return client

    def test_init_extracts_warehouse_id(self, mock_http_client):
        """Test that the constructor properly extracts the warehouse ID from the HTTP path."""
        # Test with warehouses format
        client1 = SeaDatabricksClient(
            server_hostname="test-server.databricks.com",
            port=443,
            http_path="/sql/warehouses/abc123",
            http_headers=[],
            auth_provider=AuthProvider(),
            ssl_options=SSLOptions(),
        )
        assert client1.warehouse_id == "abc123"

        # Test with endpoints format
        client2 = SeaDatabricksClient(
            server_hostname="test-server.databricks.com",
            port=443,
            http_path="/sql/endpoints/def456",
            http_headers=[],
            auth_provider=AuthProvider(),
            ssl_options=SSLOptions(),
        )
        assert client2.warehouse_id == "def456"

    def test_init_raises_error_for_invalid_http_path(self, mock_http_client):
        """Test that the constructor raises an error for invalid HTTP paths."""
        with pytest.raises(ValueError) as excinfo:
            SeaDatabricksClient(
                server_hostname="test-server.databricks.com",
                port=443,
                http_path="/invalid/path",
                http_headers=[],
                auth_provider=AuthProvider(),
                ssl_options=SSLOptions(),
            )
        assert "Could not extract warehouse ID" in str(excinfo.value)

    def test_open_session_basic(self, sea_client, mock_http_client):
        """Test the open_session method with minimal parameters."""
        # Set up mock response
        mock_http_client._make_request.return_value = {"session_id": "test-session-123"}

        # Call the method
        session_id = sea_client.open_session(None, None, None)

        # Verify the result
        assert isinstance(session_id, SessionId)
        assert session_id.backend_type == BackendType.SEA
        assert session_id.guid == "test-session-123"

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once_with(
            method="POST", path=sea_client.SESSION_PATH, data={"warehouse_id": "abc123"}
        )

    def test_open_session_with_all_parameters(self, sea_client, mock_http_client):
        """Test the open_session method with all parameters."""
        # Set up mock response
        mock_http_client._make_request.return_value = {"session_id": "test-session-456"}

        # Call the method with all parameters, including both supported and unsupported configurations
        session_config = {
            "ANSI_MODE": "FALSE",  # Supported parameter
            "STATEMENT_TIMEOUT": "3600",  # Supported parameter
            "unsupported_param": "value",  # Unsupported parameter
        }
        catalog = "test_catalog"
        schema = "test_schema"

        session_id = sea_client.open_session(session_config, catalog, schema)

        # Verify the result
        assert isinstance(session_id, SessionId)
        assert session_id.backend_type == BackendType.SEA
        assert session_id.guid == "test-session-456"

        # Verify the HTTP request - only supported parameters should be included
        # and keys should be in lowercase
        expected_data = {
            "warehouse_id": "abc123",
            "session_confs": {
                "ansi_mode": "FALSE",
                "statement_timeout": "3600",
            },
            "catalog": catalog,
            "schema": schema,
        }
        mock_http_client._make_request.assert_called_once_with(
            method="POST", path=sea_client.SESSION_PATH, data=expected_data
        )

    def test_open_session_error_handling(self, sea_client, mock_http_client):
        """Test error handling in the open_session method."""
        # Set up mock response without session_id
        mock_http_client._make_request.return_value = {}

        # Call the method and expect an error
        with pytest.raises(Error) as excinfo:
            sea_client.open_session(None, None, None)

        assert "Failed to create session" in str(excinfo.value)

    def test_close_session_valid_id(self, sea_client, mock_http_client):
        """Test closing a session with a valid session ID."""
        # Create a valid SEA session ID
        session_id = SessionId.from_sea_session_id("test-session-789")

        # Set up mock response
        mock_http_client._make_request.return_value = {}

        # Call the method
        sea_client.close_session(session_id)

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once_with(
            method="DELETE",
            path=sea_client.SESSION_PATH_WITH_ID.format("test-session-789"),
            data={"session_id": "test-session-789", "warehouse_id": "abc123"},
        )

    def test_close_session_invalid_id_type(self, sea_client):
        """Test closing a session with an invalid session ID type."""
        # Create a Thrift session ID (not SEA)
        mock_thrift_handle = MagicMock()
        mock_thrift_handle.sessionId.guid = b"guid"
        mock_thrift_handle.sessionId.secret = b"secret"
        session_id = SessionId.from_thrift_handle(mock_thrift_handle)

        # Call the method and expect an error
        with pytest.raises(ValueError) as excinfo:
            sea_client.close_session(session_id)

        assert "Not a valid SEA session ID" in str(excinfo.value)

    def test_session_configuration_helpers(self):
        """Test the session configuration helper methods."""
        # Test getting default value for a supported parameter
        default_value = SeaDatabricksClient.get_default_session_configuration_value(
            "ANSI_MODE"
        )
        assert default_value == "true"

        # Test getting default value for an unsupported parameter
        default_value = SeaDatabricksClient.get_default_session_configuration_value(
            "UNSUPPORTED_PARAM"
        )
        assert default_value is None

        # Test getting the list of allowed configurations
        allowed_configs = SeaDatabricksClient.get_allowed_session_configurations()

        expected_keys = {
            "ANSI_MODE",
            "ENABLE_PHOTON",
            "LEGACY_TIME_PARSER_POLICY",
            "MAX_FILE_PARTITION_BYTES",
            "READ_ONLY_EXTERNAL_METASTORE",
            "STATEMENT_TIMEOUT",
            "TIMEZONE",
            "USE_CACHED_RESULT",
        }
        assert set(allowed_configs) == expected_keys
