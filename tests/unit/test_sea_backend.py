import pytest
from unittest.mock import patch, MagicMock

from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.types import SessionId, BackendType, CommandId, CommandState
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

        # Test checking if a parameter is supported
        assert SeaDatabricksClient.is_session_configuration_parameter_supported("ANSI_MODE")
        assert not SeaDatabricksClient.is_session_configuration_parameter_supported("UNSUPPORTED_PARAM")

    def test_unimplemented_methods(self, sea_client):
        """Test that unimplemented methods raise NotImplementedError."""
        # This test is no longer relevant since we've implemented these methods
        # We'll modify it to just test a couple of methods with mocked responses
        
        # Create dummy parameters for testing
        session_id = SessionId.from_sea_session_id("test-session")
        command_id = MagicMock()
        cursor = MagicMock()
        
        # Mock the http_client to return appropriate responses
        sea_client.http_client._make_request.return_value = {
            "statement_id": "test-statement-id",
            "status": {"state": "FAILED", "error": {"message": "Test error message"}}
        }
        
        # Mock get_query_state to return FAILED
        sea_client.get_query_state = MagicMock(return_value=CommandState.FAILED)
        
        # Test execute_command - should raise ServerOperationError due to FAILED state
        with pytest.raises(Error) as excinfo:
            sea_client.execute_command(
                operation="SELECT 1",
                session_id=session_id,
                max_rows=100,
                max_bytes=1000,
                lz4_compression=False,
                cursor=cursor,
                use_cloud_fetch=False,
                parameters=[],
                async_op=False,
                enforce_embedded_schema_correctness=False,
            )
        assert "Statement execution did not succeed" in str(excinfo.value)
        assert "Test error message" in str(excinfo.value)
        
    def test_command_operations(self, sea_client, mock_http_client):
        """Test command operations like cancel and close."""
        # Create a command ID
        command_id = CommandId.from_sea_statement_id("test-statement-id")
        
        # Set up mock response
        mock_http_client._make_request.return_value = {}
        
        # Test cancel_command
        sea_client.cancel_command(command_id)
        mock_http_client._make_request.assert_called_with(
            method="POST",
            path=sea_client.CANCEL_STATEMENT_PATH_WITH_ID.format("test-statement-id"),
            data={"statement_id": "test-statement-id"},
        )
        
        # Reset mock
        mock_http_client._make_request.reset_mock()
        
        # Test close_command
        sea_client.close_command(command_id)
        mock_http_client._make_request.assert_called_with(
            method="DELETE",
            path=sea_client.STATEMENT_PATH_WITH_ID.format("test-statement-id"),
            data={"statement_id": "test-statement-id"},
        )
        
    def test_get_query_state(self, sea_client, mock_http_client):
        """Test get_query_state method."""
        # Create a command ID
        command_id = CommandId.from_sea_statement_id("test-statement-id")
        
        # Set up mock response
        mock_http_client._make_request.return_value = {
            "status": {"state": "RUNNING"}
        }
        
        # Test get_query_state
        state = sea_client.get_query_state(command_id)
        assert state == CommandState.RUNNING
        
        mock_http_client._make_request.assert_called_with(
            method="GET",
            path=sea_client.STATEMENT_PATH_WITH_ID.format("test-statement-id"),
            data={"statement_id": "test-statement-id"},
        )
        
    def test_metadata_operations(self, sea_client, mock_http_client):
        """Test metadata operations like get_catalogs, get_schemas, etc."""
        # Create test parameters
        session_id = SessionId.from_sea_session_id("test-session")
        cursor = MagicMock()
        cursor.connection = MagicMock()
        cursor.buffer_size_bytes = 1000000
        cursor.arraysize = 10000
        
        # Mock the execute_command method to return a mock result set
        mock_result_set = MagicMock()
        sea_client.execute_command = MagicMock(return_value=mock_result_set)
        
        # Test get_catalogs
        result = sea_client.get_catalogs(session_id, 100, 1000, cursor)
        assert result == mock_result_set
        sea_client.execute_command.assert_called_with(
            operation="SHOW CATALOGS",
            session_id=session_id,
            max_rows=100,
            max_bytes=1000,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        
        # Reset mock
        sea_client.execute_command.reset_mock()
        
        # Test get_schemas
        result = sea_client.get_schemas(session_id, 100, 1000, cursor, "test_catalog")
        assert result == mock_result_set
        sea_client.execute_command.assert_called_with(
            operation="SHOW SCHEMAS IN `test_catalog`",
            session_id=session_id,
            max_rows=100,
            max_bytes=1000,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        
        # Reset mock
        sea_client.execute_command.reset_mock()
        
        # Test get_tables
        result = sea_client.get_tables(session_id, 100, 1000, cursor, "test_catalog", "test_schema", "test_table")
        assert result == mock_result_set
        sea_client.execute_command.assert_called_with(
            operation="SHOW TABLES IN CATALOG `test_catalog` SCHEMA LIKE 'test_schema' LIKE 'test_table'",
            session_id=session_id,
            max_rows=100,
            max_bytes=1000,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        
        # Reset mock
        sea_client.execute_command.reset_mock()
        
        # Test get_columns
        result = sea_client.get_columns(session_id, 100, 1000, cursor, "test_catalog", "test_schema", "test_table", "test_column")
        assert result == mock_result_set
        sea_client.execute_command.assert_called_with(
            operation="SHOW COLUMNS IN CATALOG `test_catalog` SCHEMA LIKE 'test_schema' TABLE LIKE 'test_table' LIKE 'test_column'",
            session_id=session_id,
            max_rows=100,
            max_bytes=1000,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )

    def test_max_download_threads_property(self, sea_client):
        """Test the max_download_threads property."""
        assert sea_client.max_download_threads == 10

        # Create a client with a custom value
        custom_client = SeaDatabricksClient(
            server_hostname="test-server.databricks.com",
            port=443,
            http_path="/sql/warehouses/abc123",
            http_headers=[],
            auth_provider=AuthProvider(),
            ssl_options=SSLOptions(),
            max_download_threads=20,
        )

        # Verify the custom value is returned
        assert custom_client.max_download_threads == 20