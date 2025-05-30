"""
Tests for the SEA (Statement Execution API) backend implementation.

This module contains tests for the SeaDatabricksClient class, which implements
the Databricks SQL connector's SEA backend functionality.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.backend.sea_backend import SeaDatabricksClient
from databricks.sql.backend.types import SessionId, CommandId, CommandState, BackendType
from databricks.sql.types import SSLOptions
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.exc import Error, NotSupportedError


class TestSeaBackend:
    """Test suite for the SeaDatabricksClient class."""

    @pytest.fixture
    def mock_http_client(self):
        """Create a mock HTTP client."""
        with patch(
            "databricks.sql.backend.sea_backend.CustomHttpClient"
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

    @pytest.fixture
    def mock_cursor(self):
        """Create a mock cursor."""
        cursor = MagicMock()
        cursor.connection = MagicMock()
        cursor.buffer_size_bytes = 1048576
        cursor.arraysize = 1000
        return cursor

    @pytest.fixture
    def sea_session_id(self):
        """Create a SEA session ID for testing."""
        return SessionId.from_sea_session_id("test-session-123")

    @pytest.fixture
    def sea_command_id(self):
        """Create a SEA command ID for testing."""
        return CommandId.from_sea_statement_id("test-statement-123")

    # Tests for initialization and session management

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

        # Call the method with all parameters
        session_config = {"spark.sql.shuffle.partitions": "10"}
        catalog = "test_catalog"
        schema = "test_schema"

        session_id = sea_client.open_session(session_config, catalog, schema)

        # Verify the result
        assert isinstance(session_id, SessionId)
        assert session_id.backend_type == BackendType.SEA
        assert session_id.guid == "test-session-456"

        # Verify the HTTP request
        expected_data = {
            "warehouse_id": "abc123",
            "session_confs": session_config,
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
            data={"warehouse_id": "abc123"},
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

    # Tests for command execution and management

    def test_execute_command_sync(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test executing a command synchronously."""
        # Set up mock responses
        execute_response = {
            "statement_id": "test-statement-123",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": [
                    {
                        "name": "col1",
                        "type_name": "STRING",
                        "type_text": "string",
                        "nullable": True,
                    }
                ],
                "total_row_count": 1,
                "total_byte_count": 100,
            },
            "result": {"data": [["value1"]]},
        }
        mock_http_client._make_request.return_value = execute_response

        # Mock the get_execution_result method
        with patch.object(
            sea_client, "get_execution_result", return_value="mock_result_set"
        ) as mock_get_result:
            # Call the method
            result = sea_client.execute_command(
                operation="SELECT 1",
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                lz4_compression=False,
                cursor=mock_cursor,
                use_cloud_fetch=False,
                parameters=[],
                async_op=False,
                enforce_embedded_schema_correctness=False,
            )

            # Verify the result
            assert result == "mock_result_set"

            # Verify the HTTP request
            mock_http_client._make_request.assert_called_once()
            args, kwargs = mock_http_client._make_request.call_args
            assert kwargs["method"] == "POST"
            assert kwargs["path"] == sea_client.STATEMENT_PATH
            assert "warehouse_id" in kwargs["data"]
            assert "session_id" in kwargs["data"]
            assert "statement" in kwargs["data"]
            assert kwargs["data"]["statement"] == "SELECT 1"

            # Verify get_execution_result was called with the right command ID
            mock_get_result.assert_called_once()
            cmd_id_arg = mock_get_result.call_args[0][0]
            assert isinstance(cmd_id_arg, CommandId)
            assert cmd_id_arg.guid == "test-statement-123"

    def test_execute_command_async(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test executing a command asynchronously."""
        # Set up mock response
        execute_response = {
            "statement_id": "test-statement-456",
            "status": {"state": "PENDING"},
        }
        mock_http_client._make_request.return_value = execute_response

        # Call the method
        result = sea_client.execute_command(
            operation="SELECT 1",
            session_id=sea_session_id,
            max_rows=100,
            max_bytes=1000,
            lz4_compression=False,
            cursor=mock_cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=True,  # Async mode
            enforce_embedded_schema_correctness=False,
        )

        # Verify the result is None for async operation
        assert result is None

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once()
        args, kwargs = mock_http_client._make_request.call_args
        assert kwargs["method"] == "POST"
        assert kwargs["path"] == sea_client.STATEMENT_PATH
        assert "wait_timeout" in kwargs["data"]
        assert kwargs["data"]["wait_timeout"] == "0s"  # Async mode uses 0s timeout

        # Verify the command ID was stored in the cursor
        assert hasattr(mock_cursor, "active_command_id")
        assert isinstance(mock_cursor.active_command_id, CommandId)
        assert mock_cursor.active_command_id.guid == "test-statement-456"

    def test_execute_command_with_polling(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test executing a command that requires polling."""
        # Set up mock responses for initial request and polling
        initial_response = {
            "statement_id": "test-statement-789",
            "status": {"state": "RUNNING"},
        }
        poll_response = {
            "statement_id": "test-statement-789",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": [], "total_row_count": 0, "total_byte_count": 0},
            "result": {"data": []},
        }

        # Configure mock to return different responses on subsequent calls
        mock_http_client._make_request.side_effect = [initial_response, poll_response]

        # Mock the get_execution_result method
        with patch.object(
            sea_client, "get_execution_result", return_value="mock_result_set"
        ) as mock_get_result:
            # Mock time.sleep to avoid actual delays
            with patch("time.sleep"):
                # Call the method
                result = sea_client.execute_command(
                    operation="SELECT * FROM large_table",
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    lz4_compression=False,
                    cursor=mock_cursor,
                    use_cloud_fetch=False,
                    parameters=[],
                    async_op=False,
                    enforce_embedded_schema_correctness=False,
                )

                # Verify the result
                assert result == "mock_result_set"

                # Verify the HTTP requests (initial and poll)
                assert mock_http_client._make_request.call_count == 2

                # Verify get_execution_result was called with the right command ID
                mock_get_result.assert_called_once()
                cmd_id_arg = mock_get_result.call_args[0][0]
                assert isinstance(cmd_id_arg, CommandId)
                assert cmd_id_arg.guid == "test-statement-789"

    def test_execute_command_with_parameters(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test executing a command with parameters."""
        # Set up mock response
        execute_response = {
            "statement_id": "test-statement-123",
            "status": {"state": "SUCCEEDED"},
        }
        mock_http_client._make_request.return_value = execute_response

        # Create parameter mock
        param = MagicMock()
        param.name = "param1"
        param.value = "value1"
        param.type = "STRING"

        # Mock the get_execution_result method
        with patch.object(sea_client, "get_execution_result") as mock_get_result:
            # Call the method with parameters
            sea_client.execute_command(
                operation="SELECT * FROM table WHERE col = :param1",
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                lz4_compression=False,
                cursor=mock_cursor,
                use_cloud_fetch=False,
                parameters=[param],
                async_op=False,
                enforce_embedded_schema_correctness=False,
            )

            # Verify the HTTP request contains parameters
            mock_http_client._make_request.assert_called_once()
            args, kwargs = mock_http_client._make_request.call_args
            assert "parameters" in kwargs["data"]
            assert len(kwargs["data"]["parameters"]) == 1
            assert kwargs["data"]["parameters"][0]["name"] == "param1"
            assert kwargs["data"]["parameters"][0]["value"] == "value1"
            assert kwargs["data"]["parameters"][0]["type"] == "STRING"

    def test_execute_command_failure(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test executing a command that fails."""
        # Set up mock response for a failed execution
        error_response = {
            "statement_id": "test-statement-123",
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "Syntax error in SQL",
                    "error_code": "SYNTAX_ERROR",
                },
            },
        }

        # Configure the mock to return the error response for the initial request
        # and then raise an exception when trying to poll (to simulate immediate failure)
        mock_http_client._make_request.side_effect = [
            error_response,  # Initial response
            Error(
                "Statement execution failed: Syntax error in SQL"
            ),  # Will be raised during polling
        ]

        # Mock time.sleep to avoid actual delays
        with patch("time.sleep"):
            # Call the method and expect an error
            with pytest.raises(Error) as excinfo:
                sea_client.execute_command(
                    operation="SELECT * FROM nonexistent_table",
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    lz4_compression=False,
                    cursor=mock_cursor,
                    use_cloud_fetch=False,
                    parameters=[],
                    async_op=False,
                    enforce_embedded_schema_correctness=False,
                )

            assert "Statement execution failed" in str(excinfo.value)

    def test_cancel_command(self, sea_client, mock_http_client, sea_command_id):
        """Test canceling a command."""
        # Set up mock response
        mock_http_client._make_request.return_value = {}

        # Call the method
        sea_client.cancel_command(sea_command_id)

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once()
        args, kwargs = mock_http_client._make_request.call_args
        assert kwargs["method"] == "POST"
        assert kwargs["path"] == sea_client.CANCEL_STATEMENT_PATH_WITH_ID.format(
            "test-statement-123"
        )
        assert "warehouse_id" in kwargs["data"]
        assert kwargs["data"]["warehouse_id"] == "abc123"

    def test_close_command(self, sea_client, mock_http_client, sea_command_id):
        """Test closing a command."""
        # Set up mock response
        mock_http_client._make_request.return_value = {}

        # Call the method
        sea_client.close_command(sea_command_id)

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once()
        args, kwargs = mock_http_client._make_request.call_args
        assert kwargs["method"] == "DELETE"
        assert kwargs["path"] == sea_client.STATEMENT_PATH_WITH_ID.format(
            "test-statement-123"
        )
        assert "warehouse_id" in kwargs["data"]
        assert kwargs["data"]["warehouse_id"] == "abc123"

    def test_get_query_state(self, sea_client, mock_http_client, sea_command_id):
        """Test getting the state of a query."""
        # Set up mock response
        mock_http_client._make_request.return_value = {
            "statement_id": "test-statement-123",
            "status": {"state": "RUNNING"},
        }

        # Call the method
        state = sea_client.get_query_state(sea_command_id)

        # Verify the result
        assert state == CommandState.RUNNING

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once()
        args, kwargs = mock_http_client._make_request.call_args
        assert kwargs["method"] == "GET"
        assert kwargs["path"] == sea_client.STATEMENT_PATH_WITH_ID.format(
            "test-statement-123"
        )
        assert "warehouse_id" in kwargs["data"]
        assert kwargs["data"]["warehouse_id"] == "abc123"

    def test_get_execution_result(
        self, sea_client, mock_http_client, mock_cursor, sea_command_id
    ):
        """Test getting the result of a command execution."""
        # Set up mock response
        mock_http_client._make_request.return_value = {
            "statement_id": "test-statement-123",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": [
                    {
                        "name": "col1",
                        "type_name": "STRING",
                        "type_text": "string",
                        "nullable": True,
                    }
                ],
                "total_row_count": 1,
                "total_byte_count": 100,
            },
            "result": {"data": [["value1"]]},
        }

        # Create a real result set to verify the implementation
        result = sea_client.get_execution_result(sea_command_id, mock_cursor)

        # Verify basic properties of the result
        assert result.statement_id == "test-statement-123"
        assert result.status.state == "SUCCEEDED"
        assert len(result.description) == 1
        assert result.description[0][0] == "col1"  # column name

        # Verify the HTTP request
        mock_http_client._make_request.assert_called_once()
        args, kwargs = mock_http_client._make_request.call_args
        assert kwargs["method"] == "GET"
        assert kwargs["path"] == sea_client.STATEMENT_PATH_WITH_ID.format(
            "test-statement-123"
        )
        assert "warehouse_id" in kwargs["data"]
        assert kwargs["data"]["warehouse_id"] == "abc123"

    # Tests for metadata operations

    def test_get_catalogs(self, sea_client, mock_cursor, sea_session_id):
        """Test getting catalogs."""
        # Mock execute_command to verify it's called with the right parameters
        with patch.object(
            sea_client, "execute_command", return_value="mock_result_set"
        ) as mock_execute:
            # Call the method
            result = sea_client.get_catalogs(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
            )

            # Verify the result
            assert result == "mock_result_set"

            # Verify execute_command was called with the right parameters
            mock_execute.assert_called_once()
            args, kwargs = mock_execute.call_args
            assert kwargs["operation"] == "SHOW CATALOGS"
            assert kwargs["session_id"] == sea_session_id
            assert kwargs["max_rows"] == 100
            assert kwargs["max_bytes"] == 1000
            assert kwargs["cursor"] == mock_cursor
            assert kwargs["async_op"] is False

    def test_get_schemas(self, sea_client, mock_cursor, sea_session_id):
        """Test getting schemas."""
        # Mock execute_command to verify it's called with the right parameters
        with patch.object(
            sea_client, "execute_command", return_value="mock_result_set"
        ) as mock_execute:
            # Call the method with catalog and schema
            result = sea_client.get_schemas(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

            # Verify the result
            assert result == "mock_result_set"

            # Verify execute_command was called with the right parameters
            mock_execute.assert_called_once()
            args, kwargs = mock_execute.call_args
            assert (
                kwargs["operation"]
                == "SHOW SCHEMAS IN `test_catalog` LIKE 'test_schema'"
            )
            assert kwargs["session_id"] == sea_session_id
            assert kwargs["max_rows"] == 100
            assert kwargs["max_bytes"] == 1000
            assert kwargs["cursor"] == mock_cursor
            assert kwargs["async_op"] is False

    def test_get_tables(self, sea_client, mock_cursor, sea_session_id):
        """Test getting tables."""
        # Mock execute_command to verify it's called with the right parameters
        with patch.object(
            sea_client, "execute_command", return_value="mock_result_set"
        ) as mock_execute:
            # Call the method with catalog, schema, and table name
            result = sea_client.get_tables(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
                schema_name="test_schema",
                table_name="test_table",
            )

            # Verify the result
            assert result == "mock_result_set"

            # Verify execute_command was called with the right parameters
            mock_execute.assert_called_once()
            args, kwargs = mock_execute.call_args
            assert (
                kwargs["operation"]
                == "SHOW TABLES IN `test_catalog`.`test_schema` LIKE 'test_table'"
            )
            assert kwargs["session_id"] == sea_session_id
            assert kwargs["max_rows"] == 100
            assert kwargs["max_bytes"] == 1000
            assert kwargs["cursor"] == mock_cursor
            assert kwargs["async_op"] is False

    def test_get_columns(self, sea_client, mock_cursor, sea_session_id):
        """Test getting columns."""
        # Mock execute_command to verify it's called with the right parameters
        with patch.object(
            sea_client, "execute_command", return_value="mock_result_set"
        ) as mock_execute:
            # Call the method with catalog, schema, and table name
            result = sea_client.get_columns(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
                schema_name="test_schema",
                table_name="test_table",
            )

            # Verify the result
            assert result == "mock_result_set"

            # Verify execute_command was called with the right parameters
            mock_execute.assert_called_once()
            args, kwargs = mock_execute.call_args
            assert (
                kwargs["operation"]
                == "DESCRIBE TABLE `test_catalog`.`test_schema`.`test_table`"
            )
            assert kwargs["session_id"] == sea_session_id
            assert kwargs["max_rows"] == 100
            assert kwargs["max_bytes"] == 1000
            assert kwargs["cursor"] == mock_cursor
            assert kwargs["async_op"] is False

    def test_get_columns_no_table_name(self, sea_client, mock_cursor, sea_session_id):
        """Test getting columns without a table name raises an error."""
        with pytest.raises(ValueError) as excinfo:
            sea_client.get_columns(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
                schema_name="test_schema",
                table_name=None,  # No table name
            )

        assert "Table name is required" in str(excinfo.value)
