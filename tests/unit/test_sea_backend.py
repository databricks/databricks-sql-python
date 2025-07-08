"""
Tests for the SEA (Statement Execution API) backend implementation.

This module contains tests for the SeaDatabricksClient class, which implements
the Databricks SQL connector's SEA backend functionality.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.backend.sea.backend import (
    SeaDatabricksClient,
    _filter_session_configuration,
)
from databricks.sql.backend.types import SessionId, CommandId, CommandState, BackendType
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.exc import (
    Error,
    NotSupportedError,
    ProgrammingError,
    ServerOperationError,
    DatabaseError,
)


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

    @pytest.fixture
    def sea_session_id(self):
        """Create a SEA session ID."""
        return SessionId.from_sea_session_id("test-session-123")

    @pytest.fixture
    def sea_command_id(self):
        """Create a SEA command ID."""
        return CommandId.from_sea_statement_id("test-statement-123")

    @pytest.fixture
    def mock_cursor(self):
        """Create a mock cursor."""
        cursor = Mock()
        cursor.active_command_id = None
        cursor.buffer_size_bytes = 1000
        cursor.arraysize = 100
        return cursor

    @pytest.fixture
    def thrift_session_id(self):
        """Create a Thrift session ID (not SEA)."""
        mock_thrift_handle = MagicMock()
        mock_thrift_handle.sessionId.guid = b"guid"
        mock_thrift_handle.sessionId.secret = b"secret"
        return SessionId.from_thrift_handle(mock_thrift_handle)

    @pytest.fixture
    def thrift_command_id(self):
        """Create a Thrift command ID (not SEA)."""
        mock_thrift_operation_handle = MagicMock()
        mock_thrift_operation_handle.operationId.guid = b"guid"
        mock_thrift_operation_handle.operationId.secret = b"secret"
        return CommandId.from_thrift_handle(mock_thrift_operation_handle)

    def test_initialization(self, mock_http_client):
        """Test client initialization and warehouse ID extraction."""
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
        assert client1.max_download_threads == 10  # Default value

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

        # Test with custom max_download_threads
        client3 = SeaDatabricksClient(
            server_hostname="test-server.databricks.com",
            port=443,
            http_path="/sql/warehouses/abc123",
            http_headers=[],
            auth_provider=AuthProvider(),
            ssl_options=SSLOptions(),
            max_download_threads=5,
        )
        assert client3.max_download_threads == 5

        # Test with invalid HTTP path
        with pytest.raises(ProgrammingError) as excinfo:
            SeaDatabricksClient(
                server_hostname="test-server.databricks.com",
                port=443,
                http_path="/invalid/path",
                http_headers=[],
                auth_provider=AuthProvider(),
                ssl_options=SSLOptions(),
            )
        assert "Could not extract warehouse ID" in str(excinfo.value)

    def test_session_management(self, sea_client, mock_http_client, thrift_session_id):
        """Test session management methods."""
        # Test open_session with minimal parameters
        mock_http_client._make_request.return_value = {"session_id": "test-session-123"}
        session_id = sea_client.open_session(None, None, None)
        assert isinstance(session_id, SessionId)
        assert session_id.backend_type == BackendType.SEA
        assert session_id.guid == "test-session-123"
        mock_http_client._make_request.assert_called_with(
            method="POST", path=sea_client.SESSION_PATH, data={"warehouse_id": "abc123"}
        )

        # Test open_session with all parameters
        mock_http_client.reset_mock()
        mock_http_client._make_request.return_value = {"session_id": "test-session-456"}
        session_config = {
            "ANSI_MODE": "FALSE",  # Supported parameter
            "STATEMENT_TIMEOUT": "3600",  # Supported parameter
            "unsupported_param": "value",  # Unsupported parameter
        }
        catalog = "test_catalog"
        schema = "test_schema"
        session_id = sea_client.open_session(session_config, catalog, schema)
        assert session_id.guid == "test-session-456"
        expected_data = {
            "warehouse_id": "abc123",
            "session_confs": {
                "ansi_mode": "FALSE",
                "statement_timeout": "3600",
            },
            "catalog": catalog,
            "schema": schema,
        }
        mock_http_client._make_request.assert_called_with(
            method="POST", path=sea_client.SESSION_PATH, data=expected_data
        )

        # Test open_session error handling
        mock_http_client.reset_mock()
        mock_http_client._make_request.return_value = {}
        with pytest.raises(Error) as excinfo:
            sea_client.open_session(None, None, None)
        assert "Failed to create session" in str(excinfo.value)

        # Test close_session with valid ID
        mock_http_client.reset_mock()
        session_id = SessionId.from_sea_session_id("test-session-789")
        sea_client.close_session(session_id)
        mock_http_client._make_request.assert_called_with(
            method="DELETE",
            path=sea_client.SESSION_PATH_WITH_ID.format("test-session-789"),
            data={"session_id": "test-session-789", "warehouse_id": "abc123"},
        )

        # Test close_session with invalid ID type
        with pytest.raises(ValueError) as excinfo:
            sea_client.close_session(thrift_session_id)
        assert "Not a valid SEA session ID" in str(excinfo.value)

    def test_command_execution_sync(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test synchronous command execution."""
        # Test synchronous execution
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

        with patch.object(
            sea_client, "get_execution_result", return_value="mock_result_set"
        ) as mock_get_result:
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
            assert result == "mock_result_set"
            cmd_id_arg = mock_get_result.call_args[0][0]
            assert isinstance(cmd_id_arg, CommandId)
            assert cmd_id_arg.guid == "test-statement-123"

        # Test with invalid session ID
        with pytest.raises(ValueError) as excinfo:
            mock_thrift_handle = MagicMock()
            mock_thrift_handle.sessionId.guid = b"guid"
            mock_thrift_handle.sessionId.secret = b"secret"
            thrift_session_id = SessionId.from_thrift_handle(mock_thrift_handle)

            sea_client.execute_command(
                operation="SELECT 1",
                session_id=thrift_session_id,
                max_rows=100,
                max_bytes=1000,
                lz4_compression=False,
                cursor=mock_cursor,
                use_cloud_fetch=False,
                parameters=[],
                async_op=False,
                enforce_embedded_schema_correctness=False,
            )
        assert "Not a valid SEA session ID" in str(excinfo.value)

    def test_command_execution_async(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test asynchronous command execution."""
        # Test asynchronous execution
        execute_response = {
            "statement_id": "test-statement-456",
            "status": {"state": "PENDING"},
        }
        mock_http_client._make_request.return_value = execute_response

        result = sea_client.execute_command(
            operation="SELECT 1",
            session_id=sea_session_id,
            max_rows=100,
            max_bytes=1000,
            lz4_compression=False,
            cursor=mock_cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=True,
            enforce_embedded_schema_correctness=False,
        )
        assert result is None
        assert isinstance(mock_cursor.active_command_id, CommandId)
        assert mock_cursor.active_command_id.guid == "test-statement-456"

        # Test async with missing statement ID
        mock_http_client.reset_mock()
        mock_http_client._make_request.return_value = {"status": {"state": "PENDING"}}
        with pytest.raises(ServerOperationError) as excinfo:
            sea_client.execute_command(
                operation="SELECT 1",
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                lz4_compression=False,
                cursor=mock_cursor,
                use_cloud_fetch=False,
                parameters=[],
                async_op=True,
                enforce_embedded_schema_correctness=False,
            )
        assert "Failed to execute command: No statement ID returned" in str(
            excinfo.value
        )

    def test_command_execution_advanced(
        self, sea_client, mock_http_client, mock_cursor, sea_session_id
    ):
        """Test advanced command execution scenarios."""
        # Test with polling
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
        mock_http_client._make_request.side_effect = [initial_response, poll_response]

        with patch.object(
            sea_client, "get_execution_result", return_value="mock_result_set"
        ) as mock_get_result:
            with patch("time.sleep"):
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
                assert result == "mock_result_set"

        # Test with parameters
        mock_http_client.reset_mock()
        mock_http_client._make_request.side_effect = None  # Reset side_effect
        execute_response = {
            "statement_id": "test-statement-123",
            "status": {"state": "SUCCEEDED"},
        }
        mock_http_client._make_request.return_value = execute_response
        param = ttypes.TSparkParameter(name="param1", value="value1", type="STRING")

        with patch.object(sea_client, "get_execution_result"):
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
            args, kwargs = mock_http_client._make_request.call_args
            assert "parameters" in kwargs["data"]
            assert len(kwargs["data"]["parameters"]) == 1
            assert kwargs["data"]["parameters"][0]["name"] == "param1"
            assert kwargs["data"]["parameters"][0]["value"] == "value1"
            assert kwargs["data"]["parameters"][0]["type"] == "STRING"

        # Test execution failure
        mock_http_client.reset_mock()
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
        mock_http_client._make_request.return_value = error_response

        with patch("time.sleep"):
            with patch.object(
                sea_client, "get_query_state", return_value=CommandState.FAILED
            ):
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
                assert "Command test-statement-123 failed" in str(excinfo.value)

        # Test missing statement ID
        mock_http_client.reset_mock()
        mock_http_client._make_request.return_value = {"status": {"state": "SUCCEEDED"}}
        with pytest.raises(ServerOperationError) as excinfo:
            sea_client.execute_command(
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
        assert "Failed to execute command: No statement ID returned" in str(
            excinfo.value
        )

    def test_command_management(
        self,
        sea_client,
        mock_http_client,
        sea_command_id,
        thrift_command_id,
        mock_cursor,
    ):
        """Test command management methods."""
        # Test cancel_command
        mock_http_client._make_request.return_value = {}
        sea_client.cancel_command(sea_command_id)
        mock_http_client._make_request.assert_called_with(
            method="POST",
            path=sea_client.CANCEL_STATEMENT_PATH_WITH_ID.format("test-statement-123"),
            data={"statement_id": "test-statement-123"},
        )

        # Test cancel_command with invalid ID
        with pytest.raises(ValueError) as excinfo:
            sea_client.cancel_command(thrift_command_id)
        assert "Not a valid SEA command ID" in str(excinfo.value)

        # Test close_command
        mock_http_client.reset_mock()
        sea_client.close_command(sea_command_id)
        mock_http_client._make_request.assert_called_with(
            method="DELETE",
            path=sea_client.STATEMENT_PATH_WITH_ID.format("test-statement-123"),
            data={"statement_id": "test-statement-123"},
        )

        # Test close_command with invalid ID
        with pytest.raises(ValueError) as excinfo:
            sea_client.close_command(thrift_command_id)
        assert "Not a valid SEA command ID" in str(excinfo.value)

        # Test get_query_state
        mock_http_client.reset_mock()
        mock_http_client._make_request.return_value = {
            "statement_id": "test-statement-123",
            "status": {"state": "RUNNING"},
        }
        state = sea_client.get_query_state(sea_command_id)
        assert state == CommandState.RUNNING
        mock_http_client._make_request.assert_called_with(
            method="GET",
            path=sea_client.STATEMENT_PATH_WITH_ID.format("test-statement-123"),
            data={"statement_id": "test-statement-123"},
        )

        # Test get_query_state with invalid ID
        with pytest.raises(ValueError) as excinfo:
            sea_client.get_query_state(thrift_command_id)
        assert "Not a valid SEA command ID" in str(excinfo.value)

        # Test get_execution_result
        mock_http_client.reset_mock()
        sea_response = {
            "statement_id": "test-statement-123",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "format": "JSON_ARRAY",
                "schema": {
                    "column_count": 1,
                    "columns": [
                        {
                            "name": "test_value",
                            "type_text": "INT",
                            "type_name": "INT",
                            "position": 0,
                        }
                    ],
                },
                "total_chunk_count": 1,
                "chunks": [{"chunk_index": 0, "row_offset": 0, "row_count": 1}],
                "total_row_count": 1,
                "truncated": False,
            },
            "result": {
                "chunk_index": 0,
                "row_offset": 0,
                "row_count": 1,
                "data_array": [["1"]],
            },
        }
        mock_http_client._make_request.return_value = sea_response
        result = sea_client.get_execution_result(sea_command_id, mock_cursor)
        assert result.command_id.to_sea_statement_id() == "test-statement-123"
        assert result.status == CommandState.SUCCEEDED

        # Test get_execution_result with invalid ID
        with pytest.raises(ValueError) as excinfo:
            sea_client.get_execution_result(thrift_command_id, mock_cursor)
        assert "Not a valid SEA command ID" in str(excinfo.value)

    def test_check_command_state(self, sea_client, sea_command_id):
        """Test _check_command_not_in_failed_or_closed_state method."""
        # Test with RUNNING state (should not raise)
        sea_client._check_command_not_in_failed_or_closed_state(
            CommandState.RUNNING, sea_command_id
        )

        # Test with SUCCEEDED state (should not raise)
        sea_client._check_command_not_in_failed_or_closed_state(
            CommandState.SUCCEEDED, sea_command_id
        )

        # Test with CLOSED state (should raise DatabaseError)
        with pytest.raises(DatabaseError) as excinfo:
            sea_client._check_command_not_in_failed_or_closed_state(
                CommandState.CLOSED, sea_command_id
            )
        assert "Command test-statement-123 unexpectedly closed server side" in str(
            excinfo.value
        )

        # Test with FAILED state (should raise ServerOperationError)
        with pytest.raises(ServerOperationError) as excinfo:
            sea_client._check_command_not_in_failed_or_closed_state(
                CommandState.FAILED, sea_command_id
            )
        assert "Command test-statement-123 failed" in str(excinfo.value)

    def test_utility_methods(self, sea_client):
        """Test utility methods."""
        # Test get_default_session_configuration_value
        value = SeaDatabricksClient.get_default_session_configuration_value("ANSI_MODE")
        assert value == "true"

        # Test with unsupported configuration parameter
        value = SeaDatabricksClient.get_default_session_configuration_value(
            "UNSUPPORTED_PARAM"
        )
        assert value is None

        # Test with case-insensitive parameter name
        value = SeaDatabricksClient.get_default_session_configuration_value("ansi_mode")
        assert value == "true"

        # Test get_allowed_session_configurations
        configs = SeaDatabricksClient.get_allowed_session_configurations()
        assert isinstance(configs, list)
        assert len(configs) > 0
        assert "ANSI_MODE" in configs

        # Test getting the list of allowed configurations with specific keys
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

        # Test _extract_description_from_manifest
        manifest_obj = MagicMock()
        manifest_obj.schema = {
            "columns": [
                {
                    "name": "col1",
                    "type_name": "STRING",
                    "precision": 10,
                    "scale": 2,
                    "nullable": True,
                },
                {
                    "name": "col2",
                    "type_name": "INT",
                    "nullable": False,
                },
            ]
        }

        description = sea_client._extract_description_from_manifest(manifest_obj)
        assert description is not None
        assert len(description) == 2
        assert description[0][0] == "col1"  # name
        assert description[0][1] == "STRING"  # type_code
        assert description[0][4] == 10  # precision
        assert description[0][5] == 2  # scale
        assert description[0][6] is True  # null_ok
        assert description[1][0] == "col2"  # name
        assert description[1][1] == "INT"  # type_code
        assert description[1][6] is False  # null_ok

    def test_results_message_to_execute_response_is_staging_operation(self, sea_client):
        """Test that is_staging_operation is correctly set from manifest.is_volume_operation."""
        # Test when is_volume_operation is True
        response = MagicMock()
        response.statement_id = "test-statement-123"
        response.status.state = CommandState.SUCCEEDED
        response.manifest.is_volume_operation = True
        response.manifest.result_compression = "NONE"
        response.manifest.format = "JSON_ARRAY"

        # Mock the _extract_description_from_manifest method to return None
        with patch.object(
            sea_client, "_extract_description_from_manifest", return_value=None
        ):
            result = sea_client._results_message_to_execute_response(response)
            assert result.is_staging_operation is True

        # Test when is_volume_operation is False
        response.manifest.is_volume_operation = False
        with patch.object(
            sea_client, "_extract_description_from_manifest", return_value=None
        ):
            result = sea_client._results_message_to_execute_response(response)
            assert result.is_staging_operation is False

    def test_get_catalogs(self, sea_client, sea_session_id, mock_cursor):
        """Test the get_catalogs method."""
        # Mock the execute_command method
        mock_result_set = Mock()
        with patch.object(
            sea_client, "execute_command", return_value=mock_result_set
        ) as mock_execute:
            # Call get_catalogs
            result = sea_client.get_catalogs(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
            )

            # Verify execute_command was called with the correct parameters
            mock_execute.assert_called_once_with(
                operation="SHOW CATALOGS",
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

            # Verify the result is correct
            assert result == mock_result_set

    def test_get_schemas(self, sea_client, sea_session_id, mock_cursor):
        """Test the get_schemas method with various parameter combinations."""
        # Mock the execute_command method
        mock_result_set = Mock()
        with patch.object(
            sea_client, "execute_command", return_value=mock_result_set
        ) as mock_execute:
            # Case 1: With catalog name only
            result = sea_client.get_schemas(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
            )

            mock_execute.assert_called_with(
                operation="SHOW SCHEMAS IN test_catalog",
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

            # Case 2: With catalog and schema names
            result = sea_client.get_schemas(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

            mock_execute.assert_called_with(
                operation="SHOW SCHEMAS IN test_catalog LIKE 'test_schema'",
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

            # Case 3: Without catalog name (should raise ValueError)
            with pytest.raises(DatabaseError) as excinfo:
                sea_client.get_schemas(
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    cursor=mock_cursor,
                )
            assert "Catalog name is required for get_schemas" in str(excinfo.value)

    def test_get_tables(self, sea_client, sea_session_id, mock_cursor):
        """Test the get_tables method with various parameter combinations."""
        # Mock the execute_command method
        from databricks.sql.backend.sea.result_set import SeaResultSet

        mock_result_set = Mock(spec=SeaResultSet)

        with patch.object(
            sea_client, "execute_command", return_value=mock_result_set
        ) as mock_execute:
            # Mock the filter_tables_by_type method
            with patch(
                "databricks.sql.backend.sea.utils.filters.ResultSetFilter.filter_tables_by_type",
                return_value=mock_result_set,
            ) as mock_filter:
                # Case 1: With catalog name only
                result = sea_client.get_tables(
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    cursor=mock_cursor,
                    catalog_name="test_catalog",
                )

                mock_execute.assert_called_with(
                    operation="SHOW TABLES IN CATALOG test_catalog",
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
                mock_filter.assert_called_with(mock_result_set, None)

                # Case 2: With all parameters
                table_types = ["TABLE", "VIEW"]
                result = sea_client.get_tables(
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    cursor=mock_cursor,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                    table_name="test_table",
                    table_types=table_types,
                )

                mock_execute.assert_called_with(
                    operation="SHOW TABLES IN CATALOG test_catalog SCHEMA LIKE 'test_schema' LIKE 'test_table'",
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
                mock_filter.assert_called_with(mock_result_set, table_types)

                # Case 3: With wildcard catalog
                result = sea_client.get_tables(
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    cursor=mock_cursor,
                    catalog_name="*",
                )

                mock_execute.assert_called_with(
                    operation="SHOW TABLES IN ALL CATALOGS",
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

    def test_get_columns(self, sea_client, sea_session_id, mock_cursor):
        """Test the get_columns method with various parameter combinations."""
        # Mock the execute_command method
        mock_result_set = Mock()
        with patch.object(
            sea_client, "execute_command", return_value=mock_result_set
        ) as mock_execute:
            # Case 1: With catalog name only
            result = sea_client.get_columns(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
            )

            mock_execute.assert_called_with(
                operation="SHOW COLUMNS IN CATALOG test_catalog",
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

            # Case 2: With all parameters
            result = sea_client.get_columns(
                session_id=sea_session_id,
                max_rows=100,
                max_bytes=1000,
                cursor=mock_cursor,
                catalog_name="test_catalog",
                schema_name="test_schema",
                table_name="test_table",
                column_name="test_column",
            )

            mock_execute.assert_called_with(
                operation="SHOW COLUMNS IN CATALOG test_catalog SCHEMA LIKE 'test_schema' TABLE LIKE 'test_table' LIKE 'test_column'",
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

            # Case 3: Without catalog name (should raise ValueError)
            with pytest.raises(DatabaseError) as excinfo:
                sea_client.get_columns(
                    session_id=sea_session_id,
                    max_rows=100,
                    max_bytes=1000,
                    cursor=mock_cursor,
                )
            assert "Catalog name is required for get_columns" in str(excinfo.value)
