"""
Tests for the SeaResultSet class.

This module contains tests for the SeaResultSet class, which implements
the result set functionality for the SEA (Statement Execution API) backend.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.result_set import SeaResultSet
from databricks.sql.utils import JsonQueue
from databricks.sql.backend.types import CommandId, CommandState, BackendType


class TestSeaResultSet:
    """Test suite for the SeaResultSet class."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection."""
        connection = Mock()
        connection.open = True
        return connection

    @pytest.fixture
    def mock_sea_client(self):
        """Create a mock SEA client."""
        return Mock()

    @pytest.fixture
    def execute_response(self):
        """Create a sample execute response."""
        mock_response = Mock()
        mock_response.command_id = CommandId.from_sea_statement_id("test-statement-123")
        mock_response.status = CommandState.SUCCEEDED
        mock_response.has_been_closed_server_side = False
        mock_response.description = [
            ("test_value", "INT", None, None, None, None, None)
        ]
        mock_response.is_staging_operation = False
        mock_response.lz4_compressed = False
        mock_response.arrow_schema_bytes = b""
        return mock_response

    def test_init_with_execute_response(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with an execute response."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Verify basic properties
        assert result_set.command_id == execute_response.command_id
        assert result_set.status == CommandState.SUCCEEDED
        assert result_set.connection == mock_connection
        assert result_set.backend == mock_sea_client
        assert result_set.buffer_size_bytes == 1000
        assert result_set.arraysize == 100
        assert result_set.description == execute_response.description

    def test_close(self, mock_connection, mock_sea_client, execute_response):
        """Test closing a result set."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Close the result set
        result_set.close()

        # Verify the backend's close_command was called
        mock_sea_client.close_command.assert_called_once_with(result_set.command_id)
        assert result_set.has_been_closed_server_side is True
        assert result_set.status == CommandState.CLOSED

    def test_close_when_already_closed_server_side(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test closing a result set that has already been closed server-side."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.has_been_closed_server_side = True

        # Close the result set
        result_set.close()

        # Verify the backend's close_command was NOT called
        mock_sea_client.close_command.assert_not_called()
        assert result_set.has_been_closed_server_side is True
        assert result_set.status == CommandState.CLOSED

    def test_close_when_connection_closed(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test closing a result set when the connection is closed."""
        mock_connection.open = False
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Close the result set
        result_set.close()

        # Verify the backend's close_command was NOT called
        mock_sea_client.close_command.assert_not_called()
        assert result_set.has_been_closed_server_side is True
        assert result_set.status == CommandState.CLOSED

    @pytest.fixture
    def mock_json_queue(self):
        """Create a mock JsonQueue."""
        mock_queue = Mock(spec=JsonQueue)
        mock_queue.next_n_rows.return_value = [["value1", 123], ["value2", 456]]
        mock_queue.remaining_rows.return_value = [
            ["value1", 123],
            ["value2", 456],
            ["value3", 789],
        ]
        return mock_queue

    def test_fetchmany(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test fetchmany method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_json_queue
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]

        # Test with specific size
        rows = result_set.fetchmany(2)
        assert len(rows) == 2
        assert rows[0].col1 == "value1"
        assert rows[0].col2 == 123
        assert rows[1].col1 == "value2"
        assert rows[1].col2 == 456

        # Test with default size (arraysize)
        result_set.arraysize = 2
        mock_json_queue.next_n_rows.reset_mock()
        rows = result_set.fetchmany(result_set.arraysize)
        mock_json_queue.next_n_rows.assert_called_with(2)

        # Test with negative size
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set.fetchmany(-1)

    def test_fetchall(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test fetchall method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_json_queue
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]

        rows = result_set.fetchall()
        assert len(rows) == 3
        assert rows[0].col1 == "value1"
        assert rows[0].col2 == 123
        assert rows[1].col1 == "value2"
        assert rows[1].col2 == 456
        assert rows[2].col1 == "value3"
        assert rows[2].col2 == 789

        # Verify _next_row_index is updated
        assert result_set._next_row_index == 3

    def test_fetchmany_json(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test fetchmany_json method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_json_queue

        # Test with specific size
        result_set.fetchmany_json(2)
        mock_json_queue.next_n_rows.assert_called_with(2)

        # Test with negative size
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set.fetchmany_json(-1)

    def test_fetchall_json(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test fetchall_json method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_json_queue

        # Test fetchall_json
        result_set.fetchall_json()
        mock_json_queue.remaining_rows.assert_called_once()

    def test_convert_json_rows(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test _convert_json_rows method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Test with description and rows
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]
        rows = [["value1", 123], ["value2", 456]]
        converted_rows = result_set._convert_json_rows(rows)

        assert len(converted_rows) == 2
        assert converted_rows[0].col1 == "value1"
        assert converted_rows[0].col2 == 123
        assert converted_rows[1].col1 == "value2"
        assert converted_rows[1].col2 == 456

        # Test with no description
        result_set.description = None
        converted_rows = result_set._convert_json_rows(rows)
        assert converted_rows == rows

        # Test with empty rows
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]
        converted_rows = result_set._convert_json_rows([])
        assert converted_rows == []
