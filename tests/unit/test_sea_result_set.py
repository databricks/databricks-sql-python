"""
Tests for the SeaResultSet class.

This module contains tests for the SeaResultSet class, which implements
the result set functionality for the SEA (Statement Execution API) backend.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.result_set import SeaResultSet
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
        mock_response.has_more_rows = False
        mock_response.results_queue = None
        mock_response.description = [
            ("test_value", "INT", None, None, None, None, None)
        ]
        mock_response.is_staging_operation = False
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

    def test_unimplemented_methods(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test that unimplemented methods raise NotImplementedError."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Test each unimplemented method individually with specific error messages
        with pytest.raises(
            NotImplementedError, match="fetchone is not implemented for SEA backend"
        ):
            result_set.fetchone()

        with pytest.raises(
            NotImplementedError, match="fetchmany is not implemented for SEA backend"
        ):
            result_set.fetchmany(10)

        with pytest.raises(
            NotImplementedError, match="fetchmany is not implemented for SEA backend"
        ):
            # Test with default parameter value
            result_set.fetchmany()

        with pytest.raises(
            NotImplementedError, match="fetchall is not implemented for SEA backend"
        ):
            result_set.fetchall()

        with pytest.raises(
            NotImplementedError,
            match="fetchmany_arrow is not implemented for SEA backend",
        ):
            result_set.fetchmany_arrow(10)

        with pytest.raises(
            NotImplementedError,
            match="fetchall_arrow is not implemented for SEA backend",
        ):
            result_set.fetchall_arrow()

        with pytest.raises(
            NotImplementedError, match="fetchone is not implemented for SEA backend"
        ):
            # Test iteration protocol (calls fetchone internally)
            next(iter(result_set))

        with pytest.raises(
            NotImplementedError, match="fetchone is not implemented for SEA backend"
        ):
            # Test using the result set in a for loop
            for row in result_set:
                pass

    def test_fill_results_buffer_not_implemented(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test that _fill_results_buffer raises NotImplementedError."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        with pytest.raises(
            NotImplementedError,
            match="_fill_results_buffer is not implemented for SEA backend",
        ):
            result_set._fill_results_buffer()
