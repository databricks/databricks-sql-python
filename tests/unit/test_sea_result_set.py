"""
Tests for the SeaResultSet class.

This module contains tests for the SeaResultSet class, which implements
the result set functionality for the SEA (Statement Execution API) backend.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.result_set import SeaResultSet
from databricks.sql.backend.types import CommandId, CommandState, BackendType
from databricks.sql.utils import JsonQueue
from databricks.sql.types import Row


class TestSeaResultSet:
    """Test suite for the SeaResultSet class."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection."""
        connection = Mock()
        connection.open = True
        connection.disable_pandas = False
        return connection

    @pytest.fixture
    def mock_sea_client(self):
        """Create a mock SEA client."""
        client = Mock()
        client.max_download_threads = 10
        return client

    @pytest.fixture
    def execute_response(self):
        """Create a sample execute response."""
        mock_response = Mock()
        mock_response.command_id = CommandId.from_sea_statement_id("test-statement-123")
        mock_response.status = CommandState.SUCCEEDED
        mock_response.has_been_closed_server_side = False
        mock_response.is_direct_results = False
        mock_response.results_queue = None
        mock_response.description = [
            ("col1", "INT", None, None, None, None, None),
            ("col2", "STRING", None, None, None, None, None),
        ]
        mock_response.is_staging_operation = False
        mock_response.lz4_compressed = False
        mock_response.arrow_schema_bytes = b""
        return mock_response

    @pytest.fixture
    def mock_result_data(self):
        """Create mock result data."""
        result_data = Mock()
        result_data.data = [[1, "value1"], [2, "value2"], [3, "value3"]]
        result_data.external_links = None
        return result_data

    @pytest.fixture
    def mock_manifest(self):
        """Create a mock manifest."""
        return Mock()

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

        # Verify that a JsonQueue was created with empty data
        assert isinstance(result_set.results, JsonQueue)
        assert result_set.results.data_array == []

    def test_init_with_result_data(
        self,
        mock_connection,
        mock_sea_client,
        execute_response,
        mock_result_data,
        mock_manifest,
    ):
        """Test initializing SeaResultSet with result data."""
        with patch(
            "databricks.sql.result_set.SeaResultSetQueueFactory"
        ) as mock_factory:
            mock_queue = Mock(spec=JsonQueue)
            mock_factory.build_queue.return_value = mock_queue

            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                buffer_size_bytes=1000,
                arraysize=100,
                result_data=mock_result_data,
                manifest=mock_manifest,
            )

            # Verify that the factory was called with the correct arguments
            mock_factory.build_queue.assert_called_once_with(
                mock_result_data,
                mock_manifest,
                str(execute_response.command_id.to_sea_statement_id()),
                description=execute_response.description,
                max_download_threads=mock_sea_client.max_download_threads,
                sea_client=mock_sea_client,
                lz4_compressed=execute_response.lz4_compressed,
            )

            # Verify that the queue was set correctly
            assert result_set.results == mock_queue

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

    def test_convert_json_table(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test converting JSON data to Row objects."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Sample data
        data = [[1, "value1"], [2, "value2"]]

        # Convert to Row objects
        rows = result_set._convert_json_table(data)

        # Check that we got Row objects with the correct values
        assert len(rows) == 2
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == 1
        assert rows[0].col2 == "value1"
        assert rows[1].col1 == 2
        assert rows[1].col2 == "value2"

    def test_convert_json_table_empty(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test converting empty JSON data."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Empty data
        data = []

        # Convert to Row objects
        rows = result_set._convert_json_table(data)

        # Check that we got an empty list
        assert rows == []

    def test_convert_json_table_no_description(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test converting JSON data with no description."""
        execute_response.description = None
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Sample data
        data = [[1, "value1"], [2, "value2"]]

        # Convert to Row objects
        rows = result_set._convert_json_table(data)

        # Check that we got the original data
        assert rows == data

    def test_fetchone(
        self, mock_connection, mock_sea_client, execute_response, mock_result_data
    ):
        """Test fetching one row."""
        # Create a result set with data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=mock_result_data,
        )

        # Replace the results queue with a JsonQueue containing test data
        result_set.results = JsonQueue([[1, "value1"], [2, "value2"], [3, "value3"]])

        # Fetch one row
        row = result_set.fetchone()

        # Check that we got a Row object with the correct values
        assert isinstance(row, Row)
        assert row.col1 == 1
        assert row.col2 == "value1"

        # Check that the row index was updated
        assert result_set._next_row_index == 1

    def test_fetchone_empty(self, mock_connection, mock_sea_client, execute_response):
        """Test fetching one row from an empty result set."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Fetch one row
        row = result_set.fetchone()

        # Check that we got None
        assert row is None

    def test_fetchmany(
        self, mock_connection, mock_sea_client, execute_response, mock_result_data
    ):
        """Test fetching multiple rows."""
        # Create a result set with data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=mock_result_data,
        )

        # Replace the results queue with a JsonQueue containing test data
        result_set.results = JsonQueue([[1, "value1"], [2, "value2"], [3, "value3"]])

        # Fetch two rows
        rows = result_set.fetchmany(2)

        # Check that we got two Row objects with the correct values
        assert len(rows) == 2
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == 1
        assert rows[0].col2 == "value1"
        assert rows[1].col1 == 2
        assert rows[1].col2 == "value2"

        # Check that the row index was updated
        assert result_set._next_row_index == 2

    def test_fetchmany_negative_size(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test fetching with a negative size."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Try to fetch with a negative size
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set.fetchmany(-1)

    def test_fetchall(
        self, mock_connection, mock_sea_client, execute_response, mock_result_data
    ):
        """Test fetching all rows."""
        # Create a result set with data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=mock_result_data,
        )

        # Replace the results queue with a JsonQueue containing test data
        result_set.results = JsonQueue([[1, "value1"], [2, "value2"], [3, "value3"]])

        # Fetch all rows
        rows = result_set.fetchall()

        # Check that we got three Row objects with the correct values
        assert len(rows) == 3
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == 1
        assert rows[0].col2 == "value1"
        assert rows[1].col1 == 2
        assert rows[1].col2 == "value2"
        assert rows[2].col1 == 3
        assert rows[2].col2 == "value3"

        # Check that the row index was updated
        assert result_set._next_row_index == 3

    def test_fetchmany_json(
        self, mock_connection, mock_sea_client, execute_response, mock_result_data
    ):
        """Test fetching JSON data directly."""
        # Create a result set with data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=mock_result_data,
        )

        # Replace the results queue with a JsonQueue containing test data
        result_set.results = JsonQueue([[1, "value1"], [2, "value2"], [3, "value3"]])

        # Fetch two rows as JSON
        rows = result_set.fetchmany_json(2)

        # Check that we got the raw data
        assert rows == [[1, "value1"], [2, "value2"]]

        # Check that the row index was updated
        assert result_set._next_row_index == 2

    def test_fetchall_json(
        self, mock_connection, mock_sea_client, execute_response, mock_result_data
    ):
        """Test fetching all JSON data directly."""
        # Create a result set with data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=mock_result_data,
        )

        # Replace the results queue with a JsonQueue containing test data
        result_set.results = JsonQueue([[1, "value1"], [2, "value2"], [3, "value3"]])

        # Fetch all rows as JSON
        rows = result_set.fetchall_json()

        # Check that we got the raw data
        assert rows == [[1, "value1"], [2, "value2"], [3, "value3"]]

        # Check that the row index was updated
        assert result_set._next_row_index == 3

    def test_iteration(
        self, mock_connection, mock_sea_client, execute_response, mock_result_data
    ):
        """Test iterating over the result set."""
        # Create a result set with data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=mock_result_data,
        )

        # Replace the results queue with a JsonQueue containing test data
        result_set.results = JsonQueue([[1, "value1"], [2, "value2"], [3, "value3"]])

        # Iterate over the result set
        rows = list(result_set)

        # Check that we got three Row objects with the correct values
        assert len(rows) == 3
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == 1
        assert rows[0].col2 == "value1"
        assert rows[1].col1 == 2
        assert rows[1].col2 == "value2"
        assert rows[2].col1 == 3
        assert rows[2].col2 == "value3"

        # Check that the row index was updated
        assert result_set._next_row_index == 3
