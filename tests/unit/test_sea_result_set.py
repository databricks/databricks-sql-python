"""
Tests for the SeaResultSet class.

This module contains tests for the SeaResultSet class, which implements
the result set functionality for the SEA (Statement Execution API) backend.
"""

import pytest
from unittest.mock import Mock

from databricks.sql.result_set import SeaResultSet, Row
from databricks.sql.utils import JsonQueue
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest


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
        mock_response.is_direct_results = False
        mock_response.results_queue = None
        mock_response.description = [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
            ("col3", "boolean", None, None, None, None, None),
        ]
        mock_response.is_staging_operation = False
        mock_response.lz4_compressed = False
        mock_response.arrow_schema_bytes = None
        return mock_response

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return [
            ["value1", "1", "true"],
            ["value2", "2", "false"],
            ["value3", "3", "true"],
            ["value4", "4", "false"],
            ["value5", "5", "true"],
        ]

    @pytest.fixture
    def result_set_with_data(
        self, mock_connection, mock_sea_client, execute_response, sample_data
    ):
        """Create a SeaResultSet with sample data."""
        # Create ResultData with inline data
        result_data = ResultData(
            data=sample_data, external_links=None, row_count=len(sample_data)
        )

        # Initialize SeaResultSet with result data
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            result_data=result_data,
            manifest=None,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = JsonQueue(sample_data)

        return result_set

    @pytest.fixture
    def json_queue(self, sample_data):
        """Create a JsonQueue with sample data."""
        return JsonQueue(sample_data)

    def empty_manifest(self):
        """Create an empty manifest."""
        return ResultManifest(
            format="JSON_ARRAY",
            schema={},
            total_row_count=0,
            total_byte_count=0,
            total_chunk_count=0,
            truncated=False,
            is_volume_operation=False,
        )

    def test_init_with_execute_response(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with an execute response."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            result_data=ResultData(data=[]),
            manifest=self.empty_manifest(),
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
            result_data=ResultData(data=[]),
            manifest=self.empty_manifest(),
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
            result_data=ResultData(data=[]),
            manifest=self.empty_manifest(),
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
            result_data=ResultData(data=[]),
            manifest=self.empty_manifest(),
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Close the result set
        result_set.close()

        # Verify the backend's close_command was NOT called
        mock_sea_client.close_command.assert_not_called()
        assert result_set.has_been_closed_server_side is True
        assert result_set.status == CommandState.CLOSED

    def test_init_with_result_data(self, result_set_with_data, sample_data):
        """Test initializing SeaResultSet with result data."""
        # Verify the results queue was created correctly
        assert isinstance(result_set_with_data.results, JsonQueue)
        assert result_set_with_data.results.data_array == sample_data
        assert result_set_with_data.results.n_valid_rows == len(sample_data)

    def test_convert_json_types(self, result_set_with_data, sample_data):
        """Test the _convert_json_types method."""
        # Call _convert_json_types
        converted_rows = result_set_with_data._convert_json_types(sample_data)

        # Verify the conversion
        assert len(converted_rows) == len(sample_data)
        assert converted_rows[0][0] == "value1"  # string stays as string
        assert converted_rows[0][1] == 1  # "1" converted to int
        assert converted_rows[0][2] is True  # "true" converted to boolean

    def test_create_json_table(self, result_set_with_data, sample_data):
        """Test the _create_json_table method."""
        # Call _create_json_table
        result_rows = result_set_with_data._create_json_table(sample_data)

        # Verify the result
        assert len(result_rows) == len(sample_data)
        assert isinstance(result_rows[0], Row)
        assert result_rows[0].col1 == "value1"
        assert result_rows[0].col2 == 1
        assert result_rows[0].col3 is True

    def test_fetchmany_json(self, result_set_with_data):
        """Test the fetchmany_json method."""
        # Test fetching a subset of rows
        result = result_set_with_data.fetchmany_json(2)
        assert len(result) == 2
        assert result_set_with_data._next_row_index == 2

        # Test fetching the next subset
        result = result_set_with_data.fetchmany_json(2)
        assert len(result) == 2
        assert result_set_with_data._next_row_index == 4

        # Test fetching more than available
        result = result_set_with_data.fetchmany_json(10)
        assert len(result) == 1  # Only one row left
        assert result_set_with_data._next_row_index == 5

    def test_fetchall_json(self, result_set_with_data, sample_data):
        """Test the fetchall_json method."""
        # Test fetching all rows
        result = result_set_with_data.fetchall_json()
        assert result == sample_data
        assert result_set_with_data._next_row_index == len(sample_data)

        # Test fetching again (should return empty)
        result = result_set_with_data.fetchall_json()
        assert result == []
        assert result_set_with_data._next_row_index == len(sample_data)

    def test_fetchone(self, result_set_with_data):
        """Test the fetchone method."""
        # Test fetching one row at a time
        row1 = result_set_with_data.fetchone()
        assert isinstance(row1, Row)
        assert row1.col1 == "value1"
        assert row1.col2 == 1
        assert row1.col3 is True
        assert result_set_with_data._next_row_index == 1

        row2 = result_set_with_data.fetchone()
        assert isinstance(row2, Row)
        assert row2.col1 == "value2"
        assert row2.col2 == 2
        assert row2.col3 is False
        assert result_set_with_data._next_row_index == 2

        # Fetch the rest
        result_set_with_data.fetchall()

        # Test fetching when no more rows
        row_none = result_set_with_data.fetchone()
        assert row_none is None

    def test_fetchmany(self, result_set_with_data):
        """Test the fetchmany method."""
        # Test fetching multiple rows
        rows = result_set_with_data.fetchmany(2)
        assert len(rows) == 2
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == "value1"
        assert rows[0].col2 == 1
        assert rows[0].col3 is True
        assert rows[1].col1 == "value2"
        assert rows[1].col2 == 2
        assert rows[1].col3 is False
        assert result_set_with_data._next_row_index == 2

        # Test with invalid size
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set_with_data.fetchmany(-1)

    def test_fetchall(self, result_set_with_data, sample_data):
        """Test the fetchall method."""
        # Test fetching all rows
        rows = result_set_with_data.fetchall()
        assert len(rows) == len(sample_data)
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == "value1"
        assert rows[0].col2 == 1
        assert rows[0].col3 is True
        assert result_set_with_data._next_row_index == len(sample_data)

        # Test fetching again (should return empty)
        rows = result_set_with_data.fetchall()
        assert len(rows) == 0

    def test_iteration(self, result_set_with_data, sample_data):
        """Test iterating over the result set."""
        # Test iteration
        rows = list(result_set_with_data)
        assert len(rows) == len(sample_data)
        assert isinstance(rows[0], Row)
        assert rows[0].col1 == "value1"
        assert rows[0].col2 == 1
        assert rows[0].col3 is True

    def test_fetchmany_arrow_not_implemented(
        self, mock_connection, mock_sea_client, execute_response, sample_data
    ):
        """Test that fetchmany_arrow raises NotImplementedError for non-JSON data."""

        # Test that NotImplementedError is raised
        with pytest.raises(
            NotImplementedError,
            match="EXTERNAL_LINKS disposition is not implemented for SEA backend",
        ):
            # Create a result set without JSON data
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=None, external_links=[]),
                manifest=self.empty_manifest(),
                buffer_size_bytes=1000,
                arraysize=100,
            )

    def test_fetchall_arrow_not_implemented(
        self, mock_connection, mock_sea_client, execute_response, sample_data
    ):
        """Test that fetchall_arrow raises NotImplementedError for non-JSON data."""
        # Test that NotImplementedError is raised
        with pytest.raises(
            NotImplementedError,
            match="EXTERNAL_LINKS disposition is not implemented for SEA backend",
        ):
            # Create a result set without JSON data
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=None, external_links=[]),
                manifest=self.empty_manifest(),
                buffer_size_bytes=1000,
                arraysize=100,
            )

    def test_is_staging_operation(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test the is_staging_operation property."""
        # Set is_staging_operation to True
        execute_response.is_staging_operation = True

        # Create a result set
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            result_data=ResultData(data=[]),
            manifest=self.empty_manifest(),
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Test the property
        assert result_set.is_staging_operation is True
