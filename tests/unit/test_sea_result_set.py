"""
Tests for the SeaResultSet class.

This module contains tests for the SeaResultSet class, which implements
the result set functionality for the SEA (Statement Execution API) backend.
"""

import pytest
from unittest.mock import Mock, patch

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql.backend.sea.result_set import SeaResultSet, Row
from databricks.sql.backend.sea.queue import JsonQueue
from databricks.sql.backend.sea.utils.constants import ResultFormat
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest


class TestSeaResultSet:
    """Test suite for the SeaResultSet class."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection."""
        connection = Mock()
        connection.open = True
        connection.session = Mock()
        connection.session.ssl_options = Mock()
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
        mock_response.has_more_rows = False
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

    def _create_empty_manifest(self, format: ResultFormat):
        """Create an empty manifest."""
        return ResultManifest(
            format=format.value,
            schema={},
            total_row_count=-1,
            total_byte_count=-1,
            total_chunk_count=-1,
        )

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
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue",
            return_value=JsonQueue(sample_data),
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=result_data,
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
                buffer_size_bytes=1000,
                arraysize=100,
            )

        return result_set

    @pytest.fixture
    def mock_arrow_queue(self):
        """Create a mock Arrow queue."""
        queue = Mock()
        if pyarrow is not None:
            queue.next_n_rows.return_value = Mock(spec=pyarrow.Table)
            queue.next_n_rows.return_value.num_rows = 0
            queue.remaining_rows.return_value = Mock(spec=pyarrow.Table)
            queue.remaining_rows.return_value.num_rows = 0
        return queue

    @pytest.fixture
    def mock_json_queue(self):
        """Create a mock JSON queue."""
        queue = Mock(spec=JsonQueue)
        queue.next_n_rows.return_value = []
        queue.remaining_rows.return_value = []
        return queue

    @pytest.fixture
    def result_set_with_arrow_queue(
        self, mock_connection, mock_sea_client, execute_response, mock_arrow_queue
    ):
        """Create a SeaResultSet with an Arrow queue."""
        # Create ResultData with external links
        result_data = ResultData(data=None, external_links=[], row_count=0)

        # Initialize SeaResultSet with result data
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue",
            return_value=mock_arrow_queue,
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=result_data,
                manifest=ResultManifest(
                    format=ResultFormat.ARROW_STREAM.value,
                    schema={},
                    total_row_count=0,
                    total_byte_count=0,
                    total_chunk_count=0,
                ),
                buffer_size_bytes=1000,
                arraysize=100,
            )

        return result_set

    @pytest.fixture
    def result_set_with_json_queue(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Create a SeaResultSet with a JSON queue."""
        # Create ResultData with inline data
        result_data = ResultData(data=[], external_links=None, row_count=0)

        # Initialize SeaResultSet with result data
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue",
            return_value=mock_json_queue,
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=result_data,
                manifest=ResultManifest(
                    format=ResultFormat.JSON_ARRAY.value,
                    schema={},
                    total_row_count=0,
                    total_byte_count=0,
                    total_chunk_count=0,
                ),
                buffer_size_bytes=1000,
                arraysize=100,
            )

        return result_set

    def test_init_with_execute_response(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with an execute response."""
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue"
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=[]),
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
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

    def test_init_with_invalid_command_id(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with invalid command ID."""
        # Mock the command ID to return None
        mock_command_id = Mock()
        mock_command_id.to_sea_statement_id.return_value = None
        execute_response.command_id = mock_command_id

        with pytest.raises(ValueError, match="Command ID is not a SEA statement ID"):
            SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=[]),
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
                buffer_size_bytes=1000,
                arraysize=100,
            )

    def test_close(self, mock_connection, mock_sea_client, execute_response):
        """Test closing a result set."""
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue"
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=[]),
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
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
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue"
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=[]),
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
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
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue"
        ):
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=[]),
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
                buffer_size_bytes=1000,
                arraysize=100,
            )

        # Close the result set
        result_set.close()

        # Verify the backend's close_command was NOT called
        mock_sea_client.close_command.assert_not_called()
        assert result_set.has_been_closed_server_side is True
        assert result_set.status == CommandState.CLOSED

    def test_convert_json_types(self, result_set_with_data, sample_data):
        """Test the _convert_json_types method."""
        # Call _convert_json_types
        converted_row = result_set_with_data._convert_json_types(sample_data[0])

        # Verify the conversion
        assert converted_row[0] == "value1"  # string stays as string
        assert converted_row[1] == 1  # "1" converted to int
        assert converted_row[2] is True  # "true" converted to boolean

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_convert_json_to_arrow_table(self, result_set_with_data, sample_data):
        """Test the _convert_json_to_arrow_table method."""
        # Call _convert_json_to_arrow_table
        result_table = result_set_with_data._convert_json_to_arrow_table(sample_data)

        # Verify the result
        assert isinstance(result_table, pyarrow.Table)
        assert result_table.num_rows == len(sample_data)
        assert result_table.num_columns == 3

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_convert_json_to_arrow_table_empty(self, result_set_with_data):
        """Test the _convert_json_to_arrow_table method with empty data."""
        # Call _convert_json_to_arrow_table with empty data
        result_table = result_set_with_data._convert_json_to_arrow_table([])

        # Verify the result
        assert isinstance(result_table, pyarrow.Table)
        assert result_table.num_rows == 0

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

    def test_fetchmany_json_negative_size(self, result_set_with_data):
        """Test the fetchmany_json method with negative size."""
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set_with_data.fetchmany_json(-1)

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

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_fetchmany_arrow(self, result_set_with_data, sample_data):
        """Test the fetchmany_arrow method."""
        # Test with JSON queue (should convert to Arrow)
        result = result_set_with_data.fetchmany_arrow(2)
        assert isinstance(result, pyarrow.Table)
        assert result.num_rows == 2
        assert result_set_with_data._next_row_index == 2

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_fetchmany_arrow_negative_size(self, result_set_with_data):
        """Test the fetchmany_arrow method with negative size."""
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set_with_data.fetchmany_arrow(-1)

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_fetchall_arrow(self, result_set_with_data, sample_data):
        """Test the fetchall_arrow method."""
        # Test with JSON queue (should convert to Arrow)
        result = result_set_with_data.fetchall_arrow()
        assert isinstance(result, pyarrow.Table)
        assert result.num_rows == len(sample_data)
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

    def test_is_staging_operation(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test the is_staging_operation property."""
        # Set is_staging_operation to True
        execute_response.is_staging_operation = True

        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue"
        ):
            # Create a result set
            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                result_data=ResultData(data=[]),
                manifest=self._create_empty_manifest(ResultFormat.JSON_ARRAY),
                buffer_size_bytes=1000,
                arraysize=100,
            )

        # Test the property
        assert result_set.is_staging_operation is True

    # Edge case tests
    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_fetchone_empty_arrow_queue(self, result_set_with_arrow_queue):
        """Test fetchone with an empty Arrow queue."""
        # Setup _convert_arrow_table to return empty list
        result_set_with_arrow_queue._convert_arrow_table = Mock(return_value=[])

        # Call fetchone
        result = result_set_with_arrow_queue.fetchone()

        # Verify result is None
        assert result is None

        # Verify _convert_arrow_table was called
        result_set_with_arrow_queue._convert_arrow_table.assert_called_once()

    def test_fetchone_empty_json_queue(self, result_set_with_json_queue):
        """Test fetchone with an empty JSON queue."""
        # Setup _create_json_table to return empty list
        result_set_with_json_queue._create_json_table = Mock(return_value=[])

        # Call fetchone
        result = result_set_with_json_queue.fetchone()

        # Verify result is None
        assert result is None

        # Verify _create_json_table was called
        result_set_with_json_queue._create_json_table.assert_called_once()

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_fetchmany_empty_arrow_queue(self, result_set_with_arrow_queue):
        """Test fetchmany with an empty Arrow queue."""
        # Setup _convert_arrow_table to return empty list
        result_set_with_arrow_queue._convert_arrow_table = Mock(return_value=[])

        # Call fetchmany
        result = result_set_with_arrow_queue.fetchmany(10)

        # Verify result is an empty list
        assert result == []

        # Verify _convert_arrow_table was called
        result_set_with_arrow_queue._convert_arrow_table.assert_called_once()

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_fetchall_empty_arrow_queue(self, result_set_with_arrow_queue):
        """Test fetchall with an empty Arrow queue."""
        # Setup _convert_arrow_table to return empty list
        result_set_with_arrow_queue._convert_arrow_table = Mock(return_value=[])

        # Call fetchall
        result = result_set_with_arrow_queue.fetchall()

        # Verify result is an empty list
        assert result == []

        # Verify _convert_arrow_table was called
        result_set_with_arrow_queue._convert_arrow_table.assert_called_once()

    @patch("databricks.sql.backend.sea.utils.conversion.SqlTypeConverter.convert_value")
    def test_convert_json_types_with_errors(
        self, mock_convert_value, result_set_with_data
    ):
        """Test error handling in _convert_json_types."""
        # Mock the conversion to fail for the second and third values
        mock_convert_value.side_effect = [
            "value1",  # First value converts normally
            Exception("Invalid int"),  # Second value fails
            Exception("Invalid boolean"),  # Third value fails
        ]

        # Data with invalid values
        data_row = ["value1", "not_an_int", "not_a_boolean"]

        # Should not raise an exception but log warnings
        result = result_set_with_data._convert_json_types(data_row)

        # The first value should be converted normally
        assert result[0] == "value1"

        # The invalid values should remain as strings
        assert result[1] == "not_an_int"
        assert result[2] == "not_a_boolean"

    @patch("databricks.sql.backend.sea.result_set.logger")
    @patch("databricks.sql.backend.sea.utils.conversion.SqlTypeConverter.convert_value")
    def test_convert_json_types_with_logging(
        self, mock_convert_value, mock_logger, result_set_with_data
    ):
        """Test that errors in _convert_json_types are logged."""
        # Mock the conversion to fail for the second and third values
        mock_convert_value.side_effect = [
            "value1",  # First value converts normally
            Exception("Invalid int"),  # Second value fails
            Exception("Invalid boolean"),  # Third value fails
        ]

        # Data with invalid values
        data_row = ["value1", "not_an_int", "not_a_boolean"]

        # Call the method
        result_set_with_data._convert_json_types(data_row)

        # Verify warnings were logged
        assert mock_logger.warning.call_count == 2
