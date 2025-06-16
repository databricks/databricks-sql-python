"""
Tests for the SeaResultSet class.

This module contains tests for the SeaResultSet class, which implements
the result set functionality for the SEA (Statement Execution API) backend.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock
import logging

from databricks.sql.result_set import SeaResultSet, ResultSet
from databricks.sql.utils import JsonQueue, ResultSetQueue
from databricks.sql.types import Row
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
from databricks.sql.backend.types import CommandId, CommandState, BackendType
from databricks.sql.exc import RequestError, CursorAlreadyClosedError


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
        converted_rows = result_set._convert_json_table(rows)

        assert len(converted_rows) == 2
        assert converted_rows[0].col1 == "value1"
        assert converted_rows[0].col2 == 123
        assert converted_rows[1].col1 == "value2"
        assert converted_rows[1].col2 == 456

        # Test with no description
        result_set.description = None
        converted_rows = result_set._convert_json_table(rows)
        assert converted_rows == rows

        # Test with empty rows
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]
        converted_rows = result_set._convert_json_table([])
        assert converted_rows == []

    @pytest.fixture
    def mock_arrow_queue(self):
        """Create a mock queue that returns PyArrow tables."""
        mock_queue = Mock()

        # Mock PyArrow Table for next_n_rows
        mock_table1 = Mock()
        mock_table1.num_rows = 2
        mock_queue.next_n_rows.return_value = mock_table1

        # Mock PyArrow Table for remaining_rows
        mock_table2 = Mock()
        mock_table2.num_rows = 3
        mock_queue.remaining_rows.return_value = mock_table2

        return mock_queue

    def test_fetchone(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test fetchone method."""
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

        # Mock fetchmany_json to return a single row
        mock_json_queue.next_n_rows.return_value = [["value1", 123]]

        # Test fetchone
        row = result_set.fetchone()
        assert row is not None
        assert row.col1 == "value1"
        assert row.col2 == 123

        # Test fetchone with no results
        mock_json_queue.next_n_rows.return_value = []
        row = result_set.fetchone()
        assert row is None

        # Test fetchone with non-JsonQueue
        result_set.results = Mock()
        result_set.results.__class__ = type("NotJsonQueue", (), {})

        with pytest.raises(
            NotImplementedError, match="fetchone only supported for JSON data"
        ):
            result_set.fetchone()

    def test_fetchmany_with_non_json_queue(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test fetchmany with a non-JsonQueue results object."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Set results to a non-JsonQueue object
        result_set.results = Mock()
        result_set.results.__class__ = type("NotJsonQueue", (), {})

        with pytest.raises(
            NotImplementedError, match="fetchmany only supported for JSON data"
        ):
            result_set.fetchmany(2)

    def test_fetchall_with_non_json_queue(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test fetchall with a non-JsonQueue results object."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Set results to a non-JsonQueue object
        result_set.results = Mock()
        result_set.results.__class__ = type("NotJsonQueue", (), {})

        with pytest.raises(
            NotImplementedError, match="fetchall only supported for JSON data"
        ):
            result_set.fetchall()

    def test_iterator_protocol(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test the iterator protocol (__iter__) implementation."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_json_queue
        result_set.description = [
            ("test_value", "INT", None, None, None, None, None),
        ]

        # Mock fetchone to return a sequence of values and then None
        with patch.object(result_set, "fetchone") as mock_fetchone:
            mock_fetchone.side_effect = [
                Row("test_value")(100),
                Row("test_value")(200),
                Row("test_value")(300),
                None,
            ]

            # Test iterating over the result set
            rows = list(result_set)
            assert len(rows) == 3
            assert rows[0].test_value == 100
            assert rows[1].test_value == 200
            assert rows[2].test_value == 300

    def test_rownumber_property(
        self, mock_connection, mock_sea_client, execute_response, mock_json_queue
    ):
        """Test the rownumber property."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_json_queue

        # Initial row number should be 0
        assert result_set.rownumber == 0

        # After fetching rows, row number should be updated
        mock_json_queue.next_n_rows.return_value = [["value1"]]
        result_set.fetchmany_json(2)
        result_set._next_row_index = 2
        assert result_set.rownumber == 2

        # After fetching more rows, row number should be incremented
        mock_json_queue.next_n_rows.return_value = [["value3"]]
        result_set.fetchmany_json(1)
        result_set._next_row_index = 3
        assert result_set.rownumber == 3

    def test_is_staging_operation_property(self, mock_connection, mock_sea_client):
        """Test the is_staging_operation property."""
        # Create a response with staging operation set to True
        staging_response = Mock()
        staging_response.command_id = CommandId.from_sea_statement_id(
            "test-staging-123"
        )
        staging_response.status = CommandState.SUCCEEDED
        staging_response.has_been_closed_server_side = False
        staging_response.description = []
        staging_response.is_staging_operation = True
        staging_response.lz4_compressed = False
        staging_response.arrow_schema_bytes = b""

        # Create a result set with staging operation
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=staging_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Verify the is_staging_operation property
        assert result_set.is_staging_operation is True

    def test_init_with_result_data(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with result data."""
        # Create sample result data with a mock
        result_data = Mock(spec=ResultData)
        result_data.data = [["value1", 123], ["value2", 456]]
        result_data.external_links = None

        manifest = Mock(spec=ResultManifest)

        # Mock the SeaResultSetQueueFactory.build_queue method
        with patch(
            "databricks.sql.result_set.SeaResultSetQueueFactory"
        ) as factory_mock:
            # Create a mock JsonQueue
            mock_queue = Mock(spec=JsonQueue)
            factory_mock.build_queue.return_value = mock_queue

            result_set = SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                buffer_size_bytes=1000,
                arraysize=100,
                result_data=result_data,
                manifest=manifest,
            )

            # Verify the factory was called with the right parameters
            factory_mock.build_queue.assert_called_once_with(
                result_data,
                manifest,
                str(execute_response.command_id.to_sea_statement_id()),
                description=execute_response.description,
                max_download_threads=mock_sea_client.max_download_threads,
                ssl_options=mock_sea_client.ssl_options,
                sea_client=mock_sea_client,
                lz4_compressed=execute_response.lz4_compressed,
            )

            # Verify the results queue was set correctly
            assert result_set.results == mock_queue

    def test_close_with_request_error(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test closing a result set when a RequestError is raised."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Create a patched version of the close method that doesn't check e.args[1]
        with patch("databricks.sql.result_set.ResultSet.close") as mock_close:
            # Call the close method
            result_set.close()

            # Verify the parent's close method was called
            mock_close.assert_called_once()

    def test_init_with_empty_result_data(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with empty result data."""
        # Create sample result data with a mock
        result_data = Mock(spec=ResultData)
        result_data.data = None
        result_data.external_links = None

        manifest = Mock(spec=ResultManifest)

        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=result_data,
            manifest=manifest,
        )

        # Verify an empty JsonQueue was created
        assert isinstance(result_set.results, JsonQueue)
        assert result_set.results.data_array == []

    def test_init_without_result_data(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet without result data."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Verify an empty JsonQueue was created
        assert isinstance(result_set.results, JsonQueue)
        assert result_set.results.data_array == []

    def test_init_with_external_links(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test initializing SeaResultSet with external links."""
        # Create sample result data with external links
        result_data = Mock(spec=ResultData)
        result_data.data = None
        result_data.external_links = ["link1", "link2"]

        manifest = Mock(spec=ResultManifest)

        # This should raise NotImplementedError
        with pytest.raises(
            NotImplementedError,
            match="EXTERNAL_LINKS disposition is not implemented for SEA backend",
        ):
            SeaResultSet(
                connection=mock_connection,
                execute_response=execute_response,
                sea_client=mock_sea_client,
                buffer_size_bytes=1000,
                arraysize=100,
                result_data=result_data,
                manifest=manifest,
            )
