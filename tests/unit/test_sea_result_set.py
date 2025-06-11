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

    @pytest.fixture
    def mock_results_queue(self):
        """Create a mock results queue."""
        mock_queue = Mock()
        mock_queue.next_n_rows.return_value = [["value1", 123], ["value2", 456]]
        mock_queue.remaining_rows.return_value = [
            ["value1", 123],
            ["value2", 456],
            ["value3", 789],
        ]
        return mock_queue

    def test_fill_results_buffer(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test that _fill_results_buffer returns None."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        assert result_set._fill_results_buffer() is None

    def test_convert_to_row_objects(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test converting raw data rows to Row objects."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Test with empty description
        result_set.description = None
        rows = [["value1", 123], ["value2", 456]]
        converted_rows = result_set._convert_to_row_objects(rows)
        assert converted_rows == rows

        # Test with empty rows
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]
        assert result_set._convert_to_row_objects([]) == []

        # Test with description and rows
        rows = [["value1", 123], ["value2", 456]]
        converted_rows = result_set._convert_to_row_objects(rows)
        assert len(converted_rows) == 2
        assert converted_rows[0].col1 == "value1"
        assert converted_rows[0].col2 == 123
        assert converted_rows[1].col1 == "value2"
        assert converted_rows[1].col2 == 456

    def test_fetchone(
        self, mock_connection, mock_sea_client, execute_response, mock_results_queue
    ):
        """Test fetchone method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_results_queue
        result_set.description = [
            ("col1", "STRING", None, None, None, None, None),
            ("col2", "INT", None, None, None, None, None),
        ]

        # Mock the next_n_rows to return a single row
        mock_results_queue.next_n_rows.return_value = [["value1", 123]]

        row = result_set.fetchone()
        assert row is not None
        assert row.col1 == "value1"
        assert row.col2 == 123

        # Test when no rows are available
        mock_results_queue.next_n_rows.return_value = []
        assert result_set.fetchone() is None

    def test_fetchmany(
        self, mock_connection, mock_sea_client, execute_response, mock_results_queue
    ):
        """Test fetchmany method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_results_queue
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
        mock_results_queue.next_n_rows.reset_mock()
        rows = result_set.fetchmany()
        mock_results_queue.next_n_rows.assert_called_with(2)

        # Test with negative size
        with pytest.raises(
            ValueError, match="size argument for fetchmany is -1 but must be >= 0"
        ):
            result_set.fetchmany(-1)

    def test_fetchall(
        self, mock_connection, mock_sea_client, execute_response, mock_results_queue
    ):
        """Test fetchall method."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_results_queue
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

    @pytest.mark.skipif(
        pytest.importorskip("pyarrow", reason="PyArrow is not installed") is None,
        reason="PyArrow is not installed",
    )
    def test_create_empty_arrow_table(
        self, mock_connection, mock_sea_client, execute_response, monkeypatch
    ):
        """Test creating an empty Arrow table with schema."""
        import pyarrow

        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Mock _arrow_schema_bytes to return a valid schema
        schema = pyarrow.schema(
            [
                pyarrow.field("col1", pyarrow.string()),
                pyarrow.field("col2", pyarrow.int32()),
            ]
        )
        schema_bytes = schema.serialize().to_pybytes()
        monkeypatch.setattr(result_set, "_arrow_schema_bytes", schema_bytes)

        # Test with schema bytes
        empty_table = result_set._create_empty_arrow_table()
        assert isinstance(empty_table, pyarrow.Table)
        assert empty_table.num_rows == 0
        assert empty_table.num_columns == 2
        assert empty_table.schema.names == ["col1", "col2"]

        # Test without schema bytes but with description
        monkeypatch.setattr(result_set, "_arrow_schema_bytes", b"")
        result_set.description = [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
        ]

        empty_table = result_set._create_empty_arrow_table()
        assert isinstance(empty_table, pyarrow.Table)
        assert empty_table.num_rows == 0
        assert empty_table.num_columns == 2
        assert empty_table.schema.names == ["col1", "col2"]

    @pytest.mark.skipif(
        pytest.importorskip("pyarrow", reason="PyArrow is not installed") is None,
        reason="PyArrow is not installed",
    )
    def test_convert_rows_to_arrow_table(
        self, mock_connection, mock_sea_client, execute_response
    ):
        """Test converting rows to Arrow table."""
        import pyarrow

        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        result_set.description = [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
        ]

        rows = [["value1", 123], ["value2", 456], ["value3", 789]]

        arrow_table = result_set._convert_rows_to_arrow_table(rows)
        assert isinstance(arrow_table, pyarrow.Table)
        assert arrow_table.num_rows == 3
        assert arrow_table.num_columns == 2
        assert arrow_table.schema.names == ["col1", "col2"]

        # Check data
        assert arrow_table.column(0).to_pylist() == ["value1", "value2", "value3"]
        assert arrow_table.column(1).to_pylist() == [123, 456, 789]

    @pytest.mark.skipif(
        pytest.importorskip("pyarrow", reason="PyArrow is not installed") is None,
        reason="PyArrow is not installed",
    )
    def test_fetchmany_arrow(
        self, mock_connection, mock_sea_client, execute_response, mock_results_queue
    ):
        """Test fetchmany_arrow method."""
        import pyarrow

        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_results_queue
        result_set.description = [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
        ]

        # Test with data
        arrow_table = result_set.fetchmany_arrow(2)
        assert isinstance(arrow_table, pyarrow.Table)
        assert arrow_table.num_rows == 2
        assert arrow_table.column(0).to_pylist() == ["value1", "value2"]
        assert arrow_table.column(1).to_pylist() == [123, 456]

        # Test with no data
        mock_results_queue.next_n_rows.return_value = []

        # Mock _create_empty_arrow_table to return an empty table
        result_set._create_empty_arrow_table = Mock()
        empty_table = pyarrow.Table.from_pydict({"col1": [], "col2": []})
        result_set._create_empty_arrow_table.return_value = empty_table

        arrow_table = result_set.fetchmany_arrow(2)
        assert isinstance(arrow_table, pyarrow.Table)
        assert arrow_table.num_rows == 0
        result_set._create_empty_arrow_table.assert_called_once()

    @pytest.mark.skipif(
        pytest.importorskip("pyarrow", reason="PyArrow is not installed") is None,
        reason="PyArrow is not installed",
    )
    def test_fetchall_arrow(
        self, mock_connection, mock_sea_client, execute_response, mock_results_queue
    ):
        """Test fetchall_arrow method."""
        import pyarrow

        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_results_queue
        result_set.description = [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
        ]

        # Test with data
        arrow_table = result_set.fetchall_arrow()
        assert isinstance(arrow_table, pyarrow.Table)
        assert arrow_table.num_rows == 3
        assert arrow_table.column(0).to_pylist() == ["value1", "value2", "value3"]
        assert arrow_table.column(1).to_pylist() == [123, 456, 789]

        # Test with no data
        mock_results_queue.remaining_rows.return_value = []

        # Mock _create_empty_arrow_table to return an empty table
        result_set._create_empty_arrow_table = Mock()
        empty_table = pyarrow.Table.from_pydict({"col1": [], "col2": []})
        result_set._create_empty_arrow_table.return_value = empty_table

        arrow_table = result_set.fetchall_arrow()
        assert isinstance(arrow_table, pyarrow.Table)
        assert arrow_table.num_rows == 0
        result_set._create_empty_arrow_table.assert_called_once()

    def test_iteration_protocol(
        self, mock_connection, mock_sea_client, execute_response, mock_results_queue
    ):
        """Test iteration protocol using fetchone."""
        result_set = SeaResultSet(
            connection=mock_connection,
            execute_response=execute_response,
            sea_client=mock_sea_client,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        result_set.results = mock_results_queue
        result_set.description = [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
        ]

        # Set up mock to return different values on each call
        mock_results_queue.next_n_rows.side_effect = [
            [["value1", 123]],
            [["value2", 456]],
            [],  # End of data
        ]

        # Test iteration
        rows = list(result_set)
        assert len(rows) == 2
        assert rows[0].col1 == "value1"
        assert rows[0].col2 == 123
        assert rows[1].col1 == "value2"
        assert rows[1].col2 == 456
