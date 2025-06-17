"""
Tests for the SeaResultSet class.
"""

import pytest
import unittest 
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.result_set import SeaResultSet
from databricks.sql.backend.types import CommandId, CommandState, BackendType


class TestSeaResultSet(unittest.TestCase):
    """Tests for the SeaResultSet class."""

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
            ("test_value", "INT", None, None, None, None, None)
        ]
        mock_response.is_staging_operation = False
        return mock_response

    def test_init_with_inline_data(self):
        """Test initialization with inline data."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest

        result_data = ResultData(
            data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None
        )
        manifest = ResultManifest(
            format="JSON_ARRAY",
            schema={},
            total_row_count=3,
            total_byte_count=0,
            total_chunk_count=1,
            truncated=False,
            chunks=None,
            result_compression=None,
        )

        result_set = SeaResultSet(
            connection=self.mock_connection,
            execute_response=self.mock_execute_response_inline,
            sea_client=self.mock_backend,
            buffer_size_bytes=1000,
            arraysize=100,
            result_data=result_data,
            manifest=manifest,
        )

        # Check properties
        self.assertEqual(result_set.backend, self.mock_backend)
        self.assertEqual(result_set.buffer_size_bytes, 1000)
        self.assertEqual(result_set.arraysize, 100)

        # Check statement ID
        self.assertEqual(result_set.statement_id, "test-statement-id")

        # Check status
        self.assertEqual(result_set.status, CommandState.SUCCEEDED)

        # Check description
        self.assertEqual(result_set.description, self.sample_description)

        # Check results queue
        self.assertTrue(isinstance(result_set.results, JsonQueue))

    def test_init_without_result_data(self):
        """Test initialization without result data."""
        # Create a result set without providing result_data
        result_set = SeaResultSet(
            connection=self.mock_connection,
            execute_response=self.mock_execute_response_inline,
            sea_client=self.mock_backend,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Check properties
        self.assertEqual(result_set.backend, self.mock_backend)
        self.assertEqual(result_set.statement_id, "test-statement-id")
        self.assertEqual(result_set.status, CommandState.SUCCEEDED)
        self.assertEqual(result_set.description, self.sample_description)
        self.assertTrue(isinstance(result_set.results, JsonQueue))

        # Verify that the results queue is empty
        self.assertEqual(result_set.results.data_array, [])

    def test_init_with_error(self):
        """Test initialization with error response."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            execute_response=self.mock_execute_response_error,
            sea_client=self.mock_backend,
        )

        # Check status
        self.assertEqual(result_set.status, CommandState.FAILED)

        # Check that description is None
        self.assertIsNone(result_set.description)

    def test_close(self):
        """Test closing the result set."""
        # Setup
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest

        result_data = ResultData(data=[[1, "Alice"]], external_links=None)
        manifest = ResultManifest(
            format="JSON_ARRAY",
            schema={},
            total_row_count=1,
            total_byte_count=0,
            total_chunk_count=1,
            truncated=False,
            chunks=None,
            result_compression=None,
        )

        result_set = SeaResultSet(
            connection=self.mock_connection,
            execute_response=self.mock_execute_response_inline,
            sea_client=self.mock_backend,
            result_data=result_data,
            manifest=manifest,
        )

        # Mock the backend's close_command method
        self.mock_backend.close_command = MagicMock()

        # Execute
        result_set.close()

        # Verify
        self.mock_backend.close_command.assert_called_once_with(self.mock_command_id)

    def test_is_staging_operation(self):
        """Test is_staging_operation property."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            execute_response=self.mock_execute_response_inline,
            sea_client=self.mock_backend,
        )

        self.assertFalse(result_set.is_staging_operation)

        # Verify the backend's close_command was NOT called
        mock_sea_client.close_command.assert_not_called()
        assert result_set.has_been_closed_server_side is True
        assert result_set.status == CommandState.CLOSED
