"""
Tests for the SeaResultSet class.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
from typing import Dict, List, Any, Optional

# Add the necessary path to import the modules
sys.path.append("/home/varun.edachali/conn/databricks-sql-python/src")

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql.result_set import SeaResultSet
from databricks.sql.backend.types import CommandId, CommandState, ExecuteResponse
from databricks.sql.utils import JsonQueue


class TestSeaResultSet(unittest.TestCase):
    """Tests for the SeaResultSet class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock connection and client
        self.mock_connection = MagicMock()
        self.mock_connection.open = True
        self.mock_backend = MagicMock()

        # Sample description
        self.sample_description = [
            ("id", "INTEGER", None, None, 10, 0, False),
            ("name", "VARCHAR", None, None, None, None, True),
        ]

        # Create a mock CommandId
        self.mock_command_id = MagicMock()
        self.mock_command_id.to_sea_statement_id.return_value = "test-statement-id"

        # Create a mock ExecuteResponse for inline data
        self.mock_execute_response_inline = ExecuteResponse(
            command_id=self.mock_command_id,
            status=CommandState.SUCCEEDED,
            description=self.sample_description,
            has_more_rows=True,
            has_been_closed_server_side=False,
            lz4_compressed=False,
            is_staging_operation=False,
        )

        # Create a mock ExecuteResponse for error
        self.mock_execute_response_error = ExecuteResponse(
            command_id=self.mock_command_id,
            status=CommandState.FAILED,
            description=None,
            has_more_rows=False,
            has_been_closed_server_side=False,
            lz4_compressed=False,
            is_staging_operation=False,
        )

    def test_init_with_inline_data(self):
        """Test initialization with inline data."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        
        result_data = ResultData(data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None)
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

    def test_fetchone(self):
        """Test fetchone method."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        
        result_data = ResultData(data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None)
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
            result_data=result_data,
            manifest=manifest,
        )

        # First row
        row = result_set.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row.id, 1)
        self.assertEqual(row.name, "Alice")

        # Second row
        row = result_set.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row.id, 2)
        self.assertEqual(row.name, "Bob")

        # Third row
        row = result_set.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row.id, 3)
        self.assertEqual(row.name, "Charlie")

        # No more rows
        row = result_set.fetchone()
        self.assertIsNone(row)

    def test_fetchmany(self):
        """Test fetchmany method."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        
        result_data = ResultData(data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None)
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
            result_data=result_data,
            manifest=manifest,
        )

        # Fetch 2 rows
        rows = result_set.fetchmany(2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].id, 1)
        self.assertEqual(rows[0].name, "Alice")
        self.assertEqual(rows[1].id, 2)
        self.assertEqual(rows[1].name, "Bob")

        # Fetch remaining rows
        rows = result_set.fetchmany(2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, 3)
        self.assertEqual(rows[0].name, "Charlie")

        # No more rows
        rows = result_set.fetchmany(2)
        self.assertEqual(len(rows), 0)

    def test_fetchall(self):
        """Test fetchall method."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        
        result_data = ResultData(data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None)
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
            result_data=result_data,
            manifest=manifest,
        )

        # Fetch all rows
        rows = result_set.fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].id, 1)
        self.assertEqual(rows[0].name, "Alice")
        self.assertEqual(rows[1].id, 2)
        self.assertEqual(rows[1].name, "Bob")
        self.assertEqual(rows[2].id, 3)
        self.assertEqual(rows[2].name, "Charlie")

        # No more rows
        rows = result_set.fetchall()
        self.assertEqual(len(rows), 0)

    @unittest.skipIf(pyarrow is None, "PyArrow not installed")
    def test_fetchmany_arrow(self):
        """Test fetchmany_arrow method."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        
        result_data = ResultData(data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None)
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
            result_data=result_data,
            manifest=manifest,
        )

        # Fetch 2 rows as Arrow table
        arrow_table = result_set.fetchmany_arrow(2)
        self.assertEqual(arrow_table.num_rows, 2)
        self.assertEqual(arrow_table.column_names, ["id", "name"])
        self.assertEqual(arrow_table["id"].to_pylist(), [1, 2])
        self.assertEqual(arrow_table["name"].to_pylist(), ["Alice", "Bob"])

        # Fetch remaining rows as Arrow table
        arrow_table = result_set.fetchmany_arrow(2)
        self.assertEqual(arrow_table.num_rows, 1)
        self.assertEqual(arrow_table["id"].to_pylist(), [3])
        self.assertEqual(arrow_table["name"].to_pylist(), ["Charlie"])

        # No more rows
        arrow_table = result_set.fetchmany_arrow(2)
        self.assertEqual(arrow_table.num_rows, 0)

    @unittest.skipIf(pyarrow is None, "PyArrow not installed")
    def test_fetchall_arrow(self):
        """Test fetchall_arrow method."""
        # Create mock result data and manifest
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        
        result_data = ResultData(data=[[1, "Alice"], [2, "Bob"], [3, "Charlie"]], external_links=None)
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
            result_data=result_data,
            manifest=manifest,
        )

        # Fetch all rows as Arrow table
        arrow_table = result_set.fetchall_arrow()
        self.assertEqual(arrow_table.num_rows, 3)
        self.assertEqual(arrow_table.column_names, ["id", "name"])
        self.assertEqual(arrow_table["id"].to_pylist(), [1, 2, 3])
        self.assertEqual(arrow_table["name"].to_pylist(), ["Alice", "Bob", "Charlie"])

        # No more rows
        arrow_table = result_set.fetchall_arrow()
        self.assertEqual(arrow_table.num_rows, 0)

    def test_fill_results_buffer(self):
        """Test _fill_results_buffer method."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            execute_response=self.mock_execute_response_inline,
            sea_client=self.mock_backend,
        )

        # After filling buffer, has more rows is False for INLINE disposition
        result_set._fill_results_buffer()
        self.assertFalse(result_set.has_more_rows)


if __name__ == "__main__":
    unittest.main()