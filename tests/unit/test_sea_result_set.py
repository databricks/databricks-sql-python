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

from databricks.sql.backend.sea_result_set import SeaResultSet
from databricks.sql.backend.types import CommandState
from databricks.sql.backend.models import (
    StatementStatus,
    ResultManifest,
    ResultData,
    ColumnInfo,
    ServiceError,
)
from databricks.sql.utils import JsonQueue, SeaResultSetQueueFactory


class TestSeaResultSet(unittest.TestCase):
    """Tests for the SeaResultSet class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock connection and client
        self.mock_connection = MagicMock()
        self.mock_connection.open = True
        self.mock_backend = MagicMock()

        # Sample SEA response with inline data
        self.sample_response_inline = {
            "statement_id": "test-statement-id",
            "status": {"state": "SUCCEEDED", "sql_state": "00000"},
            "manifest": {
                "schema": {
                    "columns": [
                        {
                            "name": "id",
                            "type_name": "INTEGER",
                            "type_text": "INTEGER",
                            "nullable": False,
                            "precision": 10,
                            "scale": 0,
                            "ordinal_position": 1,
                        },
                        {
                            "name": "name",
                            "type_name": "VARCHAR",
                            "type_text": "VARCHAR(100)",
                            "nullable": True,
                            "precision": None,
                            "scale": None,
                            "ordinal_position": 2,
                        },
                    ]
                },
                "total_row_count": 3,
                "total_byte_count": 100,
                "truncated": False,
                "chunk_count": 1,
            },
            "result": {
                "data_array": [[1, "Alice"], [2, "Bob"], [3, "Charlie"]],
                "row_count": 3,
            },
        }

        # Sample SEA response with error
        self.sample_response_error = {
            "statement_id": "test-error-statement-id",
            "status": {
                "state": "FAILED",
                "sql_state": "42000",
                "error": {"message": "Syntax error", "error_code": "SYNTAX_ERROR"},
            },
        }

        # Sample SEA response with external links
        self.sample_response_external = {
            "statement_id": "test-external-statement-id",
            "status": {"state": "SUCCEEDED", "sql_state": "00000"},
            "manifest": {
                "schema": {
                    "columns": [
                        {
                            "name": "id",
                            "type_name": "INTEGER",
                            "type_text": "INTEGER",
                            "nullable": False,
                            "precision": 10,
                            "scale": 0,
                            "ordinal_position": 1,
                        },
                        {
                            "name": "name",
                            "type_name": "VARCHAR",
                            "type_text": "VARCHAR(100)",
                            "nullable": True,
                            "precision": None,
                            "scale": None,
                            "ordinal_position": 2,
                        },
                    ]
                },
                "total_row_count": 1000,
                "total_byte_count": 10000,
                "truncated": False,
                "chunk_count": 3,
            },
            "result": {
                "external_links": [
                    {
                        "chunk_index": 0,
                        "row_count": 500,
                        "byte_count": 5000,
                        "url": "https://example.com/chunk0",
                    },
                    {
                        "chunk_index": 1,
                        "row_count": 300,
                        "byte_count": 3000,
                        "url": "https://example.com/chunk1",
                    },
                    {
                        "chunk_index": 2,
                        "row_count": 200,
                        "byte_count": 2000,
                        "url": "https://example.com/chunk2",
                    },
                ]
            },
        }

    def test_init_with_inline_data(self):
        """Test initialization with inline data."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
            buffer_size_bytes=1000,
            arraysize=100,
        )

        # Check that the response is stored
        self.assertEqual(result_set._response, self.sample_response_inline)
        self.assertEqual(result_set.backend, self.mock_backend)
        self.assertEqual(result_set.buffer_size_bytes, 1000)
        self.assertEqual(result_set.arraysize, 100)

        # Check statement ID
        self.assertEqual(result_set.statement_id, "test-statement-id")

        # Check status
        self.assertEqual(result_set.status.state, CommandState.SUCCEEDED)
        self.assertEqual(result_set.status.sql_state, "00000")
        self.assertIsNone(result_set.status.error)

        # Check manifest
        self.assertIsNotNone(result_set.manifest)
        self.assertEqual(result_set.manifest.total_row_count, 3)
        self.assertEqual(result_set.manifest.total_byte_count, 100)
        self.assertEqual(result_set.manifest.chunk_count, 1)
        self.assertFalse(result_set.manifest.truncated)

        # Check schema
        self.assertEqual(len(result_set.manifest.schema), 2)
        self.assertEqual(result_set.manifest.schema[0].name, "id")
        self.assertEqual(result_set.manifest.schema[0].type_name, "INTEGER")
        self.assertEqual(result_set.manifest.schema[1].name, "name")
        self.assertEqual(result_set.manifest.schema[1].type_name, "VARCHAR")

        # Check description
        self.assertIsNotNone(result_set.description)
        self.assertEqual(len(result_set.description), 2)
        self.assertEqual(result_set.description[0][0], "id")  # name
        self.assertEqual(result_set.description[0][1], "INTEGER")  # type_code
        self.assertEqual(result_set.description[0][4], 10)  # precision
        self.assertEqual(result_set.description[0][5], 0)  # scale
        self.assertEqual(result_set.description[0][6], False)  # null_ok

        # Check result data
        self.assertIsNotNone(result_set.result)
        self.assertTrue(isinstance(result_set.results, JsonQueue))

    def test_init_with_error(self):
        """Test initialization with error response."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_error,
            sea_client=self.mock_backend,
        )

        # Check status
        self.assertEqual(result_set.status.state, CommandState.FAILED)
        self.assertEqual(result_set.status.sql_state, "42000")
        self.assertIsNotNone(result_set.status.error)
        self.assertEqual(result_set.status.error.message, "Syntax error")
        self.assertEqual(result_set.status.error.error_code, "SYNTAX_ERROR")

        # Check that manifest and result are None
        self.assertIsNone(result_set.manifest)
        self.assertIsNone(result_set.result)
        self.assertIsNone(result_set.description)

    def test_init_with_external_links(self):
        """Test initialization with external links."""
        with self.assertRaises(NotImplementedError):
            result_set = SeaResultSet(
                connection=self.mock_connection,
                sea_response=self.sample_response_external,
                sea_client=self.mock_backend,
            )

    def test_extract_description_from_manifest(self):
        """Test extraction of description from manifest."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
        )

        description = result_set._extract_description_from_manifest()
        self.assertIsNotNone(description)
        self.assertEqual(len(description), 2)

        # Check first column
        self.assertEqual(description[0][0], "id")  # name
        self.assertEqual(description[0][1], "INTEGER")  # type_code
        self.assertIsNone(description[0][2])  # display_size
        self.assertIsNone(description[0][3])  # internal_size
        self.assertEqual(description[0][4], 10)  # precision
        self.assertEqual(description[0][5], 0)  # scale
        self.assertEqual(description[0][6], False)  # null_ok

        # Check second column
        self.assertEqual(description[1][0], "name")  # name
        self.assertEqual(description[1][1], "VARCHAR")  # type_code
        self.assertIsNone(description[1][4])  # precision
        self.assertIsNone(description[1][5])  # scale
        self.assertEqual(description[1][6], True)  # null_ok

    def test_close(self):
        """Test closing the result set."""
        # Setup
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
        )

        # Create a patch for CommandId.from_sea_statement_id
        with patch(
            "databricks.sql.backend.types.CommandId.from_sea_statement_id"
        ) as mock_from_sea_statement_id:
            mock_command_id = MagicMock()
            mock_from_sea_statement_id.return_value = mock_command_id

            # Execute
            result_set.close()

            # Verify
            mock_from_sea_statement_id.assert_called_once_with("test-statement-id")
            self.mock_backend.close_command.assert_called_once_with(mock_command_id)

    def test_is_staging_operation(self):
        """Test is_staging_operation property."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
        )

        self.assertFalse(result_set.is_staging_operation)

    def test_fetchone(self):
        """Test fetchone method."""
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
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
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
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
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
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
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
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
        result_set = SeaResultSet(
            connection=self.mock_connection,
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
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
            sea_response=self.sample_response_inline,
            sea_client=self.mock_backend,
        )

        # Initially has more rows is True because we have data
        self.assertTrue(result_set._has_more_rows)

        # After filling buffer, has more rows is False for INLINE disposition
        result_set._fill_results_buffer()
        self.assertFalse(result_set._has_more_rows)


if __name__ == "__main__":
    unittest.main()
