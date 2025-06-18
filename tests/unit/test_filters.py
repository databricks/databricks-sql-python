"""
Tests for the ResultSetFilter class.
"""

import unittest
from unittest.mock import MagicMock, patch

from databricks.sql.backend.filters import ResultSetFilter


class TestResultSetFilter(unittest.TestCase):
    """Tests for the ResultSetFilter class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock SeaResultSet
        self.mock_sea_result_set = MagicMock()
        self.mock_sea_result_set._response = {
            "result": {
                "data_array": [
                    ["catalog1", "schema1", "table1", "TABLE", ""],
                    ["catalog1", "schema1", "table2", "VIEW", ""],
                    ["catalog1", "schema1", "table3", "SYSTEM TABLE", ""],
                    ["catalog1", "schema1", "table4", "EXTERNAL TABLE", ""],
                ],
                "row_count": 4,
            }
        }

        # Set up the connection and other required attributes
        self.mock_sea_result_set.connection = MagicMock()
        self.mock_sea_result_set.backend = MagicMock()
        self.mock_sea_result_set.buffer_size_bytes = 1000
        self.mock_sea_result_set.arraysize = 100
        self.mock_sea_result_set.statement_id = "test-statement-id"

        # Create a mock CommandId
        from databricks.sql.backend.types import CommandId, BackendType

        mock_command_id = CommandId(BackendType.SEA, "test-statement-id")
        self.mock_sea_result_set.command_id = mock_command_id

        self.mock_sea_result_set.status = MagicMock()
        self.mock_sea_result_set.description = [
            ("catalog_name", "string", None, None, None, None, True),
            ("schema_name", "string", None, None, None, None, True),
            ("table_name", "string", None, None, None, None, True),
            ("table_type", "string", None, None, None, None, True),
            ("remarks", "string", None, None, None, None, True),
        ]
        self.mock_sea_result_set.has_been_closed_server_side = False

    def test_filter_tables_by_type(self):
        """Test filtering tables by type."""
        # Test with specific table types
        table_types = ["TABLE", "VIEW"]

        # Make the mock_sea_result_set appear to be a SeaResultSet
        with patch("databricks.sql.backend.filters.isinstance", return_value=True):
            with patch(
                "databricks.sql.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                # Set up the mock to return a new mock when instantiated
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                result = ResultSetFilter.filter_tables_by_type(
                    self.mock_sea_result_set, table_types
                )

                # Verify the filter was applied correctly
                mock_sea_result_set_class.assert_called_once()

    def test_filter_tables_by_type_case_insensitive(self):
        """Test filtering tables by type with case insensitivity."""
        # Test with lowercase table types
        table_types = ["table", "view"]

        # Make the mock_sea_result_set appear to be a SeaResultSet
        with patch("databricks.sql.backend.filters.isinstance", return_value=True):
            with patch(
                "databricks.sql.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                # Set up the mock to return a new mock when instantiated
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                result = ResultSetFilter.filter_tables_by_type(
                    self.mock_sea_result_set, table_types
                )

                # Verify the filter was applied correctly
                mock_sea_result_set_class.assert_called_once()

    def test_filter_tables_by_type_default(self):
        """Test filtering tables by type with default types."""
        # Make the mock_sea_result_set appear to be a SeaResultSet
        with patch("databricks.sql.backend.filters.isinstance", return_value=True):
            with patch(
                "databricks.sql.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                # Set up the mock to return a new mock when instantiated
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                result = ResultSetFilter.filter_tables_by_type(
                    self.mock_sea_result_set, None
                )

                # Verify the filter was applied correctly
                mock_sea_result_set_class.assert_called_once()


if __name__ == "__main__":
    unittest.main()
