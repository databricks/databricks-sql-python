"""
Tests for the ResultSetFilter class.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
from typing import List, Dict, Any

# Add the necessary path to import the filter module
sys.path.append("/home/varun.edachali/conn/databricks-sql-python/src")

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
        self.mock_sea_result_set._sea_client = MagicMock()
        self.mock_sea_result_set._buffer_size_bytes = 1000
        self.mock_sea_result_set._arraysize = 100

    @patch("databricks.sql.backend.filters.SeaResultSet")
    def test_filter_tables_by_type(self, mock_sea_result_set_class):
        """Test filtering tables by type."""
        # Set up the mock to return a new mock when instantiated
        mock_instance = MagicMock()
        mock_sea_result_set_class.return_value = mock_instance

        # Test with specific table types
        table_types = ["TABLE", "VIEW"]

        # Make the mock_sea_result_set appear to be a SeaResultSet
        with patch("databricks.sql.backend.filters.isinstance", return_value=True):
            result = ResultSetFilter.filter_tables_by_type(
                self.mock_sea_result_set, table_types
            )

        # Verify the filter was applied correctly
        mock_sea_result_set_class.assert_called_once()
        call_kwargs = mock_sea_result_set_class.call_args[1]

        # Check that the filtered response contains only TABLE and VIEW
        filtered_data = call_kwargs["sea_response"]["result"]["data_array"]
        self.assertEqual(len(filtered_data), 2)
        self.assertEqual(filtered_data[0][3], "TABLE")
        self.assertEqual(filtered_data[1][3], "VIEW")

        # Check row count was updated
        self.assertEqual(call_kwargs["sea_response"]["result"]["row_count"], 2)

    @patch("databricks.sql.backend.filters.SeaResultSet")
    def test_filter_tables_by_type_case_insensitive(self, mock_sea_result_set_class):
        """Test filtering tables by type with case insensitivity."""
        # Set up the mock to return a new mock when instantiated
        mock_instance = MagicMock()
        mock_sea_result_set_class.return_value = mock_instance

        # Test with lowercase table types
        table_types = ["table", "view"]

        # Make the mock_sea_result_set appear to be a SeaResultSet
        with patch("databricks.sql.backend.filters.isinstance", return_value=True):
            result = ResultSetFilter.filter_tables_by_type(
                self.mock_sea_result_set, table_types
            )

        # Verify the filter was applied correctly
        mock_sea_result_set_class.assert_called_once()
        call_kwargs = mock_sea_result_set_class.call_args[1]

        # Check that the filtered response contains only TABLE and VIEW
        filtered_data = call_kwargs["sea_response"]["result"]["data_array"]
        self.assertEqual(len(filtered_data), 2)
        self.assertEqual(filtered_data[0][3], "TABLE")
        self.assertEqual(filtered_data[1][3], "VIEW")

    @patch("databricks.sql.backend.filters.SeaResultSet")
    def test_filter_tables_by_type_default(self, mock_sea_result_set_class):
        """Test filtering tables by type with default types."""
        # Set up the mock to return a new mock when instantiated
        mock_instance = MagicMock()
        mock_sea_result_set_class.return_value = mock_instance

        # Make the mock_sea_result_set appear to be a SeaResultSet
        with patch("databricks.sql.backend.filters.isinstance", return_value=True):
            result = ResultSetFilter.filter_tables_by_type(
                self.mock_sea_result_set, None
            )

        # Verify the filter was applied correctly
        mock_sea_result_set_class.assert_called_once()
        call_kwargs = mock_sea_result_set_class.call_args[1]

        # Check that the filtered response contains TABLE, VIEW, and SYSTEM TABLE
        filtered_data = call_kwargs["sea_response"]["result"]["data_array"]
        self.assertEqual(len(filtered_data), 3)
        self.assertEqual(filtered_data[0][3], "TABLE")
        self.assertEqual(filtered_data[1][3], "VIEW")
        self.assertEqual(filtered_data[2][3], "SYSTEM TABLE")


if __name__ == "__main__":
    unittest.main()
