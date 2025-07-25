"""
Tests for the ResultSetFilter class.
"""

import unittest
from unittest.mock import MagicMock, patch

from databricks.sql.backend.sea.utils.filters import ResultSetFilter


class TestResultSetFilter(unittest.TestCase):
    """Tests for the ResultSetFilter class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock SeaResultSet
        self.mock_sea_result_set = MagicMock()

        # Set up the remaining_rows method on the results attribute
        self.mock_sea_result_set.results = MagicMock()
        self.mock_sea_result_set.results.remaining_rows.return_value = [
            ["catalog1", "schema1", "table1", "owner1", "2023-01-01", "TABLE", ""],
            ["catalog1", "schema1", "table2", "owner1", "2023-01-01", "VIEW", ""],
            [
                "catalog1",
                "schema1",
                "table3",
                "owner1",
                "2023-01-01",
                "SYSTEM TABLE",
                "",
            ],
            [
                "catalog1",
                "schema1",
                "table4",
                "owner1",
                "2023-01-01",
                "EXTERNAL TABLE",
                "",
            ],
        ]

        # Set up the connection and other required attributes
        self.mock_sea_result_set.connection = MagicMock()
        self.mock_sea_result_set.backend = MagicMock()
        self.mock_sea_result_set.buffer_size_bytes = 1000
        self.mock_sea_result_set.arraysize = 100
        self.mock_sea_result_set.statement_id = "test-statement-id"
        self.mock_sea_result_set.lz4_compressed = False

        # Create a mock CommandId
        from databricks.sql.backend.types import CommandId, BackendType

        mock_command_id = CommandId(BackendType.SEA, "test-statement-id")
        self.mock_sea_result_set.command_id = mock_command_id

        self.mock_sea_result_set.status = MagicMock()
        self.mock_sea_result_set.description = [
            ("catalog_name", "string", None, None, None, None, True),
            ("schema_name", "string", None, None, None, None, True),
            ("table_name", "string", None, None, None, None, True),
            ("owner", "string", None, None, None, None, True),
            ("creation_time", "string", None, None, None, None, True),
            ("table_type", "string", None, None, None, None, True),
            ("remarks", "string", None, None, None, None, True),
        ]
        self.mock_sea_result_set.has_been_closed_server_side = False
        self.mock_sea_result_set._arrow_schema_bytes = None

    def test_filter_by_column_values(self):
        """Test filtering by column values with various options."""
        # Case 1: Case-sensitive filtering
        allowed_values = ["table1", "table3"]

        with patch(
            "databricks.sql.backend.sea.utils.filters.isinstance", return_value=True
        ):
            with patch(
                "databricks.sql.backend.sea.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                # Call filter_by_column_values on the table_name column (index 2)
                result = ResultSetFilter.filter_by_column_values(
                    self.mock_sea_result_set, 2, allowed_values, case_sensitive=True
                )

                # Verify the filter was applied correctly
                mock_sea_result_set_class.assert_called_once()

                # Check the filtered data passed to the constructor
                args, kwargs = mock_sea_result_set_class.call_args
                result_data = kwargs.get("result_data")
                self.assertIsNotNone(result_data)
                self.assertEqual(len(result_data.data), 2)
                self.assertIn(result_data.data[0][2], allowed_values)
                self.assertIn(result_data.data[1][2], allowed_values)

        # Case 2: Case-insensitive filtering
        mock_sea_result_set_class.reset_mock()
        with patch(
            "databricks.sql.backend.sea.utils.filters.isinstance", return_value=True
        ):
            with patch(
                "databricks.sql.backend.sea.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                # Call filter_by_column_values with case-insensitive matching
                result = ResultSetFilter.filter_by_column_values(
                    self.mock_sea_result_set,
                    2,
                    ["TABLE1", "TABLE3"],
                    case_sensitive=False,
                )
                mock_sea_result_set_class.assert_called_once()

    def test_filter_tables_by_type(self):
        """Test filtering tables by type with various options."""
        # Case 1: Specific table types
        table_types = ["TABLE", "VIEW"]

        with patch(
            "databricks.sql.backend.sea.utils.filters.isinstance", return_value=True
        ):
            with patch.object(
                ResultSetFilter, "filter_by_column_values"
            ) as mock_filter:
                ResultSetFilter.filter_tables_by_type(
                    self.mock_sea_result_set, table_types
                )
                args, kwargs = mock_filter.call_args
                self.assertEqual(args[0], self.mock_sea_result_set)
                self.assertEqual(args[1], 5)  # Table type column index
                self.assertEqual(args[2], table_types)
                self.assertEqual(kwargs.get("case_sensitive"), True)

        # Case 2: Default table types (None or empty list)
        with patch(
            "databricks.sql.backend.sea.utils.filters.isinstance", return_value=True
        ):
            with patch.object(
                ResultSetFilter, "filter_by_column_values"
            ) as mock_filter:
                # Test with None
                ResultSetFilter.filter_tables_by_type(self.mock_sea_result_set, None)
                args, kwargs = mock_filter.call_args
                self.assertEqual(args[2], ["TABLE", "VIEW", "SYSTEM TABLE"])

                # Test with empty list
                ResultSetFilter.filter_tables_by_type(self.mock_sea_result_set, [])
                args, kwargs = mock_filter.call_args
                self.assertEqual(args[2], ["TABLE", "VIEW", "SYSTEM TABLE"])


if __name__ == "__main__":
    unittest.main()
