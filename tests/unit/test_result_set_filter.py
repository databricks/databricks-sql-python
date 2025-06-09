"""
Tests for the ResultSetFilter class.

This module contains tests for the ResultSetFilter class, which provides
filtering capabilities for result sets returned by different backends.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock

from databricks.sql.backend.filters import ResultSetFilter
from databricks.sql.result_set import SeaResultSet
from databricks.sql.backend.types import CommandId, CommandState, BackendType


class TestResultSetFilter:
    """Test suite for the ResultSetFilter class."""

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
    def sea_response_with_tables(self):
        """Create a sample SEA response with table data based on the server schema."""
        return {
            "statement_id": "test-statement-123",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "format": "JSON_ARRAY",
                "schema": {
                    "column_count": 7,
                    "columns": [
                        {"name": "namespace", "type_text": "STRING", "position": 0},
                        {"name": "tableName", "type_text": "STRING", "position": 1},
                        {"name": "isTemporary", "type_text": "BOOLEAN", "position": 2},
                        {"name": "information", "type_text": "STRING", "position": 3},
                        {"name": "catalogName", "type_text": "STRING", "position": 4},
                        {"name": "tableType", "type_text": "STRING", "position": 5},
                        {"name": "remarks", "type_text": "STRING", "position": 6},
                    ],
                },
                "total_row_count": 4,
            },
            "result": {
                "data_array": [
                    ["schema1", "table1", False, None, "catalog1", "TABLE", None],
                    ["schema1", "table2", False, None, "catalog1", "VIEW", None],
                    ["schema1", "table3", False, None, "catalog1", "SYSTEM TABLE", None],
                    ["schema1", "table4", False, None, "catalog1", "EXTERNAL", None],
                ]
            },
        }

    @pytest.fixture
    def sea_result_set_with_tables(self, mock_connection, mock_sea_client, sea_response_with_tables):
        """Create a SeaResultSet with table data."""
        # Create a deep copy of the response to avoid test interference
        import copy
        sea_response_copy = copy.deepcopy(sea_response_with_tables)
        
        return SeaResultSet(
            connection=mock_connection,
            sea_client=mock_sea_client,
            sea_response=sea_response_copy,
            buffer_size_bytes=1000,
            arraysize=100,
        )

    def test_filter_tables_by_type_default(self, sea_result_set_with_tables):
        """Test filtering tables by type with default types."""
        # Default types are TABLE, VIEW, SYSTEM TABLE
        filtered_result = ResultSetFilter.filter_tables_by_type(sea_result_set_with_tables)

        # Verify that only the default types are included
        assert len(filtered_result._response["result"]["data_array"]) == 3
        table_types = [row[5] for row in filtered_result._response["result"]["data_array"]]
        assert "TABLE" in table_types
        assert "VIEW" in table_types
        assert "SYSTEM TABLE" in table_types
        assert "EXTERNAL" not in table_types

    def test_filter_tables_by_type_custom(self, sea_result_set_with_tables):
        """Test filtering tables by type with custom types."""
        # Filter for only TABLE and EXTERNAL
        filtered_result = ResultSetFilter.filter_tables_by_type(
            sea_result_set_with_tables, table_types=["TABLE", "EXTERNAL"]
        )

        # Verify that only the specified types are included
        assert len(filtered_result._response["result"]["data_array"]) == 2
        table_types = [row[5] for row in filtered_result._response["result"]["data_array"]]
        assert "TABLE" in table_types
        assert "EXTERNAL" in table_types
        assert "VIEW" not in table_types
        assert "SYSTEM TABLE" not in table_types

    def test_filter_tables_by_type_case_insensitive(self, sea_result_set_with_tables):
        """Test that table type filtering is case-insensitive."""
        # Filter for lowercase "table" and "view"
        filtered_result = ResultSetFilter.filter_tables_by_type(
            sea_result_set_with_tables, table_types=["table", "view"]
        )

        # Verify that the matching types are included despite case differences
        assert len(filtered_result._response["result"]["data_array"]) == 2
        table_types = [row[5] for row in filtered_result._response["result"]["data_array"]]
        assert "TABLE" in table_types
        assert "VIEW" in table_types
        assert "SYSTEM TABLE" not in table_types
        assert "EXTERNAL" not in table_types

    def test_filter_tables_by_type_empty_list(self, sea_result_set_with_tables):
        """Test filtering tables with an empty type list (should use defaults)."""
        filtered_result = ResultSetFilter.filter_tables_by_type(
            sea_result_set_with_tables, table_types=[]
        )

        # Verify that default types are used
        assert len(filtered_result._response["result"]["data_array"]) == 3
        table_types = [row[5] for row in filtered_result._response["result"]["data_array"]]
        assert "TABLE" in table_types
        assert "VIEW" in table_types
        assert "SYSTEM TABLE" in table_types
        assert "EXTERNAL" not in table_types

    def test_filter_by_column_values(self, sea_result_set_with_tables):
        """Test filtering by values in a specific column."""
        # Filter by namespace in column index 0
        filtered_result = ResultSetFilter.filter_by_column_values(
            sea_result_set_with_tables, column_index=0, allowed_values=["schema1"]
        )

        # All rows have schema1 in namespace, so all should be included
        assert len(filtered_result._response["result"]["data_array"]) == 4

        # Filter by table name in column index 1
        filtered_result = ResultSetFilter.filter_by_column_values(
            sea_result_set_with_tables, column_index=1, allowed_values=["table1", "table3"]
        )

        # Only rows with table1 or table3 should be included
        assert len(filtered_result._response["result"]["data_array"]) == 2
        table_names = [row[1] for row in filtered_result._response["result"]["data_array"]]
        assert "table1" in table_names
        assert "table3" in table_names
        assert "table2" not in table_names
        assert "table4" not in table_names

    def test_filter_by_column_values_case_sensitive(self, mock_connection, mock_sea_client, sea_response_with_tables):
        """Test case-sensitive filtering by column values."""
        import copy
        
        # Create a fresh result set for the first test
        sea_response_copy1 = copy.deepcopy(sea_response_with_tables)
        result_set1 = SeaResultSet(
            connection=mock_connection,
            sea_client=mock_sea_client,
            sea_response=sea_response_copy1,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        
        # First test: Case-sensitive filtering with lowercase values (should find no matches)
        filtered_result = ResultSetFilter.filter_by_column_values(
            result_set1,
            column_index=5,  # tableType column
            allowed_values=["table", "view"],  # lowercase
            case_sensitive=True,
        )
        
        # Verify no matches with lowercase values
        assert len(filtered_result._response["result"]["data_array"]) == 0
        
        # Create a fresh result set for the second test
        sea_response_copy2 = copy.deepcopy(sea_response_with_tables)
        result_set2 = SeaResultSet(
            connection=mock_connection,
            sea_client=mock_sea_client,
            sea_response=sea_response_copy2,
            buffer_size_bytes=1000,
            arraysize=100,
        )
        
        # Second test: Case-sensitive filtering with correct case (should find matches)
        filtered_result = ResultSetFilter.filter_by_column_values(
            result_set2,
            column_index=5,  # tableType column
            allowed_values=["TABLE", "VIEW"],  # correct case
            case_sensitive=True,
        )
        
        # Verify matches with correct case
        assert len(filtered_result._response["result"]["data_array"]) == 2
        
        # Extract the table types from the filtered results
        table_types = [row[5] for row in filtered_result._response["result"]["data_array"]]
        assert "TABLE" in table_types
        assert "VIEW" in table_types

    def test_filter_by_column_values_out_of_bounds(self, sea_result_set_with_tables):
        """Test filtering with a column index that's out of bounds."""
        # Filter by column index 10 (out of bounds)
        filtered_result = ResultSetFilter.filter_by_column_values(
            sea_result_set_with_tables, column_index=10, allowed_values=["value"]
        )

        # No rows should match
        assert len(filtered_result._response["result"]["data_array"]) == 0