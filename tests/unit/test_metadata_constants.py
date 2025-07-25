"""
Unit tests for metadata column normalization constants and functions.
"""

import unittest
from databricks.sql.backend.sea.metadata_constants import (
    CATALOG_COLUMNS,
    SCHEMA_COLUMNS,
    TABLE_COLUMNS,
    COLUMN_COLUMNS,
    get_column_names,
    get_column_mapping,
    normalize_metadata_description,
    normalize_columns_metadata_description,
)


class TestMetadataConstants(unittest.TestCase):
    """Test metadata column constants and helper functions."""

    def test_catalog_columns_structure(self):
        """Test CATALOG_COLUMNS has correct structure."""
        self.assertEqual(len(CATALOG_COLUMNS), 1)
        self.assertEqual(CATALOG_COLUMNS[0], ("TABLE_CAT", "catalog"))

    def test_schema_columns_structure(self):
        """Test SCHEMA_COLUMNS has correct structure."""
        self.assertEqual(len(SCHEMA_COLUMNS), 2)
        self.assertEqual(SCHEMA_COLUMNS[0], ("TABLE_SCHEM", "databaseName"))
        self.assertEqual(SCHEMA_COLUMNS[1], ("TABLE_CATALOG", "catalogName"))

    def test_table_columns_structure(self):
        """Test TABLE_COLUMNS has correct structure and count."""
        self.assertEqual(len(TABLE_COLUMNS), 10)
        # Check key columns
        self.assertEqual(TABLE_COLUMNS[0], ("TABLE_CAT", "catalogName"))
        self.assertEqual(TABLE_COLUMNS[1], ("TABLE_SCHEM", "namespace"))
        self.assertEqual(TABLE_COLUMNS[2], ("TABLE_NAME", "tableName"))
        self.assertEqual(TABLE_COLUMNS[3], ("TABLE_TYPE", "tableType"))
        self.assertEqual(TABLE_COLUMNS[4], ("REMARKS", "remarks"))

    def test_column_columns_structure(self):
        """Test COLUMN_COLUMNS has correct structure and count."""
        self.assertEqual(len(COLUMN_COLUMNS), 24)
        # Check key columns
        self.assertEqual(COLUMN_COLUMNS[0], ("TABLE_CAT", "catalogName"))
        self.assertEqual(COLUMN_COLUMNS[1], ("TABLE_SCHEM", "namespace"))
        self.assertEqual(COLUMN_COLUMNS[2], ("TABLE_NAME", "tableName"))
        self.assertEqual(COLUMN_COLUMNS[3], ("COLUMN_NAME", "col_name"))
        self.assertEqual(COLUMN_COLUMNS[4], ("DATA_TYPE", "dataType"))
        self.assertEqual(COLUMN_COLUMNS[5], ("TYPE_NAME", "columnType"))
        # Check that COLUMN_DEF also maps to columnType (same source)
        self.assertEqual(COLUMN_COLUMNS[12], ("COLUMN_DEF", "columnType"))

    def test_get_column_names(self):
        """Test get_column_names helper function."""
        test_columns = [("JDBC_NAME1", "sea_name1"), ("JDBC_NAME2", "sea_name2")]
        result = get_column_names(test_columns)
        self.assertEqual(result, ["JDBC_NAME1", "JDBC_NAME2"])

    def test_get_column_mapping(self):
        """Test get_column_mapping helper function."""
        test_columns = [
            ("JDBC_NAME1", "sea_name1"),
            ("JDBC_NAME2", "sea_name2"),
            ("JDBC_NAME3", None),  # Should be excluded
        ]
        result = get_column_mapping(test_columns)
        expected = {"sea_name1": "JDBC_NAME1", "sea_name2": "JDBC_NAME2"}
        self.assertEqual(result, expected)

    def test_normalize_metadata_description_basic(self):
        """Test basic metadata description normalization."""
        # Mock original description
        original_desc = [
            ("catalog", "string", None, None, None, None, True),
        ]

        result = normalize_metadata_description(original_desc, CATALOG_COLUMNS)

        expected = [
            ("TABLE_CAT", "string", None, None, None, None, True),
        ]
        self.assertEqual(result, expected)

    def test_normalize_metadata_description_with_missing_columns(self):
        """Test normalization when some columns are missing from source."""
        # Original description has only one column
        original_desc = [
            ("databaseName", "string", None, None, None, None, True),
        ]

        result = normalize_metadata_description(original_desc, SCHEMA_COLUMNS)

        expected = [
            ("TABLE_SCHEM", "string", None, None, None, None, True),
            (
                "TABLE_CATALOG",
                "string",
                None,
                None,
                None,
                None,
                None,
            ),  # Missing column gets defaults
        ]
        self.assertEqual(result, expected)

    def test_normalize_metadata_description_empty_input(self):
        """Test normalization with empty input."""
        result = normalize_metadata_description([], CATALOG_COLUMNS)
        self.assertEqual(result, [])

    def test_normalize_columns_metadata_description(self):
        """Test columns-specific normalization function."""
        # Mock original description with key columns
        original_desc = [
            ("catalogName", "string", None, None, None, None, True),
            ("namespace", "string", None, None, None, None, True),
            ("tableName", "string", None, None, None, None, True),
            ("col_name", "string", None, None, None, None, True),
            ("dataType", "int", None, None, None, None, True),
            ("columnType", "string", None, None, None, None, True),
        ]

        result = normalize_columns_metadata_description(original_desc)

        # Should have 24 columns total
        self.assertEqual(len(result), 24)

        # Check that key columns are mapped correctly
        self.assertEqual(result[0][0], "TABLE_CAT")  # catalogName -> TABLE_CAT
        self.assertEqual(result[1][0], "TABLE_SCHEM")  # namespace -> TABLE_SCHEM
        self.assertEqual(result[5][0], "TYPE_NAME")  # columnType -> TYPE_NAME
        self.assertEqual(
            result[12][0], "COLUMN_DEF"
        )  # columnType -> COLUMN_DEF (same source)

        # Both TYPE_NAME and COLUMN_DEF should have same metadata (except name)
        self.assertEqual(result[5][1:], result[12][1:])

    def test_normalize_metadata_description_preserves_metadata(self):
        """Test that normalization preserves non-name metadata."""
        original_desc = [
            ("catalog", "varchar", 100, 50, 10, 2, False),
        ]

        result = normalize_metadata_description(original_desc, CATALOG_COLUMNS)

        expected = [
            ("TABLE_CAT", "varchar", 100, 50, 10, 2, False),
        ]
        self.assertEqual(result, expected)

    def test_columns_with_duplicate_source_mapping(self):
        """Test that TYPE_NAME and COLUMN_DEF both map to columnType correctly."""
        original_desc = [
            ("columnType", "string", None, None, None, None, True),
        ]

        # Create a subset of column definitions that includes both TYPE_NAME and COLUMN_DEF
        test_columns = [
            ("TYPE_NAME", "columnType"),
            ("COLUMN_DEF", "columnType"),
        ]

        result = normalize_metadata_description(original_desc, test_columns)

        expected = [
            ("TYPE_NAME", "string", None, None, None, None, True),
            ("COLUMN_DEF", "string", None, None, None, None, True),
        ]
        self.assertEqual(result, expected)

        # Both should have identical metadata except for the name
        self.assertEqual(result[0][1:], result[1][1:])


if __name__ == "__main__":
    unittest.main()
