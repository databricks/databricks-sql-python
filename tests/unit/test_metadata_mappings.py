"""
Tests for SEA metadata column mappings and normalization.
"""

import pytest
from databricks.sql.backend.sea.utils.metadata_mappings import MetadataColumnMappings
from databricks.sql.backend.sea.utils.result_column import ResultColumn
from databricks.sql.backend.sea.utils.conversion import SqlType


class TestMetadataColumnMappings:
    """Test suite for metadata column mappings."""

    def test_result_column_creation(self):
        """Test ResultColumn data class creation and attributes."""
        col = ResultColumn("TABLE_CAT", "catalog", SqlType.STRING)
        assert col.thrift_col_name == "TABLE_CAT"
        assert col.sea_col_name == "catalog"
        assert col.thrift_col_type == SqlType.STRING

    def test_result_column_with_none_sea_name(self):
        """Test ResultColumn when SEA doesn't return this column."""
        col = ResultColumn("TYPE_CAT", None, SqlType.STRING)
        assert col.thrift_col_name == "TYPE_CAT"
        assert col.sea_col_name is None
        assert col.thrift_col_type == SqlType.STRING

    def test_catalog_columns_mapping(self):
        """Test catalog columns mapping for getCatalogs."""
        catalog_cols = MetadataColumnMappings.CATALOG_COLUMNS
        assert len(catalog_cols) == 1

        catalog_col = catalog_cols[0]
        assert catalog_col.thrift_col_name == "TABLE_CAT"
        assert catalog_col.sea_col_name == "catalog"
        assert catalog_col.thrift_col_type == SqlType.STRING

    def test_schema_columns_mapping(self):
        """Test schema columns mapping for getSchemas."""
        schema_cols = MetadataColumnMappings.SCHEMA_COLUMNS
        assert len(schema_cols) == 2

        # Check TABLE_SCHEM column
        schema_col = schema_cols[0]
        assert schema_col.thrift_col_name == "TABLE_SCHEM"
        assert schema_col.sea_col_name == "databaseName"
        assert schema_col.thrift_col_type == SqlType.STRING

        # Check TABLE_CATALOG column
        catalog_col = schema_cols[1]
        assert catalog_col.thrift_col_name == "TABLE_CATALOG"
        assert catalog_col.sea_col_name is None
        assert catalog_col.thrift_col_type == SqlType.STRING

    def test_table_columns_mapping(self):
        """Test table columns mapping for getTables."""
        table_cols = MetadataColumnMappings.TABLE_COLUMNS
        assert len(table_cols) == 10

        # Test key columns
        expected_mappings = [
            ("TABLE_CAT", "catalogName", SqlType.STRING),
            ("TABLE_SCHEM", "namespace", SqlType.STRING),
            ("TABLE_NAME", "tableName", SqlType.STRING),
            ("TABLE_TYPE", "tableType", SqlType.STRING),
            ("REMARKS", "remarks", SqlType.STRING),
            ("TYPE_CAT", None, SqlType.STRING),
            ("TYPE_SCHEM", None, SqlType.STRING),
            ("TYPE_NAME", None, SqlType.STRING),
            ("SELF_REFERENCING_COL_NAME", None, SqlType.STRING),
            ("REF_GENERATION", None, SqlType.STRING),
        ]

        for i, (thrift_name, sea_name, sql_type) in enumerate(expected_mappings):
            col = table_cols[i]
            assert col.thrift_col_name == thrift_name
            assert col.sea_col_name == sea_name
            assert col.thrift_col_type == sql_type

    def test_column_columns_mapping(self):
        """Test column columns mapping for getColumns."""
        column_cols = MetadataColumnMappings.COLUMN_COLUMNS
        # Should have 23 columns (not including IS_GENERATED_COLUMN)
        assert len(column_cols) == 23

        # Test some key columns
        key_columns = {
            "TABLE_CAT": ("catalogName", SqlType.STRING),
            "TABLE_SCHEM": ("namespace", SqlType.STRING),
            "TABLE_NAME": ("tableName", SqlType.STRING),
            "COLUMN_NAME": ("col_name", SqlType.STRING),
            "DATA_TYPE": ("columnType", SqlType.INT),
            "TYPE_NAME": ("columnType", SqlType.STRING),
            "COLUMN_SIZE": ("columnSize", SqlType.INT),
            "DECIMAL_DIGITS": ("decimalDigits", SqlType.INT),
            "NUM_PREC_RADIX": ("radix", SqlType.INT),
            "ORDINAL_POSITION": ("ordinalPosition", SqlType.INT),
            "IS_NULLABLE": ("isNullable", SqlType.STRING),
            "IS_AUTOINCREMENT": ("isAutoIncrement", SqlType.STRING),
        }

        for col in column_cols:
            if col.thrift_col_name in key_columns:
                expected_sea_name, expected_type = key_columns[col.thrift_col_name]
                assert col.sea_col_name == expected_sea_name
                assert col.thrift_col_type == expected_type

    def test_is_generated_column_not_included(self):
        """Test that IS_GENERATED_COLUMN is not included in COLUMN_COLUMNS."""
        column_names = [
            col.thrift_col_name for col in MetadataColumnMappings.COLUMN_COLUMNS
        ]
        assert "IS_GENERATEDCOLUMN" not in column_names

    def test_column_type_consistency(self):
        """Test that column types are consistent with JDBC spec."""
        # Test numeric types
        assert MetadataColumnMappings.DATA_TYPE_COLUMN.thrift_col_type == SqlType.INT
        assert MetadataColumnMappings.COLUMN_SIZE_COLUMN.thrift_col_type == SqlType.INT
        assert (
            MetadataColumnMappings.BUFFER_LENGTH_COLUMN.thrift_col_type
            == SqlType.TINYINT
        )
        assert (
            MetadataColumnMappings.DECIMAL_DIGITS_COLUMN.thrift_col_type == SqlType.INT
        )
        assert (
            MetadataColumnMappings.NUM_PREC_RADIX_COLUMN.thrift_col_type == SqlType.INT
        )
        assert (
            MetadataColumnMappings.ORDINAL_POSITION_COLUMN.thrift_col_type
            == SqlType.INT
        )
        assert MetadataColumnMappings.NULLABLE_COLUMN.thrift_col_type == SqlType.INT
        assert (
            MetadataColumnMappings.SQL_DATA_TYPE_COLUMN.thrift_col_type == SqlType.INT
        )
        assert (
            MetadataColumnMappings.SQL_DATETIME_SUB_COLUMN.thrift_col_type
            == SqlType.INT
        )
        assert (
            MetadataColumnMappings.CHAR_OCTET_LENGTH_COLUMN.thrift_col_type
            == SqlType.INT
        )
        assert (
            MetadataColumnMappings.SOURCE_DATA_TYPE_COLUMN.thrift_col_type
            == SqlType.SMALLINT
        )

        # Test string types
        assert MetadataColumnMappings.CATALOG_COLUMN.thrift_col_type == SqlType.STRING
        assert MetadataColumnMappings.SCHEMA_COLUMN.thrift_col_type == SqlType.STRING
        assert (
            MetadataColumnMappings.TABLE_NAME_COLUMN.thrift_col_type == SqlType.STRING
        )
        assert (
            MetadataColumnMappings.IS_NULLABLE_COLUMN.thrift_col_type == SqlType.STRING
        )
        assert (
            MetadataColumnMappings.IS_AUTO_INCREMENT_COLUMN.thrift_col_type
            == SqlType.STRING
        )
