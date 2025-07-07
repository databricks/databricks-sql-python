"""
Tests for the column mapping module.
"""

import pytest
from unittest.mock import MagicMock
from enum import Enum

from databricks.sql.backend.column_mapping import (
    normalise_metadata_result,
    MetadataOp,
    CATALOG_OP,
    SCHEMA_OP,
    TABLE_OP,
    COLUMN_OP,
)


class TestColumnMapping:
    """Tests for the column mapping module."""

    def test_normalize_metadata_result_catalogs(self):
        """Test normalizing catalog column names."""
        # Create a mock result set with a description
        mock_result = MagicMock()
        mock_result.description = [
            ("catalog", "string", None, None, None, None, True),
            ("other_column", "string", None, None, None, None, True),
        ]

        # Normalize the result set
        normalise_metadata_result(mock_result, MetadataOp.CATALOGS)

        # Check that the column names were normalized
        assert mock_result.description[0][0] == "TABLE_CAT"
        assert mock_result.description[1][0] == "other_column"

    def test_normalize_metadata_result_schemas(self):
        """Test normalizing schema column names."""
        # Create a mock result set with a description
        mock_result = MagicMock()
        mock_result.description = [
            ("databaseName", "string", None, None, None, None, True),
            ("catalogName", "string", None, None, None, None, True),
            ("other_column", "string", None, None, None, None, True),
        ]

        # Normalize the result set
        normalise_metadata_result(mock_result, MetadataOp.SCHEMAS)

        # Check that the column names were normalized
        assert mock_result.description[0][0] == "TABLE_SCHEM"
        assert mock_result.description[1][0] == "TABLE_CATALOG"
        assert mock_result.description[2][0] == "other_column"

    def test_normalize_metadata_result_tables(self):
        """Test normalizing table column names."""
        # Create a mock result set with a description
        mock_result = MagicMock()
        mock_result.description = [
            ("catalogName", "string", None, None, None, None, True),
            ("namespace", "string", None, None, None, None, True),
            ("tableName", "string", None, None, None, None, True),
            ("tableType", "string", None, None, None, None, True),
            ("remarks", "string", None, None, None, None, True),
            ("TYPE_CATALOG_COLUMN", "string", None, None, None, None, True),
            ("TYPE_SCHEMA_COLUMN", "string", None, None, None, None, True),
            ("TYPE_NAME", "string", None, None, None, None, True),
            ("SELF_REFERENCING_COLUMN_NAME", "string", None, None, None, None, True),
            ("REF_GENERATION_COLUMN", "string", None, None, None, None, True),
            ("other_column", "string", None, None, None, None, True),
        ]

        # Normalize the result set
        normalise_metadata_result(mock_result, MetadataOp.TABLES)

        # Check that the column names were normalized
        assert mock_result.description[0][0] == "TABLE_CAT"
        assert mock_result.description[1][0] == "TABLE_SCHEM"
        assert mock_result.description[2][0] == "TABLE_NAME"
        assert mock_result.description[3][0] == "TABLE_TYPE"
        assert mock_result.description[4][0] == "REMARKS"
        assert mock_result.description[5][0] == "TYPE_CAT"
        assert mock_result.description[6][0] == "TYPE_SCHEM"
        assert mock_result.description[7][0] == "TYPE_NAME"
        assert mock_result.description[8][0] == "SELF_REFERENCING_COL_NAME"
        assert mock_result.description[9][0] == "REF_GENERATION"
        assert mock_result.description[10][0] == "other_column"

    def test_normalize_metadata_result_columns(self):
        """Test normalizing column column names."""
        # Create a mock result set with a description
        mock_result = MagicMock()
        mock_result.description = [
            ("catalogName", "string", None, None, None, None, True),
            ("namespace", "string", None, None, None, None, True),
            ("tableName", "string", None, None, None, None, True),
            ("columnName", "string", None, None, None, None, True),
            ("dataType", "string", None, None, None, None, True),
            ("columnType", "string", None, None, None, None, True),
            ("columnSize", "string", None, None, None, None, True),
            ("bufferLength", "string", None, None, None, None, True),
            ("decimalDigits", "string", None, None, None, None, True),
            ("radix", "string", None, None, None, None, True),
            ("nullable", "string", None, None, None, None, True),
            ("remarks", "string", None, None, None, None, True),
            ("columnDef", "string", None, None, None, None, True),
            ("sqlDataType", "string", None, None, None, None, True),
            ("sqlDatetimeSub", "string", None, None, None, None, True),
            ("charOctetLength", "string", None, None, None, None, True),
            ("ordinalPosition", "string", None, None, None, None, True),
            ("isNullable", "string", None, None, None, None, True),
            ("scopeCatalog", "string", None, None, None, None, True),
            ("scopeSchema", "string", None, None, None, None, True),
            ("scopeTable", "string", None, None, None, None, True),
            ("sourceDataType", "string", None, None, None, None, True),
            ("isAutoIncrement", "string", None, None, None, None, True),
            ("isGenerated", "string", None, None, None, None, True),
            ("other_column", "string", None, None, None, None, True),
        ]

        # Normalize the result set
        normalise_metadata_result(mock_result, MetadataOp.COLUMNS)

        # Check that the column names were normalized
        assert mock_result.description[0][0] == "TABLE_CAT"
        assert mock_result.description[1][0] == "TABLE_SCHEM"
        assert mock_result.description[2][0] == "TABLE_NAME"
        assert mock_result.description[3][0] == "COLUMN_NAME"
        assert mock_result.description[4][0] == "DATA_TYPE"
        assert mock_result.description[5][0] == "TYPE_NAME"
        assert mock_result.description[6][0] == "COLUMN_SIZE"
        assert mock_result.description[7][0] == "BUFFER_LENGTH"
        assert mock_result.description[8][0] == "DECIMAL_DIGITS"
        assert mock_result.description[9][0] == "NUM_PREC_RADIX"
        assert mock_result.description[10][0] == "NULLABLE"
        assert mock_result.description[11][0] == "REMARKS"
        assert mock_result.description[12][0] == "COLUMN_DEF"
        assert mock_result.description[13][0] == "SQL_DATA_TYPE"
        assert mock_result.description[14][0] == "SQL_DATETIME_SUB"
        assert mock_result.description[15][0] == "CHAR_OCTET_LENGTH"
        assert mock_result.description[16][0] == "ORDINAL_POSITION"
        assert mock_result.description[17][0] == "IS_NULLABLE"
        assert mock_result.description[18][0] == "SCOPE_CATALOG"
        assert mock_result.description[19][0] == "SCOPE_SCHEMA"
        assert mock_result.description[20][0] == "SCOPE_TABLE"
        assert mock_result.description[21][0] == "SOURCE_DATA_TYPE"
        assert mock_result.description[22][0] == "IS_AUTOINCREMENT"
        assert mock_result.description[23][0] == "IS_GENERATEDCOLUMN"
        assert mock_result.description[24][0] == "other_column"

    def test_normalize_metadata_result_unknown_operation(self):
        """Test normalizing with an unknown operation type."""
        # Create a mock result set with a description
        mock_result = MagicMock()
        mock_result.description = [
            ("column1", "string", None, None, None, None, True),
            ("column2", "string", None, None, None, None, True),
        ]

        # Save the original description
        original_description = mock_result.description.copy()

        # Create a separate enum for testing
        class TestOp(Enum):
            UNKNOWN = "unknown"

        # Normalize the result set with an unknown operation
        normalise_metadata_result(mock_result, TestOp.UNKNOWN)

        # Check that the description was not modified
        assert mock_result.description == original_description

    def test_normalize_metadata_result_preserves_other_fields(self):
        """Test that normalization preserves other fields in the description."""
        # Create a mock result set with a description
        mock_result = MagicMock()
        mock_result.description = [
            (
                "catalog",
                "string",
                "display_size",
                "internal_size",
                "precision",
                "scale",
                True,
            ),
        ]

        # Normalize the result set
        normalise_metadata_result(mock_result, MetadataOp.CATALOGS)

        # Check that the column name was normalized but other fields preserved
        assert mock_result.description[0][0] == "TABLE_CAT"
        assert mock_result.description[0][1] == "string"
        assert mock_result.description[0][2] == "display_size"
        assert mock_result.description[0][3] == "internal_size"
        assert mock_result.description[0][4] == "precision"
        assert mock_result.description[0][5] == "scale"
        assert mock_result.description[0][6] == True
