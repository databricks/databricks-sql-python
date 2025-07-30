"""
Metadata column mappings for normalizing SEA results to match Thrift backend.

This module defines the column mappings needed to transform SEA metadata
results to use the same column names as the Thrift backend, ensuring
consistency between the two backends.
"""

from typing import Dict, List, NamedTuple, Optional


class ColumnMapping(NamedTuple):
    """Maps SEA column name to Thrift column name."""

    sea_name: Optional[str]
    thrift_name: str


# Column mappings for each metadata operation
CATALOG_COLUMN_MAPPINGS: List[ColumnMapping] = [
    ColumnMapping("catalog", "TABLE_CAT"),
]

SCHEMA_COLUMN_MAPPINGS: List[ColumnMapping] = [
    ColumnMapping("databaseName", "TABLE_SCHEM"),
    ColumnMapping(None, "TABLE_CATALOG"),  # SEA doesn't return this, but Thrift does
]

TABLE_COLUMN_MAPPINGS: List[ColumnMapping] = [
    ColumnMapping("catalogName", "TABLE_CAT"),
    ColumnMapping("namespace", "TABLE_SCHEM"),
    ColumnMapping("tableName", "TABLE_NAME"),
    ColumnMapping("tableType", "TABLE_TYPE"),
    ColumnMapping("remarks", "REMARKS"),
    # Add NULL columns for Thrift compatibility
    ColumnMapping(None, "TYPE_CAT"),  # Always NULL
    ColumnMapping(None, "TYPE_SCHEM"),  # Always NULL
    ColumnMapping(None, "TYPE_NAME"),  # Always NULL
    ColumnMapping(None, "SELF_REFERENCING_COL_NAME"),  # Always NULL
    ColumnMapping(None, "REF_GENERATION"),  # Always NULL
]

COLUMN_COLUMN_MAPPINGS: List[ColumnMapping] = [
    ColumnMapping("catalogName", "TABLE_CAT"),
    ColumnMapping("namespace", "TABLE_SCHEM"),
    ColumnMapping("tableName", "TABLE_NAME"),
    ColumnMapping("col_name", "COLUMN_NAME"),
    ColumnMapping(None, "DATA_TYPE"),  # Requires conversion from columnType
    ColumnMapping("columnType", "TYPE_NAME"),
    ColumnMapping("columnSize", "COLUMN_SIZE"),
    ColumnMapping(None, "BUFFER_LENGTH"),  # Always NULL
    ColumnMapping("decimalDigits", "DECIMAL_DIGITS"),
    ColumnMapping("radix", "NUM_PREC_RADIX"),
    ColumnMapping(None, "NULLABLE"),  # Derived from isNullable
    ColumnMapping("remarks", "REMARKS"),
    ColumnMapping(None, "COLUMN_DEF"),  # Always NULL
    ColumnMapping(None, "SQL_DATA_TYPE"),  # Always NULL
    ColumnMapping(None, "SQL_DATETIME_SUB"),  # Always NULL
    ColumnMapping(None, "CHAR_OCTET_LENGTH"),  # Always NULL
    ColumnMapping("ordinalPosition", "ORDINAL_POSITION"),
    ColumnMapping("isNullable", "IS_NULLABLE"),
    ColumnMapping(None, "SCOPE_CATALOG"),  # Always NULL
    ColumnMapping(None, "SCOPE_SCHEMA"),  # Always NULL
    ColumnMapping(None, "SCOPE_TABLE"),  # Always NULL
    ColumnMapping(None, "SOURCE_DATA_TYPE"),  # Always NULL
    ColumnMapping("isAutoIncrement", "IS_AUTOINCREMENT"),
    ColumnMapping("isGenerated", "IS_GENERATEDCOLUMN"),
]

# Operation to mapping lookup
OPERATION_MAPPINGS: Dict[str, List[ColumnMapping]] = {
    "catalogs": CATALOG_COLUMN_MAPPINGS,
    "schemas": SCHEMA_COLUMN_MAPPINGS,
    "tables": TABLE_COLUMN_MAPPINGS,
    "columns": COLUMN_COLUMN_MAPPINGS,
}
