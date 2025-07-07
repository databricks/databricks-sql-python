"""
Column name mappings between different backend protocols.

This module provides mappings between column names returned by different backends
 to ensure a consistent interface for metadata operations.
"""

from enum import Enum


class MetadataOp(Enum):
    """Enum for metadata operations."""

    CATALOGS = "catalogs"
    SCHEMAS = "schemas"
    TABLES = "tables"
    COLUMNS = "columns"


# Mappings from column names to standard column names
CATALOG_OP = {
    "catalog": "TABLE_CAT",
}

SCHEMA_OP = {
    "databaseName": "TABLE_SCHEM",
    "catalogName": "TABLE_CATALOG",
}

TABLE_OP = {
    "catalogName": "TABLE_CAT",
    "namespace": "TABLE_SCHEM",
    "tableName": "TABLE_NAME",
    "tableType": "TABLE_TYPE",
    "remarks": "REMARKS",
    "TYPE_CATALOG_COLUMN": "TYPE_CAT",
    "TYPE_SCHEMA_COLUMN": "TYPE_SCHEM",
    "TYPE_NAME": "TYPE_NAME",
    "SELF_REFERENCING_COLUMN_NAME": "SELF_REFERENCING_COL_NAME",
    "REF_GENERATION_COLUMN": "REF_GENERATION",
}

COLUMN_OP = {
    "catalogName": "TABLE_CAT",
    "namespace": "TABLE_SCHEM",
    "tableName": "TABLE_NAME",
    "columnName": "COLUMN_NAME",
    "dataType": "DATA_TYPE",
    "columnType": "TYPE_NAME",
    "columnSize": "COLUMN_SIZE",
    "bufferLength": "BUFFER_LENGTH",
    "decimalDigits": "DECIMAL_DIGITS",
    "radix": "NUM_PREC_RADIX",
    "nullable": "NULLABLE",
    "remarks": "REMARKS",
    "columnDef": "COLUMN_DEF",
    "sqlDataType": "SQL_DATA_TYPE",
    "sqlDatetimeSub": "SQL_DATETIME_SUB",
    "charOctetLength": "CHAR_OCTET_LENGTH",
    "ordinalPosition": "ORDINAL_POSITION",
    "isNullable": "IS_NULLABLE",
    "scopeCatalog": "SCOPE_CATALOG",
    "scopeSchema": "SCOPE_SCHEMA",
    "scopeTable": "SCOPE_TABLE",
    "sourceDataType": "SOURCE_DATA_TYPE",
    "isAutoIncrement": "IS_AUTOINCREMENT",
    "isGenerated": "IS_GENERATEDCOLUMN",
}


def normalise_metadata_result(result_set, operation: MetadataOp):
    """
    Normalise column names in a result set based on the operation type.
    This function modifies the result set in place.

    Args:
        result_set: The result set object to normalise
        operation: The metadata operation (from MetadataOp enum)
    """

    # Select the appropriate mapping based on the operation
    mapping = None
    if operation == MetadataOp.CATALOGS:
        mapping = CATALOG_OP
    elif operation == MetadataOp.SCHEMAS:
        mapping = SCHEMA_OP
    elif operation == MetadataOp.TABLES:
        mapping = TABLE_OP
    elif operation == MetadataOp.COLUMNS:
        mapping = COLUMN_OP

    if mapping is None:
        return

    # Normalize column names in the description
    new_description = []
    for col_desc in result_set.description:
        col_name = col_desc[0]
        if col_name in mapping:
            # Create a new column description tuple with the normalized name
            new_col_desc = (mapping[col_name],) + col_desc[1:]
            new_description.append(new_col_desc)
        else:
            new_description.append(col_desc)
    result_set.description = new_description
