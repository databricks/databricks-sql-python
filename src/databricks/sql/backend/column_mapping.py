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
# ref: CATALOG_COLUMNS in JDBC: https://github.com/databricks/databricks-jdbc/blob/e3d0d8dad683146a3afc3d501ddf0864ba086309/src/main/java/com/databricks/jdbc/common/MetadataResultConstants.java#L219
CATALOG_COLUMNS = {
    "catalog": "TABLE_CAT",
}

# ref: SCHEMA_COLUMNS in JDBC: https://github.com/databricks/databricks-jdbc/blob/e3d0d8dad683146a3afc3d501ddf0864ba086309/src/main/java/com/databricks/jdbc/common/MetadataResultConstants.java#L221
SCHEMA_COLUMNS = {
    "databaseName": "TABLE_SCHEM",
    "catalogName": "TABLE_CATALOG",
}

# ref: TABLE_COLUMNS in JDBC: https://github.com/databricks/databricks-jdbc/blob/e3d0d8dad683146a3afc3d501ddf0864ba086309/src/main/java/com/databricks/jdbc/common/MetadataResultConstants.java#L224
TABLE_COLUMNS = {
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

# ref: COLUMN_COLUMNS in JDBC: https://github.com/databricks/databricks-jdbc/blob/e3d0d8dad683146a3afc3d501ddf0864ba086309/src/main/java/com/databricks/jdbc/common/MetadataResultConstants.java#L192
# TYPE_NAME is not included because it is a duplicate target for columnType, and COLUMN_DEF is known to be returned by Thrift. 
# TODO: check if TYPE_NAME is to be returned / also used by Thrift. 
COLUMN_COLUMNS = {
    "catalogName": "TABLE_CAT",
    "namespace": "TABLE_SCHEM",
    "tableName": "TABLE_NAME",
    "col_name": "COLUMN_NAME",
    "dataType": "DATA_TYPE",
    "columnSize": "COLUMN_SIZE",
    "bufferLength": "BUFFER_LENGTH",
    "decimalDigits": "DECIMAL_DIGITS",
    "radix": "NUM_PREC_RADIX",
    "Nullable": "NULLABLE",
    "remarks": "REMARKS",
    "columnType": "COLUMN_DEF",
    "SQLDataType": "SQL_DATA_TYPE",
    "SQLDatetimeSub": "SQL_DATETIME_SUB",
    "CharOctetLength": "CHAR_OCTET_LENGTH",
    "ordinalPosition": "ORDINAL_POSITION",
    "isNullable": "IS_NULLABLE",
    "ScopeCatalog": "SCOPE_CATALOG",
    "ScopeSchema": "SCOPE_SCHEMA",
    "ScopeTable": "SCOPE_TABLE",
    "SourceDataType": "SOURCE_DATA_TYPE",
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
        mapping = CATALOG_COLUMNS
    elif operation == MetadataOp.SCHEMAS:
        mapping = SCHEMA_COLUMNS
    elif operation == MetadataOp.TABLES:
        mapping = TABLE_COLUMNS
    elif operation == MetadataOp.COLUMNS:
        mapping = COLUMN_COLUMNS

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
