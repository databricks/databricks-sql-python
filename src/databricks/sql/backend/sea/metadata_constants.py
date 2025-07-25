"""
Column normalization constants for SEA backend metadata queries.

This module defines column mappings to normalize SEA backend metadata results 
to match JDBC DatabaseMetaData standards and thrift backend behavior.
"""

from typing import List, Tuple, Dict, Any


# Columns for catalogs() - matching JDBC CATALOG_COLUMNS exactly
CATALOG_COLUMNS: List[Tuple[str, str]] = [
    ("TABLE_CAT", "catalog"),  # CATALOG_COLUMN_FOR_GET_CATALOGS
]

# Columns for schemas() - matching JDBC SCHEMA_COLUMNS exactly
SCHEMA_COLUMNS: List[Tuple[str, str]] = [
    ("TABLE_SCHEM", "databaseName"),  # SCHEMA_COLUMN_FOR_GET_SCHEMA
    ("TABLE_CATALOG", "catalogName"),  # CATALOG_FULL_COLUMN
]

# Columns for tables() - matching JDBC TABLE_COLUMNS exactly
TABLE_COLUMNS: List[Tuple[str, str]] = [
    ("TABLE_CAT", "catalogName"),  # CATALOG_COLUMN
    ("TABLE_SCHEM", "namespace"),  # SCHEMA_COLUMN
    ("TABLE_NAME", "tableName"),  # TABLE_NAME_COLUMN
    ("TABLE_TYPE", "tableType"),  # TABLE_TYPE_COLUMN
    ("REMARKS", "remarks"),  # REMARKS_COLUMN
    ("TYPE_CAT", "TYPE_CATALOG_COLUMN"),  # TYPE_CATALOG_COLUMN (likely None in data)
    ("TYPE_SCHEM", "TYPE_SCHEMA_COLUMN"),  # TYPE_SCHEMA_COLUMN (likely None in data)
    ("TYPE_NAME", "TYPE_NAME"),  # TYPE_NAME_COLUMN (likely None in data)
    (
        "SELF_REFERENCING_COL_NAME",
        "SELF_REFERENCING_COLUMN_NAME",
    ),  # (likely None in data)
    (
        "REF_GENERATION",
        "REF_GENERATION_COLUMN",
    ),  # REF_GENERATION_COLUMN (likely None in data)
]

# Columns for columns() - matching JDBC COLUMN_COLUMNS exactly
COLUMN_COLUMNS: List[Tuple[str, str]] = [
    ("TABLE_CAT", "catalogName"),  # CATALOG_COLUMN
    ("TABLE_SCHEM", "namespace"),  # SCHEMA_COLUMN
    ("TABLE_NAME", "tableName"),  # TABLE_NAME_COLUMN
    ("COLUMN_NAME", "col_name"),  # COL_NAME_COLUMN
    ("DATA_TYPE", "dataType"),  # DATA_TYPE_COLUMN
    ("TYPE_NAME", "columnType"),  # COLUMN_TYPE_COLUMN
    ("COLUMN_SIZE", "columnSize"),  # COLUMN_SIZE_COLUMN
    ("BUFFER_LENGTH", "bufferLength"),  # BUFFER_LENGTH_COLUMN
    ("DECIMAL_DIGITS", "decimalDigits"),  # DECIMAL_DIGITS_COLUMN
    ("NUM_PREC_RADIX", "radix"),  # NUM_PREC_RADIX_COLUMN
    ("NULLABLE", "Nullable"),  # NULLABLE_COLUMN
    ("REMARKS", "remarks"),  # REMARKS_COLUMN
    ("COLUMN_DEF", "columnType"),  # COLUMN_DEF_COLUMN (same source as TYPE_NAME)
    ("SQL_DATA_TYPE", "SQLDataType"),  # SQL_DATA_TYPE_COLUMN
    ("SQL_DATETIME_SUB", "SQLDateTimeSub"),  # SQL_DATETIME_SUB_COLUMN
    ("CHAR_OCTET_LENGTH", "CharOctetLength"),  # CHAR_OCTET_LENGTH_COLUMN
    ("ORDINAL_POSITION", "ordinalPosition"),  # ORDINAL_POSITION_COLUMN
    ("IS_NULLABLE", "isNullable"),  # IS_NULLABLE_COLUMN
    ("SCOPE_CATALOG", "ScopeCatalog"),  # SCOPE_CATALOG_COLUMN
    ("SCOPE_SCHEMA", "ScopeSchema"),  # SCOPE_SCHEMA_COLUMN
    ("SCOPE_TABLE", "ScopeTable"),  # SCOPE_TABLE_COLUMN
    ("SOURCE_DATA_TYPE", "SourceDataType"),  # SOURCE_DATA_TYPE_COLUMN
    ("IS_AUTOINCREMENT", "isAutoIncrement"),  # IS_AUTO_INCREMENT_COLUMN
    ("IS_GENERATEDCOLUMN", "isGenerated"),  # IS_GENERATED_COLUMN
]

# Note: COLUMN_DEF and TYPE_NAME both map to "columnType" - no special handling needed
# since they both reference the same source column in the data


# Helper functions to work with column definitions
def get_column_names(columns: List[Tuple[str, str]]) -> List[str]:
    """Extract JDBC column names from column definitions."""
    return [jdbc_name for jdbc_name, _ in columns]


def get_column_mapping(columns: List[Tuple[str, str]]) -> Dict[str, str]:
    """Create mapping dict from SEA names to JDBC names."""
    return {
        sea_name: jdbc_name for jdbc_name, sea_name in columns if sea_name is not None
    }


def normalize_metadata_description(
    original_description: List[Tuple], column_definitions: List[Tuple[str, str]]
) -> List[Tuple]:
    """
    Transform result set description to use JDBC-standard column names.

    Args:
        original_description: Original PEP-249 description from SEA backend
                            Format: [(name, type_code, display_size, internal_size,
                                     precision, scale, null_ok), ...]
        column_definitions: List of (jdbc_name, sea_source_name) tuples defining mappings

    Returns:
        Normalized description with JDBC column names
    """
    if not original_description:
        return original_description

    # Build mapping from SEA column names to their indices
    sea_col_to_idx = {}
    for idx, col_desc in enumerate(original_description):
        sea_col_to_idx[col_desc[0]] = idx

    # Build new description based on column definitions
    normalized_description = []

    for jdbc_name, sea_name in column_definitions:
        if sea_name and sea_name in sea_col_to_idx:
            # Column exists in original description
            orig_idx = sea_col_to_idx[sea_name]
            orig_desc = original_description[orig_idx]
            # Replace the column name, keep other metadata
            new_desc = (jdbc_name,) + orig_desc[1:]
            normalized_description.append(new_desc)
        else:
            # Column doesn't exist, add with default metadata
            # Use VARCHAR type and nullable=None as defaults
            normalized_description.append(
                (jdbc_name, "string", None, None, None, None, None)
            )

    return normalized_description


def normalize_columns_metadata_description(
    original_description: List[Tuple],
) -> List[Tuple]:
    """
    Normalization for columns() metadata.

    Args:
        original_description: Original description from SEA backend

    Returns:
        Normalized description matching JDBC COLUMN_COLUMNS
    """
    # COLUMN_DEF and TYPE_NAME both map to "columnType" so no special handling needed
    return normalize_metadata_description(original_description, COLUMN_COLUMNS)
