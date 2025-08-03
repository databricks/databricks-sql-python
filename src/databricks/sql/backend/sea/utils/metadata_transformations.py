from typing import Any, Optional

# Table transformations
def transform_table_type(value: Any) -> str:
    """Transform empty/null table type to 'TABLE' per JDBC spec."""
    if value is None or value == "":
        return "TABLE"
    return str(value)


# Nullable transformations
def transform_is_nullable(value: Any) -> str:
    """Transform boolean nullable to YES/NO per JDBC spec."""
    if value is None or value == "true" or value is True:
        return "YES"
    return "NO"


def transform_nullable_to_int(value: Any) -> int:
    """Transform isNullable to JDBC integer (1=nullable, 0=not nullable)."""
    if value is None or value == "true" or value is True:
        return 1
    return 0


# Default value transformations
def transform_remarks_default(value: Any) -> str:
    """Transform null remarks to empty string."""
    if value is None:
        return ""
    return str(value)


def transform_numeric_default_zero(value: Any) -> int:
    """Transform null numeric values to 0."""
    if value is None:
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


# Calculated transformations
def calculate_data_type(value: Any) -> int:
    """Calculate JDBC SQL type code from Databricks type name."""
    if value is None:
        return 1111  # SQL NULL type

    type_name = str(value).upper().split("(")[0]
    type_map = {
        "STRING": 12,
        "VARCHAR": 12,
        "INT": 4,
        "INTEGER": 4,
        "DOUBLE": 8,
        "FLOAT": 6,
        "BOOLEAN": 16,
        "DATE": 91,
        "TIMESTAMP": 93,
        "TIMESTAMP_NTZ": 93,
        "DECIMAL": 3,
        "NUMERIC": 2,
        "BINARY": -2,
        "ARRAY": 2003,
        "MAP": 2002,
        "STRUCT": 2002,
        "TINYINT": -6,
        "SMALLINT": 5,
        "BIGINT": -5,
        "LONG": -5,
    }
    return type_map.get(type_name, 1111)


def calculate_buffer_length(value: Any) -> Optional[int]:
    """Calculate buffer length from type name."""
    if value is None:
        return None

    type_name = str(value).upper()
    if "ARRAY" in type_name or "MAP" in type_name:
        return 255

    # For other types, return None (will be null in result)
    return None


def transform_ordinal_position_offset(value: Any) -> int:
    """Adjust ordinal position from 1-based to 0-based or vice versa if needed."""
    if value is None:
        return 0
    try:
        # SEA returns 1-based, Thrift expects 0-based
        return int(value) - 1
    except (ValueError, TypeError):
        return 0


# Null column transformations
def always_null(value: Any) -> None:
    """Always return null for columns that should be null per JDBC spec."""
    return None


def always_null_int(value: Any) -> None:
    """Always return null for integer columns that should be null per JDBC spec."""
    return None


def always_null_smallint(value: Any) -> None:
    """Always return null for smallint columns that should be null per JDBC spec."""
    return None


# Identity transformations (for columns that need no change)
def identity(value: Any) -> Any:
    """Return value unchanged."""
    return value
