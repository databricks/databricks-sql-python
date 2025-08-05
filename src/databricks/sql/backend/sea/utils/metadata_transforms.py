"""Simple transformation functions for metadata value normalization."""


def transform_is_autoincrement(value):
    """Transform IS_AUTOINCREMENT: boolean to YES/NO string."""
    if isinstance(value, bool) or value is None:
        return "YES" if value else "NO"
    return value


def transform_is_nullable(value):
    """Transform IS_NULLABLE: true/false to YES/NO string."""
    if value is True or value == "true":
        return "YES"
    elif value is False or value == "false":
        return "NO"
    return value

def transform_remarks(value):
    if value is None: 
        return "" 
    return value 

def transform_nullable(value):
    """Transform NULLABLE column: boolean/string to integer."""
    if value is True or value == "true" or value == "YES":
        return 1
    elif value is False or value == "false" or value == "NO":
        return 0
    return value


# Type code mapping based on JDBC specification
TYPE_CODE_MAP = {
    "STRING": 12,  # VARCHAR
    "VARCHAR": 12,  # VARCHAR
    "CHAR": 1,  # CHAR
    "INT": 4,  # INTEGER
    "INTEGER": 4,  # INTEGER
    "BIGINT": -5,  # BIGINT
    "SMALLINT": 5,  # SMALLINT
    "TINYINT": -6,  # TINYINT
    "DOUBLE": 8,  # DOUBLE
    "FLOAT": 6,  # FLOAT
    "REAL": 7,  # REAL
    "DECIMAL": 3,  # DECIMAL
    "NUMERIC": 2,  # NUMERIC
    "BOOLEAN": 16,  # BOOLEAN
    "DATE": 91,  # DATE
    "TIMESTAMP": 93,  # TIMESTAMP
    "BINARY": -2,  # BINARY
    "ARRAY": 2003,  # ARRAY
    "MAP": 2002,  # JAVA_OBJECT
    "STRUCT": 2002,  # JAVA_OBJECT
}


def transform_data_type(value):
    """Transform DATA_TYPE: type name to JDBC type code."""
    if isinstance(value, str):
        # Handle parameterized types like DECIMAL(10,2)
        base_type = value.split("(")[0].upper()
        return TYPE_CODE_MAP.get(base_type, value)
    return value


def transform_ordinal_position(value):
    """Transform ORDINAL_POSITION: decrement by 1 (1-based to 0-based)."""
    if isinstance(value, int):
        return value - 1
    return value
