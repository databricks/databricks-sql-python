"""
Type normalization utilities for SEA backend.

This module provides functionality to normalize SEA type names to match
Thrift type naming conventions.
"""

from typing import Dict, Any

# SEA types that need to be translated to Thrift types
# The list of all SEA types is available in the REST reference at:
#    https://docs.databricks.com/api/workspace/statementexecution/executestatement
# The list of all Thrift types can be found in the ttypes.TTypeId definition
# The SEA types that do not align with Thrift are explicitly mapped below
SEA_TO_THRIFT_TYPE_MAP = {
    "BYTE": "TINYINT",
    "SHORT": "SMALLINT",
    "LONG": "BIGINT",
    "INTERVAL": "INTERVAL",  # Default mapping, will be overridden if type_interval_type is present
}


def normalize_sea_type_to_thrift(type_name: str, col_data: Dict[str, Any]) -> str:
    """
    Normalize SEA type names to match Thrift type naming conventions.

    Args:
        type_name: The type name from SEA (e.g., "BYTE", "LONG", "INTERVAL")
        col_data: The full column data dictionary from manifest (for accessing type_interval_type)

    Returns:
        Normalized type name matching Thrift conventions
    """
    # Early return if type doesn't need mapping
    if type_name not in SEA_TO_THRIFT_TYPE_MAP:
        return type_name

    normalized_type = SEA_TO_THRIFT_TYPE_MAP[type_name]

    # Special handling for interval types
    if type_name == "INTERVAL":
        type_interval_type = col_data.get("type_interval_type")
        if type_interval_type:
            return (
                "INTERVAL_YEAR_MONTH"
                if any(t in type_interval_type.upper() for t in ["YEAR", "MONTH"])
                else "INTERVAL_DAY_TIME"
            )

    return normalized_type
