"""
Type normalization utilities for SEA backend.

This module provides functionality to normalize SEA type names to match
Thrift type naming conventions.
"""

from typing import Dict, Any

# Mapping of SEA types that need to be translated to Thrift conventions
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
            if any(t in type_interval_type.upper() for t in ["YEAR", "MONTH"]):
                return "INTERVAL_YEAR_MONTH"
            elif any(
                t in type_interval_type.upper()
                for t in ["DAY", "HOUR", "MINUTE", "SECOND"]
            ):
                return "INTERVAL_DAY_TIME"

    return normalized_type
