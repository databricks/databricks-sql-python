"""
Metadata normalization for SEA backend results.

This module provides functionality to normalize SEA metadata results
to match the column names and data format expected from the Thrift backend.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from databricks.sql.backend.sea.metadata_constants import (
    OPERATION_MAPPINGS,
    ColumnMapping,
)
from databricks.sql.backend.sea.utils.conversion import SqlType

logger = logging.getLogger(__name__)


# SQL type codes for metadata compatibility
class TypeCodes:
    """SQL type code constants for DATA_TYPE column values.

    These integer codes are used in the DATA_TYPE column of metadata results
    to maintain compatibility with the Thrift backend.
    """

    TINYINT = -6
    SMALLINT = 5
    INTEGER = 4
    BIGINT = -5
    FLOAT = 6
    DOUBLE = 8
    DECIMAL = 3
    BINARY = -2
    BOOLEAN = 16
    CHAR = 1
    VARCHAR = 12
    TIMESTAMP = 93
    DATE = 91
    STRUCT = 2002
    ARRAY = 2003
    OTHER = 1111


class MetadataNormalizer:
    """Normalizes SEA metadata results to match Thrift backend column names."""

    @staticmethod
    def normalize_description(description: List[Tuple], operation: str) -> List[Tuple]:
        """
        Normalize column description to use Thrift standard names.

        Args:
            description: Original description from SEA
            operation: The metadata operation (catalogs, schemas, tables, columns)

        Returns:
            Normalized description with Thrift column names
        """
        logger.debug(f"normalize_description called with operation: {operation}")
        logger.debug(f"Original description: {description}")

        mappings = OPERATION_MAPPINGS.get(operation, [])
        if not mappings:
            logger.debug(f"No mappings found for operation: {operation}")
            return description

        # Create lookup from SEA names to Thrift names
        sea_to_thrift = {
            mapping.sea_name: mapping.thrift_name
            for mapping in mappings
            if mapping.sea_name
        }
        logger.debug(f"SEA to Thrift mapping: {sea_to_thrift}")

        # Create new description with normalized names
        normalized_description = []
        for col_desc in description:
            (
                name,
                type_code,
                display_size,
                internal_size,
                precision,
                scale,
                null_ok,
            ) = col_desc

            # Skip columns that don't exist in Thrift for tables operation
            if operation == "tables" and name in ["isTemporary", "information"]:
                continue

            # Map SEA name to Thrift name
            thrift_name = sea_to_thrift.get(name, name)

            normalized_description.append(
                (
                    thrift_name,
                    type_code,
                    display_size,
                    internal_size,
                    precision,
                    scale,
                    null_ok,
                )
            )

        # Add any missing NULL columns required by Thrift spec
        existing_names = {desc[0] for desc in normalized_description}
        for mapping in mappings:
            if mapping.sea_name is None and mapping.thrift_name not in existing_names:
                # Add NULL column
                normalized_description.append(
                    (mapping.thrift_name, "string", None, None, None, None, None)
                )

        # For tables operation, ensure the columns are in Thrift order
        if operation == "tables":
            # Define the expected Thrift column order
            thrift_order = [
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION",
            ]

            # Create a mapping of column names to their descriptions
            desc_map = {desc[0]: desc for desc in normalized_description}

            # Rebuild the description in the correct order
            ordered_description = []
            for col_name in thrift_order:
                if col_name in desc_map:
                    ordered_description.append(desc_map[col_name])

            normalized_description = ordered_description

        logger.debug(f"Normalized description: {normalized_description}")
        return normalized_description

    @staticmethod
    def normalize_row_data(
        rows: List[Dict[str, Any]],
        operation: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Normalize row data to use Thrift standard column names.

        Args:
            rows: Original row data from SEA
            operation: The metadata operation (catalogs, schemas, tables, columns)

        Returns:
            Normalized row data with Thrift column names
        """
        logger.debug(f"normalize_row_data called with operation: {operation}")
        logger.debug(f"Number of rows to normalize: {len(rows)}")
        if rows:
            logger.debug(f"First row before normalization: {rows[0]}")

        mappings = OPERATION_MAPPINGS.get(operation, [])
        if not mappings:
            logger.debug(f"No mappings found for operation: {operation}")
            return rows

        # Create lookup from SEA names to Thrift names
        sea_to_thrift = {
            mapping.sea_name: mapping.thrift_name
            for mapping in mappings
            if mapping.sea_name
        }
        logger.debug(f"SEA to Thrift mapping: {sea_to_thrift}")

        normalized_rows = []
        for row in rows:
            normalized_row = {}

            # Map existing columns, but skip columns that don't have a mapping in tables operation
            for sea_name, value in row.items():
                if operation == "tables" and sea_name in ["isTemporary", "information"]:
                    # Skip these columns that exist in SEA but not in Thrift
                    continue
                thrift_name = sea_to_thrift.get(sea_name, sea_name)
                normalized_row[thrift_name] = value

            # Add NULL values for missing columns
            for mapping in mappings:
                if mapping.sea_name is None:
                    # Handle special cases that need context
                    if (
                        mapping.thrift_name == "TABLE_CATALOG"
                        and operation == "schemas"
                        and context
                    ):
                        # For schemas, populate TABLE_CATALOG with the catalog name from context
                        normalized_row[mapping.thrift_name] = context.get(
                            "catalog_name"
                        )
                    else:
                        normalized_row[mapping.thrift_name] = None
                elif mapping.thrift_name not in normalized_row:
                    # Handle special conversions if needed
                    if mapping.thrift_name == "DATA_TYPE":
                        # Convert TYPE_NAME to DATA_TYPE code
                        type_name = row.get("columnType", "")
                        normalized_row["DATA_TYPE"] = _convert_type_name_to_data_type(
                            type_name
                        )
                    elif mapping.thrift_name == "NULLABLE":
                        # Convert IS_NULLABLE to NULLABLE code
                        is_nullable = row.get("isNullable", "")
                        normalized_row["NULLABLE"] = 1 if is_nullable == "YES" else 0

            normalized_rows.append(normalized_row)

        if normalized_rows:
            logger.debug(f"First row after normalization: {normalized_rows[0]}")

        return normalized_rows


def _convert_type_name_to_data_type(type_name: str) -> int:
    """
    Convert normalized type name to SQL DATA_TYPE code.
    The type_name comes from the schema description's type_code field,
    which is already normalized by the SEA backend (see backend.py:324-327).
    This leverages the existing normalization rather than duplicating the logic.
    """
    # Simple mapping from normalized type names to SQL type codes
    # Using SqlType constants for consistency with existing codebase
    type_mapping = {
        SqlType.BYTE: TypeCodes.TINYINT,
        "tinyint": TypeCodes.TINYINT,
        SqlType.SHORT: TypeCodes.SMALLINT,
        "smallint": TypeCodes.SMALLINT,
        SqlType.INT: TypeCodes.INTEGER,
        "integer": TypeCodes.INTEGER,
        SqlType.LONG: TypeCodes.BIGINT,
        "bigint": TypeCodes.BIGINT,
        SqlType.FLOAT: TypeCodes.FLOAT,
        SqlType.DOUBLE: TypeCodes.DOUBLE,
        SqlType.DECIMAL: TypeCodes.DECIMAL,
        SqlType.BINARY: TypeCodes.BINARY,
        SqlType.BOOLEAN: TypeCodes.BOOLEAN,
        SqlType.CHAR: TypeCodes.CHAR,
        SqlType.STRING: TypeCodes.VARCHAR,
        "varchar": TypeCodes.VARCHAR,
        SqlType.TIMESTAMP: TypeCodes.TIMESTAMP,
        SqlType.DATE: TypeCodes.DATE,
        SqlType.STRUCT: TypeCodes.STRUCT,
        SqlType.ARRAY: TypeCodes.ARRAY,
        SqlType.MAP: TypeCodes.VARCHAR,  # Maps are represented as VARCHAR in Thrift
        SqlType.NULL: TypeCodes.VARCHAR,
    }

    return type_mapping.get(type_name.lower(), TypeCodes.OTHER)
