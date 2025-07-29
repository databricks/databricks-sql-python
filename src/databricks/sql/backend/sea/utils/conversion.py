"""
Type conversion utilities for the Databricks SQL Connector.

This module provides functionality to convert string values from SEA Inline results
to appropriate Python types based on column metadata.
"""

import datetime
import decimal
import logging
from dateutil import parser
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)


def _convert_decimal(
    value: str, precision: Optional[int] = None, scale: Optional[int] = None
) -> decimal.Decimal:
    """
    Convert a string value to a decimal with optional precision and scale.

    Args:
        value: The string value to convert
        precision: Optional precision (total number of significant digits) for the decimal
        scale: Optional scale (number of decimal places) for the decimal

    Returns:
        A decimal.Decimal object with appropriate precision and scale
    """

    # First create the decimal from the string value
    result = decimal.Decimal(value)

    # Apply scale (quantize to specific number of decimal places) if specified
    quantizer = None
    if scale is not None:
        quantizer = decimal.Decimal(f'0.{"0" * scale}')

    # Apply precision (total number of significant digits) if specified
    context = None
    if precision is not None:
        context = decimal.Context(prec=precision)

    if quantizer is not None:
        result = result.quantize(quantizer, context=context)

    return result


class SqlType:
    """
    SQL type constants based on Thrift TTypeId values.

    These correspond to the normalized type names that come from the SEA backend
    after normalize_sea_type_to_thrift processing (lowercase, without _TYPE suffix).
    """

    # Numeric types
    TINYINT = "tinyint"  # Maps to TTypeId.TINYINT_TYPE
    SMALLINT = "smallint"  # Maps to TTypeId.SMALLINT_TYPE
    INT = "int"  # Maps to TTypeId.INT_TYPE
    BIGINT = "bigint"  # Maps to TTypeId.BIGINT_TYPE
    FLOAT = "float"  # Maps to TTypeId.FLOAT_TYPE
    DOUBLE = "double"  # Maps to TTypeId.DOUBLE_TYPE
    DECIMAL = "decimal"  # Maps to TTypeId.DECIMAL_TYPE

    # Boolean type
    BOOLEAN = "boolean"  # Maps to TTypeId.BOOLEAN_TYPE

    # Date/Time types
    DATE = "date"  # Maps to TTypeId.DATE_TYPE
    TIMESTAMP = "timestamp"  # Maps to TTypeId.TIMESTAMP_TYPE
    INTERVAL_YEAR_MONTH = (
        "interval_year_month"  # Maps to TTypeId.INTERVAL_YEAR_MONTH_TYPE
    )
    INTERVAL_DAY_TIME = "interval_day_time"  # Maps to TTypeId.INTERVAL_DAY_TIME_TYPE

    # String types
    CHAR = "char"  # Maps to TTypeId.CHAR_TYPE
    VARCHAR = "varchar"  # Maps to TTypeId.VARCHAR_TYPE
    STRING = "string"  # Maps to TTypeId.STRING_TYPE

    # Binary type
    BINARY = "binary"  # Maps to TTypeId.BINARY_TYPE

    # Complex types
    ARRAY = "array"  # Maps to TTypeId.ARRAY_TYPE
    MAP = "map"  # Maps to TTypeId.MAP_TYPE
    STRUCT = "struct"  # Maps to TTypeId.STRUCT_TYPE

    # Other types
    NULL = "null"  # Maps to TTypeId.NULL_TYPE
    UNION = "union"  # Maps to TTypeId.UNION_TYPE
    USER_DEFINED = "user_defined"  # Maps to TTypeId.USER_DEFINED_TYPE


class SqlTypeConverter:
    """
    Utility class for converting SQL types to Python types.
    Based on the Thrift TTypeId types after normalization.
    """

    # SQL type to conversion function mapping
    # TODO: complex types
    TYPE_MAPPING: Dict[str, Callable] = {
        # Numeric types
        SqlType.TINYINT: lambda v: int(v),
        SqlType.SMALLINT: lambda v: int(v),
        SqlType.INT: lambda v: int(v),
        SqlType.BIGINT: lambda v: int(v),
        SqlType.FLOAT: lambda v: float(v),
        SqlType.DOUBLE: lambda v: float(v),
        SqlType.DECIMAL: _convert_decimal,
        # Boolean type
        SqlType.BOOLEAN: lambda v: v.lower() in ("true", "t", "1", "yes", "y"),
        # Date/Time types
        SqlType.DATE: lambda v: datetime.date.fromisoformat(v),
        SqlType.TIMESTAMP: lambda v: parser.parse(v),
        SqlType.INTERVAL_YEAR_MONTH: lambda v: v,  # Keep as string for now
        SqlType.INTERVAL_DAY_TIME: lambda v: v,  # Keep as string for now
        # String types - no conversion needed
        SqlType.CHAR: lambda v: v,
        SqlType.VARCHAR: lambda v: v,
        SqlType.STRING: lambda v: v,
        # Binary type
        SqlType.BINARY: lambda v: bytes.fromhex(v),
        # Other types
        SqlType.NULL: lambda v: None,
        # Complex types and user-defined types return as-is
        SqlType.USER_DEFINED: lambda v: v,
    }

    @staticmethod
    def convert_value(
        value: str,
        sql_type: str,
        column_name: Optional[str],
        **kwargs,
    ) -> object:
        """
        Convert a string value to the appropriate Python type based on SQL type.

        Args:
            value: The string value to convert
            sql_type: The SQL type (e.g., 'tinyint', 'decimal')
            column_name: The name of the column being converted
            **kwargs: Additional keyword arguments for the conversion function

        Returns:
            The converted value in the appropriate Python type
        """

        sql_type = sql_type.lower().strip()

        if sql_type not in SqlTypeConverter.TYPE_MAPPING:
            return value

        converter_func = SqlTypeConverter.TYPE_MAPPING[sql_type]
        try:
            if sql_type == SqlType.DECIMAL:
                precision = kwargs.get("precision", None)
                scale = kwargs.get("scale", None)
                return converter_func(value, precision, scale)
            else:
                return converter_func(value)
        except Exception as e:
            warning_message = f"Error converting value '{value}' to {sql_type}"
            if column_name:
                warning_message += f" in column {column_name}"
            warning_message += f": {e}"
            logger.warning(warning_message)
            return value
