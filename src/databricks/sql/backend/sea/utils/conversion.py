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

from databricks.sql.thrift_api.TCLIService import ttypes

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

    @staticmethod
    def _get_type_name(thrift_type_id: int) -> str:
        type_name = ttypes.TTypeId._VALUES_TO_NAMES[thrift_type_id]
        type_name = type_name.lower()
        if type_name.endswith("_type"):
            type_name = type_name[:-5]
        return type_name

    # Numeric types
    TINYINT = _get_type_name(ttypes.TTypeId.TINYINT_TYPE)
    SMALLINT = _get_type_name(ttypes.TTypeId.SMALLINT_TYPE)
    INT = _get_type_name(ttypes.TTypeId.INT_TYPE)
    BIGINT = _get_type_name(ttypes.TTypeId.BIGINT_TYPE)
    FLOAT = _get_type_name(ttypes.TTypeId.FLOAT_TYPE)
    DOUBLE = _get_type_name(ttypes.TTypeId.DOUBLE_TYPE)
    DECIMAL = _get_type_name(ttypes.TTypeId.DECIMAL_TYPE)

    # Boolean type
    BOOLEAN = _get_type_name(ttypes.TTypeId.BOOLEAN_TYPE)

    # Date/Time types
    DATE = _get_type_name(ttypes.TTypeId.DATE_TYPE)
    TIMESTAMP = _get_type_name(ttypes.TTypeId.TIMESTAMP_TYPE)
    INTERVAL_YEAR_MONTH = _get_type_name(ttypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE)
    INTERVAL_DAY_TIME = _get_type_name(ttypes.TTypeId.INTERVAL_DAY_TIME_TYPE)

    # String types
    CHAR = _get_type_name(ttypes.TTypeId.CHAR_TYPE)
    VARCHAR = _get_type_name(ttypes.TTypeId.VARCHAR_TYPE)
    STRING = _get_type_name(ttypes.TTypeId.STRING_TYPE)

    # Binary type
    BINARY = _get_type_name(ttypes.TTypeId.BINARY_TYPE)

    # Complex types
    ARRAY = _get_type_name(ttypes.TTypeId.ARRAY_TYPE)
    MAP = _get_type_name(ttypes.TTypeId.MAP_TYPE)
    STRUCT = _get_type_name(ttypes.TTypeId.STRUCT_TYPE)

    # Other types
    NULL = _get_type_name(ttypes.TTypeId.NULL_TYPE)
    UNION = _get_type_name(ttypes.TTypeId.UNION_TYPE)
    USER_DEFINED = _get_type_name(ttypes.TTypeId.USER_DEFINED_TYPE)


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

        # Handle None values directly
        if value is None:
            return None

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
