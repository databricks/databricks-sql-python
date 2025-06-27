"""
Type conversion utilities for the Databricks SQL Connector.

This module provides functionality to convert string values from SEA Inline results
to appropriate Python types based on column metadata.
"""

import datetime
import decimal
import logging
from dateutil import parser
from typing import Any, Callable, Dict, Optional, Union

logger = logging.getLogger(__name__)


class SqlType:
    """
    SQL type constants

    The list of types can be found in the SEA REST API Reference:
    https://docs.databricks.com/api/workspace/statementexecution/executestatement
    """

    # Numeric types
    BYTE = "byte"
    SHORT = "short"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"

    # Boolean type
    BOOLEAN = "boolean"

    # Date/Time types
    DATE = "date"
    TIMESTAMP = "timestamp"
    INTERVAL = "interval"

    # String types
    CHAR = "char"
    STRING = "string"

    # Binary type
    BINARY = "binary"

    # Complex types
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"

    # Other types
    NULL = "null"
    USER_DEFINED_TYPE = "user_defined_type"


class SqlTypeConverter:
    """
    Utility class for converting SQL types to Python types.
    Based on the types supported by the Databricks SDK.
    """

    # SQL type to conversion function mapping
    # TODO: complex types
    TYPE_MAPPING: Dict[str, Callable] = {
        # Numeric types
        SqlType.BYTE: lambda v: int(v),
        SqlType.SHORT: lambda v: int(v),
        SqlType.INT: lambda v: int(v),
        SqlType.LONG: lambda v: int(v),
        SqlType.FLOAT: lambda v: float(v),
        SqlType.DOUBLE: lambda v: float(v),
        SqlType.DECIMAL: lambda v, p=None, s=None: (
            decimal.Decimal(v).quantize(
                decimal.Decimal(f'0.{"0" * s}'), context=decimal.Context(prec=p)
            )
            if p is not None and s is not None
            else decimal.Decimal(v)
        ),
        # Boolean type
        SqlType.BOOLEAN: lambda v: v.lower() in ("true", "t", "1", "yes", "y"),
        # Date/Time types
        SqlType.DATE: lambda v: datetime.date.fromisoformat(v),
        SqlType.TIMESTAMP: lambda v: parser.parse(v),
        SqlType.INTERVAL: lambda v: v,  # Keep as string for now
        # String types - no conversion needed
        SqlType.CHAR: lambda v: v,
        SqlType.STRING: lambda v: v,
        # Binary type
        SqlType.BINARY: lambda v: bytes.fromhex(v),
        # Other types
        SqlType.NULL: lambda v: None,
        # Complex types and user-defined types return as-is
        SqlType.USER_DEFINED_TYPE: lambda v: v,
    }

    @staticmethod
    def convert_value(
        value: Any,
        sql_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> Any:
        """
        Convert a string value to the appropriate Python type based on SQL type.

        Args:
            value: The string value to convert
            sql_type: The SQL type (e.g., 'int', 'decimal')
            precision: Optional precision for decimal types
            scale: Optional scale for decimal types

        Returns:
            The converted value in the appropriate Python type
        """

        if value is None:
            return None

        sql_type = sql_type.lower().strip()

        if sql_type not in SqlTypeConverter.TYPE_MAPPING:
            return value

        converter_func = SqlTypeConverter.TYPE_MAPPING[sql_type]
        try:
            if sql_type == SqlType.DECIMAL:
                return converter_func(value, precision, scale)
            else:
                return converter_func(value)
        except (ValueError, TypeError, decimal.InvalidOperation) as e:
            logger.warning(f"Error converting value '{value}' to {sql_type}: {e}")
            return value
