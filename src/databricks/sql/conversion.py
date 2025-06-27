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
    """SQL type constants for improved maintainability."""

    # Numeric types
    TINYINT = "tinyint"
    SMALLINT = "smallint"
    INT = "int"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    REAL = "real"
    DOUBLE = "double"
    DECIMAL = "decimal"
    NUMERIC = "numeric"

    # Boolean types
    BOOLEAN = "boolean"
    BIT = "bit"

    # Date/Time types
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_NTZ = "timestamp_ntz"
    TIMESTAMP_LTZ = "timestamp_ltz"
    TIMESTAMP_TZ = "timestamp_tz"

    # String types
    CHAR = "char"
    VARCHAR = "varchar"
    STRING = "string"
    TEXT = "text"

    # Binary types
    BINARY = "binary"
    VARBINARY = "varbinary"

    # Complex types
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"

    @classmethod
    def is_numeric(cls, sql_type: str) -> bool:
        """Check if the SQL type is a numeric type."""
        return sql_type.lower() in (
            cls.TINYINT,
            cls.SMALLINT,
            cls.INT,
            cls.INTEGER,
            cls.BIGINT,
            cls.FLOAT,
            cls.REAL,
            cls.DOUBLE,
            cls.DECIMAL,
            cls.NUMERIC,
        )

    @classmethod
    def is_boolean(cls, sql_type: str) -> bool:
        """Check if the SQL type is a boolean type."""
        return sql_type.lower() in (cls.BOOLEAN, cls.BIT)

    @classmethod
    def is_datetime(cls, sql_type: str) -> bool:
        """Check if the SQL type is a date/time type."""
        return sql_type.lower() in (
            cls.DATE,
            cls.TIME,
            cls.TIMESTAMP,
            cls.TIMESTAMP_NTZ,
            cls.TIMESTAMP_LTZ,
            cls.TIMESTAMP_TZ,
        )

    @classmethod
    def is_string(cls, sql_type: str) -> bool:
        """Check if the SQL type is a string type."""
        return sql_type.lower() in (cls.CHAR, cls.VARCHAR, cls.STRING, cls.TEXT)

    @classmethod
    def is_binary(cls, sql_type: str) -> bool:
        """Check if the SQL type is a binary type."""
        return sql_type.lower() in (cls.BINARY, cls.VARBINARY)

    @classmethod
    def is_complex(cls, sql_type: str) -> bool:
        """Check if the SQL type is a complex type."""
        sql_type = sql_type.lower()
        return (
            sql_type.startswith(cls.ARRAY)
            or sql_type.startswith(cls.MAP)
            or sql_type.startswith(cls.STRUCT)
        )


class SqlTypeConverter:
    """
    Utility class for converting SQL types to Python types.
    Based on the JDBC ConverterHelper implementation.
    """

    # SQL type to conversion function mapping
    TYPE_MAPPING: Dict[str, Callable] = {
        # Numeric types
        SqlType.TINYINT: lambda v: int(v),
        SqlType.SMALLINT: lambda v: int(v),
        SqlType.INT: lambda v: int(v),
        SqlType.INTEGER: lambda v: int(v),
        SqlType.BIGINT: lambda v: int(v),
        SqlType.FLOAT: lambda v: float(v),
        SqlType.REAL: lambda v: float(v),
        SqlType.DOUBLE: lambda v: float(v),
        SqlType.DECIMAL: lambda v, p=None, s=None: (
            decimal.Decimal(v).quantize(
                decimal.Decimal(f'0.{"0" * s}'), context=decimal.Context(prec=p)
            )
            if p is not None and s is not None
            else decimal.Decimal(v)
        ),
        SqlType.NUMERIC: lambda v, p=None, s=None: (
            decimal.Decimal(v).quantize(
                decimal.Decimal(f'0.{"0" * s}'), context=decimal.Context(prec=p)
            )
            if p is not None and s is not None
            else decimal.Decimal(v)
        ),
        # Boolean types
        SqlType.BOOLEAN: lambda v: v.lower() in ("true", "t", "1", "yes", "y"),
        SqlType.BIT: lambda v: v.lower() in ("true", "t", "1", "yes", "y"),
        # Date/Time types
        SqlType.DATE: lambda v: datetime.date.fromisoformat(v),
        SqlType.TIME: lambda v: datetime.time.fromisoformat(v),
        SqlType.TIMESTAMP: lambda v: parser.parse(v),
        SqlType.TIMESTAMP_NTZ: lambda v: parser.parse(v).replace(tzinfo=None),
        SqlType.TIMESTAMP_LTZ: lambda v: parser.parse(v).astimezone(tz=None),
        SqlType.TIMESTAMP_TZ: lambda v: parser.parse(v),
        # String types - no conversion needed
        SqlType.CHAR: lambda v: v,
        SqlType.VARCHAR: lambda v: v,
        SqlType.STRING: lambda v: v,
        SqlType.TEXT: lambda v: v,
        # Binary types
        SqlType.BINARY: lambda v: bytes.fromhex(v),
        SqlType.VARBINARY: lambda v: bytes.fromhex(v),
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
            if sql_type in (SqlType.DECIMAL, SqlType.NUMERIC):
                return converter_func(value, precision, scale)
            else:
                return converter_func(value)
        except (ValueError, TypeError, decimal.InvalidOperation) as e:
            logger.warning(f"Error converting value '{value}' to {sql_type}: {e}")
            return value
