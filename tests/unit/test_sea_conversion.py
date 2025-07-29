"""
Tests for the conversion module in the SEA backend.

This module contains tests for the SqlType and SqlTypeConverter classes.
"""

import pytest
import datetime
import decimal
from unittest.mock import Mock, patch

from databricks.sql.backend.sea.utils.conversion import SqlType, SqlTypeConverter


class TestSqlTypeConverter:
    """Test suite for the SqlTypeConverter class."""

    def test_convert_numeric_types(self):
        """Test converting numeric types."""
        # Test integer types
        assert SqlTypeConverter.convert_value("123", SqlType.TINYINT) == 123
        assert SqlTypeConverter.convert_value("456", SqlType.SMALLINT) == 456
        assert SqlTypeConverter.convert_value("789", SqlType.INT) == 789
        assert (
            SqlTypeConverter.convert_value("1234567890", SqlType.BIGINT) == 1234567890
        )

        # Test floating point types
        assert SqlTypeConverter.convert_value("123.45", SqlType.FLOAT) == 123.45
        assert SqlTypeConverter.convert_value("678.90", SqlType.DOUBLE) == 678.90

        # Test decimal type
        decimal_value = SqlTypeConverter.convert_value("123.45", SqlType.DECIMAL)
        assert isinstance(decimal_value, decimal.Decimal)
        assert decimal_value == decimal.Decimal("123.45")

        # Test decimal with precision and scale
        decimal_value = SqlTypeConverter.convert_value(
            "123.45", SqlType.DECIMAL, precision=5, scale=2
        )
        assert isinstance(decimal_value, decimal.Decimal)
        assert decimal_value == decimal.Decimal("123.45")

        # Test invalid numeric input
        result = SqlTypeConverter.convert_value("not_a_number", SqlType.INT)
        assert result == "not_a_number"  # Returns original value on error

    def test_convert_boolean_type(self):
        """Test converting boolean types."""
        # True values
        assert SqlTypeConverter.convert_value("true", SqlType.BOOLEAN) is True
        assert SqlTypeConverter.convert_value("True", SqlType.BOOLEAN) is True
        assert SqlTypeConverter.convert_value("t", SqlType.BOOLEAN) is True
        assert SqlTypeConverter.convert_value("1", SqlType.BOOLEAN) is True
        assert SqlTypeConverter.convert_value("yes", SqlType.BOOLEAN) is True
        assert SqlTypeConverter.convert_value("y", SqlType.BOOLEAN) is True

        # False values
        assert SqlTypeConverter.convert_value("false", SqlType.BOOLEAN) is False
        assert SqlTypeConverter.convert_value("False", SqlType.BOOLEAN) is False
        assert SqlTypeConverter.convert_value("f", SqlType.BOOLEAN) is False
        assert SqlTypeConverter.convert_value("0", SqlType.BOOLEAN) is False
        assert SqlTypeConverter.convert_value("no", SqlType.BOOLEAN) is False
        assert SqlTypeConverter.convert_value("n", SqlType.BOOLEAN) is False

    def test_convert_datetime_types(self):
        """Test converting datetime types."""
        # Test date type
        date_value = SqlTypeConverter.convert_value("2023-01-15", SqlType.DATE)
        assert isinstance(date_value, datetime.date)
        assert date_value == datetime.date(2023, 1, 15)

        # Test timestamp type
        timestamp_value = SqlTypeConverter.convert_value(
            "2023-01-15T12:30:45", SqlType.TIMESTAMP
        )
        assert isinstance(timestamp_value, datetime.datetime)
        assert timestamp_value.year == 2023
        assert timestamp_value.month == 1
        assert timestamp_value.day == 15
        assert timestamp_value.hour == 12
        assert timestamp_value.minute == 30
        assert timestamp_value.second == 45

        # Test interval types (currently return as string)
        interval_ym_value = SqlTypeConverter.convert_value(
            "1-6", SqlType.INTERVAL_YEAR_MONTH
        )
        assert interval_ym_value == "1-6"

        interval_dt_value = SqlTypeConverter.convert_value(
            "1 day 2 hours", SqlType.INTERVAL_DAY_TIME
        )
        assert interval_dt_value == "1 day 2 hours"

        # Test invalid date input
        result = SqlTypeConverter.convert_value("not_a_date", SqlType.DATE)
        assert result == "not_a_date"  # Returns original value on error

    def test_convert_string_types(self):
        """Test converting string types."""
        # String types don't need conversion, they should be returned as-is
        assert (
            SqlTypeConverter.convert_value("test string", SqlType.STRING)
            == "test string"
        )
        assert SqlTypeConverter.convert_value("test char", SqlType.CHAR) == "test char"
        assert (
            SqlTypeConverter.convert_value("test varchar", SqlType.VARCHAR)
            == "test varchar"
        )

    def test_convert_binary_type(self):
        """Test converting binary type."""
        # Test valid hex string
        binary_value = SqlTypeConverter.convert_value("48656C6C6F", SqlType.BINARY)
        assert isinstance(binary_value, bytes)
        assert binary_value == b"Hello"

        # Test invalid binary input
        result = SqlTypeConverter.convert_value("not_hex", SqlType.BINARY)
        assert result == "not_hex"  # Returns original value on error

    def test_convert_unsupported_type(self):
        """Test converting an unsupported type."""
        # Should return the original value
        assert SqlTypeConverter.convert_value("test", "unsupported_type") == "test"

        # Complex types should return as-is (not yet implemented in TYPE_MAPPING)
        assert (
            SqlTypeConverter.convert_value("complex_value", SqlType.ARRAY)
            == "complex_value"
        )
        assert (
            SqlTypeConverter.convert_value("complex_value", SqlType.MAP)
            == "complex_value"
        )
        assert (
            SqlTypeConverter.convert_value("complex_value", SqlType.STRUCT)
            == "complex_value"
        )
