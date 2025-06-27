"""
Unit tests for the type conversion utilities.
"""

import unittest
from datetime import date, datetime, time
from decimal import Decimal

from databricks.sql.backend.sea.conversion import SqlType, SqlTypeConverter


class TestSqlType(unittest.TestCase):
    """Tests for the SqlType class."""

    def test_is_numeric(self):
        """Test the is_numeric method."""
        self.assertTrue(SqlType.is_numeric(SqlType.INT))
        self.assertTrue(SqlType.is_numeric(SqlType.BYTE))
        self.assertTrue(SqlType.is_numeric(SqlType.SHORT))
        self.assertTrue(SqlType.is_numeric(SqlType.LONG))
        self.assertTrue(SqlType.is_numeric(SqlType.FLOAT))
        self.assertTrue(SqlType.is_numeric(SqlType.DOUBLE))
        self.assertTrue(SqlType.is_numeric(SqlType.DECIMAL))
        self.assertFalse(SqlType.is_numeric(SqlType.BOOLEAN))
        self.assertFalse(SqlType.is_numeric(SqlType.STRING))
        self.assertFalse(SqlType.is_numeric(SqlType.DATE))

    def test_is_boolean(self):
        """Test the is_boolean method."""
        self.assertTrue(SqlType.is_boolean(SqlType.BOOLEAN))
        self.assertFalse(SqlType.is_boolean(SqlType.INT))
        self.assertFalse(SqlType.is_boolean(SqlType.STRING))

    def test_is_datetime(self):
        """Test the is_datetime method."""
        self.assertTrue(SqlType.is_datetime(SqlType.DATE))
        self.assertTrue(SqlType.is_datetime(SqlType.TIMESTAMP))
        self.assertTrue(SqlType.is_datetime(SqlType.INTERVAL))
        self.assertFalse(SqlType.is_datetime(SqlType.INT))
        self.assertFalse(SqlType.is_datetime(SqlType.STRING))

    def test_is_string(self):
        """Test the is_string method."""
        self.assertTrue(SqlType.is_string(SqlType.CHAR))
        self.assertTrue(SqlType.is_string(SqlType.STRING))
        self.assertFalse(SqlType.is_string(SqlType.INT))
        self.assertFalse(SqlType.is_string(SqlType.DATE))

    def test_is_binary(self):
        """Test the is_binary method."""
        self.assertTrue(SqlType.is_binary(SqlType.BINARY))
        self.assertFalse(SqlType.is_binary(SqlType.INT))
        self.assertFalse(SqlType.is_binary(SqlType.STRING))

    def test_is_complex(self):
        """Test the is_complex method."""
        self.assertTrue(SqlType.is_complex("array<int>"))
        self.assertTrue(SqlType.is_complex("map<string,int>"))
        self.assertTrue(SqlType.is_complex("struct<name:string,age:int>"))
        self.assertFalse(SqlType.is_complex(SqlType.INT))
        self.assertFalse(SqlType.is_complex(SqlType.STRING))


class TestSqlTypeConverter(unittest.TestCase):
    """Tests for the SqlTypeConverter class."""

    def test_numeric_conversions(self):
        """Test numeric type conversions."""
        self.assertEqual(SqlTypeConverter.convert_value("123", SqlType.INT), 123)
        self.assertEqual(SqlTypeConverter.convert_value("123", SqlType.BYTE), 123)
        self.assertEqual(SqlTypeConverter.convert_value("123", SqlType.SHORT), 123)
        self.assertEqual(SqlTypeConverter.convert_value("123", SqlType.LONG), 123)
        self.assertEqual(
            SqlTypeConverter.convert_value("123.45", SqlType.FLOAT), 123.45
        )
        self.assertEqual(
            SqlTypeConverter.convert_value("123.45", SqlType.DOUBLE), 123.45
        )
        self.assertEqual(
            SqlTypeConverter.convert_value("123.45", SqlType.DECIMAL), Decimal("123.45")
        )

        # Test decimal with precision and scale
        self.assertEqual(
            SqlTypeConverter.convert_value(
                "123.456", SqlType.DECIMAL, precision=5, scale=2
            ),
            Decimal("123.46"),  # Rounded to scale 2
        )

    def test_boolean_conversions(self):
        """Test boolean type conversions."""
        self.assertTrue(SqlTypeConverter.convert_value("true", SqlType.BOOLEAN))
        self.assertTrue(SqlTypeConverter.convert_value("TRUE", SqlType.BOOLEAN))
        self.assertTrue(SqlTypeConverter.convert_value("1", SqlType.BOOLEAN))
        self.assertTrue(SqlTypeConverter.convert_value("yes", SqlType.BOOLEAN))
        self.assertFalse(SqlTypeConverter.convert_value("false", SqlType.BOOLEAN))
        self.assertFalse(SqlTypeConverter.convert_value("FALSE", SqlType.BOOLEAN))
        self.assertFalse(SqlTypeConverter.convert_value("0", SqlType.BOOLEAN))
        self.assertFalse(SqlTypeConverter.convert_value("no", SqlType.BOOLEAN))

    def test_datetime_conversions(self):
        """Test date/time type conversions."""
        self.assertEqual(
            SqlTypeConverter.convert_value("2023-01-15", SqlType.DATE),
            date(2023, 1, 15),
        )
        self.assertEqual(
            SqlTypeConverter.convert_value("2023-01-15 14:30:45", SqlType.TIMESTAMP),
            datetime(2023, 1, 15, 14, 30, 45),
        )

    def test_string_conversions(self):
        """Test string type conversions."""
        self.assertEqual(SqlTypeConverter.convert_value("test", SqlType.STRING), "test")
        self.assertEqual(SqlTypeConverter.convert_value("test", SqlType.CHAR), "test")

    def test_binary_conversions(self):
        """Test binary type conversions."""
        hex_str = "68656c6c6f"  # "hello" in hex
        expected_bytes = b"hello"

        self.assertEqual(
            SqlTypeConverter.convert_value(hex_str, SqlType.BINARY), expected_bytes
        )

    def test_error_handling(self):
        """Test error handling in conversions."""
        self.assertEqual(SqlTypeConverter.convert_value("abc", SqlType.INT), "abc")
        self.assertEqual(SqlTypeConverter.convert_value("abc", SqlType.FLOAT), "abc")
        self.assertEqual(SqlTypeConverter.convert_value("abc", SqlType.DECIMAL), "abc")

    def test_null_handling(self):
        """Test handling of NULL values."""
        self.assertIsNone(SqlTypeConverter.convert_value(None, SqlType.INT))
        self.assertIsNone(SqlTypeConverter.convert_value(None, SqlType.STRING))
        self.assertIsNone(SqlTypeConverter.convert_value(None, SqlType.DATE))

    def test_complex_type_handling(self):
        """Test handling of complex types."""
        # Complex types should be returned as-is for now
        self.assertEqual(
            SqlTypeConverter.convert_value('{"a": 1}', "array<int>"), '{"a": 1}'
        )
        self.assertEqual(
            SqlTypeConverter.convert_value('{"a": 1}', "map<string,int>"), '{"a": 1}'
        )
        self.assertEqual(
            SqlTypeConverter.convert_value('{"a": 1}', "struct<a:int>"), '{"a": 1}'
        )
        self.assertEqual(
            SqlTypeConverter.convert_value('{"a": 1}', SqlType.USER_DEFINED_TYPE),
            '{"a": 1}',
        )


if __name__ == "__main__":
    unittest.main()
