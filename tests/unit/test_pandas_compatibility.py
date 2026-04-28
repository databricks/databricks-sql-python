"""Tests for pandas compatibility across versions (pandas 2.x and 3.x).

These tests verify that _convert_arrow_table correctly converts Arrow tables
to Row objects using pandas as an intermediary, covering various data types
including nullable integers, floats, booleans, and strings.
"""

import unittest
from unittest.mock import Mock

import pandas
import pytest

try:
    import pyarrow as pa
except ImportError:
    pa = None

from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row


class _ConcreteResultSet(ResultSet):
    """Minimal concrete subclass of ResultSet for testing _convert_arrow_table."""

    def fetchone(self):
        pass

    def fetchmany(self, size):
        pass

    def fetchall(self):
        pass

    def fetchmany_arrow(self, size):
        pass

    def fetchall_arrow(self):
        pass


@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
class TestConvertArrowTablePandasCompat(unittest.TestCase):
    """Test _convert_arrow_table with various Arrow types under current pandas version."""

    def _make_result_set(self, description):
        """Create a minimal ResultSet instance for testing _convert_arrow_table."""
        mock_connection = Mock()
        mock_connection.disable_pandas = False

        rs = object.__new__(_ConcreteResultSet)
        rs.connection = mock_connection
        rs.description = description
        return rs

    def test_integer_types(self):
        table = pa.table(
            {
                "int8_col": pa.array([1, 2, None], type=pa.int8()),
                "int16_col": pa.array([100, None, 300], type=pa.int16()),
                "int32_col": pa.array([None, 2000, 3000], type=pa.int32()),
                "int64_col": pa.array([10000, 20000, 30000], type=pa.int64()),
            }
        )
        description = [
            ("int8_col", "tinyint", None, None, None, None, None),
            ("int16_col", "smallint", None, None, None, None, None),
            ("int32_col", "int", None, None, None, None, None),
            ("int64_col", "bigint", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].int8_col, 1)
        self.assertIsNone(rows[0].int32_col)
        self.assertIsNone(rows[1].int16_col)
        self.assertEqual(rows[2].int64_col, 30000)

    def test_unsigned_integer_types(self):
        table = pa.table(
            {
                "uint8_col": pa.array([1, None], type=pa.uint8()),
                "uint16_col": pa.array([None, 200], type=pa.uint16()),
                "uint32_col": pa.array([3000, 4000], type=pa.uint32()),
                "uint64_col": pa.array([50000, None], type=pa.uint64()),
            }
        )
        description = [
            ("uint8_col", "tinyint", None, None, None, None, None),
            ("uint16_col", "smallint", None, None, None, None, None),
            ("uint32_col", "int", None, None, None, None, None),
            ("uint64_col", "bigint", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].uint8_col, 1)
        self.assertIsNone(rows[0].uint16_col)
        self.assertEqual(rows[1].uint16_col, 200)
        self.assertIsNone(rows[1].uint64_col)

    def test_float_types(self):
        table = pa.table(
            {
                "float32_col": pa.array([1.5, None, 3.5], type=pa.float32()),
                "float64_col": pa.array([None, 2.5, 4.5], type=pa.float64()),
            }
        )
        description = [
            ("float32_col", "float", None, None, None, None, None),
            ("float64_col", "double", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertAlmostEqual(rows[0].float32_col, 1.5, places=5)
        self.assertIsNone(rows[0].float64_col)
        self.assertIsNone(rows[1].float32_col)
        self.assertAlmostEqual(rows[2].float64_col, 4.5)

    def test_boolean_type(self):
        table = pa.table(
            {
                "bool_col": pa.array([True, False, None], type=pa.bool_()),
            }
        )
        description = [
            ("bool_col", "boolean", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertTrue(rows[0].bool_col)
        self.assertFalse(rows[1].bool_col)
        self.assertIsNone(rows[2].bool_col)

    def test_string_type(self):
        """Verify string conversion works with both pandas 2.x and 3.x.

        In pandas 3.x, StringDtype() defaults to PyArrow-backed storage
        instead of Python object-backed. This test ensures the conversion
        still produces correct Python str values.
        """
        table = pa.table(
            {
                "str_col": pa.array(["hello", "world", None], type=pa.string()),
            }
        )
        description = [
            ("str_col", "string", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].str_col, "hello")
        self.assertEqual(rows[1].str_col, "world")
        self.assertIsNone(rows[2].str_col)

    def test_mixed_types(self):
        """Test a table with a mix of types, similar to real query results."""
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["alice", "bob", None], type=pa.string()),
                "score": pa.array([95.5, None, 87.3], type=pa.float64()),
                "active": pa.array([True, False, None], type=pa.bool_()),
            }
        )
        description = [
            ("id", "bigint", None, None, None, None, None),
            ("name", "string", None, None, None, None, None),
            ("score", "double", None, None, None, None, None),
            ("active", "boolean", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)

        # Row 0: all values present
        self.assertEqual(rows[0].id, 1)
        self.assertEqual(rows[0].name, "alice")
        self.assertAlmostEqual(rows[0].score, 95.5)
        self.assertTrue(rows[0].active)

        # Row 1: name present, score null
        self.assertEqual(rows[1].id, 2)
        self.assertEqual(rows[1].name, "bob")
        self.assertIsNone(rows[1].score)
        self.assertFalse(rows[1].active)

        # Row 2: name and active null
        self.assertEqual(rows[2].id, 3)
        self.assertIsNone(rows[2].name)
        self.assertAlmostEqual(rows[2].score, 87.3)
        self.assertIsNone(rows[2].active)

    def test_duplicate_column_names(self):
        """Test that duplicate column names are handled correctly."""
        table = pa.table(
            [
                pa.array([1, 2], type=pa.int32()),
                pa.array([3, 4], type=pa.int32()),
            ],
            names=["col", "col"],
        )
        description = [
            ("col", "int", None, None, None, None, None),
            ("col", "int", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 2)
        # Access by index since names are duplicated
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[0][1], 3)

    def test_empty_table(self):
        table = pa.table(
            {
                "col": pa.array([], type=pa.int32()),
            }
        )
        description = [
            ("col", "int", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 0)

    def test_all_nulls(self):
        table = pa.table(
            {
                "int_col": pa.array([None, None], type=pa.int64()),
                "str_col": pa.array([None, None], type=pa.string()),
            }
        )
        description = [
            ("int_col", "bigint", None, None, None, None, None),
            ("str_col", "string", None, None, None, None, None),
        ]

        rs = self._make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 2)
        self.assertIsNone(rows[0].int_col)
        self.assertIsNone(rows[0].str_col)
        self.assertIsNone(rows[1].int_col)
        self.assertIsNone(rows[1].str_col)

    def test_disable_pandas_path(self):
        """Verify the non-pandas code path still works."""
        mock_connection = Mock()
        mock_connection.disable_pandas = True

        rs = object.__new__(_ConcreteResultSet)
        rs.connection = mock_connection
        rs.description = [
            ("id", "bigint", None, None, None, None, None),
            ("name", "string", None, None, None, None, None),
        ]

        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["a", "b"], type=pa.string()),
            }
        )

        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].id, 1)
        self.assertEqual(rows[0].name, "a")
        self.assertEqual(rows[1].id, 2)
        self.assertEqual(rows[1].name, "b")


if __name__ == "__main__":
    unittest.main()
