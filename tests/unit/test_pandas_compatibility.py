"""Tests for pandas compatibility across versions (pandas 2.x and 3.x).

These tests verify that _convert_arrow_table correctly converts Arrow tables
to Row objects using pandas as an intermediary, covering various data types
including nullable integers, floats, booleans, strings, decimal, dates,
timestamps, binary, and nested types.
"""

import datetime
import unittest
from decimal import Decimal
from unittest.mock import Mock, patch

import numpy
import pandas
import pytest

try:
    import pyarrow as pa
except ImportError:
    pa = None

from databricks.sql import result_set
from databricks.sql.result_set import ThriftResultSet


def _make_result_set(description, disable_pandas=False):
    """Create a ThriftResultSet with mocked dependencies for testing _convert_arrow_table.

    Mirrors the construction pattern used in test_client.py: pass mocked
    connection/execute_response/thrift_client to the normal constructor.
    """
    mock_connection = Mock()
    mock_connection.disable_pandas = disable_pandas

    mock_execute_response = Mock()
    mock_execute_response.description = description
    # t_row_set defaults to None, so result_format being None keeps the queue-build branch off.
    mock_execute_response.result_format = None

    mock_backend = Mock()
    # _fill_results_buffer() expects (results, has_more_rows, result_links_count).
    mock_backend.fetch_results.return_value = (Mock(), False, 0)

    return ThriftResultSet(
        connection=mock_connection,
        execute_response=mock_execute_response,
        thrift_client=mock_backend,
    )


@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
class TestConvertArrowTablePandasCompat(unittest.TestCase):
    """Test _convert_arrow_table with various Arrow types under current pandas version."""

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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].str_col, "hello")
        self.assertEqual(rows[1].str_col, "world")
        self.assertIsNone(rows[2].str_col)

    def test_large_string_type(self):
        """large_string is not in dtype_mapping → default Arrow→pandas conversion."""
        table = pa.table(
            {
                "lstr_col": pa.array(["foo", "bar", None], type=pa.large_string()),
            }
        )
        description = [
            ("lstr_col", "string", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].lstr_col, "foo")
        self.assertEqual(rows[1].lstr_col, "bar")
        self.assertIsNone(rows[2].lstr_col)

    def test_binary_type(self):
        table = pa.table(
            {
                "bin_col": pa.array([b"hello", None, b"world"], type=pa.binary()),
            }
        )
        description = [
            ("bin_col", "binary", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].bin_col, b"hello")
        self.assertIsNone(rows[1].bin_col)
        self.assertEqual(rows[2].bin_col, b"world")

    def test_decimal_type(self):
        table = pa.table(
            {
                "dec_col": pa.array(
                    [Decimal("1.23"), None, Decimal("99.99")],
                    type=pa.decimal128(5, 2),
                ),
            }
        )
        description = [
            ("dec_col", "decimal", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].dec_col, Decimal("1.23"))
        self.assertIsNone(rows[1].dec_col)
        self.assertEqual(rows[2].dec_col, Decimal("99.99"))

    def test_date_types(self):
        """date32 and date64 → datetime.date objects via date_as_object=True."""
        table = pa.table(
            {
                "date32_col": pa.array(
                    [datetime.date(2024, 1, 1), None, datetime.date(2026, 5, 19)],
                    type=pa.date32(),
                ),
                "date64_col": pa.array(
                    [None, datetime.date(2024, 12, 31), datetime.date(2026, 1, 1)],
                    type=pa.date64(),
                ),
            }
        )
        description = [
            ("date32_col", "date", None, None, None, None, None),
            ("date64_col", "date", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].date32_col, datetime.date(2024, 1, 1))
        self.assertIsNone(rows[0].date64_col)
        self.assertIsNone(rows[1].date32_col)
        self.assertEqual(rows[1].date64_col, datetime.date(2024, 12, 31))
        self.assertEqual(rows[2].date32_col, datetime.date(2026, 5, 19))
        self.assertEqual(rows[2].date64_col, datetime.date(2026, 1, 1))

    def test_timestamp_type(self):
        """timestamp → datetime.datetime objects via timestamp_as_object=True."""
        ts1 = datetime.datetime(2024, 1, 1, 12, 30, 45)
        ts2 = datetime.datetime(2026, 5, 19, 9, 15, 0)
        table = pa.table(
            {
                "ts_col": pa.array([ts1, None, ts2], type=pa.timestamp("us")),
            }
        )
        description = [
            ("ts_col", "timestamp", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].ts_col, ts1)
        self.assertIsNone(rows[1].ts_col)
        self.assertEqual(rows[2].ts_col, ts2)

    def test_list_type(self):
        table = pa.table(
            {
                "list_col": pa.array(
                    [[1, 2, 3], None, [4, 5]],
                    type=pa.list_(pa.int64()),
                ),
            }
        )
        description = [
            ("list_col", "array", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(list(rows[0].list_col), [1, 2, 3])
        self.assertIsNone(rows[1].list_col)
        self.assertEqual(list(rows[2].list_col), [4, 5])

    def test_struct_type(self):
        table = pa.table(
            {
                "struct_col": pa.array(
                    [{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}],
                    type=pa.struct([("x", pa.int64()), ("y", pa.string())]),
                ),
            }
        )
        description = [
            ("struct_col", "struct", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].struct_col, {"x": 1, "y": "a"})
        self.assertIsNone(rows[1].struct_col)
        self.assertEqual(rows[2].struct_col, {"x": 3, "y": "c"})

    def test_map_type(self):
        table = pa.table(
            {
                "map_col": pa.array(
                    [[("k1", 1), ("k2", 2)], None, [("k3", 3)]],
                    type=pa.map_(pa.string(), pa.int64()),
                ),
            }
        )
        description = [
            ("map_col", "map", None, None, None, None, None),
        ]

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 3)
        self.assertEqual(list(rows[0].map_col), [("k1", 1), ("k2", 2)])
        self.assertIsNone(rows[1].map_col)
        self.assertEqual(list(rows[2].map_col), [("k3", 3)])

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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
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

        rs = _make_result_set(description)
        rows = rs._convert_arrow_table(table)

        self.assertEqual(len(rows), 2)
        self.assertIsNone(rows[0].int_col)
        self.assertIsNone(rows[0].str_col)
        self.assertIsNone(rows[1].int_col)
        self.assertIsNone(rows[1].str_col)

    def test_disable_pandas_path(self):
        """Verify the non-pandas code path still works."""
        description = [
            ("id", "bigint", None, None, None, None, None),
            ("name", "string", None, None, None, None, None),
        ]
        rs = _make_result_set(description, disable_pandas=True)

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


@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
class TestConvertArrowTableInSlices(unittest.TestCase):
    """Tables larger than _ROWS_PER_CONVERSION are converted in slices."""

    def _table(self):
        tz = datetime.timezone.utc
        return pa.table(
            [
                pa.chunked_array([[1, None, 3], [4, 5, 6, None]], pa.int32()),
                pa.array([1.5, None, float("inf"), 4.0, 5.0, 6.0, -7.0]),
                pa.array(["a", None, "c", "d", "e", "f", "g"]),
                pa.array(
                    [Decimal(i).scaleb(-2) if i != 4 else None for i in range(7)],
                    pa.decimal128(5, 2),
                ),
                pa.array([datetime.date(2024, 1, i + 1) for i in range(7)]),
                pa.array(
                    [datetime.datetime(2024, 3, 10, i, tzinfo=tz) for i in range(7)],
                    pa.timestamp("us", tz="America/Los_Angeles"),
                ),
                pa.array([True, False, None, True, False, None, True]),
                # pyarrow picks the element dtype of every list from the whole
                # column, so the NULL in the last row affects the first row too.
                pa.array([[i, i] for i in range(6)] + [[None]], pa.list_(pa.int64())),
                pa.array(
                    [{"x": i if i != 2 else None} for i in range(7)],
                    pa.struct([("x", pa.int64())]),
                ),
                # Older pandas returns durations from a single-block DataFrame
                # as ints, so slices must keep the whole table's block layout.
                pa.array(
                    [datetime.timedelta(seconds=i) for i in range(7)],
                    pa.duration("us"),
                ),
            ],
            names=["a", "b", "a", "d", "e", "f", "g", "h", "i", "j"],
        )

    def _convert(self, table, rows_per_conversion):
        description = [
            (name, "string", None, None, None, None, None)
            for name in table.column_names
        ]
        with patch.object(result_set, "_ROWS_PER_CONVERSION", rows_per_conversion):
            return _make_result_set(description)._convert_arrow_table(table)

    def _to_numpy_row_counts(self, table, rows_per_conversion):
        counts = []
        to_numpy = pandas.DataFrame.to_numpy

        def spy(df, *args, **kwargs):
            counts.append(len(df))
            return to_numpy(df, *args, **kwargs)

        with patch.object(pandas.DataFrame, "to_numpy", spy):
            rows = self._convert(table, rows_per_conversion)
        self.assertEqual(len(rows), table.num_rows)
        return counts

    def test_sliced_conversion_matches_whole_table(self):
        table = self._table()
        whole = self._convert(table, table.num_rows)
        for rows_per_conversion in (1, 2, 3, 6):
            rows = self._convert(table, rows_per_conversion)
            self.assertEqual(len(rows), len(whole))
            for row, expected in zip(rows, whole):
                self.assertEqual(row.__fields__, expected.__fields__)
                for value, expected_value in zip(row, expected):
                    self.assertIs(type(value), type(expected_value))
                    if isinstance(expected_value, numpy.ndarray):
                        self.assertEqual(value.dtype, expected_value.dtype)
                        numpy.testing.assert_array_equal(value, expected_value)
                    else:
                        self.assertEqual(value, expected_value)

    def test_slices_are_bounded(self):
        table = self._table()
        # Nested columns in one call over all rows, the rest in slices.
        self.assertEqual(self._to_numpy_row_counts(table, 3), [7, 3, 3, 1])
        self.assertEqual(
            self._to_numpy_row_counts(table.select([0, 1, 2]), 3), [3, 3, 1]
        )

    def test_small_or_nested_only_tables_are_converted_whole(self):
        table = self._table()
        self.assertEqual(self._to_numpy_row_counts(table, 7), [7])
        self.assertEqual(self._to_numpy_row_counts(table.select([7, 8]), 3), [7])

    def test_duration_next_to_nested_column(self):
        table = self._table().select([9, 7])
        whole = self._convert(table, table.num_rows)
        rows = self._convert(table, 3)
        for row, expected in zip(rows, whole):
            self.assertIs(type(row[0]), type(expected[0]))
            self.assertEqual(row[0], expected[0])


if __name__ == "__main__":
    unittest.main()
