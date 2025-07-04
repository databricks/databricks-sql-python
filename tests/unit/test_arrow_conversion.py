import pytest

try:
    import pyarrow as pa
except ImportError:
    pa = None
import pandas
import datetime
import unittest
from unittest.mock import MagicMock

from databricks.sql.client import ResultSet, Connection, ExecuteResponse
from databricks.sql.types import Row
from databricks.sql.utils import ArrowQueue

@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
class ArrowConversionTests(unittest.TestCase):
    @staticmethod
    def mock_connection_static():
        conn = MagicMock(spec=Connection)
        conn.disable_pandas = False
        conn._arrow_pandas_type_override = {}
        conn._arrow_to_pandas_kwargs = {}
        return conn

    @staticmethod
    def sample_arrow_table_static():
        data = [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["a", "b", "c"], type=pa.string()),
        ]
        schema = pa.schema([("col_int", pa.int32()), ("col_str", pa.string())])
        return pa.Table.from_arrays(data, schema=schema)

    @staticmethod
    def mock_thrift_backend_static():
        sample_table = ArrowConversionTests.sample_arrow_table_static()
        tb = MagicMock()
        empty_arrays = [pa.array([], type=field.type) for field in sample_table.schema]
        empty_table = pa.Table.from_arrays(empty_arrays, schema=sample_table.schema)
        tb.fetch_results.return_value = (ArrowQueue(empty_table, 0), False)
        return tb

    @staticmethod
    def mock_raw_execute_response_static():
        er = MagicMock(spec=ExecuteResponse)
        er.description = [
            ("col_int", "int", None, None, None, None, None),
            ("col_str", "string", None, None, None, None, None),
        ]
        er.arrow_schema_bytes = None
        er.arrow_queue = None
        er.has_more_rows = False
        er.lz4_compressed = False
        er.command_handle = MagicMock()
        er.status = MagicMock()
        er.has_been_closed_server_side = False
        er.is_staging_operation = False
        return er

    def test_convert_arrow_table_default(self):
        mock_connection = ArrowConversionTests.mock_connection_static()
        sample_arrow_table = ArrowConversionTests.sample_arrow_table_static()
        mock_thrift_backend = ArrowConversionTests.mock_thrift_backend_static()
        mock_raw_execute_response = (
            ArrowConversionTests.mock_raw_execute_response_static()
        )

        mock_raw_execute_response.arrow_queue = ArrowQueue(
            sample_arrow_table, sample_arrow_table.num_rows
        )
        rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
        result_one = rs.fetchone()
        self.assertIsInstance(result_one, Row)
        self.assertEqual(result_one.col_int, 1)
        self.assertEqual(result_one.col_str, "a")

        mock_raw_execute_response.arrow_queue = ArrowQueue(
            sample_arrow_table, sample_arrow_table.num_rows
        )
        rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
        result_all = rs.fetchall()
        self.assertEqual(len(result_all), 3)
        self.assertIsInstance(result_all[0], Row)
        self.assertEqual(result_all[0].col_int, 1)
        self.assertEqual(result_all[1].col_str, "b")

    def test_convert_arrow_table_disable_pandas(self):
        mock_connection = ArrowConversionTests.mock_connection_static()
        sample_arrow_table = ArrowConversionTests.sample_arrow_table_static()
        mock_thrift_backend = ArrowConversionTests.mock_thrift_backend_static()
        mock_raw_execute_response = (
            ArrowConversionTests.mock_raw_execute_response_static()
        )

        mock_connection.disable_pandas = True
        mock_raw_execute_response.arrow_queue = ArrowQueue(
            sample_arrow_table, sample_arrow_table.num_rows
        )
        rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
        result = rs.fetchall()
        self.assertEqual(len(result), 3)
        self.assertIsInstance(result[0], Row)
        self.assertEqual(result[0].col_int, 1)
        self.assertEqual(result[0].col_str, "a")
        self.assertIsInstance(sample_arrow_table.column(0)[0].as_py(), int)
        self.assertIsInstance(sample_arrow_table.column(1)[0].as_py(), str)

    def test_convert_arrow_table_type_override(self):
        mock_connection = ArrowConversionTests.mock_connection_static()
        sample_arrow_table = ArrowConversionTests.sample_arrow_table_static()
        mock_thrift_backend = ArrowConversionTests.mock_thrift_backend_static()
        mock_raw_execute_response = (
            ArrowConversionTests.mock_raw_execute_response_static()
        )

        mock_connection._arrow_pandas_type_override = {
            pa.int32(): pandas.Float64Dtype()
        }
        mock_raw_execute_response.arrow_queue = ArrowQueue(
            sample_arrow_table, sample_arrow_table.num_rows
        )
        rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
        result = rs.fetchall()
        self.assertEqual(len(result), 3)
        self.assertIsInstance(result[0].col_int, float)
        self.assertEqual(result[0].col_int, 1.0)
        self.assertEqual(result[0].col_str, "a")

    def test_convert_arrow_table_to_pandas_kwargs(self):
        mock_connection = ArrowConversionTests.mock_connection_static()
        mock_thrift_backend = (
            ArrowConversionTests.mock_thrift_backend_static()
        )  # Does not use sample_arrow_table
        mock_raw_execute_response = (
            ArrowConversionTests.mock_raw_execute_response_static()
        )

        dt_obj = datetime.datetime(2021, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        ts_array = pa.array([dt_obj], type=pa.timestamp("us", tz="UTC"))
        ts_schema = pa.schema([("col_ts", pa.timestamp("us", tz="UTC"))])
        ts_table = pa.Table.from_arrays([ts_array], schema=ts_schema)

        mock_raw_execute_response.description = [
            ("col_ts", "timestamp", None, None, None, None, None)
        ]
        mock_raw_execute_response.arrow_queue = ArrowQueue(ts_table, ts_table.num_rows)

        # Scenario 1: timestamp_as_object = True. Observed as datetime.datetime in Row.
        mock_connection._arrow_to_pandas_kwargs = {"timestamp_as_object": True}
        rs_ts_true = ResultSet(
            mock_connection, mock_raw_execute_response, mock_thrift_backend
        )
        result_true = rs_ts_true.fetchall()
        self.assertEqual(len(result_true), 1)
        self.assertIsInstance(result_true[0].col_ts, datetime.datetime)

        # Scenario 2: timestamp_as_object = False. Observed as pandas.Timestamp in Row for this input.
        mock_raw_execute_response.arrow_queue = ArrowQueue(
            ts_table, ts_table.num_rows
        )  # Reset queue
        mock_connection._arrow_to_pandas_kwargs = {"timestamp_as_object": False}
        rs_ts_false = ResultSet(
            mock_connection, mock_raw_execute_response, mock_thrift_backend
        )
        result_false = rs_ts_false.fetchall()
        self.assertEqual(len(result_false), 1)
        self.assertIsInstance(result_false[0].col_ts, pandas.Timestamp)

        # Scenario 3: no override. Observed as datetime.datetime in Row since timestamp_as_object is True by default.
        mock_raw_execute_response.arrow_queue = ArrowQueue(
            ts_table, ts_table.num_rows
        )  # Reset queue
        mock_connection._arrow_to_pandas_kwargs = {}
        rs_ts_default = ResultSet(
            mock_connection, mock_raw_execute_response, mock_thrift_backend
        )
        result_default = rs_ts_default.fetchall()
        self.assertEqual(len(result_default), 1)
        self.assertIsInstance(result_default[0].col_ts, datetime.datetime)


if __name__ == "__main__":
    unittest.main()
