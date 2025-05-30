import pytest
import pyarrow
import pandas
import datetime
from unittest.mock import MagicMock, patch

from databricks.sql.client import ResultSet, Connection, ExecuteResponse
from databricks.sql.types import Row
from databricks.sql.utils import ArrowQueue


@pytest.fixture
def mock_connection():
    conn = MagicMock(spec=Connection)
    conn.disable_pandas = False
    conn._arrow_pandas_type_override = {}
    conn._arrow_to_pandas_kwargs = {}
    if not hasattr(conn, '_arrow_to_pandas_kwargs'):
        conn._arrow_to_pandas_kwargs = {}
    return conn

@pytest.fixture
def mock_thrift_backend(sample_arrow_table):
    tb = MagicMock()
    empty_arrays = [pyarrow.array([], type=field.type) for field in sample_arrow_table.schema]
    empty_table = pyarrow.Table.from_arrays(empty_arrays, schema=sample_arrow_table.schema)
    tb.fetch_results.return_value = (ArrowQueue(empty_table, 0) , False)
    return tb

@pytest.fixture
def mock_raw_execute_response():
    er = MagicMock(spec=ExecuteResponse)
    er.description = [("col_int", "int", None, None, None, None, None),
                      ("col_str", "string", None, None, None, None, None)]
    er.arrow_schema_bytes = None
    er.arrow_queue = None
    er.has_more_rows = False
    er.lz4_compressed = False
    er.command_handle = MagicMock()
    er.status = MagicMock()
    er.has_been_closed_server_side = False
    er.is_staging_operation = False
    return er

@pytest.fixture
def sample_arrow_table():
    data = [
        pyarrow.array([1, 2, 3], type=pyarrow.int32()),
        pyarrow.array(["a", "b", "c"], type=pyarrow.string())
    ]
    schema = pyarrow.schema([
        ('col_int', pyarrow.int32()),
        ('col_str', pyarrow.string())
    ])
    return pyarrow.Table.from_arrays(data, schema=schema)


def test_convert_arrow_table_default(mock_connection, mock_thrift_backend, mock_raw_execute_response, sample_arrow_table):
    mock_raw_execute_response.arrow_queue = ArrowQueue(sample_arrow_table, sample_arrow_table.num_rows)
    rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result_one = rs.fetchone()
    assert isinstance(result_one, Row)
    assert result_one.col_int == 1
    assert result_one.col_str == "a"
    mock_raw_execute_response.arrow_queue = ArrowQueue(sample_arrow_table, sample_arrow_table.num_rows)
    rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result_all = rs.fetchall()
    assert len(result_all) == 3
    assert isinstance(result_all[0], Row)
    assert result_all[0].col_int == 1
    assert result_all[1].col_str == "b"


def test_convert_arrow_table_disable_pandas(mock_connection, mock_thrift_backend, mock_raw_execute_response, sample_arrow_table):
    mock_connection.disable_pandas = True
    mock_raw_execute_response.arrow_queue = ArrowQueue(sample_arrow_table, sample_arrow_table.num_rows)
    rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result = rs.fetchall()
    assert len(result) == 3
    assert isinstance(result[0], Row)
    assert result[0].col_int == 1
    assert result[0].col_str == "a"
    assert isinstance(sample_arrow_table.column(0)[0].as_py(), int)
    assert isinstance(sample_arrow_table.column(1)[0].as_py(), str)


def test_convert_arrow_table_type_override(mock_connection, mock_thrift_backend, mock_raw_execute_response, sample_arrow_table):
    mock_connection._arrow_pandas_type_override = {pyarrow.int32(): pandas.Float64Dtype()}
    mock_raw_execute_response.arrow_queue = ArrowQueue(sample_arrow_table, sample_arrow_table.num_rows)
    rs = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result = rs.fetchall()
    assert len(result) == 3
    assert isinstance(result[0].col_int, float)
    assert result[0].col_int == 1.0
    assert result[0].col_str == "a"


def test_convert_arrow_table_to_pandas_kwargs(mock_connection, mock_thrift_backend, mock_raw_execute_response):
    dt_obj = datetime.datetime(2021, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    ts_array = pyarrow.array([dt_obj], type=pyarrow.timestamp('us', tz='UTC'))
    ts_schema = pyarrow.schema([('col_ts', pyarrow.timestamp('us', tz='UTC'))])
    ts_table = pyarrow.Table.from_arrays([ts_array], schema=ts_schema)

    mock_raw_execute_response.description = [("col_ts", "timestamp", None, None, None, None, None)]
    mock_raw_execute_response.arrow_queue = ArrowQueue(ts_table, ts_table.num_rows)

    # Scenario 1: timestamp_as_object = True. Observed as datetime.datetime in Row.
    mock_connection._arrow_to_pandas_kwargs = {"timestamp_as_object": True}
    rs_ts_true = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result_true = rs_ts_true.fetchall()
    assert len(result_true) == 1
    assert isinstance(result_true[0].col_ts, datetime.datetime)

    # Scenario 2: timestamp_as_object = False. Observed as pandas.Timestamp in Row for this input.
    mock_raw_execute_response.arrow_queue = ArrowQueue(ts_table, ts_table.num_rows)
    mock_connection._arrow_to_pandas_kwargs = {"timestamp_as_object": False}
    rs_ts_false = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result_false = rs_ts_false.fetchall()
    assert len(result_false) == 1
    assert isinstance(result_false[0].col_ts, pandas.Timestamp)

    # Scenario 3: no override. Observed as datetime.datetime in Row since timestamp_as_object is True by default.
    mock_raw_execute_response.arrow_queue = ArrowQueue(ts_table, ts_table.num_rows)
    mock_connection._arrow_to_pandas_kwargs = {}
    rs_ts_true = ResultSet(mock_connection, mock_raw_execute_response, mock_thrift_backend)
    result_true = rs_ts_true.fetchall()
    assert len(result_true) == 1
    assert isinstance(result_true[0].col_ts, datetime.datetime)
