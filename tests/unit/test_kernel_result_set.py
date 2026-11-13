"""Unit tests for ``KernelResultSet`` — the buffer behavior +
close() semantics. Uses a fake kernel handle so tests run with no
network and no Rust extension dependency."""

from __future__ import annotations

import datetime
from collections import deque
from decimal import Decimal
from typing import Deque
from unittest.mock import MagicMock, patch

import numpy
import pytest

# pyarrow is an optional connector dep; the default-deps CI test
# job runs without it. KernelResultSet imports pyarrow eagerly,
# so the whole module must skip when pyarrow is unavailable.
pa = pytest.importorskip("pyarrow")

from databricks.sql.backend.kernel import result_set as kernel_result_set
from databricks.sql.backend.kernel.result_set import KernelResultSet
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.result_set import ResultSet


class _FakeKernelHandle:
    """Stand-in for ``databricks_sql_kernel.ExecutedStatement`` /
    ``ResultStream``. Emits a configured list of ``RecordBatch``es
    via ``fetch_next_batch`` and then returns ``None``."""

    def __init__(self, schema: pa.Schema, batches):
        self._schema = schema
        self._batches: Deque[pa.RecordBatch] = deque(batches)
        self.closed = False

    def arrow_schema(self) -> pa.Schema:
        return self._schema

    def fetch_next_batch(self):
        if self.closed:
            raise RuntimeError("fetched after close")
        if not self._batches:
            return None
        return self._batches.popleft()

    def close(self):
        self.closed = True


def _make_rs(handle, *, disable_pandas=False) -> KernelResultSet:
    # The base ResultSet __init__ takes a `connection` ref it never
    # actually dereferences during these buffer tests, so a Mock is
    # fine.
    connection = MagicMock()
    connection.disable_pandas = disable_pandas
    backend = MagicMock()
    return KernelResultSet(
        connection=connection,
        backend=backend,
        kernel_handle=handle,
        command_id=CommandId.from_sea_statement_id("smoke-test"),
        arraysize=100,
        buffer_size_bytes=1024,
    )


def _batch(schema: pa.Schema, values) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(values, type=schema.field(0).type)], schema=schema
    )


# Renamed from `schema` -> `int_schema` because the connector's
# top-level conftest.py defines a session-scoped `schema` fixture
# for E2E tests; pytest's fixture-resolution complains about
# scope-mismatch if we shadow it with a function-scoped one here.
@pytest.fixture
def int_schema():
    return pa.schema([("n", pa.int64())])


def test_description_built_from_kernel_schema(int_schema):
    handle = _FakeKernelHandle(int_schema, [])
    rs = _make_rs(handle)
    # `null_ok` slot is always `None` to match the Thrift backend's
    # PEP-249 description shape.
    assert rs.description == [("n", "bigint", None, None, None, None, None)]


def test_fetchall_arrow_drains_all_batches(int_schema):
    handle = _FakeKernelHandle(
        int_schema, [_batch(int_schema, [1, 2]), _batch(int_schema, [3, 4, 5])]
    )
    rs = _make_rs(handle)
    table = rs.fetchall_arrow()
    assert table.num_rows == 5
    assert table.column(0).to_pylist() == [1, 2, 3, 4, 5]
    assert rs.status == CommandState.SUCCEEDED
    assert rs.has_more_rows is False


@pytest.mark.parametrize("disable_pandas", [False, True])
def test_geospatial_string_and_binary_values_keep_logical_type(disable_pandas):
    wkb = bytes.fromhex("0101000000000000000000F03F0000000000000040")
    geo_metadata = {
        b"databricks.type_name": b"GEOMETRY",
        b"databricks.type_text": b"GEOMETRY(ANY)",
    }

    string_schema = pa.schema([pa.field("g", pa.string(), metadata=geo_metadata)])
    string_batch = pa.RecordBatch.from_arrays(
        [pa.array(["SRID=4326;POINT(1 2)", None], type=pa.string())],
        schema=string_schema,
    )
    string_rows = _make_rs(
        _FakeKernelHandle(string_schema, [string_batch]),
        disable_pandas=disable_pandas,
    ).fetchall()
    assert [row[0] for row in string_rows] == ["SRID=4326;POINT(1 2)", None]

    binary_type = pa.struct(
        [
            pa.field("srid", pa.int32(), nullable=False),
            pa.field("wkb", pa.binary(), nullable=False),
        ]
    )
    binary_schema = pa.schema([pa.field("g", binary_type, metadata=geo_metadata)])
    binary_batch = pa.RecordBatch.from_arrays(
        [pa.array([{"srid": 4326, "wkb": wkb}, None], type=binary_type)],
        schema=binary_schema,
    )
    binary_rs = _make_rs(
        _FakeKernelHandle(binary_schema, [binary_batch]),
        disable_pandas=disable_pandas,
    )
    assert binary_rs.description[0][1] == "geometry"
    binary_rows = binary_rs.fetchall()
    assert isinstance(binary_rows[0][0], dict)
    assert isinstance(binary_rows[0][0]["srid"], int)
    assert isinstance(binary_rows[0][0]["wkb"], bytes)
    assert binary_rows[0][0] == {"srid": 4326, "wkb": wkb}
    assert binary_rows[1][0] is None


def test_fetchmany_arrow_slices_within_batch(int_schema):
    handle = _FakeKernelHandle(int_schema, [_batch(int_schema, [10, 20, 30, 40])])
    rs = _make_rs(handle)
    t1 = rs.fetchmany_arrow(2)
    assert t1.num_rows == 2 and t1.column(0).to_pylist() == [10, 20]
    t2 = rs.fetchmany_arrow(2)
    assert t2.num_rows == 2 and t2.column(0).to_pylist() == [30, 40]
    t3 = rs.fetchmany_arrow(2)
    assert t3.num_rows == 0


def test_fetchmany_arrow_spans_batch_boundary(int_schema):
    handle = _FakeKernelHandle(
        int_schema,
        [
            _batch(int_schema, [1, 2]),
            _batch(int_schema, [3, 4]),
            _batch(int_schema, [5, 6]),
        ],
    )
    rs = _make_rs(handle)
    t = rs.fetchmany_arrow(5)
    assert t.num_rows == 5
    assert t.column(0).to_pylist() == [1, 2, 3, 4, 5]
    t = rs.fetchmany_arrow(2)
    assert t.column(0).to_pylist() == [6]


def test_fetchone_returns_row_then_none(int_schema):
    handle = _FakeKernelHandle(int_schema, [_batch(int_schema, [42])])
    rs = _make_rs(handle)
    row = rs.fetchone()
    assert row is not None
    assert row[0] == 42
    assert rs.fetchone() is None


def test_fetchall_rows(int_schema):
    handle = _FakeKernelHandle(
        int_schema, [_batch(int_schema, [1, 2]), _batch(int_schema, [3])]
    )
    rs = _make_rs(handle)
    rows = rs.fetchall()
    assert [r[0] for r in rows] == [1, 2, 3]


def test_fetchmany_negative_raises(int_schema):
    rs = _make_rs(_FakeKernelHandle(int_schema, []))
    with pytest.raises(ValueError):
        rs.fetchmany(-1)
    with pytest.raises(ValueError):
        rs.fetchmany_arrow(-1)


def test_close_is_idempotent_and_calls_handle(int_schema):
    handle = _FakeKernelHandle(int_schema, [_batch(int_schema, [1])])
    rs = _make_rs(handle)
    rs.close()
    assert handle.closed is True
    assert rs.status == CommandState.CLOSED
    rs.close()  # second call is a no-op (kernel handle is None)


def test_empty_stream(int_schema):
    rs = _make_rs(_FakeKernelHandle(int_schema, []))
    assert rs.fetchone() is None
    assert rs.fetchall_arrow().num_rows == 0
    assert rs.status == CommandState.SUCCEEDED


def test_close_swallows_handle_close_failures(int_schema):
    """ResultSet.close() must not raise even if the kernel
    handle's close() fails — PEP 249 discourages exceptions from
    close paths (cursor/connection teardown depends on it)."""
    handle = _FakeKernelHandle(int_schema, [])
    handle.close = MagicMock(side_effect=RuntimeError("kernel boom"))
    rs = _make_rs(handle)
    rs.close()  # must not raise
    assert rs.status == CommandState.CLOSED


def test_close_skips_kernel_call_when_connection_already_closed(int_schema):
    """``__del__``-driven close arriving after the parent connection
    is already closed must not issue any kernel call (the kernel
    session is disposed). The result set still marks itself
    ``CLOSED`` locally so the close path stays idempotent."""
    handle = _FakeKernelHandle(int_schema, [])
    connection = MagicMock()
    connection.open = False
    backend = MagicMock()
    rs = KernelResultSet(
        connection=connection,
        backend=backend,
        kernel_handle=handle,
        command_id=CommandId.from_sea_statement_id("conn-closed-test"),
        arraysize=100,
        buffer_size_bytes=1024,
    )
    rs.close()
    # No kernel-side calls fired:
    assert handle.closed is False
    assert backend.close_command.called is False
    # …but local state is still terminal so __del__ is idempotent:
    assert rs.status == CommandState.CLOSED
    assert rs.has_been_closed_server_side is True


# ----- small results converted without pandas -----

_UTC = datetime.timezone.utc


def _columns_skipping_pandas():
    return {
        "i8": pa.array([1, None, -128, 127], pa.int8()),
        "i16": pa.array([1, None, -(2**15), 2**15 - 1], pa.int16()),
        "i32": pa.array([1, None, -(2**31), 2**31 - 1], pa.int32()),
        "i64": pa.array([1, None, -(2**63), 2**63 - 1], pa.int64()),
        "u8": pa.array([1, None, 0, 255], pa.uint8()),
        "u64": pa.array([1, None, 0, 2**64 - 1], pa.uint64()),
        "bool": pa.array([True, None, False, True]),
        "f32": pa.array([0.1, None, float("inf"), -0.0], pa.float32()),
        "f64": pa.array([0.1, None, float("-inf"), -0.0], pa.float64()),
        "str": pa.array(["a", None, "", "é\U0001f600"]),
        "large_str": pa.array(["a", None, "", "z"], pa.large_string()),
        "bin": pa.array([b"a", None, b"", b"\x00\xff"]),
        "large_bin": pa.array([b"a", None, b"", b"\x00"], pa.large_binary()),
        "fixed_bin": pa.array([b"ab", None, b"cd", b"\x00\x00"], pa.binary(2)),
        "dec": pa.array(
            [Decimal("123141078.2283"), None, Decimal("-0.0001"), Decimal("0.0000")],
            pa.decimal128(38, 4),
        ),
        "dec256": pa.array(
            [Decimal("1.5"), None, Decimal("-2.5"), Decimal("0.0")],
            pa.decimal256(40, 1),
        ),
        "date": pa.array(
            [
                datetime.date(2024, 1, 1),
                None,
                datetime.date(1, 1, 1),
                datetime.date(9999, 12, 31),
            ]
        ),
        "date64": pa.array(
            [datetime.date(2024, 2, 29), None, datetime.date(1970, 1, 1), None],
            pa.date64(),
        ),
        "ts": pa.array(
            [
                datetime.datetime(2024, 1, 1, 1, 2, 3, 4),
                None,
                datetime.datetime(1, 1, 1),
                datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            ],
            pa.timestamp("us"),
        ),
        "ts_ms": pa.array([0, None, 1, 2], pa.timestamp("ms")),
        "ts_utc": pa.array(
            [
                datetime.datetime(2024, 1, 1, tzinfo=_UTC),
                None,
                datetime.datetime(1, 1, 1, tzinfo=_UTC),
                datetime.datetime(2024, 3, 10, 10, tzinfo=_UTC),
            ],
            pa.timestamp("us", tz="Etc/UTC"),
        ),
        # Includes the instant when DST starts.
        "ts_la": pa.array(
            [0, None, 1710064800000000, 1699174800000000],
            pa.timestamp("us", tz="America/Los_Angeles"),
        ),
        "ts_offset": pa.array([0, None, 1, 2], pa.timestamp("us", tz="+05:30")),
        "time": pa.array([0, None, 1, 86399999999], pa.time64("us")),
        "time32": pa.array([0, None, 1, 2], pa.time32("ms")),
        "null": pa.nulls(4),
    }


def _columns_needing_pandas():
    return {
        # pandas maps NaN to None or keeps it, depending on its version.
        "f64_nan": pa.array([float("nan"), None, 1.0, 2.0]),
        "f32_nan": pa.array([1.0, float("nan"), None, 2.0], pa.float32()),
        "ts_ns": pa.array([1, None, 1000, 2], pa.timestamp("ns")),
        "time_ns": pa.array([1000, None, 2000, 0], pa.time64("ns")),
        "duration": pa.array([0, None, 1, 2], pa.duration("us")),
        "interval": pa.array(
            [pa.MonthDayNano([1, 2, 3]), None, pa.MonthDayNano([0, 0, 0]), None],
            pa.month_day_nano_interval(),
        ),
        "list": pa.array([[1, 2], None, [], [None]], pa.list_(pa.int64())),
        "struct": pa.array(
            [{"x": 1}, None, {"x": None}, {"x": 2}], pa.struct([("x", pa.int64())])
        ),
        "map": pa.array(
            [[("a", 1)], None, [], [("b", None)]], pa.map_(pa.string(), pa.int64())
        ),
        "dictionary": pa.array(["a", None, "a", "b"]).dictionary_encode(),
        "f16": pa.array(
            numpy.array([1, 0, 2, 3], numpy.float16),
            mask=numpy.array([False, True, False, False]),
        ),
    }


def _assert_same_rows(rows, expected):
    assert len(rows) == len(expected)
    for row, expected_row in zip(rows, expected):
        assert row.__fields__ == expected_row.__fields__
        assert len(row) == len(expected_row)
        for value, expected_value in zip(row, expected_row):
            assert type(value) is type(expected_value)
            assert repr(value) == repr(expected_value)
            if isinstance(value, datetime.datetime):
                assert type(value.tzinfo) is type(expected_value.tzinfo)
                assert value.tzinfo == expected_value.tzinfo


def _convert(table):
    """(rows from KernelResultSet, rows from the pandas path, whether the
    kernel result set used the pandas path)."""
    rs = _make_rs(_FakeKernelHandle(table.schema, []))
    pandas_path = ResultSet._convert_arrow_table
    with patch.object(
        ResultSet, "_convert_arrow_table", autospec=True, side_effect=pandas_path
    ) as spy:
        rows = rs._convert_arrow_table(table)
    return rows, pandas_path(rs, table), spy.called


@pytest.mark.parametrize("name", sorted(_columns_skipping_pandas()))
def test_small_results_skip_pandas_with_the_same_values(name):
    column = _columns_skipping_pandas()[name]
    tables = [
        pa.table({name: column}),
        pa.table({name: column.slice(0, 1)}),
        pa.table({name: column.slice(1, 1)}),
        # Chunked, next to other columns, with a duplicate column name.
        pa.table(
            [
                pa.chunked_array([[1, 2], [3, 4]]),
                pa.chunked_array([column.slice(0, 1), column.slice(1)]),
                pa.array(["a", "b", None, "d"]),
            ],
            names=["k", name, "k"],
        ),
    ]
    for table in tables:
        rows, expected, used_pandas = _convert(table)
        assert not used_pandas
        _assert_same_rows(rows, expected)


@pytest.mark.parametrize("name", sorted(_columns_needing_pandas()))
def test_results_whose_values_could_differ_use_pandas(name):
    table = pa.table(
        {"k": pa.array([1, 2, 3, 4]), name: _columns_needing_pandas()[name]}
    )
    rows, expected, used_pandas = _convert(table)
    assert used_pandas
    assert len(rows) == len(expected) == 4


def test_large_results_use_pandas():
    limit = kernel_result_set._MAX_ROWS_WITHOUT_PANDAS
    for num_rows, expect_pandas in ((limit, False), (limit + 1, True)):
        rows, expected, used_pandas = _convert(pa.table({"n": list(range(num_rows))}))
        assert used_pandas is expect_pandas
        _assert_same_rows(rows, expected)


def test_tz_aware_timestamps_skip_pandas_only_in_tiny_results():
    limit = kernel_result_set._MAX_ROWS_WITHOUT_PANDAS_TZ
    for tz, num_rows, expect_pandas in (
        ("Etc/UTC", limit, False),
        ("Etc/UTC", limit + 1, True),
        (None, limit + 1, False),
    ):
        column = pa.array(range(num_rows), pa.timestamp("us", tz=tz))
        rows, expected, used_pandas = _convert(pa.table({"t": column}))
        assert used_pandas is expect_pandas
        _assert_same_rows(rows, expected)


def test_empty_results_skip_pandas():
    rows, expected, used_pandas = _convert(pa.table({"n": pa.array([], pa.int64())}))
    assert rows == expected == []
    assert not used_pandas


def test_fetch_methods_return_the_pandas_path_values():
    schema = pa.schema([("d", pa.decimal128(38, 4)), ("t", pa.timestamp("us", "UTC"))])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                [Decimal("1.5000"), None, Decimal("-2.0001")], schema.field(0).type
            ),
            pa.array([0, 1, None], schema.field(1).type),
        ],
        schema=schema,
    )
    expected = ResultSet._convert_arrow_table(
        _make_rs(_FakeKernelHandle(schema, [])), pa.Table.from_batches([batch])
    )
    _assert_same_rows(_make_rs(_FakeKernelHandle(schema, [batch])).fetchall(), expected)
    rs = _make_rs(_FakeKernelHandle(schema, [batch]))
    _assert_same_rows([rs.fetchone()] + rs.fetchmany(5), expected)
