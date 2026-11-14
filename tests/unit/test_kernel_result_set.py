"""Unit tests for ``KernelResultSet`` — the buffer behavior +
close() semantics. Uses a fake kernel handle so tests run with no
network and no Rust extension dependency."""

from __future__ import annotations

import datetime
import inspect
import logging
import math
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
from databricks.sql.exc import OperationalError
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


def _make_rs(handle, *, disable_pandas=False, schema_cache=None) -> KernelResultSet:
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
        schema_cache=schema_cache,
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
        "u16": pa.array([1, None, 0, 2**16 - 1], pa.uint16()),
        "u32": pa.array([1, None, 0, 2**32 - 1], pa.uint32()),
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
        "ts": pa.array(
            [
                datetime.datetime(2024, 1, 1, 1, 2, 3, 4),
                None,
                datetime.datetime(1, 1, 1),
                datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            ],
            pa.timestamp("us"),
        ),
        "ts_s": pa.array([0, None, -1, 2], pa.timestamp("s")),
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
        "null": pa.nulls(4),
    }


def test_every_type_skipping_pandas_has_a_column_above():
    covered = {column.type.id for column in _columns_skipping_pandas().values()}
    assert covered == set(kernel_result_set._VALUE_WEIGHTS)


def _columns_needing_pandas():
    return {
        # pandas maps NaN to None or keeps it, depending on its version.
        "f64_nan": pa.array([float("nan"), None, 1.0, 2.0]),
        "f32_nan": pa.array([1.0, float("nan"), None, 2.0], pa.float32()),
        "ts_ns": pa.array([1, None, 1000, 2], pa.timestamp("ns")),
        # Out-of-range values convert differently; see
        # test_out_of_range_values_convert_as_on_the_pandas_path.
        "date64": pa.array([0, None, 86_400_000, -1], pa.date64()),
        "time32_s": pa.array([0, None, 1, 2], pa.time32("s")),
        "time32_ms": pa.array([0, None, 1, 2], pa.time32("ms")),
        "time64": pa.array([0, None, 1, 86_399_999_999], pa.time64("us")),
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
        rows = rs._rows(table.to_batches())
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
    # Values only; the cost budget is tested separately.
    with patch.object(kernel_result_set, "_MAX_COST_WITHOUT_PANDAS", 10**9):
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


@pytest.mark.parametrize(
    "types",
    [
        [pa.int64()],
        [pa.float64()] * 16,
        [pa.string()] * 3,
        [pa.decimal128(38, 4), pa.int32(), pa.string(), pa.date32()],
        [pa.timestamp("us", "Etc/UTC")],
        [pa.timestamp("us"), pa.binary()] * 50,
    ],
    ids=["int", "16_doubles", "3_strings", "mixed", "tz_timestamp", "wide"],
)
def test_results_over_the_cost_budget_use_pandas(types):
    limit = kernel_result_set._max_rows_without_pandas(types)
    assert 0 < limit <= kernel_result_set._MAX_ROWS_WITHOUT_PANDAS
    for num_rows, expect_pandas in ((limit, False), (limit + 1, True)):
        table = pa.table(
            [pa.nulls(num_rows, t) for t in types],
            names=[f"c{i}" for i in range(len(types))],
        )
        rows, expected, used_pandas = _convert(table)
        assert used_pandas is expect_pandas
        _assert_same_rows(rows, expected)


def test_cost_budget_scales_with_width_and_type():
    max_rows = kernel_result_set._max_rows_without_pandas
    assert max_rows([pa.int64()]) == kernel_result_set._MAX_ROWS_WITHOUT_PANDAS
    assert max_rows([pa.int64()] * 64) < max_rows([pa.int64()] * 8)
    assert max_rows([pa.date32()] * 8) < max_rows([pa.int64()] * 8)
    assert max_rows([pa.timestamp("us", "UTC")]) < max_rows([pa.timestamp("us")])
    # Too costly for even one row; only empty results skip pandas.
    assert max_rows([pa.timestamp("us", "UTC")] * 3) == 0
    assert max_rows([]) == -1
    assert max_rows([pa.int64(), pa.list_(pa.int64())]) == -1
    assert max_rows([pa.timestamp("ns")]) == -1


@pytest.mark.parametrize(
    "column, expected",
    [
        # Before 1970 and not whole days: to_pylist() rounds down.
        (pa.array([-1], pa.date64()), [datetime.date(1970, 1, 1)]),
        # Outside 0-24h: to_pylist() wraps around.
        (pa.array([86_400], pa.time32("s")), ValueError),
        (pa.array([-1], pa.time64("us")), ValueError),
        # Past datetime's range: to_pylist() raises OverflowError instead.
        (pa.array([2_932_897], pa.date32()), ValueError),
        (pa.array([253_402_300_800_000_000], pa.timestamp("us", "UTC")), ValueError),
    ],
    ids=["date64", "time32", "time64", "date32", "timestamp"],
)
def test_out_of_range_values_convert_as_on_the_pandas_path(column, expected):
    table = pa.table({"c": column})
    rs = _make_rs(_FakeKernelHandle(table.schema, []))
    for convert in (
        lambda t: rs._rows(t.to_batches()),
        lambda t: ResultSet._convert_arrow_table(rs, t),
    ):
        if isinstance(expected, list):
            assert [row[0] for row in convert(table)] == expected
        else:
            with pytest.raises(expected):
                convert(table)


def test_conversion_errors_are_logged_before_falling_back(caplog):
    table = pa.table({"c": pa.array([2_932_897], pa.date32())})
    rs = _make_rs(_FakeKernelHandle(table.schema, []))
    with caplog.at_level(logging.DEBUG, logger=kernel_result_set.__name__):
        with pytest.raises(ValueError):
            rs._rows(table.to_batches())
    assert "to_pylist() failed for a date32[day] column" in caplog.text


def test_empty_results_skip_pandas():
    rows, expected, used_pandas = _convert(pa.table({"n": pa.array([], pa.int64())}))
    assert rows == expected == []
    assert not used_pandas


def test_fetch_methods_return_the_pandas_path_values():
    schema = pa.schema([("d", pa.decimal128(38, 4)), ("t", pa.timestamp("us"))])
    assert kernel_result_set._max_rows_without_pandas(schema.types) >= 3
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


# ----- per-connection schema cache -----


def _reimported(schema: pa.Schema) -> pa.Schema:
    """An equal but distinct schema object, as each kernel statement returns."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    return pa.ipc.open_stream(sink.getvalue()).schema


def test_equal_schemas_reuse_what_is_derived_from_them():
    schema = pa.schema(
        [
            pa.field("n", pa.int64()),
            pa.field("v", pa.string(), metadata={b"databricks.type_name": b"VARIANT"}),
        ]
    )
    cache = kernel_result_set._SchemaCache()
    first = _make_rs(_FakeKernelHandle(_reimported(schema), []), schema_cache=cache)
    second = _make_rs(_FakeKernelHandle(_reimported(schema), []), schema_cache=cache)
    assert first._result_schema is second._result_schema
    assert (
        first.description
        == second.description
        == _make_rs(_FakeKernelHandle(schema, [])).description
    )
    assert [c[1] for c in first.description] == ["bigint", "variant"]
    # Each result set still gets its own description list.
    first.description.append("changed")
    assert len(second.description) == 2

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array(['{"a":1}', None])], schema=schema
    )
    rows = _make_rs(_FakeKernelHandle(schema, [batch]), schema_cache=cache).fetchall()
    assert [tuple(r) for r in rows] == [(1, '{"a":1}'), (2, None)]
    assert rows[0].v == '{"a":1}'


@pytest.mark.parametrize(
    "other",
    [
        pa.schema([pa.field("n", pa.int32())]),
        pa.schema([pa.field("m", pa.int64())]),
        pa.schema(
            [pa.field("n", pa.int64(), metadata={b"databricks.type_name": b"MAP"})]
        ),
    ],
)
def test_a_different_schema_is_derived_again(other):
    cache = kernel_result_set._SchemaCache()
    first = _make_rs(
        _FakeKernelHandle(pa.schema([pa.field("n", pa.int64())]), []),
        schema_cache=cache,
    )
    second = _make_rs(_FakeKernelHandle(other, []), schema_cache=cache)
    assert first._result_schema is not second._result_schema
    assert second.description == _make_rs(_FakeKernelHandle(other, [])).description
    assert first.description != second.description


# ----- rows built from record batches -----


def _spy_to_table():
    return patch.object(
        KernelResultSet,
        "_to_table",
        autospec=True,
        side_effect=KernelResultSet._to_table,
    )


def test_small_results_are_converted_from_batches_without_a_table():
    schema = pa.schema([("n", pa.int64()), ("s", pa.string())])

    def batch(lo, hi):
        return pa.RecordBatch.from_arrays(
            [pa.array(range(lo, hi)), pa.array([str(i) for i in range(lo, hi)])],
            schema=schema,
        )

    expected = [(i, str(i)) for i in range(7)]
    with _spy_to_table() as to_table:
        rs = _make_rs(
            _FakeKernelHandle(schema, [batch(0, 3), batch(3, 5), batch(5, 7)])
        )
        first = rs.fetchone()
        rows = [first] + rs.fetchmany(3) + rs.fetchall()
    to_table.assert_not_called()
    assert [tuple(r) for r in rows] == expected
    assert rows[-1].s == "6" and rs.rownumber == 7

    limit = kernel_result_set._max_rows_without_pandas(schema.types)
    with _spy_to_table() as to_table:
        rs = _make_rs(
            _FakeKernelHandle(schema, [batch(0, limit), batch(limit, limit + 1)])
        )
        assert len(rs.fetchall()) == limit + 1
    to_table.assert_called_once()


@pytest.mark.parametrize("handle_type", ["_FakeKernelHandle", "_FakeRowsHandle"])
def test_a_batch_whose_schema_differs_still_fails_as_before(handle_type):
    schema = pa.schema([("n", pa.int64())])
    other = pa.RecordBatch.from_arrays([pa.array(["x"])], names=["n"])
    handle = globals()[handle_type](schema, [other])
    with pytest.raises(pa.ArrowInvalid):
        _make_rs(handle).fetchall()


def test_the_cost_budget_is_computed_once_per_schema_not_per_fetch():
    schema = pa.schema([("n", pa.int64())])
    cache = kernel_result_set._SchemaCache()
    with patch.object(
        kernel_result_set,
        "_max_rows_without_pandas",
        wraps=kernel_result_set._max_rows_without_pandas,
    ) as budget:
        for _ in range(3):
            rs = _make_rs(
                _FakeKernelHandle(
                    _reimported(schema),
                    [_batch(schema, [1, 2]), _batch(schema, [3, 4])],
                ),
                schema_cache=cache,
            )
            rows = [rs.fetchone()] + rs.fetchmany(2) + rs.fetchall()
            assert [row[0] for row in rows] == [1, 2, 3, 4]
        assert budget.call_count == 1
        _make_rs(
            _FakeKernelHandle(pa.schema([("m", pa.int64())]), []), schema_cache=cache
        )
        assert budget.call_count == 2


# ----- rows built by the kernel (schema_key / fetch_all_rows) -----


class _FakeSchemaKey:
    def __init__(self, schema: pa.Schema):
        self.schema = schema

    def __eq__(self, other):
        return isinstance(other, _FakeSchemaKey) and self.schema.equals(
            other.schema, check_metadata=True
        )


class _FakeRowsHandle(_FakeKernelHandle):
    """A handle from a kernel that has ``schema_key`` and ``fetch_all_rows``,
    which returns the rows as ``to_pylist()`` values when every remaining
    row fits in ``max_rows``, and otherwise keeps the batches it pulled for
    ``fetch_next_batch``, as the kernel does."""

    def __init__(self, schema, batches, convertible=True):
        super().__init__(schema, batches)
        self._pending: Deque[pa.RecordBatch] = deque()
        self._convertible = convertible
        self.schema_calls = 0
        self.rows_calls = []

    def arrow_schema(self):
        self.schema_calls += 1
        return super().arrow_schema()

    def schema_key(self):
        return _FakeSchemaKey(self._schema)

    def fetch_all_rows(self, max_rows, nan_ok=False):
        self.rows_calls.append((max_rows, nan_ok))
        while sum(b.num_rows for b in self._pending) <= max_rows and self._batches:
            self._pending.append(self._batches.popleft())
        if self._batches or not self._convertible:
            return None
        if sum(b.num_rows for b in self._pending) > max_rows:
            return None
        # The kernel declines batches whose column types differ.
        if any(b.schema.types != self._schema.types for b in self._pending):
            return None
        columns = [[] for _ in self._schema]
        for batch in self._pending:
            for values, column in zip(columns, batch.columns):
                values.extend(column.to_pylist())
        if not nan_ok and any(
            isinstance(v, float) and v != v for values in columns for v in values
        ):
            return None
        self._pending.clear()
        return list(zip(*columns))

    def fetch_next_batch(self):
        if self._pending:
            return self._pending.popleft()
        return super().fetch_next_batch()


_ROWS_SCHEMA = pa.schema(
    [
        pa.field("n", pa.int64(), metadata={b"databricks.type_name": b"BIGINT"}),
        pa.field("d", pa.decimal128(10, 2)),
        pa.field("t", pa.timestamp("us")),
        pa.field("s", pa.string()),
    ]
)


def _rows_batch(lo, hi, schema=_ROWS_SCHEMA):
    return pa.RecordBatch.from_arrays(
        [
            pa.array(range(lo, hi), schema.field(0).type),
            pa.array(
                [Decimal(i).scaleb(-2) for i in range(lo, hi)], schema.field(1).type
            ),
            pa.array([i * 3_600_000_000 for i in range(lo, hi)], schema.field(2).type),
            pa.array([None if i % 2 else str(i) for i in range(lo, hi)]),
        ],
        schema=schema,
    )


def _expected_rows(*batches):
    table = pa.Table.from_batches(batches)
    rs = _make_rs(_FakeKernelHandle(table.schema, []))
    return ResultSet._convert_arrow_table(rs, table)


def test_kernel_rows_skip_pyarrow_for_schema_and_values():
    cache = kernel_result_set._SchemaCache()
    first = _FakeRowsHandle(_reimported(_ROWS_SCHEMA), [_rows_batch(0, 3)])
    second = _FakeRowsHandle(_reimported(_ROWS_SCHEMA), [_rows_batch(3, 5)])
    with patch.object(_FakeRowsHandle, "fetch_next_batch") as next_batch:
        rows = _make_rs(first, schema_cache=cache).fetchall()
        rs = _make_rs(second, schema_cache=cache)
        assert (
            rs.description == _make_rs(_FakeKernelHandle(_ROWS_SCHEMA, [])).description
        )
        rows += rs.fetchall()
    next_batch.assert_not_called()
    # The second statement's schema came from the cache, by key.
    assert (first.schema_calls, second.schema_calls) == (1, 0)
    _assert_same_rows(rows, _expected_rows(_rows_batch(0, 3), _rows_batch(3, 5)))
    assert rows[0].n == 0 and rs.rownumber == 2
    assert rs.status == CommandState.SUCCEEDED and not rs.has_more_rows


def test_kernel_rows_with_a_time_zone_stay_within_the_budget():
    schema = pa.schema([("t", pa.timestamp("us", tz="America/Los_Angeles"))])
    budget = kernel_result_set._max_rows_without_pandas(schema.types)

    def batch(n):
        return pa.RecordBatch.from_arrays(
            [
                pa.array(
                    range(0, n * 3_600_000_000, 3_600_000_000), schema.field(0).type
                )
            ],
            schema=schema,
        )

    handle = _FakeRowsHandle(schema, [batch(budget)])
    _assert_same_rows(_make_rs(handle).fetchall(), _expected_rows(batch(budget)))
    assert handle.rows_calls == [(budget, False)]


def test_a_different_schema_key_is_derived_again():
    cache = kernel_result_set._SchemaCache()
    _make_rs(_FakeRowsHandle(_ROWS_SCHEMA, []), schema_cache=cache)
    other = pa.schema([f.remove_metadata() for f in _ROWS_SCHEMA])
    handle = _FakeRowsHandle(other, [])
    rs = _make_rs(handle, schema_cache=cache)
    assert handle.schema_calls == 1
    assert rs.description == _make_rs(_FakeKernelHandle(other, [])).description


def test_row_fetches_are_served_from_one_kernel_call():
    handle = _FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 2), _rows_batch(2, 5)])
    rs = _make_rs(handle)
    rows = [rs.fetchone()] + rs.fetchmany(2) + rs.fetchmany(0) + rs.fetchall()
    assert rs.fetchone() is None and rs.fetchmany(3) == [] and rs.fetchall() == []
    budget = kernel_result_set._max_rows_without_pandas(_ROWS_SCHEMA.types)
    assert handle.rows_calls == [(budget, False)]
    _assert_same_rows(rows, _expected_rows(_rows_batch(0, 2), _rows_batch(2, 5)))
    assert rs.rownumber == 5
    assert [tuple(r) for r in rs] == []


@pytest.mark.parametrize("disable_pandas", [False, True])
def test_kernel_keeps_nan_only_without_pandas(disable_pandas):
    schema = pa.schema([("f", pa.float64())])
    batch = pa.RecordBatch.from_arrays([pa.array([float("nan"), 1.0])], schema=schema)
    handle = _FakeRowsHandle(schema, [batch])
    rs = _make_rs(handle, disable_pandas=disable_pandas)
    rows = rs.fetchall()
    assert handle.rows_calls[0][1] is disable_pandas
    if disable_pandas:
        # as_py(), as without kernel rows.
        assert isinstance(rows[0][0], float) and math.isnan(rows[0][0])
        assert rows[1][0] == 1.0
    else:
        # The kernel declines NaN, so pandas decides it, as before.
        _assert_same_rows(rows, ResultSet._convert_arrow_table(rs, pa.table(batch)))


def test_results_the_kernel_declines_come_from_the_batches_it_pulled():
    budget = kernel_result_set._max_rows_without_pandas(_ROWS_SCHEMA.types)
    batches = [
        _rows_batch(0, 2),
        _rows_batch(2, budget),
        _rows_batch(budget, budget + 1),
    ]
    expected = _expected_rows(*batches)
    handle = _FakeRowsHandle(_ROWS_SCHEMA, batches)
    rs = _make_rs(handle)
    rows = [rs.fetchone()]
    # Declined after pulling every batch; they are now in the kernel's queue.
    assert len(handle.rows_calls) == 1 and not handle._batches
    rows += rs.fetchmany(3) + rs.fetchall()
    assert len(handle.rows_calls) == 1
    _assert_same_rows(rows, expected)
    assert rs.rownumber == budget + 1
    with _spy_to_table() as to_table:
        handle = _FakeRowsHandle(_ROWS_SCHEMA, batches)
        _assert_same_rows(_make_rs(handle).fetchall(), expected)
    to_table.assert_called_once()


def test_schemas_the_kernel_cannot_convert_never_ask_it_for_rows():
    schema = pa.schema([("l", pa.list_(pa.int64()))])
    batch = pa.RecordBatch.from_arrays([pa.array([[1], None])], schema=schema)
    handle = _FakeRowsHandle(schema, [batch])
    assert [r[0] for r in _make_rs(handle).fetchall()] == [[1], None]
    assert handle.rows_calls == []


def test_unconverted_rows_fall_back_to_the_batches():
    handle = _FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 3)], convertible=False)
    rs = _make_rs(handle)
    _assert_same_rows(rs.fetchall(), _expected_rows(_rows_batch(0, 3)))
    assert len(handle.rows_calls) == 1


@pytest.mark.parametrize(
    "fetch",
    [
        lambda rs: rs.fetchall_arrow(),
        lambda rs: pa.concat_tables(
            [rs.fetchmany_arrow(1), rs.fetchmany_arrow(0), rs.fetchmany_arrow(10)]
        ),
    ],
    ids=["fetchall_arrow", "fetchmany_arrow"],
)
def test_arrow_fetches_after_a_row_fetch_return_the_remaining_rows(fetch):
    rs = _make_rs(_FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 4)]))
    assert rs.fetchone()[0] == 0
    table = fetch(rs)
    assert table.schema.equals(_ROWS_SCHEMA, check_metadata=True)
    assert table.equals(pa.Table.from_batches([_rows_batch(1, 4)]))
    assert rs.fetchall_arrow().num_rows == 0 and rs.rownumber == 4


@pytest.mark.parametrize(
    "name, disable_pandas",
    [(name, False) for name in sorted(_columns_skipping_pandas())]
    + [("f64_nan", True), ("f32_nan", True)],
)
def test_every_kernel_converted_type_round_trips_to_arrow(name, disable_pandas):
    columns = {**_columns_skipping_pandas(), **_columns_needing_pandas()}
    batch = pa.record_batch([pa.array(range(4)), columns[name]], names=["k", name])
    with patch.object(kernel_result_set, "_MAX_COST_WITHOUT_PANDAS", 10**9):
        handle = _FakeRowsHandle(batch.schema, [batch])
        rs = _make_rs(handle, disable_pandas=disable_pandas)
        assert rs.fetchone()[0] == 0
        table = rs.fetchall_arrow()
    assert handle.rows_calls
    expected = pa.Table.from_batches([batch.slice(1)])
    assert table.schema.equals(expected.schema, check_metadata=True)
    # repr: NaN != NaN for Table.equals.
    assert repr(table.to_pylist()) == repr(expected.to_pylist())
    assert table.equals(expected) or disable_pandas


class _KeyOnlyHandle(_FakeKernelHandle):
    def schema_key(self):
        return _FakeSchemaKey(self._schema)


def test_a_failing_schema_key_falls_back_to_the_arrow_schema(caplog):
    handle = _FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 3)])
    handle.schema_key = MagicMock(side_effect=RuntimeError("boom"))
    with caplog.at_level(logging.DEBUG, logger=kernel_result_set.__name__):
        rs = _make_rs(handle, schema_cache=kernel_result_set._SchemaCache())
    assert "schema_key() failed" in caplog.text
    assert rs.description == _make_rs(_FakeKernelHandle(_ROWS_SCHEMA, [])).description
    _assert_same_rows(rs.fetchall(), _expected_rows(_rows_batch(0, 3)))
    assert handle.rows_calls == []


def test_handles_with_schema_keys_but_without_fetch_all_rows_use_the_batches():
    rs = _make_rs(_KeyOnlyHandle(_ROWS_SCHEMA, [_rows_batch(0, 3)]))
    _assert_same_rows(rs.fetchall(), _expected_rows(_rows_batch(0, 3)))


@pytest.mark.parametrize("taken", [1, 3], ids=["mid_batch", "batch_boundary"])
def test_row_fetches_after_an_arrow_fetch_stay_on_the_arrow_path(taken):
    handle = _FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 3), _rows_batch(3, 4)])
    rs = _make_rs(handle)
    assert rs.fetchmany_arrow(taken).num_rows == taken
    rows = rs.fetchall()
    assert handle.rows_calls == []
    _assert_same_rows(rows, _expected_rows(_rows_batch(taken, 4)))


def test_kernel_row_errors_are_mapped():
    handle = _FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 1)])
    handle.fetch_all_rows = MagicMock(side_effect=RuntimeError("boom"))
    with pytest.raises(OperationalError, match="fetch_all_rows"):
        _make_rs(handle).fetchall()


def test_a_fetch_after_a_kernel_row_error_returns_every_row_once():
    batches = [_rows_batch(0, 2), _rows_batch(2, 4), _rows_batch(4, 5)]
    handle = _FakeRowsHandle(_ROWS_SCHEMA, batches)

    def pull_two_then_fail(max_rows, nan_ok=False):
        # As the kernel does when reading fails mid-way: the batches read so
        # far stay queued for fetch_next_batch.
        handle._pending.extend([handle._batches.popleft() for _ in range(2)])
        raise RuntimeError("connection reset")

    handle.fetch_all_rows = pull_two_then_fail
    rs = _make_rs(handle)
    with pytest.raises(OperationalError):
        rs.fetchone()
    rows = [rs.fetchone()] + rs.fetchall()
    _assert_same_rows(rows, _expected_rows(*batches))
    assert rs.rownumber == 5


def test_close_drops_kernel_rows():
    rs = _make_rs(_FakeRowsHandle(_ROWS_SCHEMA, [_rows_batch(0, 3)]))
    assert rs.fetchone()[0] == 0
    rs.close()
    assert rs.fetchall() == []


@pytest.mark.realkernel
class TestKernelRowsMatchThePandasPath:
    """The real kernel's conversion (``_rows_from_arrow`` runs the one behind
    ``fetch_all_rows``) returns the values of the pandas path for every type
    that skips pandas. Skipped for kernels without it."""

    @staticmethod
    def _rows_from_arrow():
        kernel = pytest.importorskip("databricks_sql_kernel")
        if not hasattr(kernel.ExecutedStatement, "fetch_all_rows"):
            pytest.skip("kernel has no fetch_all_rows")
        rows_from_arrow = getattr(kernel, "_rows_from_arrow", None)
        assert rows_from_arrow is not None, "kernel lost its conversion test hook"
        return rows_from_arrow

    @pytest.mark.parametrize("name", sorted(_columns_skipping_pandas()))
    def test_values(self, name):
        rows_from_arrow = self._rows_from_arrow()
        column = _columns_skipping_pandas()[name]
        table = pa.table({"k": pa.array(range(len(column))), name: column})
        rs = _make_rs(_FakeKernelHandle(table.schema, []))
        expected = ResultSet._convert_arrow_table(rs, table)
        values = rows_from_arrow(table.to_batches())
        _assert_same_rows([rs._result_schema.row_factory(*v) for v in values], expected)

    @pytest.mark.parametrize("name", sorted(_columns_needing_pandas()))
    def test_types_needing_pandas_are_declined(self, name):
        rows_from_arrow = self._rows_from_arrow()
        table = pa.table({name: _columns_needing_pandas()[name]})
        assert rows_from_arrow(table.to_batches()) is None

    def test_handles_take_the_arguments_this_module_passes(self):
        self._rows_from_arrow()
        import databricks_sql_kernel as kernel

        for handle_type in (kernel.ExecutedStatement, kernel.ResultStream):
            params = list(
                inspect.signature(handle_type.fetch_all_rows).parameters.values()
            )
            assert [(p.name, p.kind) for p in params[1:]] == [
                ("max_rows", inspect.Parameter.POSITIONAL_OR_KEYWORD),
                ("nan_ok", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ]
            assert callable(handle_type.schema_key)
        schema = pa.schema(
            [pa.field("n", pa.int64(), metadata={b"databricks.type_name": b"BIGINT"})]
        )
        key = kernel.SchemaKey(schema)
        assert key == kernel.SchemaKey(_reimported(schema))
        assert key != kernel.SchemaKey(pa.schema([pa.field("n", pa.int64())]))
        # Keys tell apart everything Schema.equals(check_metadata=True) does.
        for other in (
            schema.field(0).with_nullable(False),
            schema.field(0).with_name("m"),
            schema.field(0).with_type(pa.int32()),
        ):
            assert not schema.equals(pa.schema([other]), check_metadata=True)
            assert key != kernel.SchemaKey(pa.schema([other]))
        # The conversion declines a batch whose column types differ.
        batches = [
            pa.record_batch([pa.array([1])], names=["n"]),
            pa.record_batch([pa.array(["x"])], names=["n"]),
        ]
        assert self._rows_from_arrow()(batches) is None
        cache = kernel_result_set._SchemaCache()
        cache.get(schema, key)
        assert cache.get_by_key(kernel.SchemaKey(_reimported(schema))) is not None
        assert (
            cache.get_by_key(kernel.SchemaKey(pa.schema([("m", pa.int64())]))) is None
        )
