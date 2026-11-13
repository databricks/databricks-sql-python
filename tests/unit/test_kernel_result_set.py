"""Unit tests for ``KernelResultSet`` — the buffer behavior +
close() semantics. Uses a fake kernel handle so tests run with no
network and no Rust extension dependency."""

from __future__ import annotations

from collections import deque
import errno
import io
from typing import Deque
from unittest.mock import MagicMock

import pytest

# pyarrow is an optional connector dep; the default-deps CI test
# job runs without it. KernelResultSet imports pyarrow eagerly,
# so the whole module must skip when pyarrow is unavailable.
pa = pytest.importorskip("pyarrow")

from databricks.sql.backend.kernel.result_set import KernelResultSet
from databricks.sql.backend.types import CommandId, CommandState


class _FakeKernelHandle:
    """Stand-in for ``databricks_sql_kernel.ExecutedStatement`` /
    ``ResultStream``. Emits a configured list of ``RecordBatch``es
    via ``fetch_next_batch`` and then returns ``None``."""

    def __init__(self, schema: pa.Schema, batches):
        self._schema = schema
        self._batches: Deque[pa.RecordBatch] = deque(batches)
        self.closed = False
        self.is_volume_operation = False

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


@pytest.mark.parametrize("data", [b"payload", b""])
def test_volume_reads_input_only_when_native_requests_it(int_schema, data):
    handle = _FakeKernelHandle(int_schema, [_batch(int_schema, [1])])
    handle.is_volume_operation = True
    handle.complete_volume_operation = MagicMock(
        side_effect=["needs_input_stream", "completed"]
    )
    rs = _make_rs(handle)
    stream = io.BytesIO(data)
    rs.complete_staging_operation("/allowed", stream)
    assert stream.tell() == len(data)
    assert handle.complete_volume_operation.call_args_list == [
        ((["/allowed"],), {"input_bytes": None}),
        ((["/allowed"],), {"input_bytes": data}),
    ]
    assert rs.fetchall() == []
    rs.backend.finish_volume_operation.assert_called_once_with(rs.command_id)


@pytest.mark.parametrize("outcome", ["completed", "not_volume"])
def test_volume_does_not_read_irrelevant_stream(int_schema, outcome):
    handle = _FakeKernelHandle(int_schema, [_batch(int_schema, [1])])
    handle.complete_volume_operation = MagicMock(return_value=outcome)
    rs = _make_rs(handle)
    stream = MagicMock()
    rs.complete_staging_operation(None, stream)
    stream.read.assert_not_called()
    assert len(rs.fetchall()) == (1 if outcome == "not_volume" else 0)


@pytest.mark.parametrize("missing", [False, True])
def test_volume_missing_or_failed_input_closes_without_masking_error(
    int_schema, missing
):
    from databricks.sql.exc import ProgrammingError

    handle = _FakeKernelHandle(int_schema, [])
    handle.complete_volume_operation = MagicMock(return_value="needs_input_stream")
    rs = _make_rs(handle)
    error = RuntimeError("stream read failed")
    stream = MagicMock()
    stream.read.side_effect = error
    with pytest.raises(ProgrammingError if missing else RuntimeError) as raised:
        rs.complete_staging_operation([], None if missing else stream)
    if not missing:
        assert raised.value is error
    assert handle.closed
    handle.complete_volume_operation.assert_called_once()


def test_failed_volume_consumes_instructions_and_preserves_os_error(int_schema):
    handle = _FakeKernelHandle(int_schema, [_batch(int_schema, [1])])
    handle.is_volume_operation = True
    error = RuntimeError("local file failure")
    error.local_os_error = errno.ENOENT
    error.local_path = "/allowed/missing"
    handle.complete_volume_operation = MagicMock(side_effect=error)
    rs = _make_rs(handle)
    with pytest.raises(FileNotFoundError) as raised:
        rs.complete_staging_operation(["/allowed"])
    assert raised.value.errno == errno.ENOENT
    assert raised.value.filename == "/allowed/missing"
    assert rs.fetchall() == []
    rs.backend.finish_volume_operation.assert_called_once_with(rs.command_id)


def test_volume_close_signals_backend_before_native_lock(int_schema):
    handle = _FakeKernelHandle(int_schema, [])
    rs = _make_rs(handle)
    handle.close = MagicMock(
        side_effect=lambda: rs.backend.close_command.assert_called_once_with(
            rs.command_id
        )
    )
    rs.close()
    handle.close.assert_called_once()


def test_volume_http_404_is_an_operational_error(int_schema):
    from databricks.sql.exc import OperationalError

    handle = _FakeKernelHandle(int_schema, [])
    error = RuntimeError("storage response")
    error.http_status = 404
    handle.complete_volume_operation = MagicMock(side_effect=error)
    rs = _make_rs(handle)
    with pytest.raises(OperationalError, match="404"):
        rs.complete_staging_operation([])
