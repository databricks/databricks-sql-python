"""Unit tests for ``KernelResultSet`` — the buffer behavior +
close() semantics. Uses a fake kernel handle so tests run with no
network and no Rust extension dependency."""

from __future__ import annotations

from collections import deque
from typing import Deque
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

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


def _make_rs(handle) -> KernelResultSet:
    # The base ResultSet __init__ takes a `connection` ref it never
    # actually dereferences during these buffer tests, so a Mock is
    # fine.
    connection = MagicMock()
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
        [_batch(int_schema, [1, 2]), _batch(int_schema, [3, 4]), _batch(int_schema, [5, 6])],
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
