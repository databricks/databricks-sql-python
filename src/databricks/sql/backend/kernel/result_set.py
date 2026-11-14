"""Streaming ``ResultSet`` over a kernel ``ExecutedStatement`` or
``ResultStream``.

The kernel surfaces two flavours of result-bearing handle:

- ``ExecutedStatement`` — returned by ``Statement.execute()``. Has a
  ``statement_id`` and a ``cancel()`` method.
- ``ResultStream`` — returned by ``Session.metadata().list_*`` and by
  ``ExecutedAsyncStatement.await_result()``. No statement id; no
  cancel.

Both implement the same three methods this class actually calls:
``arrow_schema() / fetch_next_batch() / close()``. ``KernelResultSet``
takes either via the ``kernel_handle`` parameter and treats them
uniformly — the connector's ``ResultSet`` contract doesn't need to
distinguish them.

Buffer shape mirrors the prior ADBC POC's ``AdbcResultSet``: a FIFO
of pyarrow ``RecordBatch``es, fed one batch at a time from the
kernel as the connector calls ``fetch*``. ``fetchmany(n)`` slices
within a batch when ``n`` is smaller than the kernel's natural
batch size; ``fetchall`` drains the whole stream.

Note: ``buffer_size_bytes`` is accepted by the constructor for
contract compatibility with the base ``ResultSet`` but is not
consulted — the kernel backend currently caps buffering by rows
pulled, not bytes. Memory ceilings should be controlled by the
kernel-side batch sizing.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any, Deque, List, Optional, TYPE_CHECKING, cast

import pyarrow

from databricks.sql.backend.kernel._errors import wrap_kernel_exception
from databricks.sql.backend.kernel.type_mapping import description_from_arrow_schema
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from databricks.sql.backend.kernel.client import KernelDatabricksClient

logger = logging.getLogger(__name__)

# Small results are converted with to_pylist() instead of through a pandas
# DataFrame, whose fixed cost (100-300 µs) dominates them. to_pylist() costs
# more per value, so a result skips pandas only while its row count times the
# sum of its column weights (per-value cost relative to an integer) is within
# _MAX_COST_WITHOUT_PANDAS, which kept it at least 25% cheaper than pandas for
# every type and width measured (pyarrow 23, pandas 1.3-3.1).
_MAX_ROWS_WITHOUT_PANDAS = 128
_MAX_COST_WITHOUT_PANDAS = 1024

# Types for which to_pylist() returns the same values as the pandas path in
# ResultSet._convert_arrow_table, with their weights. Floats only qualify
# without NaN, which pandas turns into None or keeps depending on its version;
# timestamps only when not in nanoseconds.
_FLOAT_TYPE_IDS = frozenset(t.id for t in (pyarrow.float32(), pyarrow.float64()))
_TIMESTAMP_TYPE_ID = pyarrow.timestamp("us").id
_VALUE_WEIGHTS = {
    **{
        t.id: 1
        for t in (
            pyarrow.int8(),
            pyarrow.int16(),
            pyarrow.int32(),
            pyarrow.int64(),
            pyarrow.uint8(),
            pyarrow.uint16(),
            pyarrow.uint32(),
            pyarrow.uint64(),
            pyarrow.bool_(),
            pyarrow.float32(),
            pyarrow.float64(),
            pyarrow.null(),
        )
    },
    **{t.id: 3 for t in (pyarrow.string(), pyarrow.large_string())},
    **{
        t.id: 6
        for t in (
            pyarrow.binary(),
            pyarrow.large_binary(),
            pyarrow.binary(1),
            pyarrow.decimal128(1),
            pyarrow.decimal256(1),
        )
    },
    pyarrow.date32().id: 12,
    _TIMESTAMP_TYPE_ID: 12,
}
# pyarrow looks the time zone up again for every tz-aware value, which costs
# tens of microseconds when pytz isn't installed.
_TZ_TIMESTAMP_WEIGHT = 400


def _max_rows_without_pandas(types: List[pyarrow.DataType]) -> int:
    """The most rows of a result with these column types that are converted
    with to_pylist(); -1 if such results always go through pandas."""
    total = 0
    for arrow_type in types:
        weight = _VALUE_WEIGHTS.get(arrow_type.id)
        if weight is None:
            return -1
        if arrow_type.id == _TIMESTAMP_TYPE_ID:
            if arrow_type.unit == "ns":
                return -1
            if arrow_type.tz is not None:
                weight = _TZ_TIMESTAMP_WEIGHT
        total += weight
    if total == 0:
        return -1
    return min(_MAX_ROWS_WITHOUT_PANDAS, _MAX_COST_WITHOUT_PANDAS // total)


class _ResultSchema:
    """What a result set derives from its Arrow schema."""

    __slots__ = (
        "schema",
        "key",
        "description",
        "row_factory",
        "max_rows_without_pandas",
        "float_columns",
    )

    def __init__(self, schema: pyarrow.Schema, key: Any = None):
        self.schema = schema
        # The kernel's SchemaKey for ``schema``, if the kernel has them.
        self.key = key
        self.description = tuple(description_from_arrow_schema(schema))
        self.row_factory = Row(*[c[0] for c in self.description])
        types = schema.types
        self.max_rows_without_pandas = _max_rows_without_pandas(types)
        self.float_columns = tuple(
            i for i, t in enumerate(types) if t.id in _FLOAT_TYPE_IDS
        )


def _schema_key(kernel_handle: Any) -> Any:
    """The handle's ``SchemaKey``, or None for kernels without them."""
    try:
        schema_key = kernel_handle.schema_key
    except AttributeError:
        return None
    return schema_key()


class _SchemaCache:
    """Keeps the last ``_ResultSchema`` of a connection, since repeated
    statements return equal schemas."""

    __slots__ = ("_last",)

    def __init__(self) -> None:
        self._last: Optional[_ResultSchema] = None

    def get_by_key(self, key: Any) -> Optional[_ResultSchema]:
        """The last ``_ResultSchema`` if its kernel schema key equals
        ``key``, which costs far less than building and comparing a
        ``pyarrow.Schema``."""
        last = self._last
        if last is not None and last.key is not None and last.key == key:
            return last
        return None

    def get(self, schema: pyarrow.Schema, key: Any = None) -> _ResultSchema:
        last = self._last
        if last is not None and schema.equals(last.schema, check_metadata=True):
            if key is not None:
                last.key = key
            return last
        last = self._last = _ResultSchema(schema, key)
        return last


def _rows_without_pandas(
    batches: List[pyarrow.RecordBatch], result_schema: _ResultSchema
) -> Optional[List[Row]]:
    """The rows of ``batches`` built from ``to_pylist()``, or None if they
    must go through a Table and pandas (too costly, a column whose values
    could differ, or a batch whose schema does not match)."""
    num_rows = 0
    for batch in batches:
        if not batch.schema.equals(result_schema.schema):
            return None
        num_rows += batch.num_rows
    if num_rows > result_schema.max_rows_without_pandas:
        return None
    columns: List[List[Any]] = [[] for _ in result_schema.description]
    for batch in batches:
        for values, column in zip(columns, batch.columns):
            try:
                values.extend(column.to_pylist())
            except Exception:
                # E.g. a date past 9999-12-31: pandas then raises its own
                # error, as it does without this fast path.
                logger.debug(
                    "to_pylist() failed for a %s column; using pandas",
                    column.type,
                    exc_info=True,
                )
                return None
    for i in result_schema.float_columns:
        if any(v != v for v in columns[i]):
            return None
    row_factory = result_schema.row_factory
    return [row_factory(*values) for values in zip(*columns)]


class KernelResultSet(ResultSet):
    """Streaming ``ResultSet`` over a kernel handle.

    The ``kernel_handle`` is duck-typed: it must implement
    ``arrow_schema() -> pyarrow.Schema``, ``fetch_next_batch() ->
    Optional[pyarrow.RecordBatch]``, and ``close() -> None``.
    Both ``databricks_sql_kernel.ExecutedStatement`` and
    ``databricks_sql_kernel.ResultStream`` satisfy that contract. Handles
    that also have ``schema_key()`` and ``fetch_all_rows()`` let small
    results skip pyarrow.
    """

    def __init__(
        self,
        connection: "Connection",
        backend: "KernelDatabricksClient",
        kernel_handle: Any,
        command_id: CommandId,
        arraysize: int,
        buffer_size_bytes: int,
        schema_cache: Optional[_SchemaCache] = None,
    ):
        try:
            key = _schema_key(kernel_handle)
            result_schema = None
            if key is not None and schema_cache is not None:
                result_schema = schema_cache.get_by_key(key)
            if result_schema is None:
                schema = kernel_handle.arrow_schema()
                if schema_cache is None:
                    result_schema = _ResultSchema(schema, key)
                else:
                    result_schema = schema_cache.get(schema, key)
        except Exception as exc:
            raise wrap_kernel_exception("KernelResultSet.arrow_schema", exc) from exc
        self._result_schema = result_schema
        super().__init__(
            connection=connection,
            backend=backend,
            arraysize=arraysize,
            buffer_size_bytes=buffer_size_bytes,
            command_id=command_id,
            status=CommandState.RUNNING,
            has_been_closed_server_side=False,
            has_more_rows=True,
            results_queue=None,
            description=list(self._result_schema.description),
            is_staging_operation=False,
            lz4_compressed=False,
            arrow_schema_bytes=None,
        )
        self._kernel_handle = kernel_handle
        # Equal to the handle's schema, metadata included.
        self._schema: pyarrow.Schema = result_schema.schema
        # Kernels with schema keys can also return small results as rows of
        # Python values (``fetch_all_rows``), skipping pyarrow. Tried once,
        # on the first row fetch, for types the pandas-free path converts.
        self._try_kernel_rows = (
            key is not None and result_schema.max_rows_without_pandas >= 0
        )
        # Rows of values from ``fetch_all_rows`` and the next one to return.
        self._kernel_rows: Optional[List[tuple]] = None
        self._kernel_rows_pos = 0
        # FIFO of record batches plus a per-head row offset, so
        # partial fetches (fetchmany(n) for n < batch_size) don't
        # re-fetch from the kernel.
        self._buffer: Deque[pyarrow.RecordBatch] = deque()
        self._buffer_offset: int = 0
        # Running count of rows currently buffered (sum of batch
        # sizes minus the head-batch offset). Maintained by
        # _pull_one_batch / _take_buffered / _drain so _buffered_rows
        # stays O(1) instead of walking the deque.
        self._buffered_count: int = 0
        self._exhausted: bool = False

    # ----- internal helpers -----

    def _pull_one_batch(self) -> bool:
        """Pull the next batch from the kernel into the local buffer.
        Returns True if a batch was added; False if the kernel side
        is exhausted."""
        if self._exhausted:
            return False
        try:
            batch = self._kernel_handle.fetch_next_batch()
        except Exception as exc:
            raise wrap_kernel_exception("fetch_next_batch", exc) from exc
        if batch is None:
            self._exhausted = True
            self.has_more_rows = False
            self.status = CommandState.SUCCEEDED
            return False
        if batch.num_rows > 0:
            self._buffer.append(batch)
            self._buffered_count += batch.num_rows
        return True

    def _ensure_buffered(self, n_rows: int) -> int:
        """Pull batches until ``n_rows`` are buffered or the kernel
        is exhausted. Returns total rows currently buffered."""
        while self._buffered_count < n_rows:
            if not self._pull_one_batch():
                break
        return self._buffered_count

    def _buffered_rows(self) -> int:
        return self._buffered_count

    def _to_table(self, batches: List[pyarrow.RecordBatch]) -> pyarrow.Table:
        return pyarrow.Table.from_batches(batches, schema=self._schema)

    def _take_buffered(self, n: int) -> pyarrow.Table:
        """Slice up to ``n`` rows out of the buffer; advances state."""
        return self._to_table(self._take_buffered_batches(n))

    def _take_buffered_batches(self, n: int) -> List[pyarrow.RecordBatch]:
        slices: List[pyarrow.RecordBatch] = []
        remaining = n
        while remaining > 0 and self._buffer:
            head = self._buffer[0]
            avail = head.num_rows - self._buffer_offset
            take = min(avail, remaining)
            slices.append(head.slice(self._buffer_offset, take))
            self._buffer_offset += take
            remaining -= take
            if self._buffer_offset >= head.num_rows:
                self._buffer.popleft()
                self._buffer_offset = 0
        taken = n - remaining
        self._buffered_count -= taken
        self._next_row_index += taken
        return slices

    def _drain(self) -> pyarrow.Table:
        """Consume everything left in the buffer + kernel stream
        and return as a single Table."""
        return self._to_table(self._drain_batches())

    def _drain_batches(self) -> List[pyarrow.RecordBatch]:
        chunks: List[pyarrow.RecordBatch] = []
        if self._buffer and self._buffer_offset > 0:
            head = self._buffer.popleft()
            chunks.append(
                head.slice(self._buffer_offset, head.num_rows - self._buffer_offset)
            )
            self._buffer_offset = 0
        while self._buffer:
            chunks.append(self._buffer.popleft())
        if not self._exhausted:
            while True:
                try:
                    batch = self._kernel_handle.fetch_next_batch()
                except Exception as exc:
                    raise wrap_kernel_exception("fetch_next_batch", exc) from exc
                if batch is None:
                    self._exhausted = True
                    self.has_more_rows = False
                    self.status = CommandState.SUCCEEDED
                    break
                if batch.num_rows > 0:
                    chunks.append(batch)
        rows = sum(c.num_rows for c in chunks)
        self._buffered_count = 0
        self._next_row_index += rows
        return chunks

    # ----- rows converted by the kernel -----

    def _have_kernel_rows(self) -> bool:
        """Whether the rest of the result is in ``_kernel_rows``. The first
        call asks the kernel for every remaining row as Python values, which
        it returns for results within the pandas-free cost budget; otherwise
        the batches it pulled stay in the kernel for the Arrow path."""
        if self._kernel_rows is not None:
            return True
        if not self._try_kernel_rows or self._buffer or self._exhausted:
            return False
        self._try_kernel_rows = False
        try:
            rows = self._kernel_handle.fetch_all_rows(
                self._result_schema.max_rows_without_pandas,
                self.connection.disable_pandas is True,
            )
        except Exception as exc:
            raise wrap_kernel_exception("fetch_all_rows", exc) from exc
        if rows is None:
            return False
        self._kernel_rows = rows
        self._exhausted = True
        self.has_more_rows = False
        self.status = CommandState.SUCCEEDED
        return True

    def _take_kernel_rows(self, n: Optional[int]) -> List[tuple]:
        """Up to ``n`` (all if None) of the remaining kernel rows."""
        rows = cast(List[tuple], self._kernel_rows)
        start = self._kernel_rows_pos
        end = len(rows) if n is None else min(len(rows), start + n)
        self._kernel_rows_pos = end
        self._next_row_index += end - start
        return rows if start == 0 and end == len(rows) else rows[start:end]

    def _kernel_rows_to_arrow(self, n: Optional[int]) -> pyarrow.Table:
        # Only for Arrow fetches after a row fetch: the values convert back to
        # the same Arrow values for every type the kernel converts.
        rows = self._take_kernel_rows(n)
        if not rows:
            return pyarrow.Table.from_batches([], schema=self._schema)
        return pyarrow.Table.from_arrays(
            [
                pyarrow.array(column, type=field.type)
                for column, field in zip(zip(*rows), self._schema)
            ],
            schema=self._schema,
        )

    def _make_rows(self, values: List[tuple]) -> List[Row]:
        row_factory = self._result_schema.row_factory
        return [row_factory(*v) for v in values]

    # ----- Arrow fetches -----

    def fetchall_arrow(self) -> pyarrow.Table:
        if self._kernel_rows is not None:
            return self._kernel_rows_to_arrow(None)
        return self._drain()

    def fetchmany_arrow(self, size: int) -> pyarrow.Table:
        if size < 0:
            raise ValueError(f"fetchmany_arrow size must be >= 0, got {size}")
        if size == 0:
            return pyarrow.Table.from_batches([], schema=self._schema)
        if self._kernel_rows is not None:
            return self._kernel_rows_to_arrow(size)
        self._ensure_buffered(size)
        return self._take_buffered(size)

    # ----- Row fetches -----

    def _rows(self, batches: List[pyarrow.RecordBatch]) -> List[Row]:
        # Small results skip building a Table, which releases and re-takes
        # the GIL, as well as pandas.
        if self.connection.disable_pandas is not True:
            rows = _rows_without_pandas(batches, self._result_schema)
            if rows is not None:
                return rows
        return self._convert_arrow_table(self._to_table(batches))

    def fetchone(self) -> Optional[Row]:
        if self._have_kernel_rows():
            rows = self._make_rows(self._take_kernel_rows(1))
            return rows[0] if rows else None
        self._ensure_buffered(1)
        if self._buffered_rows() == 0:
            return None
        rows = self._rows(self._take_buffered_batches(1))
        return rows[0] if rows else None

    def fetchmany(self, size: int) -> List[Row]:
        if size < 0:
            raise ValueError(f"fetchmany size must be >= 0, got {size}")
        if size == 0:
            return []
        if self._have_kernel_rows():
            return self._make_rows(self._take_kernel_rows(size))
        self._ensure_buffered(size)
        return self._rows(self._take_buffered_batches(size))

    def fetchall(self) -> List[Row]:
        if self._have_kernel_rows():
            return self._make_rows(self._take_kernel_rows(None))
        return self._rows(self._drain_batches())

    def close(self) -> None:
        """Close the underlying kernel handle and notify the backend.

        Idempotent — the kernel's own ``close()`` is idempotent, and
        we guard against repeated calls so partially-drained streams
        don't double-decrement reference counts.

        Skipped entirely when the parent connection is already
        closed. A ``__del__``-driven close arriving after
        connection-close would otherwise issue a kernel call into an
        already-disposed session.
        """
        if self._kernel_handle is None:
            return
        self._kernel_rows = None
        self._try_kernel_rows = False
        if not self.connection.open:
            self._kernel_handle = None
            self._buffer.clear()
            self._buffered_count = 0
            self._exhausted = True
            self.has_been_closed_server_side = True
            self.status = CommandState.CLOSED
            return
        try:
            self._kernel_handle.close()
        except Exception as exc:
            # close() failures are not actionable at the connector
            # level; log and swallow so the cursor's __del__ /
            # connection close path stays clean.
            logger.warning("Error closing kernel handle: %s", exc)
        # Honor the base ``ResultSet`` contract: notify the backend.
        # For async results, ``backend.close_command`` drops the
        # retained owning handle and parent Statement. Sync-execute and
        # metadata paths never registered in ``_async_handles`` to begin
        # with, so this call is tolerant bookkeeping for them.
        backend = cast("KernelDatabricksClient", self.backend)
        try:
            backend.close_command(self.command_id)
        except Exception as exc:
            logger.warning(
                "backend.close_command from result-set close failed: %s", exc
            )
        self._buffer.clear()
        self._buffered_count = 0
        self._kernel_handle = None
        self._exhausted = True
        self.has_been_closed_server_side = True
        self.status = CommandState.CLOSED
