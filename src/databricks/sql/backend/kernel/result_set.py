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
from typing import Any, Deque, List, Optional, TYPE_CHECKING

import pyarrow

from databricks.sql.backend.kernel.type_mapping import description_from_arrow_schema
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from databricks.sql.backend.kernel.client import KernelDatabricksClient

logger = logging.getLogger(__name__)


class KernelResultSet(ResultSet):
    """Streaming ``ResultSet`` over a kernel handle.

    The ``kernel_handle`` is duck-typed: it must implement
    ``arrow_schema() -> pyarrow.Schema``, ``fetch_next_batch() ->
    Optional[pyarrow.RecordBatch]``, and ``close() -> None``.
    Both ``databricks_sql_kernel.ExecutedStatement`` and
    ``databricks_sql_kernel.ResultStream`` satisfy that contract.
    """

    def __init__(
        self,
        connection: "Connection",
        backend: "KernelDatabricksClient",
        kernel_handle: Any,
        command_id: CommandId,
        arraysize: int,
        buffer_size_bytes: int,
    ):
        schema = kernel_handle.arrow_schema()
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
            description=description_from_arrow_schema(schema),
            is_staging_operation=False,
            lz4_compressed=False,
            arrow_schema_bytes=None,
        )
        self._kernel_handle = kernel_handle
        self._schema: pyarrow.Schema = schema
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
        batch = self._kernel_handle.fetch_next_batch()
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

    def _take_buffered(self, n: int) -> pyarrow.Table:
        """Slice up to ``n`` rows out of the buffer; advances state."""
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
        if not slices:
            return pyarrow.Table.from_batches([], schema=self._schema)
        return pyarrow.Table.from_batches(slices, schema=self._schema)

    def _drain(self) -> pyarrow.Table:
        """Consume everything left in the buffer + kernel stream
        and return as a single Table."""
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
                batch = self._kernel_handle.fetch_next_batch()
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
        if not chunks:
            return pyarrow.Table.from_batches([], schema=self._schema)
        return pyarrow.Table.from_batches(chunks, schema=self._schema)

    # ----- Arrow fetches -----

    def fetchall_arrow(self) -> pyarrow.Table:
        return self._drain()

    def fetchmany_arrow(self, size: int) -> pyarrow.Table:
        if size < 0:
            raise ValueError(f"fetchmany_arrow size must be >= 0, got {size}")
        if size == 0:
            return pyarrow.Table.from_batches([], schema=self._schema)
        self._ensure_buffered(size)
        return self._take_buffered(size)

    # ----- Row fetches -----

    def fetchone(self) -> Optional[Row]:
        self._ensure_buffered(1)
        if self._buffered_rows() == 0:
            return None
        table = self._take_buffered(1)
        rows = self._convert_arrow_table(table)
        return rows[0] if rows else None

    def fetchmany(self, size: int) -> List[Row]:
        if size < 0:
            raise ValueError(f"fetchmany size must be >= 0, got {size}")
        if size == 0:
            return []
        self._ensure_buffered(size)
        table = self._take_buffered(size)
        return self._convert_arrow_table(table)

    def fetchall(self) -> List[Row]:
        return self._convert_arrow_table(self._drain())

    def close(self) -> None:
        """Close the underlying kernel handle. Idempotent — the
        kernel's own ``close()`` is idempotent, and we guard against
        repeated calls so partially-drained streams don't double-
        decrement reference counts."""
        if self._kernel_handle is None:
            return
        try:
            self._kernel_handle.close()
        except Exception as exc:
            # close() failures are not actionable at the connector
            # level; log and swallow so the cursor's __del__ /
            # connection close path stays clean.
            logger.warning("Error closing kernel handle: %s", exc)
        # Drop the entry from the backend's async-handle map (if
        # present) — for async-submitted statements the handle is
        # tracked there and the base ``ResultSet.close`` path would
        # otherwise leave a stale entry pointing at a closed handle.
        # No-op for the sync-execute and metadata paths, which never
        # register in ``_async_handles``.
        guid = getattr(self.command_id, "guid", None)
        if guid is not None:
            self.backend._async_handles_lock.acquire()
            try:
                self.backend._async_handles.pop(guid, None)
            finally:
                self.backend._async_handles_lock.release()
        self._buffer.clear()
        self._buffered_count = 0
        self._kernel_handle = None
        self._exhausted = True
        self.has_been_closed_server_side = True
        self.status = CommandState.CLOSED
