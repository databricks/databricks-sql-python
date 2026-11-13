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
import os
from collections import deque
from typing import Any, BinaryIO, Deque, List, Optional, TYPE_CHECKING, Union, cast

import pyarrow

from databricks.sql.backend.kernel._errors import wrap_kernel_exception
from databricks.sql.backend.kernel.type_mapping import description_from_arrow_schema
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.result_set import ResultSet
from databricks.sql.exc import OperationalError, ProgrammingError
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
        try:
            schema = kernel_handle.arrow_schema()
        except Exception as exc:
            raise wrap_kernel_exception("KernelResultSet.arrow_schema", exc) from exc
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
            is_staging_operation=kernel_handle.is_volume_operation,
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

    def complete_staging_operation(
        self,
        allowed_paths: Union[None, str, List[str]],
        input_stream: Optional[BinaryIO] = None,
    ) -> None:
        paths = (
            [allowed_paths] if isinstance(allowed_paths, str) else (allowed_paths or [])
        )
        consume_instructions = self.is_staging_operation
        try:
            outcome = self._complete_volume_operation(paths)
            if outcome == "not_volume":
                consume_instructions = False
                return
            consume_instructions = True
            if outcome == "needs_input_stream":
                try:
                    if input_stream is None:
                        raise ProgrammingError(
                            "No input stream provided for streaming operation"
                        )
                    data = input_stream.read()
                except BaseException:
                    self.close()
                    raise
                outcome = self._complete_volume_operation(paths, data)
            if outcome != "completed":
                raise ProgrammingError("Volume operation did not complete")
            self.status = CommandState.SUCCEEDED
        finally:
            if consume_instructions:
                self._buffer.clear()
                self._buffer_offset = 0
                self._buffered_count = 0
                self._exhausted = True
                self.has_more_rows = False
            cast("KernelDatabricksClient", self.backend).finish_volume_operation(
                self.command_id
            )

    def _complete_volume_operation(
        self, paths: List[str], input_bytes: Optional[bytes] = None
    ) -> str:
        try:
            return self._kernel_handle.complete_volume_operation(
                paths, input_bytes=input_bytes
            )
        except Exception as exc:
            os_code = getattr(exc, "local_os_error", None)
            if os_code is not None:
                local_path = getattr(exc, "local_path", None)
                if os.name == "nt":
                    raise OSError(
                        0, "Local file operation failed", local_path, os_code
                    ) from exc
                raise OSError(os_code, os.strerror(os_code), local_path) from exc
            http_status = getattr(exc, "http_status", None)
            if http_status is not None:
                raise OperationalError(
                    f"Staging operation over HTTP was unsuccessful: {http_status}"
                ) from exc
            raise wrap_kernel_exception("complete_volume_operation", exc) from exc

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
        if not self.connection.open:
            self._kernel_handle = None
            self._buffer.clear()
            self._buffered_count = 0
            self._exhausted = True
            self.has_been_closed_server_side = True
            self.status = CommandState.CLOSED
            return
        # Signal through the detached canceller before close waits for a transfer's native lock.
        backend = cast("KernelDatabricksClient", self.backend)
        try:
            backend.close_command(self.command_id)
        except Exception as exc:
            logger.warning(
                "backend.close_command from result-set close failed: %s", exc
            )
        try:
            self._kernel_handle.close()
        except Exception as exc:
            # close() failures are not actionable at the connector
            # level; log and swallow so the cursor's __del__ /
            # connection close path stays clean.
            logger.warning("Error closing kernel handle: %s", exc)
        self._buffer.clear()
        self._buffered_count = 0
        self._kernel_handle = None
        self._exhausted = True
        self.has_been_closed_server_side = True
        self.status = CommandState.CLOSED
