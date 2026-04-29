"""ResultSet for the ADBC-Rust-kernel backend (POC, streaming).

Wraps the streaming `databricks_adbc_pyo3.ResultSet` and pulls record batches
lazily as the connector calls `fetch*`. No full materialization on execute.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Deque, List, Optional, TYPE_CHECKING

import pyarrow

from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from databricks.sql.backend.adbc.client import AdbcDatabricksClient

logger = logging.getLogger(__name__)


def _arrow_type_to_dbapi_string(arrow_type: pyarrow.DataType) -> str:
    """Map a pyarrow type to a Databricks SQL type string for PEP-249 description."""
    if pyarrow.types.is_boolean(arrow_type):
        return "boolean"
    if pyarrow.types.is_int8(arrow_type):
        return "tinyint"
    if pyarrow.types.is_int16(arrow_type):
        return "smallint"
    if pyarrow.types.is_int32(arrow_type):
        return "int"
    if pyarrow.types.is_int64(arrow_type):
        return "bigint"
    if pyarrow.types.is_float32(arrow_type):
        return "float"
    if pyarrow.types.is_float64(arrow_type):
        return "double"
    if pyarrow.types.is_decimal(arrow_type):
        return "decimal"
    if pyarrow.types.is_string(arrow_type) or pyarrow.types.is_large_string(arrow_type):
        return "string"
    if pyarrow.types.is_binary(arrow_type) or pyarrow.types.is_large_binary(arrow_type):
        return "binary"
    if pyarrow.types.is_date(arrow_type):
        return "date"
    if pyarrow.types.is_timestamp(arrow_type):
        return "timestamp"
    if pyarrow.types.is_list(arrow_type) or pyarrow.types.is_large_list(arrow_type):
        return "array"
    if pyarrow.types.is_struct(arrow_type):
        return "struct"
    if pyarrow.types.is_map(arrow_type):
        return "map"
    return str(arrow_type)


def description_from_arrow_schema(schema: pyarrow.Schema) -> List[tuple]:
    """Build a PEP-249 description list from a pyarrow Schema."""
    return [
        (field.name, _arrow_type_to_dbapi_string(field.type), None, None, None, None, None)
        for field in schema
    ]


class AdbcResultSet(ResultSet):
    """Streaming ResultSet adapter over `databricks_adbc_pyo3.ResultSet`.

    Holds a small in-memory buffer of batches to support partial fetches
    (`fetchmany(n)`, `fetchone()`) without re-fetching from the kernel. The
    buffer is fed by pulling one batch at a time from the kernel reader.
    """

    def __init__(
        self,
        connection: "Connection",
        backend: "AdbcDatabricksClient",
        rust_result_set,  # databricks_adbc_pyo3.ResultSet
        command_id: CommandId,
        arraysize: int,
        buffer_size_bytes: int,
    ):
        schema = rust_result_set.arrow_schema()
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
        self._rust_rs = rust_result_set
        self._schema: pyarrow.Schema = schema
        # FIFO of (batch, offset_in_batch) for partial consumption.
        self._buffer: Deque[pyarrow.RecordBatch] = deque()
        self._buffer_offset: int = 0  # how many rows already fetched out of buffer[0]
        self._exhausted: bool = False

    # ----- internal helpers -----

    def _pull_one_batch(self) -> bool:
        """Pull the next batch from the kernel into the buffer.
        Returns True if a batch was added, False if reader is exhausted."""
        if self._exhausted:
            return False
        batch = self._rust_rs.fetch_next_batch()
        if batch is None:
            self._exhausted = True
            self.has_more_rows = False
            self.status = CommandState.SUCCEEDED
            return False
        if batch.num_rows > 0:
            self._buffer.append(batch)
        return True

    def _ensure_buffered(self, n_rows: int) -> int:
        """Pull batches until we have at least n_rows buffered, or reader exhausted.
        Returns total rows currently buffered (>= min(n_rows, total_remaining))."""
        target = n_rows
        while self._buffered_rows() < target:
            if not self._pull_one_batch():
                break
        return self._buffered_rows()

    def _buffered_rows(self) -> int:
        if not self._buffer:
            return 0
        first = self._buffer[0].num_rows - self._buffer_offset
        rest = sum(b.num_rows for b in list(self._buffer)[1:])
        return first + rest

    def _take_buffered(self, n: int) -> pyarrow.Table:
        """Slice up to n rows out of the buffer; advances state."""
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
        self._next_row_index += (n - remaining)
        if not slices:
            return pyarrow.Table.from_batches([], schema=self._schema)
        return pyarrow.Table.from_batches(slices, schema=self._schema)

    def _drain(self) -> pyarrow.Table:
        """Consume everything that's left and return as a single Table."""
        chunks: List[pyarrow.RecordBatch] = []
        # First flush any partially consumed head batch.
        if self._buffer and self._buffer_offset > 0:
            head = self._buffer.popleft()
            chunks.append(head.slice(self._buffer_offset, head.num_rows - self._buffer_offset))
            self._buffer_offset = 0
        # Then everything else already buffered.
        while self._buffer:
            chunks.append(self._buffer.popleft())
        # Then pull whatever remains from the kernel.
        if not self._exhausted:
            while True:
                batch = self._rust_rs.fetch_next_batch()
                if batch is None:
                    self._exhausted = True
                    self.has_more_rows = False
                    self.status = CommandState.SUCCEEDED
                    break
                if batch.num_rows > 0:
                    chunks.append(batch)
        rows = sum(c.num_rows for c in chunks)
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
        # Drop our handle to the streaming reader; PyO3 Drop releases
        # kernel-side resources (HTTP connections, buffered chunks).
        self._buffer.clear()
        self._rust_rs = None
        self._exhausted = True
        self.has_been_closed_server_side = True
        self.status = CommandState.CLOSED
