from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, TYPE_CHECKING, Tuple

import logging
import pandas

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
    from databricks.sql.client import Connection
from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.types import Row
from databricks.sql.exc import InternalError, RequestError, CursorAlreadyClosedError
from databricks.sql.utils import (
    CloudFetchQueue,
    ColumnTable,
    ColumnQueue,
    concat_table_chunks,
)
from databricks.sql.backend.types import CommandId, CommandState, ExecuteResponse
from databricks.sql.telemetry.models.event import StatementType

logger = logging.getLogger(__name__)

# Upper bound on how many rows a fetchone() refill pulls and converts at once. The
# refill uses ``arraysize`` but is capped here so a cursor with a large arraysize
# (default 100000) doesn't materialise a full page just to return one row.
_FETCHONE_BUFFER_MAX_ROWS = 1000

# fetchone() refills grow geometrically from this size (1 -> 10 -> 100 -> ...) up to
# min(arraysize, _FETCHONE_BUFFER_MAX_ROWS). The first refill converts a single row,
# so a lone fetchone() (e.g. SQLAlchemy ``.first()`` / ``.scalar()`` or an EXISTS
# probe) costs the same as before batching, while iteration loops quickly reach the
# full batch size.
_FETCHONE_BUFFER_INITIAL_ROWS = 1
_FETCHONE_BUFFER_GROWTH_FACTOR = 10

# Conversion errors that are not caused by row data. fetchone() re-raises these
# straight away instead of bisecting the batch to find the failing row(s).
_NON_DATA_CONVERSION_ERRORS = (MemoryError, RecursionError)


class _FailedRow:
    """Placeholder in the fetchone() row buffer for a row that failed to convert."""

    __slots__ = ("error", "traceback")

    def __init__(self, error: Exception):
        self.error = error
        # Snapshot the conversion traceback. A converter may raise the same
        # exception instance for many rows; restoring this before each raise keeps
        # its __traceback__ from growing (and pinning frames) on every fetchone().
        self.traceback = error.__traceback__


class ResultSet(ABC):
    """
    Abstract base class for result sets returned by different backend implementations.

    This class defines the interface that all concrete result set implementations must follow.
    """

    def __init__(
        self,
        connection: Connection,
        backend: DatabricksClient,
        arraysize: int,
        buffer_size_bytes: int,
        command_id: CommandId,
        status: CommandState,
        has_been_closed_server_side: bool = False,
        has_more_rows: bool = False,
        results_queue=None,
        description: List[Tuple] = [],
        is_staging_operation: bool = False,
        lz4_compressed: bool = False,
        arrow_schema_bytes: Optional[bytes] = None,
        num_modified_rows: Optional[int] = None,
    ):
        """
        A ResultSet manages the results of a single command.

        Parameters:
            :param connection: The parent connection that was used to execute this command
            :param backend: The specialised backend client to be invoked in the fetch phase
            :param arraysize: The max number of rows to fetch at a time (PEP-249)
            :param buffer_size_bytes: The size (in bytes) of the internal buffer + max fetch
            :param command_id: The command ID
            :param status: The command status
            :param has_been_closed_server_side: Whether the command has been closed on the server
            :param has_more_rows: Whether the command has more rows
            :param results_queue: The results queue
            :param description: column description of the results
            :param is_staging_operation: Whether the command is a staging operation
        """

        self.connection = connection
        self.backend = backend
        self.arraysize = arraysize
        self.buffer_size_bytes = buffer_size_bytes
        self._next_row_index = 0
        self.description = description
        self.command_id = command_id
        self.status = status
        self.has_been_closed_server_side = has_been_closed_server_side
        self.has_more_rows = has_more_rows
        self.results = results_queue
        self._is_staging_operation = is_staging_operation
        self.lz4_compressed = lz4_compressed
        self._arrow_schema_bytes = arrow_schema_bytes
        # Affected-row count for DML; None for SELECT / unreported.
        self.num_modified_rows = num_modified_rows
        # Small row buffer that serves fetchone()/iteration. Rows are fetched and
        # converted ``arraysize`` at a time instead of one at a time (converting a
        # single row costs a full pandas round-trip). ``_row_buffer_raw`` holds the
        # un-converted chunk (Arrow table, ColumnTable or JSON row list) aligned with
        # ``_row_buffer_rows`` so the Arrow/columnar fetch methods can drain it too.
        # Every fetch method drains this buffer first, preserving row order.
        self._row_buffer_raw: Any = None
        self._row_buffer_rows: List[Row] = []
        self._row_buffer_pos = 0
        # Set once a fetchone() conversion failure has been logged at WARNING.
        self._fetchone_conversion_warned = False
        # Size of the next fetchone() refill; grows on each refill (see
        # ``_FETCHONE_BUFFER_GROWTH_FACTOR``).
        self._fetchone_refill_rows = _FETCHONE_BUFFER_INITIAL_ROWS

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row:
                yield row
            else:
                break

    def _convert_arrow_table(self, table):
        column_names = [c[0] for c in self.description]
        ResultRow = Row(*column_names)

        if self.connection.disable_pandas is True:
            return [
                ResultRow(*[v.as_py() for v in r]) for r in zip(*table.itercolumns())
            ]

        # Need to use nullable types, as otherwise type can change when there are missing values.
        # See https://arrow.apache.org/docs/python/pandas.html#nullable-types
        # NOTE: This api is epxerimental https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        dtype_mapping = {
            pyarrow.int8(): pandas.Int8Dtype(),
            pyarrow.int16(): pandas.Int16Dtype(),
            pyarrow.int32(): pandas.Int32Dtype(),
            pyarrow.int64(): pandas.Int64Dtype(),
            pyarrow.uint8(): pandas.UInt8Dtype(),
            pyarrow.uint16(): pandas.UInt16Dtype(),
            pyarrow.uint32(): pandas.UInt32Dtype(),
            pyarrow.uint64(): pandas.UInt64Dtype(),
            pyarrow.bool_(): pandas.BooleanDtype(),
            pyarrow.float32(): pandas.Float32Dtype(),
            pyarrow.float64(): pandas.Float64Dtype(),
            pyarrow.string(): pandas.StringDtype(),
        }

        # Need to rename columns, as the to_pandas function cannot handle duplicate column names
        table_renamed = table.rename_columns([str(c) for c in range(table.num_columns)])
        df = table_renamed.to_pandas(
            types_mapper=dtype_mapping.get,
            date_as_object=True,
            timestamp_as_object=True,
        )

        res = df.to_numpy(na_value=None, dtype="object")
        return [ResultRow(*v) for v in res]

    @property
    def rownumber(self):
        return self._next_row_index

    # ----- fetchone() row buffer helpers -----

    def _row_buffer_count(self) -> int:
        return len(self._row_buffer_rows) - self._row_buffer_pos

    def _row_buffer_clear(self) -> None:
        self._row_buffer_raw = None
        self._row_buffer_rows = []
        self._row_buffer_pos = 0

    def _row_buffer_advance(self, n: Optional[int]) -> Tuple[int, int]:
        """Advance the buffer by up to ``n`` rows (all if None); return (start, length)."""
        available = self._row_buffer_count()
        length = available if n is None else max(0, min(n, available))
        start = self._row_buffer_pos
        self._row_buffer_pos += length
        # Buffered rows only count as consumed once they are handed to the caller.
        self._next_row_index += length
        return start, length

    def _row_buffer_take_rows(self, n: Optional[int] = None) -> List[Row]:
        """Pop up to ``n`` (all if None) converted rows from the buffer.

        If the requested range contains a ``_FailedRow``, only the rows up to and
        including the first failed row are consumed and its error is raised. Rows
        after it stay in the buffer for the next fetch. The good rows before it in
        the same call are consumed with the failed fetch (a fetch either returns
        all of its rows or raises), so ``rownumber`` stays aligned with what is
        left in the buffer.
        """
        if self._row_buffer_count() == 0:
            return []
        rows = self._row_buffer_rows
        available = self._row_buffer_count()
        length = available if n is None else max(0, min(n, available))
        start = self._row_buffer_pos
        failed: Optional[_FailedRow] = None
        for i in range(start, start + length):
            row = rows[i]
            if isinstance(row, _FailedRow):
                failed = row
                length = i - start + 1
                break
        self._row_buffer_advance(length)
        if self._row_buffer_count() == 0:
            self._row_buffer_clear()
        if failed is not None:
            raise failed.error.with_traceback(failed.traceback)
        return rows[start : start + length]

    def _fetch_rows_with_buffer(
        self,
        size: Optional[int],
        fetch_rest: Callable[[Optional[int]], List[Row]],
    ) -> List[Row]:
        """Return ``size`` rows (all if None), draining the fetchone() buffer first.

        Every row-level fetchmany()/fetchall() goes through here, so rows the
        fetchone() buffer already holds are always returned before newly fetched
        ones. ``fetch_rest(n)`` must fetch up to ``n`` (all if None) rows
        while bypassing the buffer. It is not called when the buffer already
        covers ``size``.
        """
        buffered = self._row_buffer_take_rows(size)
        remaining = None if size is None else size - len(buffered)
        if buffered and remaining == 0:
            return buffered
        rest = fetch_rest(remaining)
        return buffered + rest if buffered else rest

    @staticmethod
    def _raw_row_count(raw: Any) -> Optional[int]:
        """Row count of a raw chunk (Arrow table, ColumnTable or list); None if unknown."""
        if raw is None:
            return 0
        n = len(raw) if isinstance(raw, list) else getattr(raw, "num_rows", None)
        return n if isinstance(n, int) else None

    @staticmethod
    def _convert_rows_individually(
        raw: Any,
        convert: Callable[[Any], List[Row]],
        batch_error: Exception,
        log_level: int = logging.WARNING,
    ) -> Optional[List[Any]]:
        """Convert ``raw`` in smaller pieces after a batch conversion failed.

        The failed batch is bisected: each failing range is split in half and the
        halves are converted separately, down to single rows. Only rows that fail
        on their own are returned as ``_FailedRow`` placeholders (raised when that
        row is handed out); every row that converts is kept, so a failure is never
        attributed to a row that was not itself tried. Cost is about
        ``2 * k * log2(n)`` conversions for ``k`` bad rows, and at most ``2n``
        when every row fails. Returns None if ``raw`` has no rows to split, in
        which case the caller re-raises the batch error.
        """
        n = ResultSet._raw_row_count(raw)
        if not n:
            return None

        rows: List[Any] = []

        def convert_range(lo: int, hi: int, error: Optional[Exception]) -> None:
            # ``error`` is the failure already seen for [lo, hi), if it was tried.
            if error is None:
                piece = raw[lo:hi] if isinstance(raw, list) else raw.slice(lo, hi - lo)
                try:
                    rows.extend(convert(piece))
                    return
                except Exception as e:
                    error = e
            if hi - lo == 1:
                rows.append(_FailedRow(error))
                return
            mid = (lo + hi) // 2
            convert_range(lo, mid, None)
            convert_range(mid, hi, None)

        convert_range(0, n, batch_error)
        failed = sum(isinstance(r, _FailedRow) for r in rows)
        # One summary line, no traceback: the error itself still reaches the caller
        # when the failing row is fetched. The full traceback is only logged at DEBUG.
        # ``log_level`` lets the caller log this at WARNING only once per result set.
        logger.log(
            log_level,
            "fetchone(): failed to convert %d of %d row(s) in a batch; each failing "
            "row raises when fetched (%s: %s)",
            failed,
            n,
            type(batch_error).__name__,
            batch_error,
        )
        logger.debug("fetchone(): batch conversion error", exc_info=batch_error)
        return rows

    def _cap_to_current_chunk(self, size: int) -> int:
        """Cap ``size`` at the rows left in the current CloudFetch chunk, if any.

        Limits each fetchone() refill to a single CloudFetch chunk. It does not
        avoid waiting on the next download: ``next_n_rows`` advances to the next
        file as soon as the current one is fully read, inside the same call.
        Shared by the Thrift and SEA fetchone() refills so the CloudFetchQueue
        internals (``table`` / ``table_row_index``) are read in one place.
        """
        if isinstance(self.results, CloudFetchQueue) and self.results.table:
            available = self.results.table.num_rows - self.results.table_row_index
            if available > 0:
                return min(size, available)
        return size

    def _row_buffer_take_raw(self, n: Optional[int] = None) -> Any:
        """Pop up to ``n`` (all if None) un-converted rows from the buffer.

        Returns None when the buffer is empty, otherwise a slice of the raw chunk
        (same type as what the queue returned: Arrow table, ColumnTable or list).
        """
        if self._row_buffer_count() == 0:
            return None
        raw = self._row_buffer_raw
        start, length = self._row_buffer_advance(n)
        if self._row_buffer_count() == 0:
            self._row_buffer_clear()
        if isinstance(raw, list):
            return raw[start : start + length]
        return raw.slice(start, length)

    def _fetchone_buffered(
        self, fetch_raw: Callable[[int], Any], convert: Callable[[Any], List[Row]]
    ) -> Optional[Row]:
        """Serve fetchone() from the row buffer, refilling in small batches.

        Refill sizes start at ``_FETCHONE_BUFFER_INITIAL_ROWS`` and grow by
        ``_FETCHONE_BUFFER_GROWTH_FACTOR`` on each refill, capped at ``arraysize``
        and ``_FETCHONE_BUFFER_MAX_ROWS``, so a single fetchone() converts only one
        row while iteration settles into full batches. ``fetch_raw`` may return
        fewer (Thrift and the
        kernel backend only serve what is already buffered, refilling at most until
        one row is available, so latency to the first row is unchanged).

        ``_next_row_index`` keeps counting rows handed to the caller: rows parked in
        the buffer are subtracted on refill and added back as they are taken. The
        queue is only read again once the buffer is empty, so at that point the
        index equals the queue position again (Thrift relies on that for
        ``expected_row_start_offset``).

        If conversion yields a different number of rows than the raw chunk held,
        ``InternalError`` is raised and that whole chunk is dropped (the queue and
        ``rownumber`` have already moved past it). The result set should be
        considered unusable after that error.
        """
        if self._row_buffer_count() == 0:
            cap = max(min(self.arraysize, _FETCHONE_BUFFER_MAX_ROWS), 1)
            target = getattr(
                self, "_fetchone_refill_rows", _FETCHONE_BUFFER_INITIAL_ROWS
            )
            size = max(min(target, cap), 1)
            self._fetchone_refill_rows = min(
                size * _FETCHONE_BUFFER_GROWTH_FACTOR, _FETCHONE_BUFFER_MAX_ROWS
            )
            raw = fetch_raw(size)
            try:
                rows = convert(raw)
            except _NON_DATA_CONVERSION_ERRORS:
                # Resource / interpreter failures say nothing about individual
                # rows; retrying smaller slices would only repeat (or worsen) them.
                raise
            except Exception as batch_error:
                # The queue has already moved past this batch, so it can't be
                # re-read. Fall back to bisecting it down to single rows so that,
                # as before batching, only the row(s) that fail to convert are
                # lost: each failing row is parked as a _FailedRow and re-raises
                # its own error when it is reached.
                # Warn once per result set; later failing batches (e.g. a column
                # type that fails in every row) log at DEBUG to avoid a WARNING
                # per refill for callers that catch and keep fetching.
                warned = getattr(self, "_fetchone_conversion_warned", False)
                fallback = self._convert_rows_individually(
                    raw,
                    convert,
                    batch_error,
                    log_level=logging.DEBUG if warned else logging.WARNING,
                )
                if fallback is None:
                    raise
                self._fetchone_conversion_warned = True
                rows = fallback
                if all(isinstance(r, _FailedRow) for r in rows):
                    # Every row failed: likely systemic (a column type or library
                    # problem that hits each row). Restart the refill ramp so later
                    # refills convert one row at a time, as before batching, rather
                    # than paying a full ~2n-conversion bisection on every refill.
                    self._fetchone_refill_rows = _FETCHONE_BUFFER_INITIAL_ROWS
            # Check for drift before the empty-result early return, so a converter
            # that yields [] for a non-empty chunk raises instead of silently
            # ending iteration and losing those rows.
            raw_count = self._raw_row_count(raw)
            if raw_count is not None and raw_count != len(rows):
                # fetch_raw advanced _next_row_index (and the Thrift fetch offset)
                # by raw_count; the buffer must line up 1:1 with the raw chunk or
                # rownumber and fetchmany_arrow slicing would silently drift.
                # The raw chunk is not buffered, so its raw_count rows are lost
                # (yet counted in rownumber): treat the result set as unusable
                # after this error; later fetches resume from the next chunk.
                raise InternalError(
                    "fetchone(): converted {} row(s) from a chunk of {} raw row(s)".format(
                        len(rows), raw_count
                    )
                )
            if not rows:
                return None
            self._row_buffer_raw = raw
            self._row_buffer_rows = rows
            self._row_buffer_pos = 0
            self._next_row_index -= len(rows)
        return self._row_buffer_take_rows(1)[0]

    @property
    def is_staging_operation(self) -> bool:
        """Whether this result set represents a staging operation."""
        return self._is_staging_operation

    @abstractmethod
    def fetchone(self) -> Optional[Row]:
        """Fetch the next row of a query result set."""
        pass

    @abstractmethod
    def fetchmany(self, size: int) -> List[Row]:
        """Fetch the next set of rows of a query result."""
        pass

    @abstractmethod
    def fetchall(self) -> List[Row]:
        """Fetch all remaining rows of a query result."""
        pass

    @abstractmethod
    def fetchmany_arrow(self, size: int) -> "pyarrow.Table":
        """Fetch the next set of rows as an Arrow table."""
        pass

    @abstractmethod
    def fetchall_arrow(self) -> "pyarrow.Table":
        """Fetch all remaining rows as an Arrow table."""
        pass

    def close(self) -> None:
        """
        Close the result set.

        If the connection has not been closed, and the result set has not already
        been closed on the server for some other reason, issue a request to the server to close it.
        """
        try:
            if self.results is not None:
                self.results.close()
            else:
                logger.warning("result set close: queue not initialized")

            if (
                self.status != CommandState.CLOSED
                and not self.has_been_closed_server_side
                and self.connection.open
            ):
                self.backend.close_command(self.command_id)
        except RequestError as e:
            if isinstance(e.args[1], CursorAlreadyClosedError):
                logger.info("Operation was canceled by a prior request")
        finally:
            # Drop rows parked by fetchone() so post-close fetches return nothing.
            self._row_buffer_clear()
            self.has_been_closed_server_side = True
            self.status = CommandState.CLOSED


class ThriftResultSet(ResultSet):
    """ResultSet implementation for the Thrift backend."""

    def __init__(
        self,
        connection: Connection,
        execute_response: ExecuteResponse,
        thrift_client: ThriftDatabricksClient,
        buffer_size_bytes: int = 104857600,
        arraysize: int = 10000,
        use_cloud_fetch: bool = True,
        t_row_set=None,
        max_download_threads: int = 10,
        ssl_options=None,
        has_more_rows: bool = True,
    ):
        """
        Initialize a ThriftResultSet with direct access to the ThriftDatabricksClient.

        Parameters:
            :param connection: The parent connection
            :param execute_response: Response from the execute command
            :param thrift_client: The ThriftDatabricksClient instance for direct access
            :param buffer_size_bytes: Buffer size for fetching results
            :param arraysize: Default number of rows to fetch
            :param use_cloud_fetch: Whether to use cloud fetch for retrieving results
            :param t_row_set: The TRowSet containing result data (if available)
            :param max_download_threads: Maximum number of download threads for cloud fetch
            :param ssl_options: SSL options for cloud fetch
            :param has_more_rows: Whether there are more rows to fetch
        """
        self.num_chunks = 0

        # Initialize ThriftResultSet-specific attributes
        self._use_cloud_fetch = use_cloud_fetch
        self.has_more_rows = has_more_rows

        # Build the results queue if t_row_set is provided
        results_queue = None
        if t_row_set and execute_response.result_format is not None:
            from databricks.sql.utils import ThriftResultSetQueueFactory

            # Create the results queue using the provided format
            results_queue = ThriftResultSetQueueFactory.build_queue(
                row_set_type=execute_response.result_format,
                t_row_set=t_row_set,
                arrow_schema_bytes=execute_response.arrow_schema_bytes or b"",
                max_download_threads=max_download_threads,
                lz4_compressed=execute_response.lz4_compressed,
                description=execute_response.description,
                ssl_options=ssl_options,
                session_id_hex=connection.get_session_id_hex(),
                statement_id=execute_response.command_id.to_hex_guid(),
                chunk_id=self.num_chunks,
                http_client=connection.http_client,
            )
            if t_row_set.resultLinks:
                self.num_chunks += len(t_row_set.resultLinks)

        # Call parent constructor with common attributes
        super().__init__(
            connection=connection,
            backend=thrift_client,
            arraysize=arraysize,
            buffer_size_bytes=buffer_size_bytes,
            command_id=execute_response.command_id,
            status=execute_response.status,
            has_been_closed_server_side=execute_response.has_been_closed_server_side,
            has_more_rows=has_more_rows,
            results_queue=results_queue,
            description=execute_response.description,
            is_staging_operation=execute_response.is_staging_operation,
            lz4_compressed=execute_response.lz4_compressed,
            arrow_schema_bytes=execute_response.arrow_schema_bytes,
            num_modified_rows=execute_response.num_modified_rows,
        )

        # Initialize results queue if not provided
        if not self.results:
            self._fill_results_buffer()

    def _fill_results_buffer(self):
        results, has_more_rows, result_links_count = self.backend.fetch_results(
            command_id=self.command_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            expected_row_start_offset=self._next_row_index,
            lz4_compressed=self.lz4_compressed,
            arrow_schema_bytes=self._arrow_schema_bytes,
            description=self.description,
            use_cloud_fetch=self._use_cloud_fetch,
            chunk_id=self.num_chunks,
        )
        self.results = results
        self.has_more_rows = has_more_rows
        self.num_chunks += result_links_count

    def _convert_columnar_table(self, table):
        column_names = [c[0] for c in self.description]
        ResultRow = Row(*column_names)
        result = []
        for row_index in range(table.num_rows):
            curr_row = []
            for col_index in range(table.num_columns):
                curr_row.append(table.get_item(col_index, row_index))
            result.append(ResultRow(*curr_row))

        return result

    def _prepend_buffered(self, buffered, rest):
        """Concatenate rows drained from the fetchone() buffer with freshly fetched ones."""
        if buffered is None:
            return rest
        if rest is None or rest.num_rows == 0:
            return buffered
        return concat_table_chunks([buffered, rest])

    def fetchmany_arrow(self, size: int) -> "pyarrow.Table":
        """
        Fetch the next set of rows of a query result, returning a PyArrow table.

        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")
        buffered = self._row_buffer_take_raw(size)
        remaining = size - (buffered.num_rows if buffered is not None else 0)
        rest = self._fetchmany_arrow_from_queue(remaining) if remaining > 0 else None
        if buffered is None and rest is None:
            rest = self._fetchmany_arrow_from_queue(0)
        return self._prepend_buffered(buffered, rest)

    def fetchmany_columnar(self, size: int):
        """
        Fetch the next set of rows of a query result, returning a Columnar Table.
        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")
        buffered = self._row_buffer_take_raw(size)
        remaining = size - (buffered.num_rows if buffered is not None else 0)
        rest = (
            self._fetchmany_columnar_from_queue(remaining) if remaining > 0 else None
        )
        if buffered is None and rest is None:
            rest = self._fetchmany_columnar_from_queue(0)
        return self._prepend_buffered(buffered, rest)

    def _fetchmany_arrow_from_queue(self, size: int) -> "pyarrow.Table":
        """Fetch up to ``size`` rows from the results queue, bypassing the row buffer."""
        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")

        # Hold 0-row chunks aside instead of appending them to ``partial_result_chunks``.
        # CloudFetchQueue may return a placeholder empty table whose schema does not
        # match the real downloaded chunks; concatenating it would corrupt the result.
        partial_result_chunks: List["pyarrow.Table"] = []
        zero_row_table: Optional["pyarrow.Table"] = None
        n_remaining_rows = size

        results = self.results.next_n_rows(size)
        if results.num_rows == 0:
            zero_row_table = results
        else:
            partial_result_chunks.append(results)
            n_remaining_rows -= results.num_rows
            self._next_row_index += results.num_rows

        while (
            n_remaining_rows > 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            if partial_results.num_rows == 0:
                continue
            partial_result_chunks.append(partial_results)
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        if not partial_result_chunks:
            partial_result_chunks.append(zero_row_table)
        return concat_table_chunks(partial_result_chunks)

    def _fetchmany_columnar_from_queue(self, size: int):
        """Fetch up to ``size`` rows from the results queue, bypassing the row buffer."""
        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")

        results = self.results.next_n_rows(size)
        n_remaining_rows = size - results.num_rows
        self._next_row_index += results.num_rows
        partial_result_chunks = [results]
        while (
            n_remaining_rows > 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            partial_result_chunks.append(partial_results)
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        return concat_table_chunks(partial_result_chunks)

    def _fetch_available_from_queue(self, size: int):
        """Fetch up to ``size`` rows the queue already holds, for fetchone() refills.

        Unlike ``_fetchmany_*_from_queue``, this does not keep calling the server
        until ``size`` rows are collected: it only refills the queue while it is
        empty, so fetchone() makes no more round-trips than it needs to return one
        row (server pages can be smaller than ``size`` when they hit the byte limit).
        On CloudFetch, the request is also capped at what the current chunk still
        holds (see ``_cap_to_current_chunk``), so a refill never stitches rows from
        several downloaded files into one batch. This only bounds the batch: reading
        to the end of a chunk still makes ``next_n_rows`` advance to (and wait for)
        the next file before it returns, exactly as the uncapped path did.
        Only called while the fetchone() row buffer is empty.
        """
        results = self.results.next_n_rows(self._cap_to_current_chunk(size))
        while (
            results.num_rows == 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            results = self.results.next_n_rows(self._cap_to_current_chunk(size))
        self._next_row_index += results.num_rows
        return results

    def fetchall_arrow(self) -> "pyarrow.Table":
        """Fetch all (remaining) rows of a query result, returning them as a PyArrow table."""
        buffered = self._row_buffer_take_raw()
        result_table = self._prepend_buffered(
            buffered, self._fetchall_arrow_from_queue()
        )
        # If PyArrow is installed and we have a ColumnTable result, convert it to PyArrow Table
        # Valid only for metadata commands result set
        if isinstance(result_table, ColumnTable) and pyarrow:
            data = {
                name: col
                for name, col in zip(
                    result_table.column_names, result_table.column_table
                )
            }
            return pyarrow.Table.from_pydict(data)
        return result_table

    def _fetchall_arrow_from_queue(self):
        """Drain the results queue (bypassing the row buffer) without type conversion."""
        # Hold 0-row chunks aside instead of appending them to ``partial_result_chunks``.
        # CloudFetchQueue may return a placeholder empty table whose schema does not
        # match the real downloaded chunks; concatenating it would corrupt the result.
        partial_result_chunks: List = []
        zero_row_table: Optional["pyarrow.Table"] = None

        results = self.results.remaining_rows()
        if results.num_rows == 0:
            zero_row_table = results
        else:
            partial_result_chunks.append(results)
            self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            if partial_results.num_rows == 0:
                continue
            partial_result_chunks.append(partial_results)
            self._next_row_index += partial_results.num_rows

        if not partial_result_chunks:
            partial_result_chunks.append(zero_row_table)

        return concat_table_chunks(partial_result_chunks)

    def fetchall_columnar(self):
        """Fetch all (remaining) rows of a query result, returning them as a Columnar table."""
        buffered = self._row_buffer_take_raw()
        return self._prepend_buffered(buffered, self._fetchall_columnar_from_queue())

    def _fetchall_columnar_from_queue(self):
        """Drain the results queue as a Columnar table, bypassing the row buffer."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows
        partial_result_chunks = [results]
        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            partial_result_chunks.append(partial_results)
            self._next_row_index += partial_results.num_rows

        return concat_table_chunks(partial_result_chunks)

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        """
        if isinstance(self.results, ColumnQueue):
            return self._fetchone_buffered(
                self._fetch_available_from_queue, self._convert_columnar_table
            )
        return self._fetchone_buffered(
            self._fetch_available_from_queue, self._convert_arrow_table
        )

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result, returning them as a list of rows.
        """
        if isinstance(self.results, ColumnQueue):
            return self._fetch_rows_with_buffer(
                None, lambda _: self._convert_columnar_table(self.fetchall_columnar())
            )
        return self._fetch_rows_with_buffer(
            None, lambda _: self._convert_arrow_table(self.fetchall_arrow())
        )

    def fetchmany(self, size: int) -> List[Row]:
        """
        Fetch the next set of rows of a query result, returning a list of rows.

        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")
        if isinstance(self.results, ColumnQueue):
            return self._fetch_rows_with_buffer(
                size, lambda n: self._convert_columnar_table(self.fetchmany_columnar(n))
            )
        return self._fetch_rows_with_buffer(
            size, lambda n: self._convert_arrow_table(self.fetchmany_arrow(n))
        )

    @staticmethod
    def _get_schema_description(table_schema_message):
        """
        Takes a TableSchema message and returns a description 7-tuple as specified by PEP-249
        """

        def map_col_type(type_):
            if type_.startswith("decimal"):
                return "decimal"
            else:
                return type_

        return [
            (column.name, map_col_type(column.datatype), None, None, None, None, None)
            for column in table_schema_message.columns
        ]
