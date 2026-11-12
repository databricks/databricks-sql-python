from __future__ import annotations
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    Sequence,
    TYPE_CHECKING,
)

from dateutil import parser
import datetime
import decimal
from abc import ABC, abstractmethod
from collections import OrderedDict, namedtuple
from collections.abc import Mapping, Set as AbstractSet
from decimal import Decimal
from enum import Enum
import mmap
import re
import secrets

import lz4.frame

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql import OperationalError
from databricks.sql.exc import ProgrammingError
from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager

if TYPE_CHECKING:
    # Type-annotation-only imports (deferred by ``from __future__ import
    # annotations``). ``TSparkRowSetType`` is used at runtime only inside
    # ``ThriftResultSetQueueFactory.build_queue``, which is a Thrift-only code
    # path and imports it locally. This keeps the Apache Thrift ``thrift``
    # package out of the SEA/kernel load path (see ``test_lazy_thrift_import``).
    from databricks.sql.thrift_api.TCLIService.ttypes import (
        TRowSet,
        TSparkArrowResultLink,
        TSparkRowSetType,
    )
from databricks.sql.types import SSLOptions
from databricks.sql.backend.types import CommandId
from databricks.sql.telemetry.models.event import StatementType
from databricks.sql.parameters.native import ParameterStructure, TDbsqlParameter

import logging

BIT_MASKS = [1, 2, 4, 8, 16, 32, 64, 128]
DEFAULT_ERROR_CONTEXT = "Unknown error"

logger = logging.getLogger(__name__)


def escape_metadata_catalog_name(
    catalog_name: Optional[str], enabled: bool
) -> Optional[str]:
    r"""Match JDBC: escape _ unless immediately preceded by \; preserve %."""
    if not enabled or catalog_name is None:
        return catalog_name
    return re.sub(r"(?<!\\)_", r"\\_", catalog_name)


def get_session_config_value(
    session_configuration: Optional[Dict[str, Any]], key: str
) -> Optional[str]:
    """Get a session configuration value with case-insensitive key matching"""
    if not session_configuration:
        return None

    key_upper = key.upper()
    for k, v in session_configuration.items():
        if k.upper() == key_upper:
            return str(v) if v is not None else None

    return None


class ResultSetQueue(ABC):
    @abstractmethod
    def next_n_rows(self, num_rows: int):
        pass

    @abstractmethod
    def remaining_rows(self):
        pass

    @abstractmethod
    def close(self):
        pass


class ThriftResultSetQueueFactory(ABC):
    @staticmethod
    def build_queue(
        row_set_type: TSparkRowSetType,
        t_row_set: TRowSet,
        arrow_schema_bytes: bytes,
        max_download_threads: int,
        ssl_options: SSLOptions,
        session_id_hex: Optional[str],
        statement_id: str,
        chunk_id: int,
        http_client,
        lz4_compressed: bool = True,
        description: List[Tuple] = [],
    ) -> ResultSetQueue:
        """
        Factory method to build a result set queue for Thrift backend.

        Args:
            row_set_type (enum): Row set type (Arrow, Column, or URL).
            t_row_set (TRowSet): Result containing arrow batches, columns, or cloud fetch links.
            arrow_schema_bytes (bytes): Bytes representing the arrow schema.
            lz4_compressed (bool): Whether result data has been lz4 compressed.
            description (List[List[Any]]): Hive table schema description.
            max_download_threads (int): Maximum number of downloader thread pool threads.
            ssl_options (SSLOptions): SSLOptions object for CloudFetchQueue

        Returns:
            ResultSetQueue
        """

        # Function-local import: this factory is only used by the Thrift
        # backend, so deferring keeps ``thrift`` out of the SEA/kernel path.
        from databricks.sql.thrift_api.TCLIService.ttypes import TSparkRowSetType

        if row_set_type == TSparkRowSetType.ARROW_BASED_SET:
            arrow_table, n_valid_rows = convert_arrow_based_set_to_arrow_table(
                t_row_set.arrowBatches, lz4_compressed, arrow_schema_bytes
            )
            converted_arrow_table = convert_decimals_in_arrow_table(
                arrow_table, description
            )
            return ArrowQueue(converted_arrow_table, n_valid_rows)
        elif row_set_type == TSparkRowSetType.COLUMN_BASED_SET:
            column_table, column_names = convert_column_based_set_to_column_table(
                t_row_set.columns, description
            )

            converted_column_table = convert_to_assigned_datatypes_in_column_table(
                column_table, description
            )

            return ColumnQueue(ColumnTable(converted_column_table, column_names))
        elif row_set_type == TSparkRowSetType.URL_BASED_SET:
            return ThriftCloudFetchQueue(
                schema_bytes=arrow_schema_bytes,
                start_row_offset=t_row_set.startRowOffset,
                result_links=t_row_set.resultLinks,
                lz4_compressed=lz4_compressed,
                description=description,
                max_download_threads=max_download_threads,
                ssl_options=ssl_options,
                session_id_hex=session_id_hex,
                statement_id=statement_id,
                chunk_id=chunk_id,
                http_client=http_client,
            )
        else:
            raise AssertionError("Row set type is not valid")


class ColumnTable:
    def __init__(self, column_table, column_names):
        self.column_table = column_table
        self.column_names = column_names

    @property
    def num_rows(self):
        if len(self.column_table) == 0:
            return 0
        else:
            return len(self.column_table[0])

    @property
    def num_columns(self):
        return len(self.column_names)

    def get_item(self, col_index, row_index):
        return self.column_table[col_index][row_index]

    def slice(self, curr_index, length):
        sliced_column_table = [
            column[curr_index : curr_index + length] for column in self.column_table
        ]
        return ColumnTable(sliced_column_table, self.column_names)

    def __eq__(self, other):
        return (
            self.column_table == other.column_table
            and self.column_names == other.column_names
        )


class ColumnQueue(ResultSetQueue):
    def __init__(self, column_table: ColumnTable):
        self.column_table = column_table
        self.cur_row_index = 0
        self.n_valid_rows = column_table.num_rows

    def next_n_rows(self, num_rows):
        length = min(num_rows, self.n_valid_rows - self.cur_row_index)

        slice = self.column_table.slice(self.cur_row_index, length)
        self.cur_row_index += slice.num_rows
        return slice

    def remaining_rows(self):
        slice = self.column_table.slice(
            self.cur_row_index, self.n_valid_rows - self.cur_row_index
        )
        self.cur_row_index += slice.num_rows
        return slice

    def close(self):
        return


class ArrowQueue(ResultSetQueue):
    def __init__(
        self,
        arrow_table: "pyarrow.Table",
        n_valid_rows: int,
        start_row_index: int = 0,
    ):
        """
        A queue-like wrapper over an Arrow table

        :param arrow_table: The Arrow table from which we want to take rows
        :param n_valid_rows: The index of the last valid row in the table
        :param start_row_index: The first row in the table we should start fetching from
        """

        self.cur_row_index = start_row_index
        self.arrow_table = arrow_table
        self.n_valid_rows = n_valid_rows

    def next_n_rows(self, num_rows: int) -> "pyarrow.Table":
        """Get upto the next n rows of the Arrow dataframe"""

        length = min(num_rows, self.n_valid_rows - self.cur_row_index)
        # Note that the table.slice API is not the same as Python's slice
        # The second argument should be length, not end index
        slice = self.arrow_table.slice(self.cur_row_index, length)
        self.cur_row_index += slice.num_rows
        return slice

    def remaining_rows(self) -> "pyarrow.Table":
        slice = self.arrow_table.slice(
            self.cur_row_index, self.n_valid_rows - self.cur_row_index
        )
        self.cur_row_index += slice.num_rows
        return slice

    def close(self):
        return


class CloudFetchQueue(ResultSetQueue, ABC):
    """Base class for cloud fetch queues that handle EXTERNAL_LINKS disposition with ARROW format."""

    def __init__(
        self,
        max_download_threads: int,
        ssl_options: SSLOptions,
        session_id_hex: Optional[str],
        statement_id: str,
        chunk_id: int,
        http_client,
        schema_bytes: Optional[bytes] = None,
        lz4_compressed: bool = True,
        description: List[Tuple] = [],
    ):
        """
        Initialize the base CloudFetchQueue.

        Args:
            max_download_threads: Maximum number of download threads
            ssl_options: SSL options for downloads
            schema_bytes: Arrow schema bytes
            lz4_compressed: Whether the data is LZ4 compressed
            description: Column descriptions
        """

        self.schema_bytes = schema_bytes
        self.max_download_threads = max_download_threads
        self.lz4_compressed = lz4_compressed
        self.description = description
        self._ssl_options = ssl_options
        self.session_id_hex = session_id_hex
        self.statement_id = statement_id
        self.chunk_id = chunk_id
        self._http_client = http_client

        # Table state
        self.table = None
        self.table_row_index = 0

        # Initialize download manager
        self.download_manager = ResultFileDownloadManager(
            links=[],
            max_download_threads=max_download_threads,
            lz4_compressed=lz4_compressed,
            ssl_options=ssl_options,
            session_id_hex=session_id_hex,
            statement_id=statement_id,
            chunk_id=chunk_id,
            http_client=http_client,
        )

    def next_n_rows(self, num_rows: int) -> "pyarrow.Table":
        """
        Get up to the next n rows of the cloud fetch Arrow dataframes.

        Args:
            num_rows (int): Number of rows to retrieve.
        Returns:
            pyarrow.Table
        """

        if not self.table:
            logger.debug("CloudFetchQueue: no more rows available")
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()
        logger.debug("CloudFetchQueue: trying to get {} next rows".format(num_rows))
        results = self.table.slice(0, 0)
        partial_result_chunks = [results]
        while num_rows > 0 and self.table:
            # Get remaining of num_rows or the rest of the current table, whichever is smaller
            length = min(num_rows, self.table.num_rows - self.table_row_index)
            table_slice = self.table.slice(self.table_row_index, length)
            partial_result_chunks.append(table_slice)
            self.table_row_index += table_slice.num_rows

            # Replace current table with the next table if we are at the end of the current table
            if self.table_row_index == self.table.num_rows:
                self.table = self._create_next_table()
                self.table_row_index = 0
            num_rows -= table_slice.num_rows

        logger.debug("CloudFetchQueue: collected {} next rows".format(results.num_rows))
        return concat_table_chunks(partial_result_chunks)

    def remaining_rows(self) -> "pyarrow.Table":
        """
        Get all remaining rows of the cloud fetch Arrow dataframes.

        Returns:
            pyarrow.Table
        """

        if not self.table:
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()
        results = self.table.slice(0, 0)
        partial_result_chunks = [results]
        while self.table:
            table_slice = self.table.slice(
                self.table_row_index, self.table.num_rows - self.table_row_index
            )
            partial_result_chunks.append(table_slice)
            self.table_row_index += table_slice.num_rows
            self.table = self._create_next_table()
            self.table_row_index = 0
        return concat_table_chunks(partial_result_chunks)

    def _create_table_at_offset(self, offset: int) -> Union["pyarrow.Table", None]:
        """Create next table at the given row offset"""

        # Create next table by retrieving the logical next downloaded file, or return None to signal end of queue
        downloaded_file = self.download_manager.get_next_downloaded_file(offset)
        if not downloaded_file:
            logger.debug(
                "CloudFetchQueue: Cannot find downloaded file for row {}".format(offset)
            )
            # None signals no more Arrow tables can be built from the remaining handlers if any remain
            return None
        arrow_table = create_arrow_table_from_arrow_file(
            downloaded_file.file_bytes, self.description
        )

        # The server rarely prepares the exact number of rows requested by the client in cloud fetch.
        # Subsequently, we drop the extraneous rows in the last file if more rows are retrieved than requested
        if arrow_table.num_rows > downloaded_file.row_count:
            arrow_table = arrow_table.slice(0, downloaded_file.row_count)

        # At this point, whether the file has extraneous rows or not, the arrow table should have the correct num rows
        assert downloaded_file.row_count == arrow_table.num_rows

        return arrow_table

    @abstractmethod
    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        pass

    def _create_empty_table(self) -> "pyarrow.Table":
        """Create a 0-row table with just the schema bytes."""
        if not self.schema_bytes:
            return pyarrow.Table.from_pydict({})
        return create_arrow_table_from_arrow_file(self.schema_bytes, self.description)

    def close(self):
        self.download_manager._shutdown_manager()


class ThriftCloudFetchQueue(CloudFetchQueue):
    """Queue implementation for EXTERNAL_LINKS disposition with ARROW format for Thrift backend."""

    def __init__(
        self,
        schema_bytes,
        max_download_threads: int,
        ssl_options: SSLOptions,
        session_id_hex: Optional[str],
        statement_id: str,
        chunk_id: int,
        http_client,
        start_row_offset: int = 0,
        result_links: Optional[List[TSparkArrowResultLink]] = None,
        lz4_compressed: bool = True,
        description: List[Tuple] = [],
    ):
        """
        Initialize the Thrift CloudFetchQueue.

        Args:
            schema_bytes: Table schema in bytes
            max_download_threads: Maximum number of downloader thread pool threads
            ssl_options: SSL options for downloads
            start_row_offset: The offset of the first row of the cloud fetch links
            result_links: Links containing the downloadable URL and metadata
            lz4_compressed: Whether the files are lz4 compressed
            description: Hive table schema description
        """
        super().__init__(
            max_download_threads=max_download_threads,
            ssl_options=ssl_options,
            schema_bytes=schema_bytes,
            lz4_compressed=lz4_compressed,
            description=description,
            session_id_hex=session_id_hex,
            statement_id=statement_id,
            chunk_id=chunk_id,
            http_client=http_client,
        )

        self.start_row_index = start_row_offset
        self.result_links = result_links or []
        self.session_id_hex = session_id_hex
        self.statement_id = statement_id
        self.chunk_id = chunk_id

        logger.debug(
            "Initialize CloudFetch loader, row set start offset: {}, file list:".format(
                start_row_offset
            )
        )
        if self.result_links:
            for result_link in self.result_links:
                logger.debug(
                    "- start row offset: {}, row count: {}".format(
                        result_link.startRowOffset, result_link.rowCount
                    )
                )
                self.download_manager.add_link(result_link)

        # Initialize table and position
        self.table = self._create_next_table()

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        logger.debug(
            "ThriftCloudFetchQueue: Trying to get downloaded file for row {}".format(
                self.start_row_index
            )
        )
        arrow_table = self._create_table_at_offset(self.start_row_index)
        if arrow_table:
            self.start_row_index += arrow_table.num_rows
            logger.debug(
                "ThriftCloudFetchQueue: Found downloaded file, row count: {}, new start offset: {}".format(
                    arrow_table.num_rows, self.start_row_index
                )
            )
        return arrow_table


def _bound(min_x, max_x, x):
    """Bound x by [min_x, max_x]

    min_x or max_x being None means unbounded in that respective side.
    """
    if min_x is None and max_x is None:
        return x
    if min_x is None:
        return min(max_x, x)
    if max_x is None:
        return max(min_x, x)
    return min(max_x, max(min_x, x))


class NoRetryReason(Enum):
    OUT_OF_TIME = "out of time"
    OUT_OF_ATTEMPTS = "out of attempts"
    NOT_RETRYABLE = "non-retryable error"


class RequestErrorInfo(
    namedtuple(
        "RequestErrorInfo_", "error error_message retry_delay http_code method request"
    )
):
    @property
    def request_session_id(self):
        if hasattr(self.request, "sessionHandle"):
            return self.request.sessionHandle.sessionId.guid
        else:
            return None

    @property
    def request_query_id(self):
        if hasattr(self.request, "operationHandle"):
            return self.request.operationHandle.operationId.guid
        else:
            return None

    def full_info_logging_context(
        self, no_retry_reason, attempt, max_attempts, elapsed, max_duration
    ):
        log_base_data_dict = OrderedDict(
            [
                ("method", self.method),
                ("session-id", self.request_session_id),
                ("query-id", self.request_query_id),
                ("http-code", self.http_code),
                ("error-message", self.error_message),
                ("original-exception", str(self.error)),
            ]
        )

        log_base_data_dict["no-retry-reason"] = (
            no_retry_reason and no_retry_reason.value
        )
        log_base_data_dict["bounded-retry-delay"] = self.retry_delay
        log_base_data_dict["attempt"] = "{}/{}".format(attempt, max_attempts)
        log_base_data_dict["elapsed-seconds"] = "{}/{}".format(elapsed, max_duration)

        return log_base_data_dict

    def user_friendly_error_message(self, no_retry_reason, attempt, elapsed):
        # This should be kept at the level that is appropriate to return to a Redash user
        user_friendly_error_message = "Error during request to server"
        if self.error_message:
            user_friendly_error_message = "{}: {}".format(
                user_friendly_error_message, self.error_message
            )
        try:
            error_context = str(self.error)
        except:
            error_context = DEFAULT_ERROR_CONTEXT

        return user_friendly_error_message + ". " + error_context


# A probe token directly after the IN keyword (also matches NOT IN). Where Spark's grammar accepts a
# value directly after IN, in the IN predicate and in PIVOT, it requires a parenthesised list, so
# `IN ARRAY(...)` is a syntax error. (The one other place, `position(a IN b)`, takes a string and
# rejects an ARRAY too.) Requiring the marker to follow IN directly leaves `IN (%(p)s)` alone: there
# the caller's parentheses already form the list and an ARRAY value is a valid list element.
#
# This matches the SQL text rather than parsing it, so it is a heuristic. It skips an IN that ends
# a `--` comment line (see `_in_ends_line_comment`), but an IN inside a string literal or a block
# comment can still count as the keyword when a marker follows it.
_IN_MARKER_PROBE = re.compile(r"\bIN\s*\x00([0-9a-f]+):(\d+)\x00", re.IGNORECASE)


def _in_ends_line_comment(text: str, match: re.Match[str]) -> bool:
    """Whether the IN that `match` found looks like it is in a `--` comment, with the marker on a
    later line.

    This is a check on the text, not a parse. In that case the marker is most likely live code, so
    it keeps its ARRAY rendering. A marker on the same line as a commented IN is usually inside the
    comment too, so it is not checked. A `--` inside a string literal earlier on the line also
    counts, which falls back to the ARRAY rendering.
    """
    if not re.search(r"[\r\n]", match.group(0)):
        return False
    line_start = max(
        text.rfind("\n", 0, match.start()), text.rfind("\r", 0, match.start())
    )
    return "--" in text[line_start + 1 : match.start()]


def _renders_as_array(value: Any) -> bool:
    """Whether ParamEscaper.escape_item renders `value` as an ARRAY literal."""
    return isinstance(value, Sequence) and not isinstance(value, str)


def _is_set(value: Any) -> bool:
    """Whether `value` is a set, frozenset or dict key view. A set has no order, so it can only
    render as an IN list, never as an ARRAY."""
    return isinstance(value, AbstractSet)


def _renders_as_in_list(value: Any) -> bool:
    """Whether `value` renders as an IN list when its marker directly follows IN.

    bytes and bytearray are sequences of ints, so escape_item renders them as ARRAY literals of
    byte values. They are left out here, so `x IN %s` with bytes still fails loudly as it does on
    main, instead of quietly comparing x with the byte values.
    """
    if isinstance(value, (bytes, bytearray)):
        return False
    return _renders_as_array(value) or _is_set(value)


def _in_list_items(value: Any) -> Any:
    """Return the items to render for an IN list, with a set's items sorted.

    Sorting usually makes the SQL text the same from run to run, but not always: items without a
    consistent order, such as `float("nan")` among other numbers, can still come out in a
    different order. The IN list matches the same rows either way.
    """
    if not _is_set(value):
        return value
    try:
        return sorted(value)
    except TypeError:
        # The items do not compare with each other, for example None and an int.
        return sorted(value, key=repr)


class _InListOnly:
    """Stands in for an escaped set, which can only be rendered as an IN list.

    `render_inline` rejects a set at any other marker before formatting. Raising here as well
    keeps an unescaped set from ever reaching the SQL text.
    """

    def __init__(self, value: Any) -> None:
        self.value = value

    def __str__(self) -> str:
        raise ProgrammingError("Unsupported object {}".format(self.value))

    __repr__ = __str__


class _MarkerProbe:
    """Formats as a token that identifies the marker it was substituted into."""

    def __init__(self, nonce: str, index: int) -> None:
        self._token = f"\x00{nonce}:{index}\x00"

    def __str__(self) -> str:
        return self._token

    __repr__ = __str__


class _NumberedLookups:
    """Mapping stand-in for %-formatting that numbers its lookups.

    %-formatting looks up each %(name)s marker once, left to right, so lookup n is always the n-th
    named marker of the operation. `value_for(n, name)` supplies the value substituted there.
    """

    def __init__(
        self, value_for: Callable[[int, str], Any], escaped: Dict[str, Any]
    ) -> None:
        self._value_for = value_for
        self._escaped = escaped
        self._count = 0

    def __getitem__(self, name: str) -> Any:
        index = self._count
        self._count += 1
        return self._value_for(index, name)

    def __str__(self) -> str:
        # A bare %s marker formats the whole mapping; keep rendering the escaped dict there.
        return str(self._escaped)

    __repr__ = __str__


def _reject_sets_outside_in_lists(
    parameters: Any, in_list_markers: Set[int], marker_keys: Dict[int, Any]
) -> None:
    """Raise the error `escape_args` raises for a set, unless the set is used and every marker
    it fills renders as an IN list."""
    items = (
        parameters.items() if isinstance(parameters, dict) else enumerate(parameters)
    )
    for key, value in items:
        if not _is_set(value):
            continue
        markers = {index for index, k in marker_keys.items() if k == key}
        if not markers or not markers <= in_list_markers:
            raise ProgrammingError("Unsupported object {}".format(value))


def _format_markers(
    operation: str, escaped: Any, value_for: Callable[[int, Any], Any]
) -> str:
    """%-format `operation`, taking the value of marker n from `value_for(n, key)`.

    `escaped` holds the escaped parameter values. `key` is the marker's name when it is a dict and
    the marker's position otherwise.
    """
    values: Any
    if isinstance(escaped, dict):
        values = _NumberedLookups(value_for, escaped)
    else:
        values = tuple(value_for(i, i) for i in range(len(escaped)))
    return inject_parameters(operation, values)


# Taken from PyHive
class ParamEscaper:
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f %z"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise ProgrammingError("Unsupported param format: {}".format(parameters))

    def render_inline(self, operation: str, parameters) -> str:
        """Render `parameters` into the markers of `operation` as inline SQL literals.

        Values are escaped as `escape_args` escapes them, with one exception: a sequence at a
        marker that directly follows IN (e.g. `x IN %(ids)s`) renders as the parenthesised list
        Spark requires there, `x IN (1,2,3)`, instead of `x IN ARRAY(1,2,3)`, which Spark rejects.
        The choice is made per marker, so a parameter used both after IN and elsewhere keeps its
        ARRAY rendering at the other markers. Inside the list, tuples render as rows and lists as
        ARRAY literals.

        IN is found by matching the SQL text, not by parsing it (see `_IN_MARKER_PROBE`), so an
        unusual query, such as one with IN inside a string literal just before a marker, can
        still get a list where an ARRAY was meant.

        A set (or frozenset, or dict key view) is accepted only where every marker it fills
        directly follows IN, and its items are sorted. Anywhere else it raises the same
        `ProgrammingError` as `escape_args`, because a set has no order to put into an ARRAY.
        """
        escaped = self._escape_values(parameters)
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if not any(_renders_as_in_list(value) for value in values):
            # Only a sequence or a set can render as an IN list, so without one render exactly
            # as before.
            return inject_parameters(operation, escaped)

        in_list_markers, marker_keys = self._markers_following_in(
            operation, parameters, escaped
        )
        _reject_sets_outside_in_lists(parameters, in_list_markers, marker_keys)

        def render_marker(index: int, key: Any) -> Any:
            if index in in_list_markers:
                return self.escape_in_list(_in_list_items(parameters[key]))
            return escaped[key]

        return _format_markers(operation, escaped, render_marker)

    def _escape_values(self, parameters):
        """Escape like `escape_args`, except that a set becomes an `_InListOnly` placeholder
        instead of raising, so `render_inline` can decide where it is allowed."""

        def escape(value: Any) -> Any:
            return _InListOnly(value) if _is_set(value) else self.escape_item(value)

        if isinstance(parameters, dict):
            return {k: escape(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(escape(x) for x in parameters)
        else:
            raise ProgrammingError("Unsupported param format: {}".format(parameters))

    def _markers_following_in(
        self, operation: str, parameters, escaped
    ) -> Tuple[Set[int], Dict[int, Any]]:
        """Return the numbers of the markers that directly follow IN and receive a value that can
        render as an IN list, and the parameter key that each marker number refers to.

        Markers are numbered as `_format_markers` numbers them. To find them, the operation is
        formatted once through the same %-formatting as the real substitution, so `%%` and
        positional markers line up exactly. Each value that can render as an IN list (see
        `_renders_as_in_list`) is replaced by a probe token. Numbers keep their value, so
        markers such as `%d` still format, and escaped strings are replaced by an empty string
        literal, so the text of a value cannot change the result. If the operation does not
        format, both results are empty.
        """
        # The nonce stops text that only looks like a probe token from being taken for one.
        nonce = secrets.token_hex(8)
        marker_keys: Dict[int, Any] = {}

        def probe_marker(index: int, key: Any) -> Any:
            # A missing name raises KeyError here, as it does when rendering.
            escaped_value = escaped[key]
            marker_keys[index] = key
            value = parameters[key]
            if _renders_as_in_list(value):
                return _MarkerProbe(nonce, index)
            # Replace text with '' so the content of a value cannot change the result. A string
            # of two or more characters formats under exactly the markers '' does. A one-character
            # string can only be a single-digit Decimal, which %c accepts, so it is kept.
            if isinstance(escaped_value, str) and len(escaped_value) > 1:
                return "''"
            return escaped_value

        try:
            probed_operation = _format_markers(operation, escaped, probe_marker)
        except (KeyError, TypeError, ValueError):
            # Rendering raises the same error: a probe only replaces a sequence, and a marker that
            # cannot format a probe (such as %d) cannot format the sequence's ARRAY string either.
            # A set gets the ProgrammingError escape_args raises, since none of its markers count.
            return set(), {}

        in_list_markers = {
            int(match.group(2))
            for match in _IN_MARKER_PROBE.finditer(probed_operation)
            if match.group(1) == nonce
            and not _in_ends_line_comment(probed_operation, match)
        }
        return in_list_markers, marker_keys

    def escape_in_list(self, item):
        return "(" + ",".join(map(str, map(self._escape_in_list_element, item))) + ")"

    def _escape_in_list_element(self, item):
        # Inside an IN list a tuple is a row, as in `(a, b) IN ((1, 2), (3, 4))`. A list stays an
        # ARRAY literal so ARRAY columns can still be matched, as in `arr IN (ARRAY(1, 2))`.
        if isinstance(item, tuple):
            return self.escape_in_list(item)
        return self.escape_item(item)

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'"))

    def escape_sequence(self, item):
        l = map(self.escape_item, item)
        l = list(map(str, l))
        return "ARRAY(" + ",".join(l) + ")"

    def escape_mapping(self, item):
        l = map(
            self.escape_item,
            (element for key, value in item.items() for element in (key, value)),
        )
        l = list(map(str, l))
        return "MAP(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted.strip())

    def escape_decimal(self, item):
        return str(item)

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        elif isinstance(item, decimal.Decimal):
            return self.escape_decimal(item)
        elif isinstance(item, Sequence):
            return self.escape_sequence(item)
        elif isinstance(item, Mapping):
            return self.escape_mapping(item)
        else:
            raise ProgrammingError("Unsupported object {}".format(item))


def inject_parameters(operation: str, parameters: Dict[str, str]):
    return operation % parameters


def _dbsqlparameter_names(params: List[TDbsqlParameter]) -> list[str]:
    return [p.name if p.name else "" for p in params]


def _generate_named_interpolation_values(
    params: List[TDbsqlParameter],
) -> dict[str, str]:
    """Returns a dictionary of the form {name: ":name"} for each parameter in params"""

    names = _dbsqlparameter_names(params)

    return {name: f":{name}" for name in names}


def _may_contain_inline_positional_markers(operation: str) -> bool:
    """Check for the presence of `%s` in the operation string."""

    interpolated = operation.replace("%s", "?")
    return interpolated != operation


def _interpolate_named_markers(
    operation: str, parameters: List[TDbsqlParameter]
) -> str:
    """Replace all instances of `%(param)s` in `operation` with `:param`.

    If `operation` contains no instances of `%(param)s` then the input string is returned unchanged.

    ```
    "SELECT * FROM table WHERE field = %(field)s and other_field = %(other_field)s"
    ```

    Yields

    ```
    SELECT * FROM table WHERE field = :field and other_field = :other_field
    ```
    """

    _output_operation = operation

    PYFORMAT_PARAMSTYLE_REGEX = r"%\((\w+)\)s"
    pat = re.compile(PYFORMAT_PARAMSTYLE_REGEX)
    NAMED_PARAMSTYLE_FMT = ":{}"
    PYFORMAT_PARAMSTYLE_FMT = "%({})s"

    pyformat_markers = pat.findall(operation)
    for marker in pyformat_markers:
        pyformat_marker = PYFORMAT_PARAMSTYLE_FMT.format(marker)
        named_marker = NAMED_PARAMSTYLE_FMT.format(marker)
        _output_operation = _output_operation.replace(pyformat_marker, named_marker)

    return _output_operation


def transform_paramstyle(
    operation: str,
    parameters: List[TDbsqlParameter],
    param_structure: ParameterStructure,
) -> str:
    """
    Performs a Python string interpolation such that any occurence of `%(param)s` will be replaced with `:param`

    This utility function is built to assist users in the transition between the default paramstyle in
    this connector prior to version 3.0.0 (`pyformat`) and the new default paramstyle (`named`).

    Args:
        operation: The operation or SQL text to transform.
        parameters: The parameters to use for the transformation.

    Returns:
        str
    """

    output = operation
    if (
        param_structure == ParameterStructure.POSITIONAL
        and _may_contain_inline_positional_markers(operation)
    ):
        logger.warning(
            "It looks like this query may contain un-named query markers like `%s`"
            " This format is not supported when use_inline_params=False."
            " Use `?` instead or set use_inline_params=True"
        )
    elif param_structure == ParameterStructure.NAMED:
        output = _interpolate_named_markers(operation, parameters)

    return output


def create_arrow_table_from_arrow_file(
    file_bytes: bytes, description
) -> "pyarrow.Table":
    arrow_table = convert_arrow_based_file_to_arrow_table(file_bytes)
    return convert_decimals_in_arrow_table(arrow_table, description)


def convert_arrow_based_file_to_arrow_table(file_bytes: bytes):
    try:
        return pyarrow.ipc.open_stream(file_bytes).read_all()
    except Exception as e:
        raise RuntimeError("Failure to convert arrow based file to arrow table", e)


def convert_arrow_based_set_to_arrow_table(arrow_batches, lz4_compressed, schema_bytes):
    compressed_size = sum(len(arrow_batch.batch) for arrow_batch in arrow_batches)
    stream = _ArrowStreamBuffer(
        len(schema_bytes)
        + compressed_size * (_LZ4_RATIO_ESTIMATE if lz4_compressed else 1)
    )
    stream.append(schema_bytes)
    n_rows = 0
    for arrow_batch in arrow_batches:
        n_rows += arrow_batch.rowCount
        stream.append(
            lz4.frame.decompress(arrow_batch.batch)
            if lz4_compressed
            else arrow_batch.batch
        )
    arrow_table = pyarrow.ipc.open_stream(stream.getbuffer()).read_all()
    return arrow_table, n_rows


# Below glibc's default mmap threshold a dedicated mapping only adds a syscall.
_INLINE_ARROW_MMAP_MIN_BYTES = 128 * 1024
# Untouched pages of the reservation cost no memory; overflow grows the map.
_LZ4_RATIO_ESTIMATE = 4


class _ArrowStreamBuffer:
    """Accumulates an inline Arrow IPC stream in a private anonymous memory map.

    The table reads the stream zero-copy, so this buffer lives as long as the result.
    On the malloc heap such buffers often stayed resident after the result was freed
    (~1 GB per 1 GB of inline results); a mapping is returned to the OS. Small streams
    and mmap failures use a bytearray, as before.
    """

    def __init__(self, size_estimate: int):
        self._map: Optional[mmap.mmap] = None
        self._bytes = bytearray()
        self._size = 0
        if size_estimate >= _INLINE_ARROW_MMAP_MIN_BYTES:
            try:
                self._map = _private_anonymous_mmap(size_estimate)
            except (OSError, ValueError):
                pass

    def append(self, data: bytes) -> None:
        end = self._size + len(data)
        if self._map is not None and end > len(self._map):
            try:
                self._map.resize(max(end, 2 * len(self._map)))
            except (OSError, SystemError, ValueError):
                # No mremap (e.g. macOS): continue on the heap
                self._bytes += self._map[: self._size]
                self._map = None
        if self._map is not None:
            self._map[self._size : end] = data
        else:
            self._bytes += data
        self._size = end

    def getbuffer(self):
        if self._map is None:
            return self._bytes
        return pyarrow.py_buffer(self._map).slice(0, self._size)


def _private_anonymous_mmap(size: int) -> mmap.mmap:
    # Python defaults anonymous maps to MAP_SHARED (shmem, shared with forked children).
    if hasattr(mmap, "MAP_PRIVATE"):
        return mmap.mmap(-1, size, flags=mmap.MAP_PRIVATE)
    return mmap.mmap(-1, size)


def convert_decimals_in_arrow_table(table, description) -> "pyarrow.Table":
    new_columns = []
    new_fields = []

    for i, col in enumerate(table.itercolumns()):
        field = table.field(i)

        if description[i][1] == "decimal":
            precision, scale = description[i][4], description[i][5]
            assert scale is not None
            assert precision is not None
            # create the target decimal type
            dtype = pyarrow.decimal128(precision, scale)

            new_col = col.cast(dtype)
            new_field = field.with_type(dtype)

            new_columns.append(new_col)
            new_fields.append(new_field)
        else:
            new_columns.append(col)
            new_fields.append(field)

    new_schema = pyarrow.schema(new_fields)

    return pyarrow.Table.from_arrays(new_columns, schema=new_schema)


def convert_to_assigned_datatypes_in_column_table(column_table, description):

    converted_column_table = []
    for i, col in enumerate(column_table):
        if description[i][1] == "decimal":
            converted_column_table.append(
                tuple(v if v is None else Decimal(v) for v in col)
            )
        elif description[i][1] == "date":
            converted_column_table.append(
                tuple(v if v is None else datetime.date.fromisoformat(v) for v in col)
            )
        elif description[i][1] == "timestamp":
            converted_column_table.append(
                tuple((v if v is None else parser.parse(v)) for v in col)
            )
        else:
            converted_column_table.append(col)

    return converted_column_table


def convert_column_based_set_to_arrow_table(columns, description):
    arrow_table = pyarrow.Table.from_arrays(
        [_convert_column_to_arrow_array(c) for c in columns],
        # Only use the column names from the schema, the types are determined by the
        # physical types used in column based set, as they can differ from the
        # mapping used in _hive_schema_to_arrow_schema.
        names=[c[0] for c in description],
    )
    return arrow_table, arrow_table.num_rows


def convert_column_based_set_to_column_table(columns, description):
    column_names = [c[0] for c in description]
    column_table = [_convert_column_to_list(c) for c in columns]

    return column_table, column_names


def _convert_column_to_arrow_array(t_col):
    """
    Return a pyarrow array from the values in a TColumn instance.
    Note that ColumnBasedSet has no native support for complex types, so they will be converted
    to strings server-side.
    """
    field_name_to_arrow_type = {
        "boolVal": pyarrow.bool_(),
        "byteVal": pyarrow.int8(),
        "i16Val": pyarrow.int16(),
        "i32Val": pyarrow.int32(),
        "i64Val": pyarrow.int64(),
        "doubleVal": pyarrow.float64(),
        "stringVal": pyarrow.string(),
        "binaryVal": pyarrow.binary(),
    }
    for field in field_name_to_arrow_type.keys():
        wrapper = getattr(t_col, field)
        if wrapper:
            return _create_arrow_array(wrapper, field_name_to_arrow_type[field])

    raise OperationalError("Empty TColumn instance {}".format(t_col))


def _convert_column_to_list(t_col):
    SUPPORTED_FIELD_TYPES = (
        "boolVal",
        "byteVal",
        "i16Val",
        "i32Val",
        "i64Val",
        "doubleVal",
        "stringVal",
        "binaryVal",
    )

    for field in SUPPORTED_FIELD_TYPES:
        wrapper = getattr(t_col, field)
        if wrapper:
            return _create_python_tuple(wrapper)

    raise OperationalError("Empty TColumn instance {}".format(t_col))


def _create_arrow_array(t_col_value_wrapper, arrow_type):
    result = t_col_value_wrapper.values
    nulls = t_col_value_wrapper.nulls  # bitfield describing which values are null
    assert isinstance(nulls, bytes)

    # The number of bits in nulls can be both larger or smaller than the number of
    # elements in result, so take the minimum of both to iterate over.
    length = min(len(result), len(nulls) * 8)

    for i in range(length):
        if nulls[i >> 3] & BIT_MASKS[i & 0x7]:
            result[i] = None

    return pyarrow.array(result, type=arrow_type)


def _create_python_tuple(t_col_value_wrapper):
    result = t_col_value_wrapper.values
    nulls = t_col_value_wrapper.nulls  # bitfield describing which values are null
    assert isinstance(nulls, bytes)

    # The number of bits in nulls can be both larger or smaller than the number of
    # elements in result, so take the minimum of both to iterate over.
    length = min(len(result), len(nulls) * 8)

    for i in range(length):
        if nulls[i >> 3] & BIT_MASKS[i & 0x7]:
            result[i] = None

    return tuple(result)


def concat_table_chunks(
    table_chunks: List[Union["pyarrow.Table", ColumnTable]],
) -> Union["pyarrow.Table", ColumnTable]:
    if len(table_chunks) == 0:
        return table_chunks

    if isinstance(table_chunks[0], ColumnTable):
        ## Check if all have the same column names
        if not all(
            table.column_names == table_chunks[0].column_names for table in table_chunks
        ):
            raise ValueError("The columns in the results don't match")

        result_table: List[List[Any]] = [[] for _ in range(table_chunks[0].num_columns)]
        for i in range(0, len(table_chunks)):
            for j in range(table_chunks[i].num_columns):
                result_table[j].extend(table_chunks[i].column_table[j])
        return ColumnTable(result_table, table_chunks[0].column_names)
    else:
        return _concat_arrow_tables(table_chunks)


def _concat_arrow_tables(table_chunks: List["pyarrow.Table"]) -> "pyarrow.Table":
    """Concatenate Arrow tables, tolerant of the installed pyarrow version.

    ``promote_options`` was added in pyarrow 14.0.0, replacing the older
    ``promote`` boolean. The connector's declared floor is pyarrow>=14, but a
    base install (no ``[pyarrow]`` extra) can run against a runtime's older
    bundled pyarrow -- e.g. DBR 13.3 (12.x) / 14.3 -- where ``promote_options``
    raises ``TypeError: unexpected keyword argument``. Fall back to the legacy
    ``promote=True`` there (equivalent to ``promote_options="default"``).

    Schema promotion triggers Arrow's schema unification, which rejects
    duplicate field names (``ArrowInvalid: Can't unify schema with duplicate
    field names``) even when no promotion is actually needed -- breaking valid
    SQL like ``SELECT 1 AS a, 2 AS a`` or ``SELECT * FROM a JOIN b`` (GH #989).
    Promotion is only needed when chunk schemas actually differ (e.g. the
    nullability case from GH #418), so when every chunk shares the first
    chunk's schema we concatenate without promotion.

    Known limitation: a result set that has *both* duplicate column names *and*
    chunks whose schemas genuinely differ falls through to the promotion path
    and still re-raises ``ArrowInvalid: Can't unify schema with duplicate field
    names``. Arrow cannot unify a schema with duplicate field names, so there is
    no promotion-based fix for that combination without renaming fields; it is a
    narrow, pre-existing edge left unhandled here.
    """
    if all(table.schema.equals(table_chunks[0].schema) for table in table_chunks[1:]):
        return pyarrow.concat_tables(table_chunks)

    try:
        return pyarrow.concat_tables(table_chunks, promote_options="default")
    except TypeError:
        return pyarrow.concat_tables(table_chunks, promote=True)


def serialize_query_tags(
    query_tags: Optional[Dict[str, Optional[str]]],
) -> Optional[str]:
    """
    Serialize query_tags dictionary to a string format.

    Format: "key1:value1,key2:value2"
    Special cases:
    - If value is None, omit the colon and value (e.g., "key1:value1,key2,key3:value3")
    - Escape special characters (:, ,, \\) in values with a leading backslash
    - Backslashes in keys are escaped; other special characters in keys are not escaped

    Args:
        query_tags: Dictionary of query tags where keys are strings and values are optional strings

    Returns:
        Serialized string or None if query_tags is None or empty
    """
    if not query_tags:
        return None

    def escape_value(value: str) -> str:
        """Escape special characters in tag values."""
        # Escape backslash first to avoid double-escaping
        value = value.replace("\\", r"\\")
        # Escape colon and comma
        value = value.replace(":", r"\:")
        value = value.replace(",", r"\,")
        return value

    serialized_parts = []
    for key, value in query_tags.items():
        escaped_key = key.replace("\\", r"\\")
        if value is None:
            # No colon or value when value is None
            serialized_parts.append(escaped_key)
        else:
            escaped_value = escape_value(value)
            serialized_parts.append(f"{escaped_key}:{escaped_value}")

    return ",".join(serialized_parts)


def build_client_context(server_hostname: str, version: str, **kwargs):
    """Build ClientContext for HTTP client configuration."""
    from databricks.sql.auth.common import ClientContext
    from databricks.sql.types import SSLOptions

    # Extract SSL options
    ssl_options = SSLOptions(
        tls_verify=not kwargs.get("_tls_no_verify", False),
        tls_verify_hostname=kwargs.get("_tls_verify_hostname", True),
        tls_trusted_ca_file=kwargs.get("_tls_trusted_ca_file"),
        tls_client_cert_file=kwargs.get("_tls_client_cert_file"),
        tls_client_cert_key_file=kwargs.get("_tls_client_cert_key_file"),
        tls_client_cert_key_password=kwargs.get("_tls_client_cert_key_password"),
    )

    # Build user agent
    from databricks.sql.common.agent import detect as detect_agent

    user_agent_entry = kwargs.get("user_agent_entry", "")
    if user_agent_entry:
        user_agent = f"PyDatabricksSqlConnector/{version} ({user_agent_entry})"
    else:
        user_agent = f"PyDatabricksSqlConnector/{version}"

    agent_product = detect_agent()
    if agent_product:
        user_agent += f" agent/{agent_product}"

    # Explicitly construct ClientContext with proper types
    return ClientContext(
        hostname=server_hostname,
        ssl_options=ssl_options,
        user_agent=user_agent,
        socket_timeout=kwargs.get("_socket_timeout"),
        retry_stop_after_attempts_count=kwargs.get("_retry_stop_after_attempts_count"),
        retry_delay_min=kwargs.get("_retry_delay_min"),
        retry_delay_max=kwargs.get("_retry_delay_max"),
        retry_stop_after_attempts_duration=kwargs.get(
            "_retry_stop_after_attempts_duration"
        ),
        retry_delay_default=kwargs.get("_retry_delay_default"),
        retry_dangerous_codes=kwargs.get("_retry_dangerous_codes"),
        respect_server_retry_after_header=kwargs.get(
            "_respect_server_retry_after_header"
        ),
        proxy_auth_method=kwargs.get("_proxy_auth_method"),
        pool_connections=kwargs.get("_pool_connections"),
        pool_maxsize=kwargs.get("_pool_maxsize"),
        telemetry_circuit_breaker_enabled=kwargs.get(
            "_telemetry_circuit_breaker_enabled"
        ),
    )
