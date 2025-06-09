from abc import ABC, abstractmethod
from typing import List, Optional, Any, Union, Tuple, TYPE_CHECKING

import logging
import time
import pandas

from databricks.sql.backend.sea.backend import SeaDatabricksClient

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
    from databricks.sql.client import Connection
from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import Row
from databricks.sql.exc import Error, RequestError, CursorAlreadyClosedError
from databricks.sql.utils import ColumnTable, ColumnQueue
from databricks.sql.backend.types import CommandId, CommandState, ExecuteResponse

logger = logging.getLogger(__name__)


class ResultSet(ABC):
    """
    Abstract base class for result sets returned by different backend implementations.

    This class defines the interface that all concrete result set implementations must follow.
    """

    def __init__(
        self,
        connection: "Connection",
        backend: "DatabricksClient",
        arraysize: int,
        buffer_size_bytes: int,
        command_id: CommandId,
        status: CommandState,
        has_been_closed_server_side: bool = False,
        has_more_rows: bool = False,
        results_queue=None,
        description=None,
        is_staging_operation: bool = False,
    ):
        """
        A ResultSet manages the results of a single command.

        Args:
            connection: The parent connection
            backend: The backend client
            arraysize: The max number of rows to fetch at a time (PEP-249)
            buffer_size_bytes: The size (in bytes) of the internal buffer + max fetch
            command_id: The command ID
            status: The command status
            has_been_closed_server_side: Whether the command has been closed on the server
            has_more_rows: Whether the command has more rows
            results_queue: The results queue
            description: column description of the results
            is_staging_operation: Whether the command is a staging operation
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

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row:
                yield row
            else:
                break

    @property
    def rownumber(self):
        return self._next_row_index

    @property
    def is_staging_operation(self) -> bool:
        """Whether this result set represents a staging operation."""
        return self._is_staging_operation

    # Define abstract methods that concrete implementations must implement
    @abstractmethod
    def _fill_results_buffer(self):
        """Fill the results buffer from the backend."""
        pass

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
            self.has_been_closed_server_side = True
            self.status = CommandState.CLOSED


class ThriftResultSet(ResultSet):
    """ResultSet implementation for the Thrift backend."""

    def __init__(
        self,
        connection: "Connection",
        execute_response: "ExecuteResponse",
        thrift_client: "ThriftDatabricksClient",
        buffer_size_bytes: int = 104857600,
        arraysize: int = 10000,
        use_cloud_fetch: bool = True,
    ):
        """
        Initialize a ThriftResultSet with direct access to the ThriftDatabricksClient.

        Args:
            connection: The parent connection
            execute_response: Response from the execute command
            thrift_client: The ThriftDatabricksClient instance for direct access
            buffer_size_bytes: Buffer size for fetching results
            arraysize: Default number of rows to fetch
            use_cloud_fetch: Whether to use cloud fetch for retrieving results
        """
        # Initialize ThriftResultSet-specific attributes
        self._arrow_schema_bytes = execute_response.arrow_schema_bytes
        self._use_cloud_fetch = use_cloud_fetch
        self.lz4_compressed = execute_response.lz4_compressed

        # Call parent constructor with common attributes
        super().__init__(
            connection=connection,
            backend=thrift_client,
            arraysize=arraysize,
            buffer_size_bytes=buffer_size_bytes,
            command_id=execute_response.command_id,
            status=execute_response.status,
            has_been_closed_server_side=execute_response.has_been_closed_server_side,
            has_more_rows=execute_response.has_more_rows,
            results_queue=execute_response.results_queue,
            description=execute_response.description,
            is_staging_operation=execute_response.is_staging_operation,
        )

        # Initialize results queue if not provided
        if not self.results:
            self._fill_results_buffer()

    def _fill_results_buffer(self):
        results, has_more_rows = self.backend.fetch_results(
            command_id=self.command_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            expected_row_start_offset=self._next_row_index,
            lz4_compressed=self.lz4_compressed,
            arrow_schema_bytes=self._arrow_schema_bytes,
            description=self.description,
            use_cloud_fetch=self._use_cloud_fetch,
        )
        self.results = results
        self.has_more_rows = has_more_rows

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

    def merge_columnar(self, result1, result2) -> "ColumnTable":
        """
        Function to merge / combining the columnar results into a single result
        :param result1:
        :param result2:
        :return:
        """

        if result1.column_names != result2.column_names:
            raise ValueError("The columns in the results don't match")

        merged_result = [
            result1.column_table[i] + result2.column_table[i]
            for i in range(result1.num_columns)
        ]
        return ColumnTable(merged_result, result1.column_names)

    def fetchmany_arrow(self, size: int) -> "pyarrow.Table":
        """
        Fetch the next set of rows of a query result, returning a PyArrow table.

        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError("size argument for fetchmany is %s but must be >= 0", size)
        results = self.results.next_n_rows(size)
        n_remaining_rows = size - results.num_rows
        self._next_row_index += results.num_rows

        while (
            n_remaining_rows > 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            results = pyarrow.concat_tables([results, partial_results])
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        return results

    def fetchmany_columnar(self, size: int):
        """
        Fetch the next set of rows of a query result, returning a Columnar Table.
        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError("size argument for fetchmany is %s but must be >= 0", size)

        results = self.results.next_n_rows(size)
        n_remaining_rows = size - results.num_rows
        self._next_row_index += results.num_rows

        while (
            n_remaining_rows > 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            results = self.merge_columnar(results, partial_results)
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        return results

    def fetchall_arrow(self) -> "pyarrow.Table":
        """Fetch all (remaining) rows of a query result, returning them as a PyArrow table."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            if isinstance(results, ColumnTable) and isinstance(
                partial_results, ColumnTable
            ):
                results = self.merge_columnar(results, partial_results)
            else:
                results = pyarrow.concat_tables([results, partial_results])
            self._next_row_index += partial_results.num_rows

        # If PyArrow is installed and we have a ColumnTable result, convert it to PyArrow Table
        # Valid only for metadata commands result set
        if isinstance(results, ColumnTable) and pyarrow:
            data = {
                name: col
                for name, col in zip(results.column_names, results.column_table)
            }
            return pyarrow.Table.from_pydict(data)
        return results

    def fetchall_columnar(self):
        """Fetch all (remaining) rows of a query result, returning them as a Columnar table."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            results = self.merge_columnar(results, partial_results)
            self._next_row_index += partial_results.num_rows

        return results

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        """
        if isinstance(self.results, ColumnQueue):
            res = self._convert_columnar_table(self.fetchmany_columnar(1))
        else:
            res = self._convert_arrow_table(self.fetchmany_arrow(1))

        if len(res) > 0:
            return res[0]
        else:
            return None

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result, returning them as a list of rows.
        """
        if isinstance(self.results, ColumnQueue):
            return self._convert_columnar_table(self.fetchall_columnar())
        else:
            return self._convert_arrow_table(self.fetchall_arrow())

    def fetchmany(self, size: int) -> List[Row]:
        """
        Fetch the next set of rows of a query result, returning a list of rows.

        An empty sequence is returned when no more rows are available.
        """
        if isinstance(self.results, ColumnQueue):
            return self._convert_columnar_table(self.fetchmany_columnar(size))
        else:
            return self._convert_arrow_table(self.fetchmany_arrow(size))

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
