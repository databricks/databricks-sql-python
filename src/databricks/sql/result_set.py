from abc import ABC, abstractmethod
from typing import List, Optional, Any, Union, Tuple

import logging
import time
import pandas

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import Row
from databricks.sql.exc import Error, RequestError, CursorAlreadyClosedError
from databricks.sql.utils import ExecuteResponse, ColumnTable, ColumnQueue

logger = logging.getLogger(__name__)


class ResultSet(ABC):
    """
    Abstract base class for result sets returned by different backend implementations.

    This class defines the interface that all concrete result set implementations must follow.
    """

    def __init__(self, connection, backend, arraysize: int, buffer_size_bytes: int):
        """Initialize the base ResultSet with common properties."""
        self.connection = connection
        self.backend = backend  # Store the backend client directly
        self.arraysize = arraysize
        self.buffer_size_bytes = buffer_size_bytes
        self._next_row_index = 0
        self.description: Optional[Any] = None

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
    @abstractmethod
    def is_staging_operation(self) -> bool:
        """Whether this result set represents a staging operation."""
        pass

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
    def fetchmany_arrow(self, size: int) -> Any:
        """Fetch the next set of rows as an Arrow table."""
        pass

    @abstractmethod
    def fetchall_arrow(self) -> Any:
        """Fetch all remaining rows as an Arrow table."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the result set and release any resources."""
        pass


class ThriftResultSet(ResultSet):
    """ResultSet implementation for the Thrift backend."""

    def __init__(
        self,
        connection,
        execute_response: ExecuteResponse,
        thrift_client,  # Pass the specific ThriftDatabricksClient instance
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
        super().__init__(connection, thrift_client, arraysize, buffer_size_bytes)

        # Initialize ThriftResultSet-specific attributes
        self.command_id = execute_response.command_id
        self.op_state = execute_response.status
        self.has_been_closed_server_side = execute_response.has_been_closed_server_side
        self.has_more_rows = execute_response.has_more_rows
        self.lz4_compressed = execute_response.lz4_compressed
        self.description = execute_response.description
        self._arrow_schema_bytes = execute_response.arrow_schema_bytes
        self._use_cloud_fetch = use_cloud_fetch
        self._is_staging_operation = execute_response.is_staging_operation

        # Initialize results queue
        if execute_response.arrow_queue:
            self.results = execute_response.arrow_queue
        else:
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

    def merge_columnar(self, result1, result2):
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

    def close(self) -> None:
        """
        Close the cursor.

        If the connection has not been closed, and the cursor has not already
        been closed on the server for some other reason, issue a request to the server to close it.
        """
        try:
            if (
                self.op_state != ttypes.TOperationState.CLOSED_STATE
                and not self.has_been_closed_server_side
                and self.connection.open
            ):
                self.backend.close_command(self.command_id)
        except RequestError as e:
            if isinstance(e.args[1], CursorAlreadyClosedError):
                logger.info("Operation was canceled by a prior request")
        finally:
            self.has_been_closed_server_side = True
            self.op_state = ttypes.TOperationState.CLOSED_STATE

    @property
    def is_staging_operation(self) -> bool:
        """Whether this result set represents a staging operation."""
        return self._is_staging_operation
