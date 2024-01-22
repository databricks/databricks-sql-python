from typing import List, Optional, TYPE_CHECKING

import pandas
import pyarrow


from databricks.sql import *
from databricks.sql import __version__
from databricks.sql.exc import (
    CursorAlreadyClosedError,
)
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import Row
from databricks.sql.utils import ExecuteResponse

if TYPE_CHECKING:
    from databricks.sql.ae import AsyncExecution, AsyncExecutionStatus
    from databricks.sql.client import Connection
    from databricks.sql.thrift_backend import ThriftBackend

import logging

logger = logging.getLogger(__name__)

# TODO: this is duplicated from client.py to avoid ImportError. Fix this.
DEFAULT_RESULT_BUFFER_SIZE_BYTES = 104857600


class ResultSet:
    def __init__(
        self,
        connection: "Connection",
        execute_response: ExecuteResponse,
        thrift_backend: "ThriftBackend",
        result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
        arraysize: int = 10000,
    ):
        """
        A ResultSet manages the results of a single command.

        :param connection: The parent connection that was used to execute this command
        :param execute_response: A `ExecuteResponse` class returned by a command execution
        :param result_buffer_size_bytes: The size (in bytes) of the internal buffer + max fetch
        amount :param arraysize: The max number of rows to fetch at a time (PEP-249)
        """
        self.connection = connection
        self.command_id = execute_response.command_handle
        self.op_state = execute_response.status
        self.has_been_closed_server_side = execute_response.has_been_closed_server_side
        self.has_more_rows = execute_response.has_more_rows
        self.buffer_size_bytes = result_buffer_size_bytes
        self.lz4_compressed = execute_response.lz4_compressed
        self.arraysize = arraysize
        self.thrift_backend = thrift_backend
        self.description = execute_response.description
        self._arrow_schema_bytes = execute_response.arrow_schema_bytes
        self._next_row_index = 0

        if execute_response.arrow_queue:
            # In this case the server has taken the fast path and returned an initial batch of
            # results
            self.results = execute_response.arrow_queue
        else:
            # In this case, there are results waiting on the server so we fetch now for simplicity
            self._fill_results_buffer()

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row:
                yield row
            else:
                break

    def _fill_results_buffer(self):
        # At initialization or if the server does not have cloud fetch result links available
        results, has_more_rows = self.thrift_backend.fetch_results(
            op_handle=self.command_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            expected_row_start_offset=self._next_row_index,
            lz4_compressed=self.lz4_compressed,
            arrow_schema_bytes=self._arrow_schema_bytes,
            description=self.description,
        )
        self.results = results
        self.has_more_rows = has_more_rows

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

        res = df.to_numpy(na_value=None)
        return [ResultRow(*v) for v in res]

    @property
    def rownumber(self):
        return self._next_row_index

    def fetchmany_arrow(self, size: int) -> pyarrow.Table:
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

    def fetchall_arrow(self) -> pyarrow.Table:
        """Fetch all (remaining) rows of a query result, returning them as a PyArrow table."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            results = pyarrow.concat_tables([results, partial_results])
            self._next_row_index += partial_results.num_rows

        return results

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        """
        res = self._convert_arrow_table(self.fetchmany_arrow(1))
        if len(res) > 0:
            return res[0]
        else:
            return None

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result, returning them as a list of rows.
        """
        return self._convert_arrow_table(self.fetchall_arrow())

    def fetchmany(self, size: int) -> List[Row]:
        """
        Fetch the next set of rows of a query result, returning a list of rows.

        An empty sequence is returned when no more rows are available.
        """
        return self._convert_arrow_table(self.fetchmany_arrow(size))

    def close(self) -> None:
        """
        Close the cursor.

        If the connection has not been closed, and the cursor has not already
        been closed on the server for some other reason, issue a request to the server to close it.
        """
        try:
            if (
                self.op_state != self.thrift_backend.CLOSED_OP_STATE
                and not self.has_been_closed_server_side
                and self.connection.open
            ):
                self.thrift_backend.close_command(self.command_id)
        except RequestError as e:
            if isinstance(e.args[1], CursorAlreadyClosedError):
                logger.info("Operation was canceled by a prior request")
        finally:
            self.has_been_closed_server_side = True
            self.op_state = self.thrift_backend.CLOSED_OP_STATE

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


def execute_response_contains_direct_results(
    execute_response: ttypes.TExecuteStatementResp,
) -> bool:
    """
    Returns True if the thrift TExecuteStatementResp returned a direct result.
    
    When directResults is used the server just batches these rpcs together,
    if the entire result can be returned in a single round-trip:

        struct TSparkDirectResults {
        1: optional TGetOperationStatusResp operationStatus
        2: optional TGetResultSetMetadataResp resultSetMetadata
        3: optional TFetchResultsResp resultSet
        4: optional TCloseOperationResp closeOperation
        }
    """

    has_op_status = execute_response.directResults.operationStatus
    has_result_set = execute_response.directResults.resultSet
    has_metadata = execute_response.directResults.resultSetMetadata
    has_close_op = execute_response.directResults.closeOperation

    return has_op_status and has_result_set and has_metadata and has_close_op
