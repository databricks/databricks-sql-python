from __future__ import annotations

from typing import Any, List, Optional, TYPE_CHECKING

import logging

from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
from databricks.sql.backend.sea.utils.conversion import SqlTypeConverter

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    from databricks.sql.client import Connection
from databricks.sql.exc import ProgrammingError
from databricks.sql.types import Row
from databricks.sql.backend.sea.queue import JsonQueue, SeaResultSetQueueFactory
from databricks.sql.backend.types import ExecuteResponse
from databricks.sql.result_set import ResultSet

logger = logging.getLogger(__name__)


class SeaResultSet(ResultSet):
    """ResultSet implementation for SEA backend."""

    def __init__(
        self,
        connection: Connection,
        execute_response: ExecuteResponse,
        result_data: ResultData,
        manifest: ResultManifest,
        buffer_size_bytes: int = 104857600,
        arraysize: int = 10000,
    ):
        """
        Initialize a SeaResultSet with the response from a SEA query execution.

        Args:
            connection: The parent connection
            execute_response: Response from the execute command
            buffer_size_bytes: Buffer size for fetching results
            arraysize: Default number of rows to fetch
            result_data: Result data from SEA response
            manifest: Manifest from SEA response
        """

        self.manifest = manifest

        statement_id = execute_response.command_id.to_sea_statement_id()
        if statement_id is None:
            raise ValueError("Command ID is not a SEA statement ID")

        # Call parent constructor with common attributes
        super().__init__(
            connection=connection,
            arraysize=arraysize,
            buffer_size_bytes=buffer_size_bytes,
            command_id=execute_response.command_id,
            status=execute_response.status,
            has_been_closed_server_side=execute_response.has_been_closed_server_side,
            description=execute_response.description,
            is_staging_operation=execute_response.is_staging_operation,
            lz4_compressed=execute_response.lz4_compressed,
            arrow_schema_bytes=execute_response.arrow_schema_bytes,
        )

        # Assert that the backend is of the correct type
        assert isinstance(
            self.backend, SeaDatabricksClient
        ), "Backend must be a SeaDatabricksClient"

        results_queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            self.manifest,
            statement_id,
            description=execute_response.description,
            max_download_threads=self.backend.max_download_threads,
            sea_client=self.backend,
            lz4_compressed=execute_response.lz4_compressed,
        )

        # Set the results queue
        self.results = results_queue

    def _convert_json_types(self, row: List[str]) -> List[Any]:
        """
        Convert string values in the row to appropriate Python types based on column metadata.
        """

        # JSON + INLINE gives us string values, so we convert them to appropriate
        #   types based on column metadata
        converted_row = []

        for i, value in enumerate(row):
            column_type = self.description[i][1]
            precision = self.description[i][4]
            scale = self.description[i][5]

            try:
                converted_value = SqlTypeConverter.convert_value(
                    value, column_type, precision=precision, scale=scale
                )
                converted_row.append(converted_value)
            except Exception as e:
                logger.warning(
                    f"Error converting value '{value}' to {column_type}: {e}"
                )
                converted_row.append(value)

        return converted_row

    def _convert_json_to_arrow_table(self, rows: List[List[str]]) -> "pyarrow.Table":
        """
        Convert raw data rows to Arrow table.

        Args:
            rows: List of raw data rows

        Returns:
            PyArrow Table containing the converted values
        """

        if not rows:
            return pyarrow.Table.from_pydict({})

        # create a generator for row conversion
        converted_rows_iter = (self._convert_json_types(row) for row in rows)
        cols = list(map(list, zip(*converted_rows_iter)))

        names = [col[0] for col in self.description]
        return pyarrow.Table.from_arrays(cols, names=names)

    def _create_json_table(self, rows: List[List[str]]) -> List[Row]:
        """
        Convert raw data rows to Row objects with named columns based on description.

        Args:
            rows: List of raw data rows
        Returns:
            List of Row objects with named columns and converted values
        """

        ResultRow = Row(*[col[0] for col in self.description])
        return [ResultRow(*self._convert_json_types(row)) for row in rows]

    def fetchmany_json(self, size: int) -> List[List[str]]:
        """
        Fetch the next set of rows as a columnar table.

        Args:
            size: Number of rows to fetch

        Returns:
            Columnar table containing the fetched rows

        Raises:
            ValueError: If size is negative
        """

        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")

        if self.results is None:
            raise RuntimeError("Results queue is not initialized")

        results = self.results.next_n_rows(size)
        self._next_row_index += len(results)

        return results

    def fetchall_json(self) -> List[List[str]]:
        """
        Fetch all remaining rows as a columnar table.

        Returns:
            Columnar table containing all remaining rows
        """

        if self.results is None:
            raise RuntimeError("Results queue is not initialized")

        results = self.results.remaining_rows()
        self._next_row_index += len(results)

        return results

    def fetchmany_arrow(self, size: int) -> "pyarrow.Table":
        """
        Fetch the next set of rows as an Arrow table.

        Args:
            size: Number of rows to fetch

        Returns:
            PyArrow Table containing the fetched rows

        Raises:
            ImportError: If PyArrow is not installed
            ValueError: If size is negative
        """

        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")

        if not isinstance(self.results, JsonQueue):
            raise NotImplementedError("fetchmany_arrow only supported for JSON data")

        results = self._convert_json_to_arrow_table(self.results.next_n_rows(size))
        self._next_row_index += results.num_rows

        return results

    def fetchall_arrow(self) -> "pyarrow.Table":
        """
        Fetch all remaining rows as an Arrow table.
        """

        if not isinstance(self.results, JsonQueue):
            raise NotImplementedError("fetchall_arrow only supported for JSON data")

        results = self._convert_json_to_arrow_table(self.results.remaining_rows())
        self._next_row_index += results.num_rows

        return results

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.

        Returns:
            A single Row object or None if no more rows are available
        """

        if isinstance(self.results, JsonQueue):
            res = self._create_json_table(self.fetchmany_json(1))
        else:
            raise NotImplementedError("fetchone only supported for JSON data")

        return res[0] if res else None

    def fetchmany(self, size: int) -> List[Row]:
        """
        Fetch the next set of rows of a query result, returning a list of rows.

        Args:
            size: Number of rows to fetch (defaults to arraysize if None)

        Returns:
            List of Row objects

        Raises:
            ValueError: If size is negative
        """

        if isinstance(self.results, JsonQueue):
            return self._create_json_table(self.fetchmany_json(size))
        else:
            raise NotImplementedError("fetchmany only supported for JSON data")

    def fetchall(self) -> List[Row]:
        """
        Fetch all remaining rows of a query result, returning them as a list of rows.

        Returns:
            List of Row objects containing all remaining rows
        """

        if isinstance(self.results, JsonQueue):
            return self._create_json_table(self.fetchall_json())
        else:
            raise NotImplementedError("fetchall only supported for JSON data")
