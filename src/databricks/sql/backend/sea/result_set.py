from __future__ import annotations

from typing import Any, List, Optional, TYPE_CHECKING

import logging

from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
from databricks.sql.backend.sea.utils.conversion import SqlTypeConverter

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.types import Row
from databricks.sql.backend.sea.queue import JsonQueue, SeaResultSetQueueFactory
from databricks.sql.backend.types import ExecuteResponse
from databricks.sql.result_set import ResultSet
from databricks.sql.utils import concat_table_chunks

logger = logging.getLogger(__name__)


class SeaResultSet(ResultSet):
    """ResultSet implementation for SEA backend."""

    def __init__(
        self,
        connection: Connection,
        execute_response: ExecuteResponse,
        sea_client: SeaDatabricksClient,
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
            sea_client: The SeaDatabricksClient instance for direct access
            buffer_size_bytes: Buffer size for fetching results
            arraysize: Default number of rows to fetch
            result_data: Result data from SEA response
            manifest: Manifest from SEA response
        """

        self.manifest = manifest

        statement_id = execute_response.command_id.to_sea_statement_id()
        if statement_id is None:
            raise ValueError("Command ID is not a SEA statement ID")

        results_queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            self.manifest,
            statement_id,
            ssl_options=connection.session.ssl_options,
            description=execute_response.description,
            max_download_threads=sea_client.max_download_threads,
            sea_client=sea_client,
            lz4_compressed=execute_response.lz4_compressed,
            http_client=connection.session.http_client,
        )

        # Call parent constructor with common attributes
        super().__init__(
            connection=connection,
            backend=sea_client,
            arraysize=arraysize,
            buffer_size_bytes=buffer_size_bytes,
            command_id=execute_response.command_id,
            status=execute_response.status,
            has_been_closed_server_side=execute_response.has_been_closed_server_side,
            results_queue=results_queue,
            description=execute_response.description,
            is_staging_operation=execute_response.is_staging_operation,
            lz4_compressed=execute_response.lz4_compressed,
            arrow_schema_bytes=execute_response.arrow_schema_bytes,
        )

    def _convert_json_types(self, row: List[str]) -> List[Any]:
        """
        Convert string values in the row to appropriate Python types based on column metadata.
        """

        # JSON + INLINE gives us string values, so we convert them to appropriate
        #   types based on column metadata
        converted_row = []

        for i, value in enumerate(row):
            column_name = self.description[i][0]
            column_type = self.description[i][1]
            precision = self.description[i][4]
            scale = self.description[i][5]

            converted_value = SqlTypeConverter.convert_value(
                value,
                column_type,
                column_name=column_name,
                precision=precision,
                scale=scale,
            )
            converted_row.append(converted_value)

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

        return self._fetch_raw_with_buffer(size)

    def fetchall_json(self) -> List[List[str]]:
        """
        Fetch all remaining rows as a columnar table.

        Returns:
            Columnar table containing all remaining rows
        """

        return self._fetch_raw_with_buffer(None)

    @staticmethod
    def _raw_num_rows(raw: Any) -> int:
        return len(raw) if isinstance(raw, list) else raw.num_rows

    def _fetch_raw_from_queue(self, size: Optional[int]) -> Any:
        """Fetch ``size`` (all if None) raw rows from the queue, bypassing the row buffer."""
        if size is None:
            results = self.results.remaining_rows()
        else:
            results = self.results.next_n_rows(size)
        self._next_row_index += self._raw_num_rows(results)
        return results

    def _fetch_available_from_queue(self, size: int) -> Any:
        """Fetch up to ``size`` rows the queue already holds, for Arrow fetchone() refills.

        On CloudFetch, the request is capped at what the current chunk still holds
        (matching Thrift) via the shared ``ResultSet._cap_to_current_chunk``, so a
        refill is limited to one chunk rather than stitching several links into one
        batch. It does not avoid waiting on the next download: reading to the end of
        a chunk still makes ``next_n_rows`` advance to the next link in that call.
        Only called while the fetchone() row buffer is empty.
        """
        return self._fetch_raw_from_queue(self._cap_to_current_chunk(size))

    def _fetch_raw_with_buffer(self, size: Optional[int]) -> Any:
        """Fetch ``size`` (all if None) raw rows, draining the fetchone() buffer first."""
        buffered = self._row_buffer_take_raw(size)
        if buffered is None:
            return self._fetch_raw_from_queue(size)
        remaining = None if size is None else size - self._raw_num_rows(buffered)
        if remaining == 0:
            return buffered
        rest = self._fetch_raw_from_queue(remaining)
        if self._raw_num_rows(rest) == 0:
            return buffered
        if isinstance(buffered, list):
            return buffered + list(rest)
        return concat_table_chunks([buffered, rest])

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

        results = self._fetch_raw_with_buffer(size)
        if isinstance(self.results, JsonQueue):
            results = self._convert_json_to_arrow_table(results)

        return results

    def fetchall_arrow(self) -> "pyarrow.Table":
        """
        Fetch all remaining rows as an Arrow table.
        """

        results = self._fetch_raw_with_buffer(None)
        if isinstance(self.results, JsonQueue):
            results = self._convert_json_to_arrow_table(results)

        return results

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.

        Returns:
            A single Row object or None if no more rows are available
        """

        if isinstance(self.results, JsonQueue):
            return self._fetchone_buffered(self.fetchmany_json, self._create_json_table)
        return self._fetchone_buffered(
            self._fetch_available_from_queue, self._convert_arrow_table
        )

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

        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")
        buffered = self._row_buffer_take_rows(size)
        remaining = size - len(buffered)
        if buffered and remaining == 0:
            return buffered
        if isinstance(self.results, JsonQueue):
            rest = self._create_json_table(self.fetchmany_json(remaining))
        else:
            rest = self._convert_arrow_table(self.fetchmany_arrow(remaining))
        return buffered + rest if buffered else rest

    def fetchall(self) -> List[Row]:
        """
        Fetch all remaining rows of a query result, returning them as a list of rows.

        Returns:
            List of Row objects containing all remaining rows
        """

        buffered = self._row_buffer_take_rows()
        if isinstance(self.results, JsonQueue):
            rest = self._create_json_table(self.fetchall_json())
        else:
            rest = self._convert_arrow_table(self.fetchall_arrow())
        return buffered + rest if buffered else rest
