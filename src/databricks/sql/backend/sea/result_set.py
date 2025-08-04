from __future__ import annotations

from typing import Any, List, Optional, TYPE_CHECKING, Dict, Union

import logging

from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
from databricks.sql.backend.sea.utils.conversion import SqlTypeConverter
from databricks.sql.backend.sea.utils.result_column import ResultColumn

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

        # Initialize metadata columns for post-fetch transformation
        self._metadata_columns: Optional[List[ResultColumn]] = None
        self._column_index_mapping: Optional[Dict[int, Union[int, None]]] = None

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

        results = self.results.next_n_rows(size)
        results = self._transform_json_rows(results)
        self._next_row_index += len(results)

        return results

    def fetchall_json(self) -> List[List[str]]:
        """
        Fetch all remaining rows as a columnar table.

        Returns:
            Columnar table containing all remaining rows
        """

        results = self.results.remaining_rows()
        results = self._transform_json_rows(results)
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

        results = self.results.next_n_rows(size)
        if isinstance(self.results, JsonQueue):
            # Transform JSON first, then convert to Arrow
            transformed_json = self._transform_json_rows(results)
            results = self._convert_json_to_arrow_table(transformed_json)
        else:
            # Transform Arrow table directly
            results = self._transform_arrow_table(results)

        self._next_row_index += results.num_rows

        return results

    def fetchall_arrow(self) -> "pyarrow.Table":
        """
        Fetch all remaining rows as an Arrow table.
        """

        results = self.results.remaining_rows()
        if isinstance(self.results, JsonQueue):
            # Transform JSON first, then convert to Arrow
            transformed_json = self._transform_json_rows(results)
            results = self._convert_json_to_arrow_table(transformed_json)
        else:
            # Transform Arrow table directly
            results = self._transform_arrow_table(results)

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
            res = self._convert_arrow_table(self.fetchmany_arrow(1))

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
            return self._convert_arrow_table(self.fetchmany_arrow(size))

    def fetchall(self) -> List[Row]:
        """
        Fetch all remaining rows of a query result, returning them as a list of rows.

        Returns:
            List of Row objects containing all remaining rows
        """

        if isinstance(self.results, JsonQueue):
            return self._create_json_table(self.fetchall_json())
        else:
            return self._convert_arrow_table(self.fetchall_arrow())

    def prepare_metadata_columns(self, metadata_columns: List[ResultColumn]) -> None:
        """
        Prepare result set for metadata column normalization.

        Args:
            metadata_columns: List of ResultColumn objects defining the expected columns
                            and their mappings from SEA column names
        """
        self._metadata_columns = metadata_columns
        self._prepare_column_mapping()

    def _prepare_column_mapping(self) -> None:
        """
        Prepare column index mapping for metadata queries.
        Updates description to use JDBC column names.
        """
        # Ensure description is available
        if not self.description:
            raise ValueError("Cannot prepare column mapping without result description")

        # Build mapping from SEA column names to their indices
        sea_column_indices = {}
        for idx, col in enumerate(self.description):
            sea_column_indices[col[0]] = idx

        # Create new description and index mapping
        new_description = []
        self._column_index_mapping = {}  # Maps new index -> old index

        for new_idx, result_column in enumerate(self._metadata_columns or []):
            # Find the corresponding SEA column
            if (
                result_column.result_set_column_name
                and result_column.result_set_column_name in sea_column_indices
            ):
                old_idx = sea_column_indices[result_column.result_set_column_name]
                self._column_index_mapping[new_idx] = old_idx
                # Use the original column metadata but with JDBC name
                old_col = self.description[old_idx]
                new_description.append(
                    (
                        result_column.column_name,  # JDBC name
                        result_column.column_type,  # Expected type
                        old_col[2],  # display_size
                        old_col[3],  # internal_size
                        old_col[4],  # precision
                        old_col[5],  # scale
                        old_col[6],  # null_ok
                    )
                )
            else:
                # Column doesn't exist in SEA - add with None values
                new_description.append(
                    (
                        result_column.column_name,
                        result_column.column_type,
                        None,
                        None,
                        None,
                        None,
                        True,
                    )
                )
                self._column_index_mapping[new_idx] = None

        self.description = new_description

    def _transform_arrow_table(self, table: "pyarrow.Table") -> "pyarrow.Table":
        """Transform arrow table columns for metadata normalization."""
        if not self._metadata_columns:
            return table

        # Reorder columns and add missing ones
        new_columns = []
        column_names = []

        for new_idx, result_column in enumerate(self._metadata_columns or []):
            old_idx = (
                self._column_index_mapping.get(new_idx)
                if self._column_index_mapping
                else None
            )

            # Get the source data
            if old_idx is not None:
                column = table.column(old_idx)
                values = column.to_pylist()
            else:
                values = None

            # Apply transformation and create column
            if values is not None:
                column = pyarrow.array(values)
                new_columns.append(column)
            else:
                # Create column with default/transformed values
                null_array = pyarrow.nulls(table.num_rows)
                new_columns.append(null_array)

            column_names.append(result_column.column_name)

        return pyarrow.Table.from_arrays(new_columns, names=column_names)

    def _transform_json_rows(self, rows: List[List[str]]) -> List[List[Any]]:
        """Transform JSON rows for metadata normalization."""
        if not self._metadata_columns:
            return rows

        transformed_rows = []
        for row in rows:
            new_row = []
            for new_idx, result_column in enumerate(self._metadata_columns or []):
                old_idx = (
                    self._column_index_mapping.get(new_idx)
                    if self._column_index_mapping
                    else None
                )
                if old_idx is not None:
                    value = row[old_idx]
                else:
                    value = None

                new_row.append(value)
            transformed_rows.append(new_row)
        return transformed_rows
