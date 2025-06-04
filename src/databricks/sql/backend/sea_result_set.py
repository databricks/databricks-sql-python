"""
SEA (Statement Execution API) specific ResultSet implementation.

This module provides the ResultSet implementation for the SEA backend,
which handles the result data returned by the SEA API.
"""

import json
import logging
from typing import Optional, List, Any, Dict, Tuple, cast

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.exc import Error
from databricks.sql.utils import SeaResultSetQueueFactory, JsonQueue

from databricks.sql.backend.models import (
    StatementStatus,
    ResultManifest,
    ResultData,
    ColumnInfo,
    ServiceError,
)

logger = logging.getLogger(__name__)


class SeaResultSet(ResultSet):
    """ResultSet implementation for SEA backend."""

    def __init__(
        self,
        connection,
        sea_response: Dict[str, Any],  # Response from SEA execute command
        sea_client,  # The SeaDatabricksClient instance
        buffer_size_bytes: int = 104857600,
        arraysize: int = 10000,
    ):
        """Initialize a SeaResultSet with the response from a SEA query execution."""
        super().__init__(connection, sea_client, arraysize, buffer_size_bytes)

        # Store the original response for filtering operations
        self._response = sea_response
        self._sea_client = sea_client
        self._buffer_size_bytes = buffer_size_bytes
        self._arraysize = arraysize

        # Extract and store SEA-specific properties
        self.statement_id = sea_response.get("statement_id")

        # Parse the status
        status_data = sea_response.get("status", {})
        error = None
        if "error" in status_data:
            error_data = status_data["error"]
            error = ServiceError(
                message=error_data.get("message", ""),
                error_code=error_data.get("error_code"),
            )

        self.status = StatementStatus(
            state=CommandState.from_sea_state(status_data.get("state", "")),
            error=error,
            sql_state=status_data.get("sql_state"),
        )

        # Parse the manifest
        manifest_data = sea_response.get("manifest")
        if manifest_data:
            columns = []
            for col_data in manifest_data.get("schema", {}).get("columns", []):
                columns.append(
                    ColumnInfo(
                        name=col_data.get("name", ""),
                        type_name=col_data.get("type_name", ""),
                        type_text=col_data.get("type_text", ""),
                        nullable=col_data.get("nullable", True),
                        precision=col_data.get("precision"),
                        scale=col_data.get("scale"),
                        ordinal_position=col_data.get("ordinal_position"),
                    )
                )

            self.manifest: Optional[ResultManifest] = ResultManifest(
                schema=columns,
                total_row_count=manifest_data.get("total_row_count", 0),
                total_byte_count=manifest_data.get("total_byte_count", 0),
                truncated=manifest_data.get("truncated", False),
                chunk_count=manifest_data.get("chunk_count"),
            )
        else:
            self.manifest = None

        # Parse the result data
        result_data = sea_response.get("result")
        if result_data:
            self.result: Optional[ResultData] = ResultData(
                data=result_data.get("data_array"),  # Changed from data to data_array
                external_links=result_data.get("external_links"),
            )
        else:
            self.result = None

        # Extract description from manifest
        self.description = self._extract_description_from_manifest()

        # Initialize other properties
        self._is_staging_operation = False  # SEA doesn't have staging operations
        self._has_more_rows = False
        self._current_chunk_index = 0

        # Initialize queue for result data
        if self.result:
            self.results = SeaResultSetQueueFactory.build_queue(
                sea_result_data=self.result,
                description=cast(Optional[List[List[Any]]], self.description),
            )
            self._has_more_rows = True if self.result.data else False
        else:
            self.results = JsonQueue([])
            self._has_more_rows = False

    @property
    def is_staging_operation(self) -> bool:
        """Whether this result set represents a staging operation."""
        return self._is_staging_operation

    def _extract_description_from_manifest(
        self,
    ) -> Optional[
        List[Tuple[str, str, None, None, Optional[int], Optional[int], bool]]
    ]:
        """
        Extract column descriptions from the SEA manifest.

        Returns:
            A list of column descriptions in the format expected by PEP 249:
            (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        if not self.manifest or not self.manifest.schema:
            return None

        description = []
        for col in self.manifest.schema:
            # Format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            description.append(
                (
                    col.name,  # name
                    col.type_name,  # type_code
                    None,  # display_size (not provided by SEA)
                    None,  # internal_size (not provided by SEA)
                    col.precision,  # precision
                    col.scale,  # scale
                    col.nullable,  # null_ok
                )
            )

        return description

    def _fill_results_buffer(self) -> None:
        """Fill the results buffer from the backend for INLINE disposition."""
        if not self.result or not self.result.data:
            self._has_more_rows = False
            return

        # For INLINE disposition, we already have all the data
        # No need to fetch more data from the backend
        self._has_more_rows = False  # No more rows to fetch for INLINE

    def _convert_rows_to_arrow_table(self, rows):
        """Convert rows to Arrow table."""
        if not self.description:
            return pyarrow.Table.from_pylist([])

        # Create dict of column data
        column_data = {}
        column_names = [col[0] for col in self.description]

        for i, name in enumerate(column_names):
            column_data[name] = [row[i] for row in rows]

        return pyarrow.Table.from_pydict(column_data)

    def _create_empty_arrow_table(self):
        """Create an empty Arrow table with the correct schema."""
        if not self.description:
            return pyarrow.Table.from_pylist([])

        column_names = [col[0] for col in self.description]
        return pyarrow.Table.from_pydict({name: [] for name in column_names})

    def fetchone(self) -> Optional[Row]:
        """Fetch the next row of a query result set."""
        if isinstance(self.results, JsonQueue):
            rows = self.results.next_n_rows(1)
            if not rows:
                return None

            row = rows[0]

            # Convert to Row object
            if self.description:
                column_names = [col[0] for col in self.description]
                ResultRow = Row(*column_names)
                return ResultRow(*row)
            return row
        else:
            # This should not happen with current implementation
            # but added for future compatibility
            raise NotImplementedError("Unsupported queue type")

    def fetchmany(self, size: Optional[int] = None) -> List[Row]:
        """Fetch the next set of rows of a query result."""
        if size is None:
            size = self.arraysize

        if size < 0:
            raise ValueError(f"size argument for fetchmany is {size} but must be >= 0")

        if isinstance(self.results, JsonQueue):
            rows = self.results.next_n_rows(size)

            # Convert to Row objects
            if self.description:
                column_names = [col[0] for col in self.description]
                ResultRow = Row(*column_names)
                return [ResultRow(*row) for row in rows]
            return rows
        else:
            # This should not happen with current implementation
            # but added for future compatibility
            raise NotImplementedError("Unsupported queue type")

    def fetchall(self) -> List[Row]:
        """Fetch all remaining rows of a query result."""
        if isinstance(self.results, JsonQueue):
            rows = self.results.remaining_rows()

            # Convert to Row objects
            if self.description:
                column_names = [col[0] for col in self.description]
                ResultRow = Row(*column_names)
                return [ResultRow(*row) for row in rows]
            return rows
        else:
            # This should not happen with current implementation
            # but added for future compatibility
            raise NotImplementedError("Unsupported queue type")

    def fetchmany_arrow(self, size: int) -> Any:
        """Fetch the next set of rows as an Arrow table."""
        if not pyarrow:
            raise ImportError("PyArrow is required for Arrow support")

        rows = self.fetchmany(size)
        if not rows:
            # Return empty Arrow table with schema
            return self._create_empty_arrow_table()

        # Convert rows to Arrow table
        return self._convert_rows_to_arrow_table(rows)

    def fetchall_arrow(self) -> Any:
        """Fetch all remaining rows as an Arrow table."""
        if not pyarrow:
            raise ImportError("PyArrow is required for Arrow support")

        rows = self.fetchall()
        if not rows:
            # Return empty Arrow table with schema
            return self._create_empty_arrow_table()

        # Convert rows to Arrow table
        return self._convert_rows_to_arrow_table(rows)

    def close(self) -> None:
        """Close the result set and release any resources."""
        # Basic implementation to close the statement
        if self.connection.open and self.statement_id:
            try:
                self.backend.close_command(
                    CommandId.from_sea_statement_id(self.statement_id)
                )
            except Exception as e:
                logger.warning(f"Error closing SEA statement: {e}")
