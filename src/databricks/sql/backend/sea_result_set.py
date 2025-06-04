"""
SEA (Statement Execution API) specific ResultSet implementation.

This module provides the ResultSet implementation for the SEA backend,
which handles the result data returned by the SEA API.
"""

import json
import logging
from typing import Optional, List, Any, Dict, Tuple

from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.exc import Error

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
        self._rows_buffer = []
        self._current_row_index = 0
        self._has_more_rows = True
        self._current_chunk_index = 0

        # If we have inline data, fill the buffer
        if self.result and self.result.data:
            self._rows_buffer = self.result.data

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
        """Fill the results buffer from the backend."""
        raise NotImplementedError("Not implemented yet")

    def fetchone(self) -> Optional[Row]:
        """Fetch the next row of a query result set."""
        raise NotImplementedError("Not implemented yet")

    def fetchmany(self, size: int) -> List[Row]:
        """Fetch the next set of rows of a query result."""
        raise NotImplementedError("Not implemented yet")

    def fetchall(self) -> List[Row]:
        """Fetch all remaining rows of a query result."""
        raise NotImplementedError("Not implemented yet")

    def fetchmany_arrow(self, size: int) -> Any:
        """Fetch the next set of rows as an Arrow table."""
        raise NotImplementedError("Not implemented yet")

    def fetchall_arrow(self) -> Any:
        """Fetch all remaining rows as an Arrow table."""
        raise NotImplementedError("Not implemented yet")

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
