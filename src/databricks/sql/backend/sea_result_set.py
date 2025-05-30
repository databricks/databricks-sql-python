"""
SEA (Statement Execution API) specific ResultSet implementation.

This module provides the ResultSet implementation for the SEA backend,
which handles the result data returned by the SEA API.
"""

import logging
from typing import Optional, List, Any, Dict, Tuple

from databricks.sql.result_set import ResultSet
from databricks.sql.types import Row
from databricks.sql.backend.types import CommandId
from databricks.sql.exc import Error

from databricks.sql.backend.models import (
    StatementStatus,
    ResultManifest,
    ResultData,
    ColumnInfo
)

logger = logging.getLogger(__name__)


class SeaResultSet(ResultSet):
    """ResultSet implementation for SEA backend."""
    
    def __init__(
        self,
        connection,
        sea_response: Dict[str, Any],  # Response from SEA execute command
        sea_client,    # The SeaDatabricksClient instance
        buffer_size_bytes: int = 104857600,
        arraysize: int = 10000,
    ):
        """Initialize a SeaResultSet with the response from a SEA query execution."""
        super().__init__(connection, sea_client, arraysize, buffer_size_bytes)
        
        # Extract and store SEA-specific properties
        self.statement_id = sea_response.get("statement_id")
        
        # Parse the status
        status_data = sea_response.get("status", {})
        self.status = StatementStatus(
            state=status_data.get("state", ""),
            error=None if "error" not in status_data else {
                "message": status_data["error"].get("message", ""),
                "error_code": status_data["error"].get("error_code")
            },
            sql_state=status_data.get("sql_state")
        )
        
        # Parse the manifest
        manifest_data = sea_response.get("manifest")
        if manifest_data:
            schema_data = manifest_data.get("schema", [])
            columns = []
            for col_data in schema_data:
                columns.append(ColumnInfo(
                    name=col_data.get("name", ""),
                    type_name=col_data.get("type_name", ""),
                    type_text=col_data.get("type_text", ""),
                    nullable=col_data.get("nullable", True),
                    precision=col_data.get("precision"),
                    scale=col_data.get("scale"),
                    ordinal_position=col_data.get("ordinal_position")
                ))
            
            self.manifest = ResultManifest(
                schema=columns,
                total_row_count=manifest_data.get("total_row_count", 0),
                total_byte_count=manifest_data.get("total_byte_count", 0),
                truncated=manifest_data.get("truncated", False),
                chunk_count=manifest_data.get("chunk_count")
            )
        else:
            self.manifest = None
        
        # Parse the result data
        result_data = sea_response.get("result")
        if result_data:
            self.result = ResultData(
                data=result_data.get("data"),
                external_links=result_data.get("external_links")
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
    
    def _extract_description_from_manifest(self) -> List[Tuple[str, str, None, None, None, None, bool]]:
        """
        Extract column descriptions from the SEA manifest.
        
        Returns:
            A list of column descriptions in the format expected by PEP 249:
            (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        if not self.manifest or not self.manifest.schema:
            return []
        
        description = []
        for col in self.manifest.schema:
            # Format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            description.append((
                col.name,                # name
                col.type_name,           # type_code
                None,                    # display_size (not provided by SEA)
                None,                    # internal_size (not provided by SEA)
                col.precision,           # precision
                col.scale,               # scale
                col.nullable             # null_ok
            ))
        
        return description
    
    def _fill_results_buffer(self) -> None:
        """
        Fill the results buffer from the backend.
        
        This method fetches the next chunk of data from the SEA backend
        if external links are available.
        """
        # If we don't have more rows or don't have external links, return
        if not self._has_more_rows or not self.result or not self.result.external_links:
            return
        
        # If we've already processed all chunks, mark as done
        if self.manifest and self.manifest.chunk_count and self._current_chunk_index >= self.manifest.chunk_count:
            self._has_more_rows = False
            return
        
        # Find the next external link to fetch
        next_link = None
        for link in self.result.external_links:
            if link.chunk_index == self._current_chunk_index:
                next_link = link
                break
        
        if not next_link:
            self._has_more_rows = False
            return
        
        # Fetch the next chunk using the external link
        # This would typically involve making an HTTP request to the external link URL
        # and parsing the response, but for now we'll just raise NotImplementedError
        raise NotImplementedError("Fetching data from external links is not yet implemented")
    
    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set.
        
        Returns:
            A Row object representing the next row or None if no more rows
        """
        # If we've reached the end of the buffer, try to fill it
        if self._current_row_index >= len(self._rows_buffer):
            if not self._has_more_rows:
                return None
            
            try:
                self._fill_results_buffer()
                self._current_row_index = 0
                self._current_chunk_index += 1
            except NotImplementedError:
                # For now, just return None if we can't fetch more data
                return None
            
            # If buffer is still empty after filling, return None
            if len(self._rows_buffer) == 0:
                return None
        
        # Get the next row from the buffer
        row_data = self._rows_buffer[self._current_row_index]
        self._current_row_index += 1
        
        # Convert to Row object
        return Row(row_data)
    
    def fetchmany(self, size: int) -> List[Row]:
        """
        Fetch the next set of rows of a query result.
        
        Args:
            size: The maximum number of rows to fetch
            
        Returns:
            A list of Row objects
        """
        if size <= 0:
            size = self.arraysize
        
        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        
        return result
    
    def fetchall(self) -> List[Row]:
        """
        Fetch all remaining rows of a query result.
        
        Returns:
            A list of all remaining Row objects
        """
        result = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        
        return result
    
    def fetchmany_arrow(self, size: int) -> Any:
        """
        Fetch the next set of rows as an Arrow table.
        
        Args:
            size: The maximum number of rows to fetch
            
        Returns:
            An Arrow table
            
        Raises:
            NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError("fetchmany_arrow is not yet implemented for SEA backend")
    
    def fetchall_arrow(self) -> Any:
        """
        Fetch all remaining rows as an Arrow table.
        
        Returns:
            An Arrow table
            
        Raises:
            NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError("fetchall_arrow is not yet implemented for SEA backend")
    
    def close(self) -> None:
        """Close the result set and release any resources."""
        # Basic implementation to close the statement
        if self.connection.open and self.statement_id:
            try:
                self.backend.close_command(CommandId.from_sea_statement_id(self.statement_id))
            except Exception as e:
                logger.warning(f"Error closing SEA statement: {e}")