"""
Client-side filtering utilities for Databricks SQL connector.

This module provides filtering capabilities for result sets returned by different backends.
"""

from __future__ import annotations

import io
import logging
from copy import deepcopy
from typing import (
    List,
    Optional,
    Any,
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from databricks.sql.backend.sea.result_set import SeaResultSet

from databricks.sql.backend.types import ExecuteResponse
from databricks.sql.backend.sea.models.base import ResultData
from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.utils import CloudFetchQueue, ArrowQueue

try:
    import pyarrow
    import pyarrow.compute as pc
except ImportError:
    pyarrow = None
    pc = None

logger = logging.getLogger(__name__)


class ResultSetFilter:
    """
    A general-purpose filter for result sets.
    """

    @staticmethod
    def _create_execute_response(result_set: SeaResultSet) -> ExecuteResponse:
        """
        Create an ExecuteResponse with parameters from the original result set.

        Args:
            result_set: Original result set to copy parameters from

        Returns:
            ExecuteResponse: New execute response object
        """
        return ExecuteResponse(
            command_id=result_set.command_id,
            status=result_set.status,
            description=result_set.description,
            has_been_closed_server_side=result_set.has_been_closed_server_side,
            lz4_compressed=result_set.lz4_compressed,
            arrow_schema_bytes=result_set._arrow_schema_bytes,
            is_staging_operation=False,
        )

    @staticmethod
    def _create_filtered_manifest(result_set: SeaResultSet, new_row_count: int):
        """
        Create a copy of the manifest with updated row count.

        Args:
            result_set: Original result set to copy manifest from
            new_row_count: New total row count for filtered data

        Returns:
            Updated manifest copy
        """
        filtered_manifest = deepcopy(result_set.manifest)
        filtered_manifest.total_row_count = new_row_count
        return filtered_manifest

    @staticmethod
    def _create_filtered_result_set(
        result_set: SeaResultSet,
        result_data: ResultData,
        row_count: int,
    ) -> "SeaResultSet":
        """
        Create a new filtered SeaResultSet with the provided data.

        Args:
            result_set: Original result set to copy parameters from
            result_data: New result data for the filtered set
            row_count: Number of rows in the filtered data

        Returns:
            New filtered SeaResultSet
        """
        from databricks.sql.backend.sea.result_set import SeaResultSet

        execute_response = ResultSetFilter._create_execute_response(result_set)
        filtered_manifest = ResultSetFilter._create_filtered_manifest(
            result_set, row_count
        )

        return SeaResultSet(
            connection=result_set.connection,
            execute_response=execute_response,
            sea_client=cast(SeaDatabricksClient, result_set.backend),
            result_data=result_data,
            manifest=filtered_manifest,
            buffer_size_bytes=result_set.buffer_size_bytes,
            arraysize=result_set.arraysize,
        )

    @staticmethod
    def _validate_column_index(result_set: SeaResultSet, column_index: int) -> str:
        """
        Validate column index and return the column name.

        Args:
            result_set: Result set to validate against
            column_index: Index of the column to validate

        Returns:
            str: Column name at the specified index

        Raises:
            ValueError: If column index is out of bounds
        """
        if column_index >= len(result_set.description):
            raise ValueError(f"Column index {column_index} is out of bounds")
        return result_set.description[column_index][0]

    @staticmethod
    def _filter_arrow_table(
        table: Any,  # pyarrow.Table
        column_name: str,
        allowed_values: List[str],
    ) -> Any:  # returns pyarrow.Table
        """
        Filter a PyArrow table by column values.

        Args:
            table: The PyArrow table to filter
            column_name: The name of the column to filter on
            allowed_values: List of allowed values for the column

        Returns:
            A filtered PyArrow table
        """
        if not pyarrow:
            raise ImportError("PyArrow is required for Arrow table filtering")

        if table.num_rows == 0:
            return table

        # Convert allowed_values to PyArrow Array for better performance
        allowed_array = pyarrow.array(allowed_values)

        # Construct a boolean mask: True where column is in allowed_list
        mask = pc.is_in(table[column_name], value_set=allowed_array)
        return table.filter(mask)

    @staticmethod
    def _filter_arrow_result_set(
        result_set: SeaResultSet,
        column_index: int,
        allowed_values: List[str],
    ) -> SeaResultSet:
        """
        Filter a SEA result set that contains Arrow tables.

        Args:
            result_set: The SEA result set to filter (containing Arrow data)
            column_index: The index of the column to filter on
            allowed_values: List of allowed values for the column

        Returns:
            A filtered SEA result set
        """
        # Validate column index and get column name
        column_name = ResultSetFilter._validate_column_index(result_set, column_index)

        # Get all remaining rows as Arrow table and filter it
        arrow_table = result_set.results.remaining_rows()
        filtered_table = ResultSetFilter._filter_arrow_table(
            arrow_table, column_name, allowed_values
        )

        # Convert the filtered table to Arrow stream format for ResultData
        sink = io.BytesIO()
        with pyarrow.ipc.new_stream(sink, filtered_table.schema) as writer:
            writer.write_table(filtered_table)
        arrow_stream_bytes = sink.getvalue()

        # Create ResultData with attachment containing the filtered data
        result_data = ResultData(
            data=None,  # No JSON data
            external_links=None,  # No external links
            attachment=arrow_stream_bytes,  # Arrow data as attachment
        )

        return ResultSetFilter._create_filtered_result_set(
            result_set, result_data, filtered_table.num_rows
        )

    @staticmethod
    def _filter_json_result_set(
        result_set: SeaResultSet,
        column_index: int,
        allowed_values: List[str],
        case_sensitive: bool = False,
    ) -> SeaResultSet:
        """
        Filter a result set by values in a specific column.

        Args:
            result_set: The result set to filter
            column_index: The index of the column to filter on
            allowed_values: List of allowed values for the column
            case_sensitive: Whether to perform case-sensitive comparison

        Returns:
            A filtered result set
        """
        # Validate column index (optional - not in arrow version but good practice)
        if column_index >= len(result_set.description):
            raise ValueError(f"Column index {column_index} is out of bounds")

        # Extract rows
        all_rows = result_set.results.remaining_rows()

        # Convert allowed values if case-insensitive
        if not case_sensitive:
            allowed_values = [v.upper() for v in allowed_values]
        # Helper lambda to get column value based on case sensitivity
        get_column_value = (
            lambda row: row[column_index].upper()
            if not case_sensitive
            else row[column_index]
        )

        # Filter rows based on allowed values
        filtered_rows = [
            row
            for row in all_rows
            if len(row) > column_index and get_column_value(row) in allowed_values
        ]

        # Create filtered result set
        result_data = ResultData(data=filtered_rows, external_links=None)

        # Return
        return ResultSetFilter._create_filtered_result_set(
            result_set, result_data, len(filtered_rows)
        )

    @staticmethod
    def filter_tables_by_type(
        result_set: SeaResultSet, table_types: Optional[List[str]] = None
    ) -> SeaResultSet:
        """
        Filter a result set of tables by the specified table types.

        This is a client-side filter that processes the result set after it has been
        retrieved from the server. It filters out tables whose type does not match
        any of the types in the table_types list.

        Args:
            result_set: The original result set containing tables
            table_types: List of table types to include (e.g., ["TABLE", "VIEW"])

        Returns:
            A filtered result set containing only tables of the specified types
        """
        # Default table types if none specified
        DEFAULT_TABLE_TYPES = ["TABLE", "VIEW", "SYSTEM TABLE"]
        valid_types = table_types if table_types else DEFAULT_TABLE_TYPES

        # Check if we have an Arrow table (cloud fetch) or JSON data
        # Table type is the 6th column (index 5)
        if isinstance(result_set.results, (CloudFetchQueue, ArrowQueue)):
            # For Arrow tables, we need to handle filtering differently
            return ResultSetFilter._filter_arrow_result_set(
                result_set, column_index=5, allowed_values=valid_types
            )
        else:
            # For JSON data, use the existing filter method
            return ResultSetFilter._filter_json_result_set(
                result_set, 5, valid_types, case_sensitive=True
            )
