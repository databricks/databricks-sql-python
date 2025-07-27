"""
Client-side filtering utilities for Databricks SQL connector.

This module provides filtering capabilities for result sets returned by different backends.
"""

from __future__ import annotations

import logging
from typing import (
    List,
    Optional,
    Any,
    Callable,
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from databricks.sql.backend.sea.result_set import SeaResultSet

from databricks.sql.backend.types import ExecuteResponse

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
    def _filter_sea_result_set(
        result_set: SeaResultSet, filter_func: Callable[[List[Any]], bool]
    ) -> SeaResultSet:
        """
        Filter a SEA result set using the provided filter function.

        Args:
            result_set: The SEA result set to filter
            filter_func: Function that takes a row and returns True if the row should be included

        Returns:
            A filtered SEA result set
        """

        # Get all remaining rows
        all_rows = result_set.results.remaining_rows()

        # Filter rows
        filtered_rows = [row for row in all_rows if filter_func(row)]

        # Reuse the command_id from the original result set
        command_id = result_set.command_id

        # Create an ExecuteResponse for the filtered data
        execute_response = ExecuteResponse(
            command_id=command_id,
            status=result_set.status,
            description=result_set.description,
            has_been_closed_server_side=result_set.has_been_closed_server_side,
            lz4_compressed=result_set.lz4_compressed,
            arrow_schema_bytes=result_set._arrow_schema_bytes,
            is_staging_operation=False,
        )

        # Create a new ResultData object with filtered data
        from databricks.sql.backend.sea.models.base import ResultData

        result_data = ResultData(data=filtered_rows, external_links=None)

        from databricks.sql.backend.sea.backend import SeaDatabricksClient
        from databricks.sql.backend.sea.result_set import SeaResultSet

        # Create a new SeaResultSet with the filtered data
        manifest = result_set.manifest
        manifest.total_row_count = len(filtered_rows)

        filtered_result_set = SeaResultSet(
            connection=result_set.connection,
            execute_response=execute_response,
            sea_client=cast(SeaDatabricksClient, result_set.backend),
            result_data=result_data,
            manifest=manifest,
            buffer_size_bytes=result_set.buffer_size_bytes,
            arraysize=result_set.arraysize,
        )

        return filtered_result_set

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

        # Get all remaining rows as Arrow table
        arrow_table = result_set.results.remaining_rows()

        # Get the column name from the description
        if column_index >= len(result_set.description):
            raise ValueError(f"Column index {column_index} is out of bounds")

        column_name = result_set.description[column_index][0]

        # Filter the Arrow table
        filtered_table = ResultSetFilter._filter_arrow_table(
            arrow_table, column_name, allowed_values
        )

        # Create a new result set with filtered data
        command_id = result_set.command_id

        # Create an ExecuteResponse for the filtered data
        execute_response = ExecuteResponse(
            command_id=command_id,
            status=result_set.status,
            description=result_set.description,
            has_been_closed_server_side=result_set.has_been_closed_server_side,
            lz4_compressed=result_set.lz4_compressed,
            arrow_schema_bytes=result_set._arrow_schema_bytes,
            is_staging_operation=False,
        )

        # Create ResultData with the filtered arrow table as attachment
        # This mimics the hybrid disposition flow in build_queue
        from databricks.sql.backend.sea.models.base import ResultData
        from databricks.sql.backend.sea.result_set import SeaResultSet
        from databricks.sql.backend.sea.backend import SeaDatabricksClient
        import io

        # Convert the filtered table to Arrow stream format
        sink = io.BytesIO()
        with pyarrow.ipc.new_stream(sink, filtered_table.schema) as writer:
            writer.write_table(filtered_table)
        arrow_stream_bytes = sink.getvalue()

        # Create ResultData with attachment containing the filtered data
        filtered_result_data = ResultData(
            data=None,  # No JSON data
            external_links=None,  # No external links
            attachment=arrow_stream_bytes,  # Arrow data as attachment
        )

        # Update manifest to reflect new row count
        manifest = result_set.manifest
        # Create a copy of the manifest to avoid modifying the original
        from copy import deepcopy

        filtered_manifest = deepcopy(manifest)
        filtered_manifest.total_row_count = filtered_table.num_rows

        # Create a new SeaResultSet with the filtered data
        filtered_result_set = SeaResultSet(
            connection=result_set.connection,
            execute_response=execute_response,
            sea_client=cast(SeaDatabricksClient, result_set.backend),
            result_data=filtered_result_data,
            manifest=filtered_manifest,
            buffer_size_bytes=result_set.buffer_size_bytes,
            arraysize=result_set.arraysize,
        )

        return filtered_result_set

    @staticmethod
    def filter_by_column_values(
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

        # Convert to uppercase for case-insensitive comparison if needed
        if not case_sensitive:
            allowed_values = [v.upper() for v in allowed_values]

        return ResultSetFilter._filter_sea_result_set(
            result_set,
            lambda row: (
                len(row) > column_index
                and (
                    row[column_index].upper()
                    if not case_sensitive
                    else row[column_index]
                )
                in allowed_values
            ),
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
        valid_types = (
            table_types if table_types and len(table_types) > 0 else DEFAULT_TABLE_TYPES
        )

        # Check if we have an Arrow table (cloud fetch) or JSON data
        from databricks.sql.utils import CloudFetchQueue, ArrowQueue

        if isinstance(result_set.results, (CloudFetchQueue, ArrowQueue)):
            # For Arrow tables, we need to handle filtering differently
            return ResultSetFilter._filter_arrow_result_set(
                result_set, column_index=5, allowed_values=valid_types
            )
        else:
            # For JSON data, use the existing filter method
            # Table type is the 6th column (index 5)
            return ResultSetFilter.filter_by_column_values(
                result_set, 5, valid_types, case_sensitive=True
            )
