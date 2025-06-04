"""
Client-side filtering utilities for Databricks SQL connector.

This module provides filtering capabilities for result sets returned by different backends.
"""

import logging
from typing import List, Optional, Any, Dict, Callable, TypeVar, Generic, cast

# Import SeaResultSet for type checking
from databricks.sql.backend.sea_result_set import SeaResultSet

# Type variable for the result set type
T = TypeVar('T')

logger = logging.getLogger(__name__)


class ResultSetFilter:
    """
    A general-purpose filter for result sets that can be applied to any backend.
    
    This class provides methods to filter result sets based on various criteria,
    similar to the client-side filtering in the JDBC connector.
    """
    
    @staticmethod
    def filter_by_column_values(
        result_set: Any,
        column_index: int,
        allowed_values: List[str],
        case_sensitive: bool = False
    ) -> Any:
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
        
        # Determine the type of result set and apply appropriate filtering
        if isinstance(result_set, SeaResultSet):
            return ResultSetFilter._filter_sea_result_set(
                result_set, 
                lambda row: (
                    len(row) > column_index and 
                    isinstance(row[column_index], str) and
                    (row[column_index].upper() if not case_sensitive else row[column_index]) in allowed_values
                )
            )
        
        # For other result set types, return the original (should be handled by specific implementations)
        logger.warning(
            f"Filtering not implemented for result set type: {type(result_set).__name__}"
        )
        return result_set
    
    @staticmethod
    def filter_tables_by_type(
        result_set: Any,
        table_types: Optional[List[str]] = None
    ) -> Any:
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
        valid_types = table_types if table_types and len(table_types) > 0 else DEFAULT_TABLE_TYPES
        
        # Table type is typically in the 4th column (index 3)
        return ResultSetFilter.filter_by_column_values(result_set, 3, valid_types, case_sensitive=False)
    
    @staticmethod
    def _filter_sea_result_set(result_set: Any, filter_func: Callable[[List[Any]], bool]) -> Any:
        """
        Filter a SEA result set using the provided filter function.
        
        Args:
            result_set: The SEA result set to filter
            filter_func: Function that takes a row and returns True if the row should be included
            
        Returns:
            A filtered SEA result set
        """
        # Type checking for SeaResultSet
        if not isinstance(result_set, SeaResultSet):
            return result_set
            
        # Create a filtered version of the result set
        sea_result = cast(SeaResultSet, result_set)
        filtered_response = sea_result._response.copy()  # type: ignore
        
        # If there's a result with rows, filter them
        if "result" in filtered_response and "data_array" in filtered_response["result"]:
            rows = filtered_response["result"]["data_array"]
            filtered_rows = [row for row in rows if filter_func(row)]
            filtered_response["result"]["data_array"] = filtered_rows
            
            # Update row count if present
            if "row_count" in filtered_response["result"]:
                filtered_response["result"]["row_count"] = len(filtered_rows)
        
        # Create a new result set with the filtered data
        return SeaResultSet(
            connection=sea_result.connection,
            sea_response=filtered_response,
            sea_client=sea_result._sea_client,  # type: ignore
            buffer_size_bytes=sea_result._buffer_size_bytes,  # type: ignore
            arraysize=sea_result._arraysize,  # type: ignore
        )