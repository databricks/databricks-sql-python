from dataclasses import dataclass
from typing import Optional, Callable, Any


@dataclass(frozen=True)
class ResultColumn:
    """
    Represents a mapping between JDBC specification column names and actual result set column names.

    Attributes:
        column_name: JDBC specification column name (e.g., "TABLE_CAT")
        result_set_column_name: Server result column name from SEA (e.g., "catalog")
        column_type: SQL type code from databricks.sql.types
        transform_value: Optional function to transform values for this column
    """

    column_name: str
    result_set_column_name: Optional[str]  # None if SEA doesn't return this column
    column_type: str
    transform_value: Optional[Callable[[Any], Any]] = None
