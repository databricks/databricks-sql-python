from dataclasses import dataclass
from typing import Optional, Callable, Any


@dataclass(frozen=True)
class ResultColumn:
    """
    Represents a mapping between Thrift specification column names and SEA column names.

    Attributes:
        thrift_col_name: Column name as returned by Thrift (e.g., "TABLE_CAT")
        sea_col_name: Server result column name from SEA (e.g., "catalog")
        thrift_col_type: SQL type name
        transform_value: Optional callback to transform values for this column
    """

    thrift_col_name: str
    sea_col_name: Optional[str]  # None if SEA doesn't return this column
    thrift_col_type: str
    transform_value: Optional[Callable[[Any], Any]] = None
