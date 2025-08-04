from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ResultColumn:
    """
    Represents a mapping between JDBC specification column names and actual result set column names.

    Attributes:
        thrift_col_name: Column name as returned by Thrift (e.g., "TABLE_CAT")
        sea_col_name: Server result column name from SEA (e.g., "catalog")
        thrift_col_type: SQL type name
    """

    thrift_col_name: str
    sea_col_name: Optional[str]  # None if SEA doesn't return this column
    thrift_col_type: str
