from enum import Enum


class StatementType(Enum):
    NONE = "none"
    QUERY = "query"
    SQL = "sql"
    UPDATE = "update"
    METADATA = "metadata"
