from enum import Enum


class AuthFlow(Enum):
    TOKEN_PASSTHROUGH = "token_passthrough"
    BROWSER_BASED_AUTHENTICATION = "browser_based_authentication"


class AuthMech(Enum):
    OTHER = "OTHER"
    PAT = "PAT"
    DATABRICKS_OAUTH = "DATABRICKS_OAUTH"
    EXTERNAL_AUTH = "EXTERNAL_AUTH"


class DatabricksClientType(Enum):
    SEA = "SEA"
    THRIFT = "THRIFT"


class DriverVolumeOperationType(Enum):
    TYPE_UNSPECIFIED = "type_unspecified"
    PUT = "put"
    GET = "get"
    DELETE = "delete"
    LIST = "list"
    QUERY = "query"


class ExecutionResultFormat(Enum):
    FORMAT_UNSPECIFIED = "format_unspecified"
    INLINE_ARROW = "inline_arrow"
    EXTERNAL_LINKS = "external_links"
    COLUMNAR_INLINE = "columnar_inline"


class StatementType(Enum):
    NONE = "none"
    QUERY = "query"
    SQL = "sql"
    UPDATE = "update"
    METADATA = "metadata"
