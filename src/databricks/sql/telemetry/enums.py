from enum import Enum


class AuthFlow(Enum):
    TOKEN_PASSTHROUGH = "token_passthrough"
    CLIENT_CREDENTIALS = "client_credentials"
    BROWSER_BASED_AUTHENTICATION = "browser_based_authentication"
    AZURE_MANAGED_IDENTITIES = "azure_managed_identities"


class AuthMech(Enum):
    OTHER = "other"
    PAT = "pat"
    OAUTH = "oauth"


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