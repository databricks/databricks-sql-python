from enum import Enum


class DriverVolumeOperationType(Enum):
    TYPE_UNSPECIFIED = "type_unspecified"
    PUT = "put"
    GET = "get"
    DELETE = "delete"
    LIST = "list"
    QUERY = "query"
