from enum import Enum


class AuthMech(Enum):
    OTHER = "other"
    PAT = "pat"
    OAUTH = "oauth"
