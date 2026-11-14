from enum import Enum
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


# Enums for HTTP Methods
class HttpMethod(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


# HTTP request headers
class HttpHeader(str, Enum):
    CONTENT_TYPE = "Content-Type"
    AUTHORIZATION = "Authorization"


# Dataclass for OAuthHTTP Response
@dataclass
class OAuthResponse:
    token_type: str = ""
    expires_in: int = 0
    ext_expires_in: int = 0
    expires_on: int = 0
    not_before: int = 0
    resource: str = ""
    access_token: str = ""
    refresh_token: str = ""
