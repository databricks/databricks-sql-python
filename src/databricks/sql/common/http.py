import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from enum import Enum
import threading
from dataclasses import dataclass

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


# Singleton class for common Http Client
class DatabricksHttpClient:
    ## TODO: Unify all the http clients in the PySQL Connector

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=5,
            pool_maxsize=10,
            max_retries=Retry(total=10, backoff_factor=0.1),
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @classmethod
    def get_instance(cls) -> "DatabricksHttpClient":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = DatabricksHttpClient()
        return cls._instance

    def execute(self, method: HttpMethod, url: str, **kwargs) -> requests.Response:
        with self.session.request(method.value, url, **kwargs) as response:
            return response

    def close(self):
        self.session.close()
