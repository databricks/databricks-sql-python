import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from enum import Enum
import threading
from dataclasses import dataclass
from contextlib import contextmanager
from typing import Generator, Optional
import logging
from requests.adapters import HTTPAdapter
from databricks.sql.auth.retry import DatabricksRetryPolicy, CommandType
from pybreaker import CircuitBreaker, CircuitBreakerError

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

    @contextmanager
    def execute(
        self, method: HttpMethod, url: str, **kwargs
    ) -> Generator[requests.Response, None, None]:
        logger.info("Executing HTTP request: %s with url: %s", method.value, url)
        response = None
        try:
            response = self.session.request(method.value, url, **kwargs)
            yield response
        except Exception as e:
            logger.error("Error executing HTTP request in DatabricksHttpClient: %s", e)
            raise e
        finally:
            if response is not None:
                response.close()

    def close(self):
        self.session.close()


class TelemetryHTTPAdapter(HTTPAdapter):
    """
    Custom HTTP adapter to prepare our DatabricksRetryPolicy before each request.
    This ensures the retry timer is started and the command type is set correctly,
    allowing the policy to manage its state for the duration of the request retries.
    """

    def send(self, request, **kwargs):
        self.max_retries.command_type = CommandType.OTHER
        self.max_retries.start_retry_timer()
        return super().send(request, **kwargs)


class TelemetryHttpClient:  # TODO: Unify all the http clients in the PySQL Connector
    """Singleton HTTP client for sending telemetry data."""

    _instance: Optional["TelemetryHttpClient"] = None
    _lock = threading.Lock()

    TELEMETRY_RETRY_STOP_AFTER_ATTEMPTS_COUNT = 3
    TELEMETRY_RETRY_DELAY_MIN = 1.0
    TELEMETRY_RETRY_DELAY_MAX = 10.0
    TELEMETRY_RETRY_STOP_AFTER_ATTEMPTS_DURATION = 30.0

    CIRCUIT_BREAKER_FAIL_MAX = 5
    CIRCUIT_BREAKER_RESET_TIMEOUT = 60

    def __init__(self):
        """Initializes the session and mounts the custom retry adapter."""
        retry_policy = DatabricksRetryPolicy(
            delay_min=self.TELEMETRY_RETRY_DELAY_MIN,
            delay_max=self.TELEMETRY_RETRY_DELAY_MAX,
            stop_after_attempts_count=self.TELEMETRY_RETRY_STOP_AFTER_ATTEMPTS_COUNT,
            stop_after_attempts_duration=self.TELEMETRY_RETRY_STOP_AFTER_ATTEMPTS_DURATION,
            delay_default=1.0,
            force_dangerous_codes=[],
        )
        adapter = TelemetryHTTPAdapter(max_retries=retry_policy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.breaker = CircuitBreaker(
            fail_max=self.CIRCUIT_BREAKER_FAIL_MAX,
            reset_timeout=self.CIRCUIT_BREAKER_RESET_TIMEOUT,
        )

    @classmethod
    def get_instance(cls) -> "TelemetryHttpClient":
        """Get the singleton instance of the TelemetryHttpClient."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.debug("Initializing singleton TelemetryHttpClient")
                    cls._instance = TelemetryHttpClient()
        return cls._instance

    def post(self, url: str, **kwargs) -> requests.Response:
        """
        Executes a POST request using the configured session.

        This is a blocking call intended to be run in a background thread.
        """
        logger.debug("Executing telemetry POST request to: %s", url)
        try:
            return self.breaker.call(self.session.post, url, **kwargs)
        except CircuitBreakerError as e:
            logger.error("Circuit breaker error: %s", e)
            raise e
        except Exception as e:
            logger.error("Error executing telemetry POST request: %s", e)
            raise e

    def close(self):
        """Closes the underlying requests.Session."""
        logger.debug("Closing TelemetryHttpClient session.")
        self.session.close()
        # Clear the instance to allow for re-initialization if needed
        with TelemetryHttpClient._lock:
            TelemetryHttpClient._instance = None
