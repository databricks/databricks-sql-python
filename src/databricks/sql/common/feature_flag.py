import json
import threading
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, List, Any, TYPE_CHECKING

from databricks.sql.common.http import HttpMethod

if TYPE_CHECKING:
    from databricks.sql.client import Connection


@dataclass
class FeatureFlagEntry:
    """Represents a single feature flag from the server response."""

    name: str
    value: str


@dataclass
class FeatureFlagsResponse:
    """Represents the full JSON response from the feature flag endpoint."""

    flags: List[FeatureFlagEntry] = field(default_factory=list)
    ttl_seconds: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FeatureFlagsResponse":
        """Factory method to create an instance from a dictionary (parsed JSON)."""
        flags_data = data.get("flags", [])
        flags_list = [FeatureFlagEntry(**flag) for flag in flags_data]
        return cls(flags=flags_list, ttl_seconds=data.get("ttl_seconds"))


# --- Constants ---
FEATURE_FLAGS_ENDPOINT_SUFFIX_FORMAT = (
    "/api/2.0/connector-service/feature-flags/PYTHON/{}"
)
DEFAULT_TTL_SECONDS = 900  # 15 minutes
REFRESH_BEFORE_EXPIRY_SECONDS = 10  # Start proactive refresh 10s before expiry


class FeatureFlagsContext:
    """
    Manages fetching and caching of server-side feature flags for a connection.

    1. The very first check for any flag is a synchronous, BLOCKING operation.
    2. Subsequent refreshes (triggered near TTL expiry) are done asynchronously
       in the background, returning stale data until the refresh completes.
    """

    def __init__(
        self, connection: "Connection", executor: ThreadPoolExecutor, http_client
    ):
        from databricks.sql import __version__

        self._connection = connection
        self._executor = executor  # Used for ASYNCHRONOUS refreshes
        self._lock = threading.RLock()

        # Cache state: `None` indicates the cache has never been loaded.
        self._flags: Optional[Dict[str, str]] = None
        self._ttl_seconds: int = DEFAULT_TTL_SECONDS
        self._last_refresh_time: float = 0

        endpoint_suffix = FEATURE_FLAGS_ENDPOINT_SUFFIX_FORMAT.format(__version__)
        self._feature_flag_endpoint = (
            f"https://{self._connection.session.host}{endpoint_suffix}"
        )

        # Use the provided HTTP client
        self._http_client = http_client

    def _is_refresh_needed(self) -> bool:
        """Checks if the cache is due for a proactive background refresh."""
        if self._flags is None:
            return False  # Not eligible for refresh until loaded once.

        refresh_threshold = self._last_refresh_time + (
            self._ttl_seconds - REFRESH_BEFORE_EXPIRY_SECONDS
        )
        return time.monotonic() > refresh_threshold

    def get_flag_value(self, name: str, default_value: Any) -> Any:
        """
        Checks if a feature is enabled.
        - BLOCKS on the first call until flags are fetched.
        - Returns cached values on subsequent calls, triggering non-blocking refreshes if needed.
        """
        with self._lock:
            # If cache has never been loaded, perform a synchronous, blocking fetch.
            if self._flags is None:
                self._refresh_flags()

            # If a proactive background refresh is needed, start one. This is non-blocking.
            elif self._is_refresh_needed():
                # We don't check for an in-flight refresh; the executor queues the task, which is safe.
                self._executor.submit(self._refresh_flags)

            assert self._flags is not None

            # Now, return the value from the populated cache.
            return self._flags.get(name, default_value)

    def _refresh_flags(self):
        """Performs a synchronous network request to fetch and update flags."""
        headers = {}
        try:
            # Authenticate the request
            self._connection.session.auth_provider.add_headers(headers)
            headers["User-Agent"] = self._connection.session.useragent_header

            response = self._http_client.request(
                HttpMethod.GET, self._feature_flag_endpoint, headers=headers, timeout=30
            )

            if response.status == 200:
                # Parse JSON response from urllib3 response data
                response_data = json.loads(response.data.decode())
                ff_response = FeatureFlagsResponse.from_dict(response_data)
                self._update_cache_from_response(ff_response)
            else:
                # On failure, initialize with an empty dictionary to prevent re-blocking.
                if self._flags is None:
                    self._flags = {}

        except Exception as e:
            # On exception, initialize with an empty dictionary to prevent re-blocking.
            if self._flags is None:
                self._flags = {}

    def _update_cache_from_response(self, ff_response: FeatureFlagsResponse):
        """Atomically updates the internal cache state from a successful server response."""
        with self._lock:
            self._flags = {flag.name: flag.value for flag in ff_response.flags}
            if ff_response.ttl_seconds is not None and ff_response.ttl_seconds > 0:
                self._ttl_seconds = ff_response.ttl_seconds
            self._last_refresh_time = time.monotonic()


class FeatureFlagsContextFactory:
    """
    Manages a singleton instance of FeatureFlagsContext per connection session.
    Also manages a shared ThreadPoolExecutor for all background refresh operations.
    """

    _context_map: Dict[str, FeatureFlagsContext] = {}
    _executor: Optional[ThreadPoolExecutor] = None
    _lock = threading.Lock()

    @classmethod
    def _initialize(cls):
        """Initializes the shared executor for async refreshes if it doesn't exist."""
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(
                max_workers=3, thread_name_prefix="feature-flag-refresher"
            )

    @classmethod
    def get_instance(cls, connection: "Connection") -> FeatureFlagsContext:
        """Gets or creates a FeatureFlagsContext for the given connection."""
        with cls._lock:
            cls._initialize()
            assert cls._executor is not None

            # Use the unique session ID as the key
            key = connection.get_session_id_hex()
            if key not in cls._context_map:
                cls._context_map[key] = FeatureFlagsContext(
                    connection, cls._executor, connection.session.http_client
                )
            return cls._context_map[key]

    @classmethod
    def remove_instance(cls, connection: "Connection"):
        """Removes the context for a given connection and shuts down the executor if no clients remain."""
        with cls._lock:
            key = connection.get_session_id_hex()
            if key in cls._context_map:
                cls._context_map.pop(key, None)

            # If this was the last active context, clean up the thread pool.
            if not cls._context_map and cls._executor is not None:
                cls._executor.shutdown(wait=False)
                cls._executor = None
