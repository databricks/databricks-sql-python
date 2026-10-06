import json
import math
import threading
import time
from dataclasses import dataclass, field
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, Optional, List, Any

from databricks.sql.common.http import HttpMethod
from databricks.sql.common.url_utils import normalize_host_with_protocol


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


@dataclass
class _CacheState:
    # Only values/coordination are shared; credentials and HTTP clients are not.
    flags: Optional[Dict[str, str]] = None
    ttl_seconds: int = DEFAULT_TTL_SECONDS
    last_refresh_time: float = 0
    lock: Any = field(default_factory=threading.RLock)
    refresh: Optional[Future] = None


def _cache_key(host, headers):
    workspace_id = (headers or {}).get("x-databricks-org-id")
    return (
        ("workspace", workspace_id)
        if workspace_id
        else ("host", normalize_host_with_protocol(host).lower())
    )


class FeatureFlagsContext:
    """
    Authenticated flag reader usable before any session/backend is opened.

    1. The very first check for any flag is a synchronous, BLOCKING operation.
    2. Subsequent refreshes (triggered near TTL expiry) are done asynchronously
       in the background, returning stale data until the refresh completes.
    """

    def __init__(
        self, host, executor, http_client, auth_provider, user_agent, headers, state
    ):
        from databricks.sql import __version__

        self._executor = executor  # Used for ASYNCHRONOUS refreshes
        self._state = state
        self._auth_provider = auth_provider
        self._headers = {"User-Agent": user_agent, **headers}

        endpoint_suffix = FEATURE_FLAGS_ENDPOINT_SUFFIX_FORMAT.format(__version__)
        self._feature_flag_endpoint = (
            normalize_host_with_protocol(host) + endpoint_suffix
        )

        # Use the provided HTTP client
        self._http_client = http_client

    def _is_refresh_needed(self) -> bool:
        """Checks if the cache is due for a proactive background refresh."""
        if self._state.flags is None:
            return False  # Not eligible for refresh until loaded once.

        refresh_threshold = self._state.last_refresh_time + (
            self._state.ttl_seconds - REFRESH_BEFORE_EXPIRY_SECONDS
        )
        return time.monotonic() > refresh_threshold

    def _get_value(self, name: str) -> Any:
        """
        Reads and parses a flag's JSON value.
        - BLOCKS on the first call until flags are fetched.
        - Returns cached values on subsequent calls, triggering non-blocking refreshes if needed.
        """
        with self._state.lock:
            # If cache has never been loaded, perform a synchronous, blocking fetch.
            if self._state.flags is None:
                self._refresh_flags()

            # If a proactive background refresh is needed, start one. This is non-blocking.
            elif self._is_refresh_needed() and (
                self._state.refresh is None or self._state.refresh.done()
            ):
                self._state.refresh = self._executor.submit(self._refresh_flags)

            raw = (self._state.flags or {}).get(name)
        try:
            return json.loads(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    def get_bool(self, name: str, default_value: bool = False) -> bool:
        value = self._get_value(name)
        return value if type(value) is bool else default_value

    def _get_int(self, name, bits, default_value):
        value = self._get_value(name)
        if type(value) is int and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            return value
        return default_value

    def get_int32(self, name: str, default_value=None) -> Optional[int]:
        return self._get_int(name, 32, default_value)

    def get_int64(self, name: str, default_value=None) -> Optional[int]:
        return self._get_int(name, 64, default_value)

    def get_double(self, name: str, default_value=None) -> Optional[float]:
        value = self._get_value(name)
        try:
            if type(value) in (int, float) and math.isfinite(value):
                return float(value)
        except OverflowError:
            pass
        return default_value

    def get_string(self, name: str, default_value=None) -> Optional[str]:
        value = self._get_value(name)
        return value if isinstance(value, str) else default_value

    def get_string_list(self, name: str, default_value=None) -> Optional[List[str]]:
        value = self._get_value(name)
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
        return default_value

    def _refresh_flags(self):
        """Performs a synchronous network request to fetch and update flags."""
        headers = dict(self._headers)
        try:
            # Authenticate the request
            self._auth_provider.add_headers(headers)

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
                if self._state.flags is None:
                    self._state.flags = {}

        except Exception:
            # On exception, initialize with an empty dictionary to prevent re-blocking.
            if self._state.flags is None:
                self._state.flags = {}

    def _update_cache_from_response(self, ff_response: FeatureFlagsResponse):
        """Atomically updates the internal cache state from a successful server response."""
        with self._state.lock:
            self._state.flags = {flag.name: flag.value for flag in ff_response.flags}
            if ff_response.ttl_seconds is not None and ff_response.ttl_seconds > 0:
                self._state.ttl_seconds = ff_response.ttl_seconds
            self._state.last_refresh_time = time.monotonic()


class FeatureFlagsContextFactory:
    """
    Shares flag values per workspace, independent of telemetry/session lifetime.
    Also manages a shared ThreadPoolExecutor for all background refresh operations.
    """

    _context_map: Dict[tuple, _CacheState] = {}
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
    def get_instance(
        cls, host, http_client, auth_provider, user_agent, headers=None
    ) -> FeatureFlagsContext:
        """Reuse the cache with this caller's authenticated transport, even pre-session."""
        headers = {name.lower(): value for name, value in (headers or {}).items()}
        with cls._lock:
            cls._initialize()
            assert cls._executor is not None

            key = _cache_key(host, headers)
            if key not in cls._context_map:
                cls._context_map[key] = _CacheState()
            return FeatureFlagsContext(
                host,
                cls._executor,
                http_client,
                auth_provider,
                user_agent,
                headers,
                cls._context_map[key],
            )

    @classmethod
    def remove_instance(cls, host, headers=None):
        """Evicts a workspace's values and shuts down the executor if the cache is empty."""
        with cls._lock:
            headers = {name.lower(): value for name, value in (headers or {}).items()}
            key = _cache_key(host, headers)
            if key in cls._context_map:
                cls._context_map.pop(key, None)

            # If this was the last active context, clean up the thread pool.
            if not cls._context_map and cls._executor is not None:
                cls._executor.shutdown(wait=False)
                cls._executor = None
