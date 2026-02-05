import threading
import time
import logging
import json
from queue import Queue, Full
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from databricks.sql.telemetry.models.event import (
    TelemetryEvent,
    DriverSystemConfiguration,
    DriverErrorInfo,
    DriverConnectionParameters,
    HostDetails,
)
from databricks.sql.telemetry.models.frontend_logs import (
    TelemetryFrontendLog,
    TelemetryClientContext,
    FrontendLogContext,
    FrontendLogEntry,
)
from databricks.sql.telemetry.models.enums import (
    AuthMech,
    AuthFlow,
    DatabricksClientType,
)
from databricks.sql.telemetry.models.endpoint_models import (
    TelemetryRequest,
    TelemetryResponse,
)
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    DatabricksOAuthProvider,
    ExternalAuthProvider,
)
import sys
import platform
import uuid
import locale
from databricks.sql.telemetry.utils import BaseTelemetryClient
from databricks.sql.common.feature_flag import FeatureFlagsContextFactory
from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.common.http import HttpMethod
from databricks.sql.exc import RequestError
from databricks.sql.telemetry.telemetry_push_client import (
    ITelemetryPushClient,
    TelemetryPushClient,
    CircuitBreakerTelemetryPushClient,
)
from databricks.sql.common.url_utils import normalize_host_with_protocol

if TYPE_CHECKING:
    from databricks.sql.client import Connection

logger = logging.getLogger(__name__)


class TelemetryHelper:
    """Helper class for getting telemetry related information."""

    _DRIVER_SYSTEM_CONFIGURATION = None
    TELEMETRY_FEATURE_FLAG_NAME = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForPythonDriver"

    @classmethod
    def get_driver_system_configuration(cls) -> DriverSystemConfiguration:
        if cls._DRIVER_SYSTEM_CONFIGURATION is None:
            from databricks.sql import __version__

            cls._DRIVER_SYSTEM_CONFIGURATION = DriverSystemConfiguration(
                driver_name="Databricks SQL Python Connector",
                driver_version=__version__,
                runtime_name=f"Python {sys.version.split()[0]}",
                runtime_vendor=platform.python_implementation(),
                runtime_version=platform.python_version(),
                os_name=platform.system(),
                os_version=platform.release(),
                os_arch=platform.machine(),
                client_app_name=None,  # TODO: Add client app name
                locale_name=locale.getlocale()[0] or locale.getdefaultlocale()[0],
                char_set_encoding=sys.getdefaultencoding(),
            )
        return cls._DRIVER_SYSTEM_CONFIGURATION

    @staticmethod
    def get_auth_mechanism(auth_provider):
        """Get the auth mechanism for the auth provider."""
        # AuthMech is an enum with the following values:
        # TYPE_UNSPECIFIED, OTHER, PAT, OAUTH

        if not auth_provider:
            return None
        if isinstance(auth_provider, AccessTokenAuthProvider):
            return AuthMech.PAT
        elif isinstance(auth_provider, DatabricksOAuthProvider):
            return AuthMech.OAUTH
        else:
            return AuthMech.OTHER

    @staticmethod
    def get_auth_flow(auth_provider):
        """Get the auth flow for the auth provider."""
        # AuthFlow is an enum with the following values:
        # TYPE_UNSPECIFIED, TOKEN_PASSTHROUGH, CLIENT_CREDENTIALS, BROWSER_BASED_AUTHENTICATION

        if not auth_provider:
            return None
        if isinstance(auth_provider, DatabricksOAuthProvider):
            if auth_provider._access_token and auth_provider._refresh_token:
                return AuthFlow.TOKEN_PASSTHROUGH
            else:
                return AuthFlow.BROWSER_BASED_AUTHENTICATION
        elif isinstance(auth_provider, ExternalAuthProvider):
            return AuthFlow.CLIENT_CREDENTIALS
        else:
            return None

    @staticmethod
    def is_telemetry_enabled(connection: "Connection") -> bool:
        # Fast path: force enabled - skip feature flag fetch entirely
        if connection.force_enable_telemetry:
            return True

        # Fast path: disabled - no need to check feature flag
        if not connection.enable_telemetry:
            return False

        # Only fetch feature flags when enable_telemetry=True and not forced
        context = FeatureFlagsContextFactory.get_instance(connection)
        flag_value = context.get_flag_value(
            TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME, default_value=False
        )
        return str(flag_value).lower() == "true"


class NoopTelemetryClient(BaseTelemetryClient):
    """
    NoopTelemetryClient is a telemetry client that does not send any events to the server.
    It is used when telemetry is disabled.
    """

    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(NoopTelemetryClient, cls).__new__(cls)
        return cls._instance

    def export_initial_telemetry_log(
        self, driver_connection_params, user_agent, session_id=None
    ):
        pass

    def export_failure_log(self, error_name, error_message, session_id=None):
        pass

    def export_latency_log(
        self, latency_ms, sql_execution_event, sql_statement_id, session_id=None
    ):
        pass

    def close(self):
        pass

    def _flush(self):
        pass


class TelemetryClient(BaseTelemetryClient):
    """
    Telemetry client class that handles sending telemetry events in batches to the server.
    It uses a thread pool to handle asynchronous operations, that it gets from the TelemetryClientFactory.
    """

    # Telemetry endpoint paths
    TELEMETRY_AUTHENTICATED_PATH = "/telemetry-ext"
    TELEMETRY_UNAUTHENTICATED_PATH = "/telemetry-unauth"

    def __init__(
        self,
        telemetry_enabled: bool,
        session_id_hex: str,
        auth_provider,
        host_url: str,
        executor,
        batch_size: int,
        client_context,
    ) -> None:
        logger.debug("Initializing TelemetryClient for connection: %s", session_id_hex)
        self._telemetry_enabled = telemetry_enabled
        self._batch_size = batch_size
        self._session_id_hex = session_id_hex
        self._auth_provider = auth_provider
        self._user_agent = None

        # OPTIMIZATION: Use lock-free Queue instead of list + lock
        # Queue is thread-safe internally and has better performance under concurrency
        self._events_queue: Queue[TelemetryFrontendLog] = Queue(maxsize=batch_size * 2)

        self._driver_connection_params = None
        self._host_url = host_url
        self._executor = executor

        # Create own HTTP client from client context
        self._http_client = UnifiedHttpClient(client_context)

        # Create telemetry push client based on circuit breaker enabled flag
        if client_context.telemetry_circuit_breaker_enabled:
            # Create circuit breaker telemetry push client
            # (circuit breakers created on-demand)
            self._telemetry_push_client: ITelemetryPushClient = (
                CircuitBreakerTelemetryPushClient(
                    TelemetryPushClient(self._http_client),
                    host_url,
                )
            )
        else:
            # Circuit breaker disabled - use direct telemetry push client
            self._telemetry_push_client = TelemetryPushClient(self._http_client)

    def _export_event(self, event):
        """Add an event to the batch queue and flush if batch is full"""
        logger.debug("Exporting event for connection %s", self._session_id_hex)

        # OPTIMIZATION: Use non-blocking put with queue
        # No explicit lock needed - Queue is thread-safe internally
        try:
            self._events_queue.put_nowait(event)
        except Full:
            # Queue is full, trigger immediate flush
            logger.debug("Event queue full, triggering flush")
            self._flush()
            # Try again after flush
            try:
                self._events_queue.put_nowait(event)
            except Full:
                # Still full, drop event (acceptable for telemetry)
                logger.debug("Dropped telemetry event - queue still full")

        # Check if we should flush based on queue size
        if self._events_queue.qsize() >= self._batch_size:
            logger.debug(
                "Batch size limit reached (%s), flushing events", self._batch_size
            )
            self._flush()

    def _flush(self):
        """Flush the current batch of events to the server"""
        # OPTIMIZATION: Drain queue without locks
        # Collect all events currently in the queue
        events_to_flush = []
        while not self._events_queue.empty():
            try:
                event = self._events_queue.get_nowait()
                events_to_flush.append(event)
            except:
                # Queue is empty
                break

        if events_to_flush:
            logger.debug("Flushing %s telemetry events to server", len(events_to_flush))
            self._send_telemetry(events_to_flush)

    def _send_telemetry(self, events):
        """Send telemetry events to the server"""

        request = TelemetryRequest(
            uploadTime=int(time.time() * 1000),
            items=[],
            protoLogs=[event.to_json() for event in events],
        )

        sent_count = len(events)

        path = (
            self.TELEMETRY_AUTHENTICATED_PATH
            if self._auth_provider
            else self.TELEMETRY_UNAUTHENTICATED_PATH
        )
        url = normalize_host_with_protocol(self._host_url) + path

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        if self._auth_provider:
            self._auth_provider.add_headers(headers)

        try:
            logger.debug("Submitting telemetry request to thread pool")

            # Use unified HTTP client
            future = self._executor.submit(
                self._send_with_unified_client,
                url,
                data=request.to_json(),
                headers=headers,
                timeout=900,
            )

            future.add_done_callback(
                lambda fut: self._telemetry_request_callback(fut, sent_count=sent_count)
            )
        except Exception as e:
            logger.debug("Failed to submit telemetry request: %s", e)

    def _send_with_unified_client(self, url, data, headers, timeout=900):
        """Helper method to send telemetry using the unified HTTP client."""
        try:
            response = self._telemetry_push_client.request(
                HttpMethod.POST, url, body=data, headers=headers, timeout=timeout
            )
            return response
        except Exception as e:
            logger.debug("Failed to send telemetry with unified client: %s", e)
            raise

    def _telemetry_request_callback(self, future, sent_count: int):
        """Callback function to handle telemetry request completion"""
        try:
            response = future.result()

            # Check if response is successful (urllib3 uses response.status)
            is_success = 200 <= response.status < 300
            if not is_success:
                logger.debug(
                    "Telemetry request failed with status code: %s, response: %s",
                    response.status,
                    response.data.decode() if response.data else "",
                )

            # Parse JSON response (urllib3 uses response.data)
            response_data = json.loads(response.data.decode()) if response.data else {}
            telemetry_response = TelemetryResponse(**response_data)

            logger.debug(
                "Pushed Telemetry logs with success count: %s, error count: %s",
                telemetry_response.numProtoSuccess,
                len(telemetry_response.errors),
            )

            if telemetry_response.errors:
                logger.debug(
                    "Telemetry push failed for some events with errors: %s",
                    telemetry_response.errors,
                )

            # Check for partial failures
            if sent_count != telemetry_response.numProtoSuccess:
                logger.debug(
                    "Partial failure pushing telemetry. Sent: %s, Succeeded: %s, Errors: %s",
                    sent_count,
                    telemetry_response.numProtoSuccess,
                    telemetry_response.errors,
                )

        except Exception as e:
            logger.debug("Telemetry request failed with exception: %s", e)

    def _export_telemetry_log(self, session_id=None, **telemetry_event_kwargs):
        """
        Common helper method for exporting telemetry logs.

        Args:
            session_id: Optional session ID for this event. If not provided, uses the client's session ID.
            **telemetry_event_kwargs: Keyword arguments to pass to TelemetryEvent constructor
        """
        # Use provided session_id or fall back to client's session_id
        actual_session_id = session_id or self._session_id_hex
        logger.debug("Exporting telemetry log for connection %s", actual_session_id)

        try:
            # Set common fields for all telemetry events
            event_kwargs = {
                "session_id": actual_session_id,
                "system_configuration": TelemetryHelper.get_driver_system_configuration(),
                "driver_connection_params": self._driver_connection_params,
            }
            # Add any additional fields passed in
            event_kwargs.update(telemetry_event_kwargs)

            telemetry_frontend_log = TelemetryFrontendLog(
                frontend_log_event_id=str(uuid.uuid4()),
                context=FrontendLogContext(
                    client_context=TelemetryClientContext(
                        timestamp_millis=int(time.time() * 1000),
                        user_agent=self._user_agent,
                    )
                ),
                entry=FrontendLogEntry(sql_driver_log=TelemetryEvent(**event_kwargs)),
            )

            self._export_event(telemetry_frontend_log)

        except Exception as e:
            logger.debug("Failed to export telemetry log: %s", e)

    def export_initial_telemetry_log(
        self, driver_connection_params, user_agent, session_id=None
    ):
        self._driver_connection_params = driver_connection_params
        self._user_agent = user_agent
        self._export_telemetry_log(session_id=session_id)

    def export_failure_log(self, error_name, error_message, session_id=None):
        error_info = DriverErrorInfo(error_name=error_name, stack_trace=error_message)
        self._export_telemetry_log(session_id=session_id, error_info=error_info)

    def export_latency_log(
        self, latency_ms, sql_execution_event, sql_statement_id, session_id=None
    ):
        self._export_telemetry_log(
            session_id=session_id,
            sql_statement_id=sql_statement_id,
            sql_operation=sql_execution_event,
            operation_latency_ms=latency_ms,
        )

    def close(self):
        """Flush remaining events before closing

        IMPORTANT: This method does NOT close self._http_client.

        Rationale:
        - _flush() submits async work to the executor that uses _http_client
        - If we closed _http_client here, async callbacks would fail with AttributeError
        - Instead, we let _http_client live as long as needed:
          * Pending futures hold references to self (via bound methods)
          * This keeps self alive, which keeps self._http_client alive
          * When all futures complete, Python GC will clean up naturally
        - The __del__ method ensures eventual cleanup during garbage collection

        This design prevents race conditions while keeping telemetry truly async.
        """
        logger.debug("Closing TelemetryClient for connection %s", self._session_id_hex)
        self._flush()

    def __del__(self):
        """Cleanup when TelemetryClient is garbage collected

        This ensures _http_client is eventually closed when the TelemetryClient
        object is destroyed. By this point, all async work should be complete
        (since the futures held references keeping us alive), so it's safe to
        close the http client.
        """
        try:
            if hasattr(self, "_http_client") and self._http_client:
                self._http_client.close()
        except Exception:
            pass


class _TelemetryClientHolder:
    """
    Holds a telemetry client with reference counting.
    Multiple connections to the same host share one client.
    """

    def __init__(self, client: BaseTelemetryClient):
        self.client = client
        self.refcount = 1

    def increment(self):
        """Increment reference count when a new connection uses this client"""
        self.refcount += 1

    def decrement(self):
        """Decrement reference count when a connection closes"""
        self.refcount -= 1
        return self.refcount


class TelemetryClientFactory:
    """
    Static factory class for creating and managing telemetry clients.
    It uses a thread pool to handle asynchronous operations and a single flush thread for all clients.

    Clients are shared at the HOST level - multiple connections to the same host
    share a single TelemetryClient to enable efficient batching and reduce load
    on the telemetry endpoint.
    """

    _clients: Dict[
        str, _TelemetryClientHolder
    ] = {}  # Map of host_url -> TelemetryClientHolder
    _executor: Optional[ThreadPoolExecutor] = None
    _initialized: bool = False
    _lock = threading.RLock()  # Thread safety for factory operations
    # used RLock instead of Lock to avoid deadlocks when garbage collection is triggered
    _original_excepthook = None
    _excepthook_installed = False

    # Shared flush thread for all clients
    _flush_thread = None
    _flush_event = threading.Event()
    _flush_interval_seconds = 300  # 5 minutes

    DEFAULT_BATCH_SIZE = 100
    UNKNOWN_HOST = "unknown-host"

    @staticmethod
    def getHostUrlSafely(host_url):
        """
        Safely get host URL with fallback to UNKNOWN_HOST.

        Args:
            host_url: The host URL to validate

        Returns:
            The host_url if valid, otherwise UNKNOWN_HOST
        """
        if not host_url or not isinstance(host_url, str) or not host_url.strip():
            return TelemetryClientFactory.UNKNOWN_HOST
        return host_url

    @classmethod
    def _initialize(cls):
        """Initialize the factory if not already initialized"""

        if not cls._initialized:
            cls._clients = {}
            cls._executor = ThreadPoolExecutor(
                max_workers=10
            )  # Thread pool for async operations
            cls._install_exception_hook()
            cls._start_flush_thread()
            cls._initialized = True
            logger.debug(
                "TelemetryClientFactory initialized with thread pool (max_workers=10)"
            )

    @classmethod
    def _start_flush_thread(cls):
        """Start the shared background thread for periodic flushing of all clients"""
        cls._flush_event.clear()
        cls._flush_thread = threading.Thread(target=cls._flush_worker, daemon=True)
        cls._flush_thread.start()

    @classmethod
    def _flush_worker(cls):
        """Background worker thread for periodic flushing of all clients"""
        while not cls._flush_event.wait(cls._flush_interval_seconds):
            logger.debug("Performing periodic flush for all telemetry clients")

            with cls._lock:
                clients_to_flush = list(cls._clients.values())

                for holder in clients_to_flush:
                    holder.client._flush()

    @classmethod
    def _stop_flush_thread(cls):
        """Stop the shared background flush thread"""
        if cls._flush_thread is not None:
            cls._flush_event.set()
            cls._flush_thread.join(timeout=1.0)
            cls._flush_thread = None

    @classmethod
    def _install_exception_hook(cls):
        """Install global exception handler for unhandled exceptions"""
        if not cls._excepthook_installed:
            cls._original_excepthook = sys.excepthook
            sys.excepthook = cls._handle_unhandled_exception
            cls._excepthook_installed = True
            logger.debug("Global exception handler installed for telemetry")

    @classmethod
    def _handle_unhandled_exception(cls, exc_type, exc_value, exc_traceback):
        """Handle unhandled exceptions by sending telemetry and flushing thread pool"""
        logger.debug("Handling unhandled exception: %s", exc_type.__name__)

        clients_to_close = list(cls._clients.values())
        for holder in clients_to_close:
            holder.client.close()

        # Call the original exception handler to maintain normal behavior
        if cls._original_excepthook:
            cls._original_excepthook(exc_type, exc_value, exc_traceback)

    @staticmethod
    def initialize_telemetry_client(
        telemetry_enabled,
        session_id_hex,
        auth_provider,
        host_url,
        batch_size,
        client_context,
    ):
        """
        Initialize a telemetry client for a specific connection if telemetry is enabled.

        Clients are shared at the HOST level - multiple connections to the same host
        will share a single TelemetryClient with reference counting.
        """
        try:
            # Safely get host_url with fallback to UNKNOWN_HOST
            host_url = TelemetryClientFactory.getHostUrlSafely(host_url)

            with TelemetryClientFactory._lock:
                TelemetryClientFactory._initialize()

                if host_url in TelemetryClientFactory._clients:
                    # Reuse existing client for this host
                    holder = TelemetryClientFactory._clients[host_url]
                    holder.increment()
                    logger.debug(
                        "Reusing TelemetryClient for host %s (session %s, refcount=%d)",
                        host_url,
                        session_id_hex,
                        holder.refcount,
                    )
                else:
                    # Create new client for this host
                    logger.debug(
                        "Creating new TelemetryClient for host %s (session %s)",
                        host_url,
                        session_id_hex,
                    )
                    if telemetry_enabled:
                        client = TelemetryClient(
                            telemetry_enabled=telemetry_enabled,
                            session_id_hex=session_id_hex,
                            auth_provider=auth_provider,
                            host_url=host_url,
                            executor=TelemetryClientFactory._executor,
                            batch_size=batch_size,
                            client_context=client_context,
                        )
                        TelemetryClientFactory._clients[
                            host_url
                        ] = _TelemetryClientHolder(client)
                    else:
                        TelemetryClientFactory._clients[
                            host_url
                        ] = _TelemetryClientHolder(NoopTelemetryClient())
        except Exception as e:
            logger.debug("Failed to initialize telemetry client: %s", e)
            # Fallback to NoopTelemetryClient to ensure connection doesn't fail
            TelemetryClientFactory._clients[host_url] = _TelemetryClientHolder(
                NoopTelemetryClient()
            )

    @staticmethod
    def get_telemetry_client(host_url):
        """
        Get the shared telemetry client for a specific host.

        Args:
            host_url: The host URL to look up the client. If None/empty, uses UNKNOWN_HOST.

        Returns:
            The shared TelemetryClient for this host, or NoopTelemetryClient if not found
        """
        host_url = TelemetryClientFactory.getHostUrlSafely(host_url)

        if host_url in TelemetryClientFactory._clients:
            return TelemetryClientFactory._clients[host_url].client
        return NoopTelemetryClient()

    @staticmethod
    def close(host_url):
        """
        Close the telemetry client for a specific host.

        Decrements the reference count for the host's client. Only actually closes
        the client when the reference count reaches zero (all connections to this host closed).

        Args:
            host_url: The host URL whose client to close. If None/empty, uses UNKNOWN_HOST.
        """
        host_url = TelemetryClientFactory.getHostUrlSafely(host_url)

        with TelemetryClientFactory._lock:
            # Get the holder for this host
            holder = TelemetryClientFactory._clients.get(host_url)
            if holder is None:
                logger.debug("No telemetry client found for host %s", host_url)
                return

            # Decrement refcount
            remaining_refs = holder.decrement()
            logger.debug(
                "Decremented refcount for host %s (refcount=%d)",
                host_url,
                remaining_refs,
            )

            # Only close if no more references
            if remaining_refs <= 0:
                logger.debug(
                    "Closing telemetry client for host %s (no more references)",
                    host_url,
                )
                TelemetryClientFactory._clients.pop(host_url, None)
                holder.client.close()

            # Shutdown executor if no more clients
            if not TelemetryClientFactory._clients and TelemetryClientFactory._executor:
                logger.debug(
                    "No more telemetry clients, shutting down thread pool executor"
                )
                try:
                    TelemetryClientFactory._stop_flush_thread()
                    # Use wait=False to allow process to exit immediately
                    TelemetryClientFactory._executor.shutdown(wait=False)
                except Exception as e:
                    logger.debug("Failed to shutdown thread pool executor: %s", e)
                TelemetryClientFactory._executor = None
                TelemetryClientFactory._initialized = False

    @staticmethod
    def connection_failure_log(
        error_name: str,
        error_message: str,
        host_url: str,
        http_path: str,
        port: int,
        client_context,
        user_agent: Optional[str] = None,
        enable_telemetry: bool = True,
    ):
        """Send error telemetry when connection creation fails, using provided client context"""

        # Respect user's telemetry preference - don't force-enable
        if not enable_telemetry:
            logger.debug("Telemetry disabled, skipping connection failure log")
            return

        UNAUTH_DUMMY_SESSION_ID = "unauth_session_id"

        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=True,
            session_id_hex=UNAUTH_DUMMY_SESSION_ID,
            auth_provider=None,
            host_url=host_url,
            batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
            client_context=client_context,
        )

        telemetry_client = TelemetryClientFactory.get_telemetry_client(
            host_url=host_url
        )
        telemetry_client._driver_connection_params = DriverConnectionParameters(
            http_path=http_path,
            mode=DatabricksClientType.THRIFT,  # TODO: Add SEA mode
            host_info=HostDetails(host_url=host_url, port=port),
        )
        telemetry_client._user_agent = user_agent

        telemetry_client.export_failure_log(error_name, error_message)
