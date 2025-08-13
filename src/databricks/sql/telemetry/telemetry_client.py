import threading
import time
import logging
import json
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
        if connection.force_enable_telemetry:
            return True

        if connection.enable_telemetry:
            context = FeatureFlagsContextFactory.get_instance(connection)
            flag_value = context.get_flag_value(
                TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME, default_value=False
            )
            return str(flag_value).lower() == "true"
        else:
            return False


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

    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        pass

    def export_failure_log(self, error_name, error_message):
        pass

    def export_latency_log(self, latency_ms, sql_execution_event, sql_statement_id):
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
        telemetry_enabled,
        session_id_hex,
        auth_provider,
        host_url,
        executor,
        batch_size,
        client_context,
    ):
        logger.debug("Initializing TelemetryClient for connection: %s", session_id_hex)
        self._telemetry_enabled = telemetry_enabled
        self._batch_size = batch_size
        self._session_id_hex = session_id_hex
        self._auth_provider = auth_provider
        self._user_agent = None
        self._events_batch = []
        self._lock = threading.RLock()
        self._driver_connection_params = None
        self._host_url = host_url
        self._executor = executor

        # Create own HTTP client from client context
        self._http_client = UnifiedHttpClient(client_context)

    def _export_event(self, event):
        """Add an event to the batch queue and flush if batch is full"""
        logger.debug("Exporting event for connection %s", self._session_id_hex)
        with self._lock:
            self._events_batch.append(event)
        if len(self._events_batch) >= self._batch_size:
            logger.debug(
                "Batch size limit reached (%s), flushing events", self._batch_size
            )
            self._flush()

    def _flush(self):
        """Flush the current batch of events to the server"""
        with self._lock:
            events_to_flush = self._events_batch.copy()
            self._events_batch = []

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
        url = f"https://{self._host_url}{path}"

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
            response = self._http_client.request(
                HttpMethod.POST, url, body=data, headers=headers, timeout=timeout
            )
            return response
        except Exception as e:
            logger.error("Failed to send telemetry with unified client: %s", e)
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

    def _export_telemetry_log(self, **telemetry_event_kwargs):
        """
        Common helper method for exporting telemetry logs.

        Args:
            **telemetry_event_kwargs: Keyword arguments to pass to TelemetryEvent constructor
        """
        logger.debug("Exporting telemetry log for connection %s", self._session_id_hex)

        try:
            # Set common fields for all telemetry events
            event_kwargs = {
                "session_id": self._session_id_hex,
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

    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        self._driver_connection_params = driver_connection_params
        self._user_agent = user_agent
        self._export_telemetry_log()

    def export_failure_log(self, error_name, error_message):
        error_info = DriverErrorInfo(error_name=error_name, stack_trace=error_message)
        self._export_telemetry_log(error_info=error_info)

    def export_latency_log(self, latency_ms, sql_execution_event, sql_statement_id):
        self._export_telemetry_log(
            sql_statement_id=sql_statement_id,
            sql_operation=sql_execution_event,
            operation_latency_ms=latency_ms,
        )

    def close(self):
        """Flush remaining events before closing"""
        logger.debug("Closing TelemetryClient for connection %s", self._session_id_hex)
        self._flush()


class TelemetryClientFactory:
    """
    Static factory class for creating and managing telemetry clients.
    It uses a thread pool to handle asynchronous operations and a single flush thread for all clients.
    """

    _clients: Dict[
        str, BaseTelemetryClient
    ] = {}  # Map of session_id_hex -> BaseTelemetryClient
    _executor: Optional[ThreadPoolExecutor] = None
    _initialized: bool = False
    _lock = threading.RLock()  # Thread safety for factory operations
    # used RLock instead of Lock to avoid deadlocks when garbage collection is triggered
    _original_excepthook = None
    _excepthook_installed = False

    # Shared flush thread for all clients
    _flush_thread = None
    _flush_event = threading.Event()
    _flush_interval_seconds = 90

    DEFAULT_BATCH_SIZE = 100

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

                for client in clients_to_flush:
                    client._flush()

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
        for client in clients_to_close:
            client.close()

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
        """Initialize a telemetry client for a specific connection if telemetry is enabled"""
        try:

            with TelemetryClientFactory._lock:
                TelemetryClientFactory._initialize()

                if session_id_hex not in TelemetryClientFactory._clients:
                    logger.debug(
                        "Creating new TelemetryClient for connection %s",
                        session_id_hex,
                    )
                    if telemetry_enabled:
                        TelemetryClientFactory._clients[
                            session_id_hex
                        ] = TelemetryClient(
                            telemetry_enabled=telemetry_enabled,
                            session_id_hex=session_id_hex,
                            auth_provider=auth_provider,
                            host_url=host_url,
                            executor=TelemetryClientFactory._executor,
                            batch_size=batch_size,
                            client_context=client_context,
                        )
                    else:
                        TelemetryClientFactory._clients[
                            session_id_hex
                        ] = NoopTelemetryClient()
        except Exception as e:
            logger.debug("Failed to initialize telemetry client: %s", e)
            # Fallback to NoopTelemetryClient to ensure connection doesn't fail
            TelemetryClientFactory._clients[session_id_hex] = NoopTelemetryClient()

    @staticmethod
    def get_telemetry_client(session_id_hex):
        """Get the telemetry client for a specific connection"""
        return TelemetryClientFactory._clients.get(
            session_id_hex, NoopTelemetryClient()
        )

    @staticmethod
    def close(session_id_hex):
        """Close and remove the telemetry client for a specific connection"""

        with TelemetryClientFactory._lock:
            if (
                telemetry_client := TelemetryClientFactory._clients.pop(
                    session_id_hex, None
                )
            ) is not None:
                logger.debug(
                    "Removing telemetry client for connection %s", session_id_hex
                )
                telemetry_client.close()

            # Shutdown executor if no more clients
            if not TelemetryClientFactory._clients and TelemetryClientFactory._executor:
                logger.debug(
                    "No more telemetry clients, shutting down thread pool executor"
                )
                try:
                    TelemetryClientFactory._stop_flush_thread()
                    TelemetryClientFactory._executor.shutdown(wait=True)
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
    ):
        """Send error telemetry when connection creation fails, using provided client context"""

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
            UNAUTH_DUMMY_SESSION_ID
        )
        telemetry_client._driver_connection_params = DriverConnectionParameters(
            http_path=http_path,
            mode=DatabricksClientType.THRIFT,  # TODO: Add SEA mode
            host_info=HostDetails(host_url=host_url, port=port),
        )
        telemetry_client._user_agent = user_agent

        telemetry_client.export_failure_log(error_name, error_message)
