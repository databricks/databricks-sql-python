import threading
import time
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional
from databricks.sql.telemetry.models.event import (
    TelemetryEvent,
    DriverSystemConfiguration,
    DriverErrorInfo,
)
from databricks.sql.telemetry.models.frontend_logs import (
    TelemetryFrontendLog,
    TelemetryClientContext,
    FrontendLogContext,
    FrontendLogEntry,
)
from databricks.sql.telemetry.models.enums import AuthMech, AuthFlow
from databricks.sql.auth.authenticators import (
    AccessTokenAuthProvider,
    DatabricksOAuthProvider,
    ExternalAuthProvider,
)
import sys
import platform
import uuid
import locale
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class TelemetryHelper:
    """Helper class for getting telemetry related information."""

    _DRIVER_SYSTEM_CONFIGURATION = None

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
        # PAT, DATABRICKS_OAUTH, EXTERNAL_AUTH, CLIENT_CERT

        if not auth_provider:
            return None
        if isinstance(auth_provider, AccessTokenAuthProvider):
            return AuthMech.PAT  # Personal Access Token authentication
        elif isinstance(auth_provider, DatabricksOAuthProvider):
            return AuthMech.DATABRICKS_OAUTH  # Databricks-managed OAuth flow
        elif isinstance(auth_provider, ExternalAuthProvider):
            return (
                AuthMech.EXTERNAL_AUTH
            )  # External identity provider (AWS, Azure, etc.)
        return AuthMech.CLIENT_CERT  # Client certificate (ssl)

    @staticmethod
    def get_auth_flow(auth_provider):
        """Get the auth flow for the auth provider."""
        # AuthFlow is an enum with the following values:
        # TOKEN_PASSTHROUGH, BROWSER_BASED_AUTHENTICATION

        if not auth_provider:
            return None

        if isinstance(auth_provider, DatabricksOAuthProvider):
            if auth_provider._access_token and auth_provider._refresh_token:
                return (
                    AuthFlow.TOKEN_PASSTHROUGH
                )  # Has existing tokens, no user interaction needed
            if hasattr(auth_provider, "oauth_manager"):
                return (
                    AuthFlow.BROWSER_BASED_AUTHENTICATION
                )  # Will initiate OAuth flow requiring browser

        return None


class BaseTelemetryClient(ABC):
    """
    Base class for telemetry clients.
    It is used to define the interface for telemetry clients.
    """

    @abstractmethod
    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        raise NotImplementedError(
            "Subclasses must implement export_initial_telemetry_log"
        )

    @abstractmethod
    def export_failure_log(self, error_name, error_message):
        raise NotImplementedError("Subclasses must implement export_failure_log")

    @abstractmethod
    def close(self):
        raise NotImplementedError("Subclasses must implement close")


class NoopTelemetryClient(BaseTelemetryClient):
    """
    NoopTelemetryClient is a telemetry client that does not send any events to the server.
    It is used when telemetry is disabled.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NoopTelemetryClient, cls).__new__(cls)
        return cls._instance

    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        pass

    def export_failure_log(self, error_name, error_message):
        pass

    def close(self):
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
    ):
        logger.debug("Initializing TelemetryClient for connection: %s", session_id_hex)
        self._telemetry_enabled = telemetry_enabled
        self._batch_size = 10  # TODO: Decide on batch size
        self._session_id_hex = session_id_hex
        self._auth_provider = auth_provider
        self._user_agent = None
        self._events_batch = []
        self._lock = threading.Lock()
        self._driver_connection_params = None
        self._host_url = host_url
        self._executor = executor

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

        request = {
            "uploadTime": int(time.time() * 1000),
            "items": [],
            "protoLogs": [event.to_json() for event in events],
        }

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
            future = self._executor.submit(
                requests.post,
                url,
                data=json.dumps(request),
                headers=headers,
                timeout=10,
            )
            future.add_done_callback(self._telemetry_request_callback)
        except Exception as e:
            logger.debug("Failed to submit telemetry request: %s", e)

    def _telemetry_request_callback(self, future):
        """Callback function to handle telemetry request completion"""
        try:
            response = future.result()

            if response.status_code == 200:
                logger.debug("Telemetry request completed successfully")
            else:
                logger.debug(
                    "Telemetry request failed with status code: %s",
                    response.status_code,
                )

        except Exception as e:
            logger.debug("Telemetry request failed with exception: %s", e)

    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        logger.debug(
            "Exporting initial telemetry log for connection %s", self._session_id_hex
        )

        try:
            self._driver_connection_params = driver_connection_params
            self._user_agent = user_agent

            telemetry_frontend_log = TelemetryFrontendLog(
                frontend_log_event_id=str(uuid.uuid4()),
                context=FrontendLogContext(
                    client_context=TelemetryClientContext(
                        timestamp_millis=int(time.time() * 1000),
                        user_agent=self._user_agent,
                    )
                ),
                entry=FrontendLogEntry(
                    sql_driver_log=TelemetryEvent(
                        session_id=self._session_id_hex,
                        system_configuration=TelemetryHelper.get_driver_system_configuration(),
                        driver_connection_params=self._driver_connection_params,
                    )
                ),
            )

            self._export_event(telemetry_frontend_log)

        except Exception as e:
            logger.debug("Failed to export initial telemetry log: %s", e)

    def export_failure_log(self, error_name, error_message):
        logger.debug("Exporting failure log for connection %s", self._session_id_hex)
        try:
            error_info = DriverErrorInfo(
                error_name=error_name, stack_trace=error_message
            )
            telemetry_frontend_log = TelemetryFrontendLog(
                frontend_log_event_id=str(uuid.uuid4()),
                context=FrontendLogContext(
                    client_context=TelemetryClientContext(
                        timestamp_millis=int(time.time() * 1000),
                        user_agent=self._user_agent,
                    )
                ),
                entry=FrontendLogEntry(
                    sql_driver_log=TelemetryEvent(
                        session_id=self._session_id_hex,
                        system_configuration=TelemetryHelper.get_driver_system_configuration(),
                        driver_connection_params=self._driver_connection_params,
                        error_info=error_info,
                    )
                ),
            )
            self._export_event(telemetry_frontend_log)
        except Exception as e:
            logger.debug("Failed to export failure log: %s", e)

    def close(self):
        """Flush remaining events before closing"""
        logger.debug("Closing TelemetryClient for connection %s", self._session_id_hex)
        self._flush()


# Module-level state
_clients: Dict[str, BaseTelemetryClient] = {}
_executor: Optional[ThreadPoolExecutor] = None
_initialized: bool = False
_lock = threading.Lock()
_original_excepthook = None
_excepthook_installed = False


def _initialize():
    """Initialize the telemetry system if not already initialized"""
    global _initialized, _executor
    if not _initialized:
        _clients.clear()
        _executor = ThreadPoolExecutor(max_workers=10)
        _install_exception_hook()
        _initialized = True
        logger.debug(
            "Telemetry system initialized with thread pool (max_workers=10)"
        )


def _install_exception_hook():
    """Install global exception handler for unhandled exceptions"""
    global _excepthook_installed, _original_excepthook
    if not _excepthook_installed:
        _original_excepthook = sys.excepthook
        sys.excepthook = _handle_unhandled_exception
        _excepthook_installed = True
        logger.debug("Global exception handler installed for telemetry")


def _handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """Handle unhandled exceptions by sending telemetry and flushing thread pool"""
    logger.debug("Handling unhandled exception: %s", exc_type.__name__)

    clients_to_close = list(_clients.values())
    for client in clients_to_close:
        client.close()

    # Call the original exception handler to maintain normal behavior
    if _original_excepthook:
        _original_excepthook(exc_type, exc_value, exc_traceback)


def initialize_telemetry_client(
    telemetry_enabled, session_id_hex, auth_provider, host_url
):
    """Initialize a telemetry client for a specific connection if telemetry is enabled"""
    try:
        with _lock:
            _initialize()
            if session_id_hex not in _clients:
                logger.debug(
                    "Creating new TelemetryClient for connection %s", session_id_hex
                )
                if telemetry_enabled:
                    _clients[session_id_hex] = TelemetryClient(
                        telemetry_enabled=telemetry_enabled,
                        session_id_hex=session_id_hex,
                        auth_provider=auth_provider,
                        host_url=host_url,
                        executor=_executor,
                    )
                    print("i have initialized the telemetry client yes")
                else:
                    _clients[session_id_hex] = NoopTelemetryClient()
                    print("i have initialized the noop client yes")
    except Exception as e:
        logger.debug("Failed to initialize telemetry client: %s", e)
        # Fallback to NoopTelemetryClient to ensure connection doesn't fail
        _clients[session_id_hex] = NoopTelemetryClient()


def get_telemetry_client(session_id_hex):
    """Get the telemetry client for a specific connection"""
    try:
        if session_id_hex in _clients:
            return _clients[session_id_hex]
        else:
            logger.error(
                "Telemetry client not initialized for connection %s", session_id_hex
            )
            return NoopTelemetryClient()
    except Exception as e:
        logger.debug("Failed to get telemetry client: %s", e)
        return NoopTelemetryClient()


def close_telemetry_client(session_id_hex):
    """Remove the telemetry client for a specific connection"""
    global _initialized, _executor
    with _lock:
        # if (telemetry_client := _clients.pop(session_id_hex, None)) is not None:
        telemetry_client = _clients.pop(session_id_hex, None)
        if telemetry_client is not None:
            logger.debug("Removing telemetry client for connection %s", session_id_hex)
            telemetry_client.close()

        # Shutdown executor if no more clients
        try:
            if not _clients and _executor:
                logger.debug(
                    "No more telemetry clients, shutting down thread pool executor"
                )
                _executor.shutdown(wait=True)
                _executor = None
                _initialized = False
        except Exception as e:
            logger.debug("Failed to shutdown thread pool executor: %s", e)
