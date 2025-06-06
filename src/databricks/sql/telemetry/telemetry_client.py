import threading
import time
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from databricks.sql.telemetry.models.event import (
    TelemetryEvent,
    DriverSystemConfiguration,
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
from databricks.sql import __version__

logger = logging.getLogger(__name__)


class TelemetryHelper:
    """Helper class for getting telemetry related information."""

    _DRIVER_SYSTEM_CONFIGURATION = DriverSystemConfiguration(
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

    @classmethod
    def getDriverSystemConfiguration(cls) -> DriverSystemConfiguration:
        return cls._DRIVER_SYSTEM_CONFIGURATION

    @staticmethod
    def get_auth_mechanism(auth_provider):
        """Get the auth mechanism for the auth provider."""
        if not auth_provider:
            return None
        if isinstance(auth_provider, AccessTokenAuthProvider):
            return AuthMech.PAT  # Personal Access Token authentication
        elif isinstance(auth_provider, DatabricksOAuthProvider):
            return AuthMech.OAUTH  # Databricks-managed OAuth flow
        elif isinstance(auth_provider, ExternalAuthProvider):
            return (
                AuthMech.EXTERNAL
            )  # External identity provider (AWS IAM, Azure AD, etc.)
        return AuthMech.OTHER  # Custom or unknown authentication provider

    @staticmethod
    def get_auth_flow(auth_provider):
        """Get the auth flow for the auth provider."""
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

    @staticmethod
    def get_discovery_url(auth_provider):
        """Get the discovery URL for the auth provider."""
        if not auth_provider:
            return None

        if isinstance(auth_provider, DatabricksOAuthProvider):
            return auth_provider.oauth_manager.idp_endpoint.get_openid_config_url(
                auth_provider.hostname
            )
        return None


class BaseTelemetryClient(ABC):
    """
    Base class for telemetry clients.
    It is used to define the interface for telemetry clients.
    """

    @abstractmethod
    def export_initial_telemetry_log(self, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass


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

    def close(self):
        pass


class TelemetryClient(BaseTelemetryClient):
    """
    Telemetry client class that handles sending telemetry events in batches to the server.
    It uses a thread pool to handle asynchronous operations, that it gets from the TelemetryClientFactory.
    """

    def __init__(
        self,
        telemetry_enabled,
        connection_uuid,
        auth_provider,
        host_url,
        executor,
    ):
        logger.info(f"Initializing TelemetryClient for connection: {connection_uuid}")
        self.telemetry_enabled = telemetry_enabled
        self.batch_size = 10  # TODO: Decide on batch size
        self.connection_uuid = connection_uuid
        self.auth_provider = auth_provider
        self.user_agent = None
        self.events_batch = []
        self.lock = threading.Lock()
        self.driver_connection_params = None
        self.host_url = host_url
        self.executor = executor

    def export_event(self, event):
        """Add an event to the batch queue and flush if batch is full"""
        logger.debug(f"Exporting event for connection {self.connection_uuid}")
        with self.lock:
            self.events_batch.append(event)
        if len(self.events_batch) >= self.batch_size:
            logger.debug(
                f"Batch size limit reached ({self.batch_size}), flushing events"
            )
            self.flush()

    def flush(self):
        """Flush the current batch of events to the server"""
        with self.lock:
            events_to_flush = self.events_batch.copy()
            self.events_batch = []

        if events_to_flush:
            logger.info(f"Flushing {len(events_to_flush)} telemetry events to server")
            self._send_telemetry(events_to_flush)

    def _send_telemetry(self, events):
        """Send telemetry events to the server"""

        request = {
            "uploadTime": int(time.time() * 1000),
            "items": [],
            "protoLogs": [event.to_json() for event in events],
        }

        path = "/telemetry-ext" if self.auth_provider else "/telemetry-unauth"
        url = f"https://{self.host_url}{path}"

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        if self.auth_provider:
            self.auth_provider.add_headers(headers)

        try:
            logger.debug("Submitting telemetry request to thread pool")
            self.executor.submit(
                requests.post,
                url,
                data=json.dumps(request),
                headers=headers,
                timeout=10,
            )
        except Exception as e:
            logger.error(f"Failed to submit telemetry request: {e}")

    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        logger.info(
            f"Exporting initial telemetry log for connection {self.connection_uuid}"
        )

        self.driver_connection_params = driver_connection_params
        self.user_agent = user_agent

        telemetry_frontend_log = TelemetryFrontendLog(
            frontend_log_event_id=str(uuid.uuid4()),
            context=FrontendLogContext(
                client_context=TelemetryClientContext(
                    timestamp_millis=int(time.time() * 1000), user_agent=self.user_agent
                )
            ),
            entry=FrontendLogEntry(
                sql_driver_log=TelemetryEvent(
                    session_id=self.connection_uuid,
                    system_configuration=TelemetryHelper.getDriverSystemConfiguration(),
                    driver_connection_params=self.driver_connection_params,
                )
            ),
        )

        self.export_event(telemetry_frontend_log)

    def close(self):
        """Flush remaining events before closing"""
        logger.info(f"Closing TelemetryClient for connection {self.connection_uuid}")
        self.flush()
        TelemetryClientFactory.close(self.connection_uuid)


class TelemetryClientFactory:
    """
    Static factory class for creating and managing telemetry clients.
    It uses a thread pool to handle asynchronous operations.
    """

    _clients: Dict[
        str, TelemetryClient
    ] = {}  # Map of connection_uuid -> TelemetryClient
    _executor: ThreadPoolExecutor = None
    _initialized: bool = False
    _lock = threading.Lock()  # Thread safety for factory operations

    @classmethod
    def _initialize(cls):
        """Initialize the factory if not already initialized"""
        with cls._lock:
            if not cls._initialized:
                logger.info("Initializing TelemetryClientFactory")
                cls._clients = {}
                cls._executor = ThreadPoolExecutor(
                    max_workers=10
                )  # Thread pool for async operations TODO: Decide on max workers
                cls._initialized = True
                logger.debug(
                    "TelemetryClientFactory initialized with thread pool (max_workers=10)"
                )

    @staticmethod
    def initialize_telemetry_client(
        telemetry_enabled,
        connection_uuid,
        auth_provider,
        host_url,
    ):
        """Initialize a telemetry client for a specific connection if telemetry is enabled"""
        TelemetryClientFactory._initialize()

        if telemetry_enabled:
            with TelemetryClientFactory._lock:
                if connection_uuid not in TelemetryClientFactory._clients:
                    logger.info(
                        f"Creating new TelemetryClient for connection {connection_uuid}"
                    )
                    TelemetryClientFactory._clients[connection_uuid] = TelemetryClient(
                        telemetry_enabled=telemetry_enabled,
                        connection_uuid=connection_uuid,
                        auth_provider=auth_provider,
                        host_url=host_url,
                        executor=TelemetryClientFactory._executor,
                    )
                return TelemetryClientFactory._clients[connection_uuid]
        else:
            return NoopTelemetryClient()

    @staticmethod
    def get_telemetry_client(connection_uuid):
        """Get the telemetry client for a specific connection"""
        if connection_uuid in TelemetryClientFactory._clients:
            return TelemetryClientFactory._clients[connection_uuid]
        else:
            return NoopTelemetryClient()

    @staticmethod
    def close(connection_uuid):
        """Close and remove the telemetry client for a specific connection"""

        with TelemetryClientFactory._lock:
            if connection_uuid in TelemetryClientFactory._clients:
                logger.debug(
                    f"Removing telemetry client for connection {connection_uuid}"
                )
                del TelemetryClientFactory._clients[connection_uuid]

            # Shutdown executor if no more clients
            if not TelemetryClientFactory._clients and TelemetryClientFactory._executor:
                logger.info(
                    "No more telemetry clients, shutting down thread pool executor"
                )
                TelemetryClientFactory._executor.shutdown(wait=True)
                TelemetryClientFactory._executor = None
                TelemetryClientFactory._initialized = False
