import threading
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor
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


class TelemetryHelper:
    """Helper class for getting telemetry related information."""

    _DRIVER_SYSTEM_CONFIGURATION = None

    @classmethod
    def getDriverSystemConfiguration(cls) -> DriverSystemConfiguration:
        """Get the driver system configuration."""
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
        if not auth_provider:
            return None
        if isinstance(auth_provider, AccessTokenAuthProvider):
            return AuthMech.PAT
        elif isinstance(auth_provider, DatabricksOAuthProvider):
            return AuthMech.OAUTH
        elif isinstance(auth_provider, ExternalAuthProvider):
            return AuthMech.EXTERNAL
        return AuthMech.OTHER

    @staticmethod
    def get_auth_flow(auth_provider):
        """Get the auth flow for the auth provider."""
        if not auth_provider:
            return None

        if isinstance(auth_provider, DatabricksOAuthProvider):
            if (
                hasattr(auth_provider, "_refresh_token")
                and auth_provider._refresh_token
            ):
                return AuthFlow.TOKEN_PASSTHROUGH

            if hasattr(auth_provider, "oauth_manager"):
                return AuthFlow.BROWSER_BASED_AUTHENTICATION

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

    def export_initial_telemetry_log(self, **kwargs):
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
        batch_size,
        connection_uuid,
        auth_provider,
        user_agent,
        driver_connection_params,
        executor,
    ):
        self.telemetry_enabled = telemetry_enabled
        self.batch_size = batch_size
        self.connection_uuid = connection_uuid
        self.auth_provider = auth_provider
        self.user_agent = user_agent
        self.events_batch = []
        self.lock = threading.Lock()
        self.driver_connection_params = driver_connection_params
        self.host_url = driver_connection_params.host_info.host_url
        self.executor = executor

    def export_event(self, event):
        """Add an event to the batch queue and flush if batch is full"""
        with self.lock:
            self.events_batch.append(event)
        if len(self.events_batch) >= self.batch_size:
            self.flush()

    def flush(self):
        """Flush the current batch of events to the server"""
        with self.lock:
            events_to_flush = self.events_batch.copy()
            self.events_batch = []

        if events_to_flush:
            self._send_telemetry(events_to_flush)

    def _send_telemetry(self, events):
        """Send telemetry events to the server"""
        try:
            request = {
                "uploadTime": int(time.time() * 1000),
                "items": [],
                "protoLogs": [event.to_json() for event in events],
            }
        except Exception as e:
            print(f"[DEBUG] Error creating telemetry request: {e}", flush=True)
            raise e

        path = "/telemetry-ext" if self.auth_provider else "/telemetry-unauth"
        url = f"https://{self.host_url}{path}"

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        if self.auth_provider:
            self.auth_provider.add_headers(headers)

        self.executor.submit(
            requests.post, url, data=json.dumps(request), headers=headers, timeout=10
        )

    def export_initial_telemetry_log(self):

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
        self.flush()
        telemetry_client_factory.close(self.connection_uuid)


class TelemetryClientFactory:
    """
    Factory class for creating and managing telemetry clients.
    It uses a thread pool to handle asynchronous operations.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelemetryClientFactory, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._clients = {}  # Map of connection_uuid -> TelemetryClient
        self.executor = ThreadPoolExecutor(
            max_workers=10
        )  # Thread pool for async operations TODO: Decide on max workers
        self._initialized = True

    def get_telemetry_client(
        self,
        telemetry_enabled,
        batch_size,
        connection_uuid,
        auth_provider,
        user_agent,
        driver_connection_params,
    ):
        """Initialize a telemetry client for a specific connection if telemetry is enabled"""
        if telemetry_enabled:
            if connection_uuid not in self._clients:
                self._clients[connection_uuid] = TelemetryClient(
                    telemetry_enabled=telemetry_enabled,
                    batch_size=batch_size,
                    connection_uuid=connection_uuid,
                    auth_provider=auth_provider,
                    user_agent=user_agent,
                    driver_connection_params=driver_connection_params,
                    executor=self.executor,
                )
            return self._clients[connection_uuid]
        else:
            return NoopTelemetryClient()

    def close(self, connection_uuid):
        if connection_uuid in self._clients:
            del self._clients[connection_uuid]

        # Shutdown executor if no more clients
        if not self._clients:
            self.executor.shutdown(wait=True)


# Create a global instance
telemetry_client_factory = TelemetryClientFactory()
