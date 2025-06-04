import threading
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from databricks.sql.telemetry.models.event import (
    TelemetryEvent,
    DriverConnectionParameters,
    DriverSystemConfiguration,
    HostDetails,
)
from databricks.sql.telemetry.models.frontend_logs import (
    TelemetryFrontendLog,
    TelemetryClientContext,
    FrontendLogContext,
    FrontendLogEntry,
)
from databricks.sql.telemetry.models.enums import DatabricksClientType
import sys
import platform
import uuid
import locale


class TelemetryClient:
    def __init__(
        self,
        telemetry_enabled,
        batch_size,
        connection_uuid,
        **kwargs
    ):
        self.telemetry_enabled = telemetry_enabled
        self.batch_size = batch_size
        self.connection_uuid = connection_uuid
        self.host_url = kwargs.get("host_url", None)
        self.auth_provider = kwargs.get("auth_provider", None)
        self.is_authenticated = kwargs.get("is_authenticated", False)
        self.user_agent = kwargs.get("user_agent", None)
        self.events_batch = []
        self.lock = threading.Lock()
        self.DriverConnectionParameters = None

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
            telemetry_manager._send_telemetry(events_to_flush, self.host_url, self.is_authenticated, self.auth_provider)

    def close(self):
        """Flush remaining events before closing"""
        self.flush()

    def export_initial_telemetry_log(self, **kwargs):
        http_path = kwargs.get("http_path", None)
        port = kwargs.get("port", None)
        socket_timeout = kwargs.get("socket_timeout", None)

        discovery_url = None
        if hasattr(self.auth_provider, "oauth_manager") and hasattr(
            self.auth_provider.oauth_manager, "idp_endpoint"
        ):
            discovery_url = (
                self.auth_provider.oauth_manager.idp_endpoint.get_openid_config_url(
                    self.host_url
                )
            )

        self.DriverConnectionParameters = DriverConnectionParameters(
            http_path=http_path,
            mode=DatabricksClientType.THRIFT,
            host_info=HostDetails(host_url=self.host_url, port=port),
            discovery_url=discovery_url,
            socket_timeout=socket_timeout,
        )

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
                    system_configuration=TelemetryManager.getDriverSystemConfiguration(),
                    driver_connection_params=self.DriverConnectionParameters,
                )
            ),
        )

        self.export_event(telemetry_frontend_log)


class TelemetryManager:
    """A singleton manager class that handles telemetry operations for SQL connections.

    This class maintains a map of connection_uuid to TelemetryClient instances. The initialize()
    method is only called from the connection class when telemetry is enabled for that connection.
    All telemetry operations (initial logs, failure logs, latency logs) first check if the
    connection_uuid exists in the map. If it doesn't exist (meaning telemetry was not enabled
    for that connection), the operation is skipped. If it exists, the operation is delegated
    to the corresponding TelemetryClient instance.

    This design ensures that telemetry operations are only performed for connections where
    telemetry was explicitly enabled during initialization.
    """

    _instance = None
    _DRIVER_SYSTEM_CONFIGURATION = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelemetryManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._clients = {}  # Map of connection_uuid -> TelemetryClient
        self.executor = ThreadPoolExecutor(max_workers=10)  # Thread pool for async operations TODO: Decide on max workers
        self._initialized = True

    def initialize_telemetry_client(
        self,
        telemetry_enabled,
        batch_size,
        connection_uuid,
        **kwargs
    ):
        """Initialize a telemetry client for a specific connection if telemetry is enabled"""
        if telemetry_enabled:
            if connection_uuid not in self._clients:
                self._clients[connection_uuid] = TelemetryClient(
                    telemetry_enabled=telemetry_enabled,
                    batch_size=batch_size,
                    connection_uuid=connection_uuid,
                    **kwargs
                )

    def _send_telemetry(self, events, host_url, is_authenticated, auth_provider):
        """Send telemetry events to the server"""
        request = {
            "uploadTime": int(time.time() * 1000),
            "items": [],
            "protoLogs": [event.to_json() for event in events],
        }

        path = "/telemetry-ext" if is_authenticated else "/telemetry-unauth"
        url = f"https://{host_url}{path}"

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        if is_authenticated and auth_provider:
            auth_provider.add_headers(headers)

        self.executor.submit(
            requests.post,
            url,
            data=json.dumps(request),
            headers=headers,
            timeout=10
        )

    def export_initial_telemetry_log(
        self, connection_uuid, **kwargs
    ):
        """Export initial telemetry for a specific connection"""
        if connection_uuid in self._clients:
            self._clients[connection_uuid].export_initial_telemetry_log(**kwargs)

    @classmethod
    def getDriverSystemConfiguration(cls) -> DriverSystemConfiguration:
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

    def close_telemetry_client(self, connection_uuid):
        """Close telemetry client"""
        if connection_uuid:
            if connection_uuid in self._clients:
                self._clients[connection_uuid].close()
                del self._clients[connection_uuid]
        
        # Shutdown executor if no more clients
        if not self._clients:
            self.executor.shutdown(wait=True)


# Create a global instance
telemetry_manager = TelemetryManager()
