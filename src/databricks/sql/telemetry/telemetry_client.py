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
from abc import ABC, abstractmethod


<<<<<<< HEAD
class BaseTelemetryClient(ABC):
    @abstractmethod
    def export_initial_telemetry_log(self, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass


class NoopTelemetryClient(BaseTelemetryClient):
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
    def __init__(self, telemetry_enabled, batch_size, connection_uuid, **kwargs):
        self.telemetry_enabled = telemetry_enabled
        self.batch_size = batch_size
=======
class TelemetryClient:
    def __init__(
        self,
        host,
        connection_uuid,
        batch_size,
        auth_provider=None,
        is_authenticated=False,
        user_agent=None,
    ):
        self.host_url = host
>>>>>>> parent of 419dac3 (shifted thread pool executor to telemetry manager)
        self.connection_uuid = connection_uuid
        self.auth_provider = auth_provider
        self.is_authenticated = is_authenticated
        self.batch_size = batch_size
        self.user_agent = user_agent
        self.events_batch = []
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(
            max_workers=10  # TODO: Decide on max workers
        )  # Thread pool for async operations
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
<<<<<<< HEAD
            telemetry_client_factory._send_telemetry(
                events_to_flush,
                self.host_url,
                self.is_authenticated,
                self.auth_provider,
            )
=======
            self.executor.submit(self._send_telemetry, events_to_flush)

    def _send_telemetry(self, events):
        """Send telemetry events to the server"""
        request = {
            "uploadTime": int(time.time() * 1000),
            "items": [],
            "protoLogs": [event.to_json() for event in events],
        }

        path = "/telemetry-ext" if self.is_authenticated else "/telemetry-unauth"
        url = f"https://{self.host_url}{path}"

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        if self.is_authenticated and self.auth_provider:
            self.auth_provider.add_headers(headers)

        # print("\n=== Request Details ===", flush=True)
        # print(f"URL: {url}", flush=True)
        # print("\nHeaders:", flush=True)
        # for key, value in headers.items():
        #     print(f"  {key}: {value}", flush=True)

        # print("\nRequest Body:", flush=True)
        # print(json.dumps(request, indent=2), flush=True)
        # sys.stdout.flush()

        response = requests.post(
            url, data=json.dumps(request), headers=headers, timeout=10
        )

        # print("\n=== Response Details ===", flush=True)
        # print(f"Status Code: {response.status_code}", flush=True)
        # print("\nResponse Headers:", flush=True)
        # for key, value in response.headers.items():
        #     print(f"  {key}: {value}", flush=True)

        # print("\nResponse Body:", flush=True)
        # try:
        #     response_json = response.json()
        #     print(json.dumps(response_json, indent=2), flush=True)
        # except json.JSONDecodeError:
        #     print(response.text, flush=True)
        # sys.stdout.flush()

    def close(self):
        """Flush remaining events and shut down executor"""
        self.flush()
        self.executor.shutdown(wait=True)
>>>>>>> parent of 419dac3 (shifted thread pool executor to telemetry manager)

    def export_initial_telemetry_log(self, http_path, port, socket_timeout):
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
                    system_configuration=telemetry_client_factory.getDriverSystemConfiguration(),
                    driver_connection_params=self.DriverConnectionParameters,
                )
            ),
        )

        self.export_event(telemetry_frontend_log)

<<<<<<< HEAD
    def close(self):
        """Flush remaining events before closing"""
        self.flush()
        telemetry_client_factory.close(self.connection_uuid)
=======
    def export_failure_log(self, errorName, errorMessage):
        pass

    def export_sql_latency_log(
        self, latency_ms, sql_execution_event, sql_statement_id=None
    ):
        """Export telemetry for sql execution"""
        pass

    def export_volume_latency_log(self, latency_ms, volume_operation):
        """Export telemetry for volume operation"""
        pass

>>>>>>> parent of 419dac3 (shifted thread pool executor to telemetry manager)


class TelemetryClientFactory:

    _instance = None
    _DRIVER_SYSTEM_CONFIGURATION = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelemetryClientFactory, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._clients = {}  # Map of connection_uuid -> TelemetryClient
<<<<<<< HEAD
        self.executor = ThreadPoolExecutor(
            max_workers=10
        )  # Thread pool for async operations TODO: Decide on max workers
        self._initialized = True

    def get_telemetry_client(
        self, telemetry_enabled, batch_size, connection_uuid, **kwargs
    ):
        """Initialize a telemetry client for a specific connection if telemetry is enabled"""
        if telemetry_enabled:
            if connection_uuid not in self._clients:
                self._clients[connection_uuid] = TelemetryClient(
                    telemetry_enabled=telemetry_enabled,
                    batch_size=batch_size,
                    connection_uuid=connection_uuid,
                    **kwargs,
                )
            return self._clients[connection_uuid]
        else:
            return NoopTelemetryClient()

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
            requests.post, url, data=json.dumps(request), headers=headers, timeout=10
        )

=======
        self._initialized = True

    def initialize(
        self,
        host,
        connection_uuid,
        batch_size,
        auth_provider=None,
        is_authenticated=False,
        user_agent=None,
    ):
        """Initialize a telemetry client for a specific connection"""
        if connection_uuid not in self._clients:
            self._clients[connection_uuid] = TelemetryClient(
                host=host,
                connection_uuid=connection_uuid,
                batch_size=batch_size,
                auth_provider=auth_provider,
                is_authenticated=is_authenticated,
                user_agent=user_agent,
            )

    def export_failure_log(self, error_name, error_message, connection_uuid):
        """Export error logs for a specific connection or all connections if connection_uuid is None"""
        pass

    def export_initial_telemetry_log(
        self, http_path, port, socket_timeout, connection_uuid
    ):
        """Export initial telemetry for a specific connection"""
        if connection_uuid in self._clients:
            self._clients[connection_uuid].export_initial_telemetry_log(
                http_path, port, socket_timeout
            )

    def export_sql_latency_log(
        self,
        latency_ms,
        sql_execution_event,
        sql_statement_id=None,
        connection_uuid=None,
    ):
        """Export latency logs for sql execution for a specific connection"""
        pass

    def export_volume_latency_log(
        self, latency_ms, volume_operation, connection_uuid=None
    ):
        """Export latency logs for volume operation for a specific connection"""
        pass

>>>>>>> parent of 419dac3 (shifted thread pool executor to telemetry manager)
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

    def close(self, connection_uuid):
<<<<<<< HEAD
        del self._clients[connection_uuid]

        # Shutdown executor if no more clients
        if not self._clients:
            self.executor.shutdown(wait=True)


# Create a global instance
telemetry_client_factory = TelemetryClientFactory()
=======
        """Close telemetry client(s)"""
        if connection_uuid:
            if connection_uuid in self._clients:
                self._clients[connection_uuid].close()
                del self._clients[connection_uuid]


# Create a global instance
telemetry_client = TelemetryManager()
>>>>>>> parent of 419dac3 (shifted thread pool executor to telemetry manager)
