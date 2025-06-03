import threading
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
from abc import ABC, abstractmethod
from databricks.sql.telemetry.models.event import (
    TelemetryEvent,
    DriverConnectionParameters,
    DriverSystemConfiguration,
    DriverErrorInfo,
    DriverVolumeOperation,
    SqlExecutionEvent,
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

logger = logging.getLogger(__name__)


class TelemetryHelper:
    # Singleton instance of DriverSystemConfiguration
    _DRIVER_SYSTEM_CONFIGURATION = None

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
                client_app_name="unknown",  # TODO: Add client app name
                locale_name=locale.getlocale()[0] or locale.getdefaultlocale()[0],
                char_set_encoding=sys.getdefaultencoding(),
            )
        return cls._DRIVER_SYSTEM_CONFIGURATION


class BaseTelemetryClient(ABC):
    """Abstract base class for telemetry clients."""

    @abstractmethod
    def export_event(self, event):
        pass

    @abstractmethod
    def flush(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def export_initial_telemetry_log(self, http_path, port, socket_timeout):
        pass


class TelemetryClient(BaseTelemetryClient):
    def __init__(
        self,
        host,
        connection_uuid,
        auth_provider=None,
        is_authenticated=False,
        batch_size=200,
        user_agent=None,
    ):
        self.host_url = host
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
        # Add an event to the batch queue and flush if batch is full
        with self.lock:
            self.events_batch.append(event)
        if len(self.events_batch) >= self.batch_size:
            self.flush()

    def flush(self):
        # Flush the current batch of events to the server
        with self.lock:
            events_to_flush = self.events_batch.copy()
            self.events_batch = []
        print(f"Flushing {len(events_to_flush)} events", flush=True)
        if events_to_flush:
            self.executor.submit(self._send_telemetry, events_to_flush)

    def _send_telemetry(self, events):
        # Send telemetry events to the server
        try:
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

            response = requests.post(
                url, data=json.dumps(request), headers=headers, timeout=10
            )

            if response.status_code != 200:
                logger.debug(
                    "Failed to send telemetry: HTTP %d - %s",
                    response.status_code,
                    response.text,
                )

        except Exception as e:
            logger.debug("Failed to send telemetry: %s", str(e))

    def close(self):
        # Flush remaining events and shut down executor
        self.flush()
        self.executor.shutdown(wait=True)

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
                    # session_id = "test-session-auth-1234",
                    # session_id = "test-session-unauth-1234",
                    system_configuration=TelemetryHelper.getDriverSystemConfiguration(),
                    driver_connection_params=self.DriverConnectionParameters,
                )
            ),
        )

        self.export_event(telemetry_frontend_log)

    def export_failure_log(self, errorName, errorMessage):
        error_info = DriverErrorInfo(error_name=errorName, stack_trace=errorMessage)

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
                    driver_connection_params=self.DriverConnectionParameters,
                    error_info=error_info,
                )
            ),
        )
        self.export_event(telemetry_frontend_log)

    def export_sql_latency_log(
        self, latency_ms, sql_execution_event, sql_statement_id=None
    ):
        """Export telemetry for sql execution"""
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
                    driver_connection_params=self.DriverConnectionParameters,
                    sql_statement_id=sql_statement_id,
                    sql_operation=sql_execution_event,
                    operation_latency_ms=latency_ms,
                )
            ),
        )
        self.export_event(telemetry_frontend_log)

    def export_volume_latency_log(self, latency_ms, volume_operation):
        """Export telemetry for volume operation"""
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
                    driver_connection_params=self.DriverConnectionParameters,
                    volume_operation=volume_operation,
                    operation_latency_ms=latency_ms,
                )
            ),
        )
        self.export_event(telemetry_frontend_log)


class NoopTelemetryClient(BaseTelemetryClient):
    """A no-operation telemetry client that implements the same interface but does nothing"""

    def export_event(self, event):
        pass

    def flush(self):
        pass

    def close(self):
        pass

    def export_initial_telemetry_log(self, http_path, port, socket_timeout):
        pass


class SingletonTelemetryClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SingletonTelemetryClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._clients = {}  # Map of connection_uuid -> TelemetryClient
        self._initialized = True

    def initialize(
        self,
        host,
        connection_uuid,
        auth_provider,
        is_authenticated,
        batch_size,
        user_agent,
    ):
        """Initialize a telemetry client for a specific connection"""
        if connection_uuid not in self._clients:
            self._clients[connection_uuid] = TelemetryClient(
                host=host,
                connection_uuid=connection_uuid,
                auth_provider=auth_provider,
                is_authenticated=is_authenticated,
                batch_size=batch_size,
                user_agent=user_agent,
            )

    def export_failure_log(self, error_name, error_message, connection_uuid):
        """Export error logs for a specific connection or all connections if connection_uuid is None"""
        if connection_uuid:
            if connection_uuid in self._clients:
                self._clients[connection_uuid].export_failure_log(
                    error_name, error_message
                )

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
        if connection_uuid:
            if connection_uuid in self._clients:
                self._clients[connection_uuid].export_sql_latency_log(
                    latency_ms, sql_execution_event, sql_statement_id
                )

    def export_volume_latency_log(
        self, latency_ms, volume_operation, connection_uuid=None
    ):
        """Export latency logs for volume operation for a specific connection"""
        if connection_uuid:
            if connection_uuid in self._clients:
                self._clients[connection_uuid].export_volume_latency_log(
                    latency_ms, volume_operation
                )

    def close(self, connection_uuid):
        """Close telemetry client(s)"""
        if connection_uuid:
            if connection_uuid in self._clients:
                self._clients[connection_uuid].close()
                del self._clients[connection_uuid]


# Create a global instance
telemetry_client = SingletonTelemetryClient()
