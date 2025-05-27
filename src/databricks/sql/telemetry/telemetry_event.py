import json
from dataclasses import dataclass, asdict
from databricks.sql.telemetry.enums import (
    AuthMech,
    AuthFlow,
    DatabricksClientType,
    DriverVolumeOperationType,
    StatementType,
    ExecutionResultFormat,
)


@dataclass
class HostDetails:
    host_url: str
    port: int

    """ Part of DriverConnectionParameters
    HostDetails hostDetails = new HostDetails(
        hostUrl = "https://my-workspace.cloud.databricks.com",
        port = 443
    )
    """

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class DriverConnectionParameters:
    http_path: str
    mode: DatabricksClientType
    host_info: HostDetails
    auth_mech: AuthMech
    auth_flow: AuthFlow
    auth_scope: str
    discovery_url: str
    allowed_volume_ingestion_paths: str
    azure_tenant_id: str
    socket_timeout: int

    """ Part of TelemetryEvent
    DriverConnectionParameters connectionParams = new DriverConnectionParameters(
        httpPath = " /sql/1.0/endpoints/1234567890abcdef",
        driverMode = "THRIFT",
        hostDetails = new HostDetails(
            hostUrl = "https://my-workspace.cloud.databricks.com",
            port = 443
        ),
        authMech = "OAUTH",
        authFlow = "AZURE_MANAGED_IDENTITIES",
        authScope = "sql",
        discoveryUrl = "https://example-url",
        allowedVolumeIngestionPaths = "[]",
        azureTenantId = "1234567890abcdef",
        socketTimeout = 10000
    )"""

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class DriverSystemConfiguration:
    driver_version: str
    os_name: str
    os_version: str
    os_arch: str
    runtime_name: str
    runtime_version: str
    runtime_vendor: str
    client_app_name: str
    locale_name: str
    driver_name: str
    char_set_encoding: str

    """Part of TelemetryEvent
    DriverSystemConfiguration systemConfig = new DriverSystemConfiguration(
        driver_version = "2.9.3",
        os_name = "Darwin",
        os_version = "24.4.0",
        os_arch = "arm64",
        runtime_name = "CPython",
        runtime_version = "3.13.3",
        runtime_vendor = "cpython",
        client_app_name = "databricks-sql-python",
        locale_name = "en_US",
        driver_name = "databricks-sql-python",
        char_set_encoding = "UTF-8"
    )
    """

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class DriverVolumeOperation:
    volume_operation_type: DriverVolumeOperationType
    volume_path: str

    """ Part of TelemetryEvent
    DriverVolumeOperation volumeOperation = new DriverVolumeOperation(
        volumeOperationType = "LIST",
        volumePath = "/path/to/volume"
    )
    """

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class DriverErrorInfo:
    error_name: str
    stack_trace: str

    """Required for ErrorLogs
    DriverErrorInfo errorInfo = new DriverErrorInfo(
        errorName="CONNECTION_ERROR",
        stackTrace="Connection failure while using the Databricks SQL Python connector. Failed to connect to server: https://my-workspace.cloud.databricks.com\n" +
                "databricks.sql.exc.OperationalError: Connection refused: connect\n" +
                "at databricks.sql.thrift_backend.ThriftBackend.make_request(ThriftBackend.py:329)\n" +
                "at databricks.sql.thrift_backend.ThriftBackend.attempt_request(ThriftBackend.py:366)\n" +
                "at databricks.sql.thrift_backend.ThriftBackend.open_session(ThriftBackend.py:575)\n" +
                "at databricks.sql.client.Connection.__init__(client.py:69)\n" +
                "at databricks.sql.client.connect(connection.py:123)")
    """

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class SqlExecutionEvent:
    statement_type: StatementType
    is_compressed: bool
    execution_result: ExecutionResultFormat
    retry_count: int

    """Part of TelemetryEvent
    SqlExecutionEvent sqlExecutionEvent = new SqlExecutionEvent(
        statementType = "QUERY",
        isCompressed = true,
        executionResult = "INLINE_ARROW",
        retryCount = 0
    )"""

    def to_json(self):
        return json.dumps(asdict(self))


@dataclass
class TelemetryEvent:
    session_id: str
    sql_statement_id: str
    system_configuration: DriverSystemConfiguration
    driver_connection_params: DriverConnectionParameters
    auth_type: str
    vol_operation: DriverVolumeOperation
    sql_operation: SqlExecutionEvent
    error_info: DriverErrorInfo
    operation_latency_ms: int

    def to_json(self):
        return json.dumps(asdict(self))
