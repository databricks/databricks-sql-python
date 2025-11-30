from dataclasses import dataclass
from databricks.sql.telemetry.models.enums import (
    AuthMech,
    AuthFlow,
    DatabricksClientType,
    DriverVolumeOperationType,
    StatementType,
    ExecutionResultFormat,
)
from typing import Optional
from databricks.sql.telemetry.utils import JsonSerializableMixin


@dataclass
class HostDetails(JsonSerializableMixin):
    """
    Represents the host connection details for a Databricks workspace.

    Attributes:
        host_url (str): The URL of the Databricks workspace (e.g., https://my-workspace.cloud.databricks.com)
        port (int): The port number for the connection (typically 443 for HTTPS)
    """

    host_url: str
    port: int


@dataclass
class DriverConnectionParameters(JsonSerializableMixin):
    """
    Contains all connection parameters used to establish a connection to Databricks SQL.
    This includes authentication details, host information, and connection settings.

    Attributes:
        http_path (str): The HTTP path for the SQL endpoint
        mode (DatabricksClientType): The type of client connection (e.g., THRIFT)
        host_info (HostDetails): Details about the host connection
        auth_mech (AuthMech): The authentication mechanism used
        auth_flow (AuthFlow): The authentication flow type
        socket_timeout (int): Connection timeout in milliseconds
    """

    http_path: str
    mode: DatabricksClientType
    host_info: HostDetails
    auth_mech: Optional[AuthMech] = None
    auth_flow: Optional[AuthFlow] = None
    socket_timeout: Optional[int] = None


@dataclass
class DriverSystemConfiguration(JsonSerializableMixin):
    """
    Contains system-level configuration information about the client environment.
    This includes details about the operating system, runtime, and driver version.

    Attributes:
        driver_version (str): Version of the Databricks SQL driver
        os_name (str): Name of the operating system
        os_version (str): Version of the operating system
        os_arch (str): Architecture of the operating system
        runtime_name (str): Name of the Python runtime (e.g., CPython)
        runtime_version (str): Version of the Python runtime
        runtime_vendor (str): Vendor of the Python runtime
        client_app_name (str): Name of the client application
        locale_name (str): System locale setting
        driver_name (str): Name of the driver
        char_set_encoding (str): Character set encoding used
    """

    driver_version: str
    os_name: str
    os_version: str
    os_arch: str
    runtime_name: str
    runtime_version: str
    runtime_vendor: str
    driver_name: str
    char_set_encoding: str
    client_app_name: Optional[str] = None
    locale_name: Optional[str] = None


@dataclass
class DriverVolumeOperation(JsonSerializableMixin):
    """
    Represents a volume operation performed by the driver.
    Used for tracking volume-related operations in telemetry.

    Attributes:
        volume_operation_type (DriverVolumeOperationType): Type of volume operation (e.g., LIST)
        volume_path (str): Path to the volume being operated on
    """

    volume_operation_type: DriverVolumeOperationType
    volume_path: str


@dataclass
class DriverErrorInfo(JsonSerializableMixin):
    """
    Contains detailed information about errors that occur during driver operations.
    Used for error tracking and debugging in telemetry.

    Attributes:
        error_name (str): Name/type of the error
        stack_trace (str): Full stack trace of the error
    """

    error_name: str
    stack_trace: str


@dataclass
class SqlExecutionEvent(JsonSerializableMixin):
    """
    Represents a SQL query execution event.
    Contains details about the query execution, including type, compression, and result format.

    Attributes:
        statement_type (StatementType): Type of SQL statement
        is_compressed (bool): Whether the result is compressed
        execution_result (ExecutionResultFormat): Format of the execution result
        retry_count (int): Number of retry attempts made
        chunk_id (int): ID of the chunk if applicable
    """

    statement_type: StatementType
    is_compressed: bool
    execution_result: ExecutionResultFormat
    retry_count: Optional[int]
    chunk_id: Optional[int]


@dataclass
class TelemetryEvent(JsonSerializableMixin):
    """
    Main telemetry event class that aggregates all telemetry data.
    Contains information about the session, system configuration, connection parameters,
    and any operations or errors that occurred.

    Attributes:
        session_id (str): Unique identifier for the session
        sql_statement_id (Optional[str]): ID of the SQL statement if applicable
        system_configuration (DriverSystemConfiguration): System configuration details
        driver_connection_params (DriverConnectionParameters): Connection parameters
        auth_type (Optional[str]): Type of authentication used
        vol_operation (Optional[DriverVolumeOperation]): Volume operation details if applicable
        sql_operation (Optional[SqlExecutionEvent]): SQL execution details if applicable
        error_info (Optional[DriverErrorInfo]): Error information if an error occurred
        operation_latency_ms (Optional[int]): Operation latency in milliseconds
    """

    system_configuration: DriverSystemConfiguration
    driver_connection_params: DriverConnectionParameters
    session_id: Optional[str] = None
    sql_statement_id: Optional[str] = None
    auth_type: Optional[str] = None
    vol_operation: Optional[DriverVolumeOperation] = None
    sql_operation: Optional[SqlExecutionEvent] = None
    error_info: Optional[DriverErrorInfo] = None
    operation_latency_ms: Optional[int] = None
