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
        azure_workspace_resource_id (str): Azure workspace resource ID
        azure_tenant_id (str): Azure tenant ID
        use_proxy (bool): Whether proxy is being used
        use_system_proxy (bool): Whether system proxy is being used
        proxy_host_info (HostDetails): Proxy host details if configured
        use_cf_proxy (bool): Whether CloudFlare proxy is being used
        cf_proxy_host_info (HostDetails): CloudFlare proxy host details if configured
        non_proxy_hosts (list): List of hosts that bypass proxy
        allow_self_signed_support (bool): Whether self-signed certificates are allowed
        use_system_trust_store (bool): Whether system trust store is used
        enable_arrow (bool): Whether Arrow format is enabled
        enable_direct_results (bool): Whether direct results are enabled
        enable_sea_hybrid_results (bool): Whether SEA hybrid results are enabled
        http_connection_pool_size (int): HTTP connection pool size
        rows_fetched_per_block (int): Number of rows fetched per block
        async_poll_interval_millis (int): Async polling interval in milliseconds
        support_many_parameters (bool): Whether many parameters are supported
        enable_complex_datatype_support (bool): Whether complex datatypes are supported
        allowed_volume_ingestion_paths (str): Allowed paths for volume ingestion
        query_tags (str): Query tags for tracking and attribution
    """

    http_path: str
    mode: DatabricksClientType
    host_info: HostDetails
    auth_mech: Optional[AuthMech] = None
    auth_flow: Optional[AuthFlow] = None
    socket_timeout: Optional[int] = None
    azure_workspace_resource_id: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    use_proxy: Optional[bool] = None
    use_system_proxy: Optional[bool] = None
    proxy_host_info: Optional[HostDetails] = None
    use_cf_proxy: Optional[bool] = None
    cf_proxy_host_info: Optional[HostDetails] = None
    non_proxy_hosts: Optional[list] = None
    allow_self_signed_support: Optional[bool] = None
    use_system_trust_store: Optional[bool] = None
    enable_arrow: Optional[bool] = None
    enable_direct_results: Optional[bool] = None
    enable_sea_hybrid_results: Optional[bool] = None
    http_connection_pool_size: Optional[int] = None
    rows_fetched_per_block: Optional[int] = None
    async_poll_interval_millis: Optional[int] = None
    support_many_parameters: Optional[bool] = None
    enable_complex_datatype_support: Optional[bool] = None
    allowed_volume_ingestion_paths: Optional[str] = None
    query_tags: Optional[str] = None


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
class ChunkDetails(JsonSerializableMixin):
    """
    Contains detailed metrics about chunk downloads during result fetching.

    These metrics are accumulated across all chunk downloads for a single statement.

    Attributes:
        initial_chunk_latency_millis (int): Latency of the first chunk download
        slowest_chunk_latency_millis (int): Latency of the slowest chunk download
        total_chunks_present (int): Total number of chunks available
        total_chunks_iterated (int): Number of chunks actually downloaded
        sum_chunks_download_time_millis (int): Total time spent downloading all chunks
    """

    initial_chunk_latency_millis: Optional[int] = None
    slowest_chunk_latency_millis: Optional[int] = None
    total_chunks_present: Optional[int] = None
    total_chunks_iterated: Optional[int] = None
    sum_chunks_download_time_millis: Optional[int] = None


@dataclass
class ResultLatency(JsonSerializableMixin):
    """
    Contains latency metrics for different phases of query execution.

    This tracks two distinct phases:
    1. result_set_ready_latency_millis: Time from query submission until results are available (execute phase)
       - Set when execute() completes
    2. result_set_consumption_latency_millis: Time spent iterating/fetching results (fetch phase)
       - Measured from first fetch call until no more rows available
       - In Java: tracked via markResultSetConsumption(hasNext) method
       - Records start time on first fetch, calculates total on last fetch

    Attributes:
        result_set_ready_latency_millis (int): Time until query results are ready (execution phase)
        result_set_consumption_latency_millis (int): Time spent fetching/consuming results (fetch phase)

    """

    result_set_ready_latency_millis: Optional[int] = None
    result_set_consumption_latency_millis: Optional[int] = None


@dataclass
class OperationDetail(JsonSerializableMixin):
    """
    Contains detailed information about the operation being performed.

    Attributes:
        n_operation_status_calls (int): Number of status polling calls made
        operation_status_latency_millis (int): Total latency of all status calls
        operation_type (str): Specific operation type (e.g., EXECUTE_STATEMENT, LIST_TABLES, CANCEL_STATEMENT)
        is_internal_call (bool): Whether this is an internal driver operation
    """

    n_operation_status_calls: Optional[int] = None
    operation_status_latency_millis: Optional[int] = None
    operation_type: Optional[str] = None
    is_internal_call: Optional[bool] = None


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
        chunk_id (int): ID of the chunk if applicable (used for error tracking)
        chunk_details (ChunkDetails): Aggregated chunk download metrics
        result_latency (ResultLatency): Latency breakdown by execution phase
        operation_detail (OperationDetail): Detailed operation information
    """

    statement_type: StatementType
    is_compressed: bool
    execution_result: ExecutionResultFormat
    retry_count: Optional[int]
    chunk_id: Optional[int]
    chunk_details: Optional[ChunkDetails] = None
    result_latency: Optional[ResultLatency] = None
    operation_detail: Optional[OperationDetail] = None


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
