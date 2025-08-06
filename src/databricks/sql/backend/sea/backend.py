from __future__ import annotations

import logging
import time
import re
from typing import Any, Dict, Tuple, List, Optional, Union, TYPE_CHECKING, Set

from databricks.sql.backend.sea.models.base import (
    ExternalLink,
    ResultManifest,
    StatementStatus,
)
from databricks.sql.backend.sea.models.responses import GetChunksResponse
from databricks.sql.backend.sea.utils.constants import (
    ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP,
    ResultFormat,
    ResultDisposition,
    ResultCompression,
    WaitTimeout,
    MetadataCommands,
)
from databricks.sql.backend.sea.utils.normalize import normalize_sea_type_to_thrift
from databricks.sql.thrift_api.TCLIService import ttypes

if TYPE_CHECKING:
    from databricks.sql.client import Cursor

from databricks.sql.backend.sea.result_set import SeaResultSet

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import (
    SessionId,
    CommandId,
    CommandState,
    BackendType,
    ExecuteResponse,
)
from databricks.sql.exc import DatabaseError, ServerOperationError
from databricks.sql.backend.sea.utils.http_client import SeaHttpClient
from databricks.sql.types import SSLOptions

from databricks.sql.backend.sea.models import (
    ExecuteStatementRequest,
    GetStatementRequest,
    CancelStatementRequest,
    CloseStatementRequest,
    CreateSessionRequest,
    DeleteSessionRequest,
    StatementParameter,
    ExecuteStatementResponse,
    GetStatementResponse,
    CreateSessionResponse,
)

logger = logging.getLogger(__name__)


def _filter_session_configuration(
    session_configuration: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Filter and normalise the provided session configuration parameters.

    The Statement Execution API supports only a subset of SQL session
    configuration options.  This helper validates the supplied
    ``session_configuration`` dictionary against the allow-list defined in
    ``ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP`` and returns a new
    dictionary that contains **only** the supported parameters.

    Args:
        session_configuration: Optional mapping of session configuration
            names to their desired values.  Key comparison is
            case-insensitive.

    Returns:
        Dict[str, str]: A dictionary containing only the supported
        configuration parameters with lower-case keys and string values.  If
        *session_configuration* is ``None`` or empty, an empty dictionary is
        returned.
    """

    if not session_configuration:
        return {}

    filtered_session_configuration = {}
    ignored_configs: Set[str] = set()

    for key, value in session_configuration.items():
        if key.upper() in ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP:
            filtered_session_configuration[key.lower()] = str(value)
        else:
            ignored_configs.add(key)

    if ignored_configs:
        logger.warning(
            "Some session configurations were ignored because they are not supported: %s",
            ignored_configs,
        )
        logger.warning(
            "Supported session configurations are: %s",
            list(ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keys()),
        )

    return filtered_session_configuration


class SeaDatabricksClient(DatabricksClient):
    """
    Statement Execution API (SEA) implementation of the DatabricksClient interface.
    """

    # SEA API paths
    BASE_PATH = "/api/2.0/sql/"
    SESSION_PATH = BASE_PATH + "sessions"
    SESSION_PATH_WITH_ID = SESSION_PATH + "/{}"
    STATEMENT_PATH = BASE_PATH + "statements"
    STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "/{}"
    CANCEL_STATEMENT_PATH_WITH_ID = STATEMENT_PATH + "/{}/cancel"
    CHUNK_PATH_WITH_ID_AND_INDEX = STATEMENT_PATH + "/{}/result/chunks/{}"

    # SEA constants
    POLL_INTERVAL_SECONDS = 0.2

    def __init__(
        self,
        server_hostname: str,
        port: int,
        http_path: str,
        http_headers: List[Tuple[str, str]],
        auth_provider,
        ssl_options: SSLOptions,
        **kwargs,
    ):
        """
        Initialize the SEA backend client.

        Args:
            server_hostname: Hostname of the Databricks server
            port: Port number for the connection
            http_path: HTTP path for the connection
            http_headers: List of HTTP headers to include in requests
            auth_provider: Authentication provider
            ssl_options: SSL configuration options
            **kwargs: Additional keyword arguments
        """

        logger.debug(
            "SeaDatabricksClient.__init__(server_hostname=%s, port=%s, http_path=%s)",
            server_hostname,
            port,
            http_path,
        )

        self._max_download_threads = kwargs.get("max_download_threads", 10)
        self._ssl_options = ssl_options
        self._use_arrow_native_complex_types = kwargs.get(
            "_use_arrow_native_complex_types", True
        )

        self.use_hybrid_disposition = kwargs.get("use_hybrid_disposition", True)
        self.use_cloud_fetch = kwargs.get("use_cloud_fetch", True)

        # Extract warehouse ID from http_path
        self.warehouse_id = self._extract_warehouse_id(http_path)

        # Initialize HTTP client
        self._http_client = SeaHttpClient(
            server_hostname=server_hostname,
            port=port,
            http_path=http_path,
            http_headers=http_headers,
            auth_provider=auth_provider,
            ssl_options=ssl_options,
            **kwargs,
        )

    def _extract_warehouse_id(self, http_path: str) -> str:
        """
        Extract the warehouse ID from the HTTP path.

        Args:
            http_path: The HTTP path from which to extract the warehouse ID

        Returns:
            The extracted warehouse ID

        Raises:
            ValueError: If the warehouse ID cannot be extracted from the path
        """

        warehouse_pattern = re.compile(r".*/warehouses/(.+)")
        endpoint_pattern = re.compile(r".*/endpoints/(.+)")

        for pattern in [warehouse_pattern, endpoint_pattern]:
            match = pattern.match(http_path)
            if not match:
                continue
            warehouse_id = match.group(1)
            logger.debug(
                f"Extracted warehouse ID: {warehouse_id} from path: {http_path}"
            )
            return warehouse_id

        # If no match found, raise error
        error_message = (
            f"Could not extract warehouse ID from http_path: {http_path}. "
            f"Expected format: /path/to/warehouses/{{warehouse_id}} or "
            f"/path/to/endpoints/{{warehouse_id}}."
            f"Note: SEA only works for warehouses."
        )
        logger.error(error_message)
        raise ValueError(error_message)

    @property
    def max_download_threads(self) -> int:
        """Get the maximum number of download threads for cloud fetch operations."""
        return self._max_download_threads

    def open_session(
        self,
        session_configuration: Optional[Dict[str, Any]],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> SessionId:
        """
        Opens a new session with the Databricks SQL service using SEA.

        Args:
            session_configuration: Optional dictionary of configuration parameters for the session.
                                   Only specific parameters are supported as documented at:
                                   https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters
            catalog: Optional catalog name to use as the initial catalog for the session
            schema: Optional schema name to use as the initial schema for the session

        Returns:
            SessionId: A session identifier object that can be used for subsequent operations

        Raises:
            Error: If the session configuration is invalid
            OperationalError: If there's an error establishing the session
        """

        logger.debug(
            "SeaDatabricksClient.open_session(session_configuration=%s, catalog=%s, schema=%s)",
            session_configuration,
            catalog,
            schema,
        )

        session_configuration = _filter_session_configuration(session_configuration)

        request_data = CreateSessionRequest(
            warehouse_id=self.warehouse_id,
            session_confs=session_configuration,
            catalog=catalog,
            schema=schema,
        )

        response = self._http_client._make_request(
            method="POST", path=self.SESSION_PATH, data=request_data.to_dict()
        )

        session_response = CreateSessionResponse.from_dict(response)
        session_id = session_response.session_id
        if not session_id:
            raise ServerOperationError(
                "Failed to create session: No session ID returned",
                {
                    "operation-id": None,
                    "diagnostic-info": None,
                },
            )

        return SessionId.from_sea_session_id(session_id)

    def close_session(self, session_id: SessionId) -> None:
        """
        Closes an existing session with the Databricks SQL service.

        Args:
            session_id: The session identifier returned by open_session()

        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error closing the session
        """

        logger.debug("SeaDatabricksClient.close_session(session_id=%s)", session_id)

        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")
        sea_session_id = session_id.to_sea_session_id()

        request_data = DeleteSessionRequest(
            warehouse_id=self.warehouse_id,
            session_id=sea_session_id,
        )

        self._http_client._make_request(
            method="DELETE",
            path=self.SESSION_PATH_WITH_ID.format(sea_session_id),
            data=request_data.to_dict(),
        )

    def _extract_description_from_manifest(
        self, manifest: ResultManifest
    ) -> List[Tuple]:
        """
        Extract column description from a manifest object, in the format defined by
        the spec: https://peps.python.org/pep-0249/#description

        Args:
            manifest: The ResultManifest object containing schema information

        Returns:
            Optional[List]: A list of column tuples or None if no columns are found
        """

        schema_data = manifest.schema
        columns_data = schema_data.get("columns", [])

        columns = []
        for col_data in columns_data:
            # Format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            name = col_data.get("name", "")
            type_name = col_data.get("type_name", "")

            # Normalize SEA type to Thrift conventions before any processing
            type_name = normalize_sea_type_to_thrift(type_name, col_data)

            # Now strip _TYPE suffix and convert to lowercase
            type_name = (
                type_name[:-5] if type_name.endswith("_TYPE") else type_name
            ).lower()
            precision = col_data.get("type_precision")
            scale = col_data.get("type_scale")

            columns.append(
                (
                    name,  # name
                    type_name,  # type_code
                    None,  # display_size (not provided by SEA)
                    None,  # internal_size (not provided by SEA)
                    precision,  # precision
                    scale,  # scale
                    None,  # null_ok
                )
            )

        return columns

    def _results_message_to_execute_response(
        self, response: Union[ExecuteStatementResponse, GetStatementResponse]
    ) -> ExecuteResponse:
        """
        Convert a SEA response to an ExecuteResponse and extract result data.

        Args:
            sea_response: The response from the SEA API
            command_id: The command ID

        Returns:
            ExecuteResponse: The normalized execute response
        """

        # Extract description from manifest schema
        description = self._extract_description_from_manifest(response.manifest)

        # Check for compression
        lz4_compressed = (
            response.manifest.result_compression == ResultCompression.LZ4_FRAME.value
        )

        execute_response = ExecuteResponse(
            command_id=CommandId.from_sea_statement_id(response.statement_id),
            status=response.status.state,
            description=description,
            has_been_closed_server_side=False,
            lz4_compressed=lz4_compressed,
            is_staging_operation=response.manifest.is_volume_operation,
            arrow_schema_bytes=None,
            result_format=response.manifest.format,
        )

        return execute_response

    def _response_to_result_set(
        self,
        response: Union[ExecuteStatementResponse, GetStatementResponse],
        cursor: Cursor,
    ) -> SeaResultSet:
        """
        Convert a SEA response to a SeaResultSet.
        """

        execute_response = self._results_message_to_execute_response(response)

        return SeaResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            sea_client=self,
            result_data=response.result,
            manifest=response.manifest,
            buffer_size_bytes=cursor.buffer_size_bytes,
            arraysize=cursor.arraysize,
        )

    def _check_command_not_in_failed_or_closed_state(
        self, status: StatementStatus, command_id: CommandId
    ) -> None:
        state = status.state
        if state == CommandState.CLOSED:
            raise DatabaseError(
                "Command {} unexpectedly closed server side".format(command_id),
                {
                    "operation-id": command_id,
                },
            )
        if state == CommandState.FAILED:
            error = status.error
            error_code = error.error_code if error else "UNKNOWN_ERROR_CODE"
            error_message = error.message if error else "UNKNOWN_ERROR_MESSAGE"
            raise ServerOperationError(
                "Command failed: {} - {}".format(error_code, error_message),
                {
                    "operation-id": command_id,
                },
            )

    def _wait_until_command_done(
        self, response: ExecuteStatementResponse
    ) -> Union[ExecuteStatementResponse, GetStatementResponse]:
        """
        Wait until a command is done.
        """

        final_response: Union[ExecuteStatementResponse, GetStatementResponse] = response
        command_id = CommandId.from_sea_statement_id(final_response.statement_id)

        while final_response.status.state in [
            CommandState.PENDING,
            CommandState.RUNNING,
        ]:
            time.sleep(self.POLL_INTERVAL_SECONDS)
            final_response = self._poll_query(command_id)

        self._check_command_not_in_failed_or_closed_state(
            final_response.status, command_id
        )

        return final_response

    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: Cursor,
        use_cloud_fetch: bool,
        parameters: List[ttypes.TSparkParameter],
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
        row_limit: Optional[int] = None,
    ) -> Union[SeaResultSet, None]:
        """
        Execute a SQL command using the SEA backend.

        Args:
            operation: SQL command to execute
            session_id: Session identifier
            max_rows: Maximum number of rows to fetch
            max_bytes: Maximum number of bytes to fetch
            lz4_compression: Whether to use LZ4 compression
            cursor: Cursor executing the command
            use_cloud_fetch: Whether to use cloud fetch
            parameters: SQL parameters
            async_op: Whether to execute asynchronously
            enforce_embedded_schema_correctness: Whether to enforce schema correctness

        Returns:
            ResultSet: A SeaResultSet instance for the executed command
        """

        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")

        sea_session_id = session_id.to_sea_session_id()

        # Convert parameters to StatementParameter objects
        sea_parameters = []
        if parameters:
            for param in parameters:
                sea_parameters.append(
                    StatementParameter(
                        name=param.name,
                        value=(
                            param.value.stringValue if param.value is not None else None
                        ),
                        type=param.type,
                    )
                )

        format = (
            ResultFormat.ARROW_STREAM if use_cloud_fetch else ResultFormat.JSON_ARRAY
        ).value
        disposition = (
            (
                ResultDisposition.HYBRID
                if self.use_hybrid_disposition
                else ResultDisposition.EXTERNAL_LINKS
            )
            if use_cloud_fetch
            else ResultDisposition.INLINE
        ).value
        result_compression = (
            ResultCompression.LZ4_FRAME if lz4_compression else ResultCompression.NONE
        ).value

        request = ExecuteStatementRequest(
            warehouse_id=self.warehouse_id,
            session_id=sea_session_id,
            statement=operation,
            disposition=disposition,
            format=format,
            wait_timeout=(WaitTimeout.ASYNC if async_op else WaitTimeout.SYNC).value,
            on_wait_timeout="CONTINUE",
            row_limit=row_limit,
            parameters=sea_parameters if sea_parameters else None,
            result_compression=result_compression,
        )

        response_data = self._http_client._make_request(
            method="POST", path=self.STATEMENT_PATH, data=request.to_dict()
        )
        response = ExecuteStatementResponse.from_dict(response_data)
        statement_id = response.statement_id

        command_id = CommandId.from_sea_statement_id(statement_id)

        # Store the command ID in the cursor
        cursor.active_command_id = command_id

        # If async operation, return and let the client poll for results
        if async_op:
            return None

        final_response: Union[ExecuteStatementResponse, GetStatementResponse] = response
        if response.status.state != CommandState.SUCCEEDED:
            final_response = self._wait_until_command_done(response)

        return self._response_to_result_set(final_response, cursor)

    def cancel_command(self, command_id: CommandId) -> None:
        """
        Cancel a running command.

        Args:
            command_id: Command identifier to cancel

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = CancelStatementRequest(statement_id=sea_statement_id)
        self._http_client._make_request(
            method="POST",
            path=self.CANCEL_STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict(),
        )

    def close_command(self, command_id: CommandId) -> None:
        """
        Close a command and release resources.

        Args:
            command_id: Command identifier to close

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = CloseStatementRequest(statement_id=sea_statement_id)
        self._http_client._make_request(
            method="DELETE",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict(),
        )

    def _poll_query(self, command_id: CommandId) -> GetStatementResponse:
        """
        Poll for the current command info.
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = GetStatementRequest(statement_id=sea_statement_id)
        response_data = self._http_client._make_request(
            method="GET",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict(),
        )
        response = GetStatementResponse.from_dict(response_data)

        return response

    def get_query_state(self, command_id: CommandId) -> CommandState:
        """
        Get the state of a running query.

        Args:
            command_id: Command identifier

        Returns:
            CommandState: The current state of the command

        Raises:
            ValueError: If the command ID is invalid
        """

        response = self._poll_query(command_id)
        return response.status.state

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: Cursor,
    ) -> SeaResultSet:
        """
        Get the result of a command execution.

        Args:
            command_id: Command identifier
            cursor: Cursor executing the command

        Returns:
            SeaResultSet: A SeaResultSet instance with the execution results

        Raises:
            ValueError: If the command ID is invalid
        """

        response = self._poll_query(command_id)
        return self._response_to_result_set(response, cursor)

    def get_chunk_links(
        self, statement_id: str, chunk_index: int
    ) -> List[ExternalLink]:
        """
        Get links for chunks starting from the specified index.
        Args:
            statement_id: The statement ID
            chunk_index: The starting chunk index
        Returns:
            ExternalLink: External link for the chunk
        """

        response_data = self._http_client._make_request(
            method="GET",
            path=self.CHUNK_PATH_WITH_ID_AND_INDEX.format(statement_id, chunk_index),
        )
        response = GetChunksResponse.from_dict(response_data)

        links = response.external_links or []
        return links

    # == Metadata Operations ==

    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
    ) -> SeaResultSet:
        """Get available catalogs by executing 'SHOW CATALOGS'."""
        result = self.execute_command(
            operation=MetadataCommands.SHOW_CATALOGS.value,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=self.use_cloud_fetch,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result

    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> SeaResultSet:
        """Get schemas by executing 'SHOW SCHEMAS IN catalog [LIKE pattern]'."""
        if not catalog_name:
            raise DatabaseError("Catalog name is required for get_schemas")

        operation = MetadataCommands.SHOW_SCHEMAS.value.format(catalog_name)

        if schema_name:
            operation += MetadataCommands.LIKE_PATTERN.value.format(schema_name)

        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=self.use_cloud_fetch,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result

    def get_tables(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ) -> SeaResultSet:
        """Get tables by executing 'SHOW TABLES IN catalog [SCHEMA LIKE pattern] [LIKE pattern]'."""
        operation = (
            MetadataCommands.SHOW_TABLES_ALL_CATALOGS.value
            if catalog_name in [None, "*", "%"]
            else MetadataCommands.SHOW_TABLES.value.format(
                MetadataCommands.CATALOG_SPECIFIC.value.format(catalog_name)
            )
        )

        if schema_name:
            operation += MetadataCommands.SCHEMA_LIKE_PATTERN.value.format(schema_name)

        if table_name:
            operation += MetadataCommands.LIKE_PATTERN.value.format(table_name)

        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=self.use_cloud_fetch,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"

        # Apply client-side filtering by table_types
        from databricks.sql.backend.sea.utils.filters import ResultSetFilter

        result = ResultSetFilter.filter_tables_by_type(result, table_types)

        return result

    def get_columns(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> SeaResultSet:
        """Get columns by executing 'SHOW COLUMNS IN CATALOG catalog [SCHEMA LIKE pattern] [TABLE LIKE pattern] [LIKE pattern]'."""
        if not catalog_name:
            raise DatabaseError("Catalog name is required for get_columns")

        operation = MetadataCommands.SHOW_COLUMNS.value.format(catalog_name)

        if schema_name:
            operation += MetadataCommands.SCHEMA_LIKE_PATTERN.value.format(schema_name)

        if table_name:
            operation += MetadataCommands.TABLE_LIKE_PATTERN.value.format(table_name)

        if column_name:
            operation += MetadataCommands.LIKE_PATTERN.value.format(column_name)

        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=self.use_cloud_fetch,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result
