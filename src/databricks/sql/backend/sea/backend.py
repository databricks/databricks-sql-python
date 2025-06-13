import logging
import time
import re
from typing import Dict, Tuple, List, Optional, Union, TYPE_CHECKING, Set

from databricks.sql.backend.sea.utils.constants import (
    ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP,
    ResultFormat,
    ResultDisposition,
    ResultCompression,
    WaitTimeout,
)

if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import (
    SessionId,
    CommandId,
    CommandState,
    BackendType,
    ExecuteResponse,
)
from databricks.sql.exc import ServerOperationError
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
from databricks.sql.backend.sea.models.responses import (
    parse_status,
    parse_manifest,
    parse_result,
)

logger = logging.getLogger(__name__)


def _filter_session_configuration(
    session_configuration: Optional[Dict[str, str]]
) -> Optional[Dict[str, str]]:
    if not session_configuration:
        return None

    filtered_session_configuration = {}
    ignored_configs: Set[str] = set()

    for key, value in session_configuration.items():
        if key.upper() in ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP:
            filtered_session_configuration[key.lower()] = value
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

        # Extract warehouse ID from http_path
        self.warehouse_id = self._extract_warehouse_id(http_path)

        # Initialize HTTP client
        self.http_client = SeaHttpClient(
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
        session_configuration: Optional[Dict[str, str]],
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

        response = self.http_client._make_request(
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

        self.http_client._make_request(
            method="DELETE",
            path=self.SESSION_PATH_WITH_ID.format(sea_session_id),
            data=request_data.to_dict(),
        )

    @staticmethod
    def get_default_session_configuration_value(name: str) -> Optional[str]:
        """
        Get the default value for a session configuration parameter.

        Args:
            name: The name of the session configuration parameter

        Returns:
            The default value if the parameter is supported, None otherwise
        """
        return ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.get(name.upper())

    @staticmethod
    def get_allowed_session_configurations() -> List[str]:
        """
        Get the list of allowed session configuration parameters.

        Returns:
            List of allowed session configuration parameter names
        """
        return list(ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keys())

    def _extract_description_from_manifest(self, manifest_obj) -> Optional[List]:
        """
        Extract column description from a manifest object.

        Args:
            manifest_obj: The ResultManifest object containing schema information

        Returns:
            Optional[List]: A list of column tuples or None if no columns are found
        """

        schema_data = manifest_obj.schema
        columns_data = schema_data.get("columns", [])

        if not columns_data:
            return None

        columns = []
        for col_data in columns_data:
            if not isinstance(col_data, dict):
                continue

            # Format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            columns.append(
                (
                    col_data.get("name", ""),  # name
                    col_data.get("type_name", ""),  # type_code
                    None,  # display_size (not provided by SEA)
                    None,  # internal_size (not provided by SEA)
                    col_data.get("precision"),  # precision
                    col_data.get("scale"),  # scale
                    col_data.get("nullable", True),  # null_ok
                )
            )

        return columns if columns else None

    def _results_message_to_execute_response(self, sea_response, command_id):
        """
        Convert a SEA response to an ExecuteResponse and extract result data.

        Args:
            sea_response: The response from the SEA API
            command_id: The command ID

        Returns:
            tuple: (ExecuteResponse, ResultData, ResultManifest) - The normalized execute response,
                  result data object, and manifest object
        """

        # Parse the response
        status = parse_status(sea_response)
        manifest_obj = parse_manifest(sea_response)
        result_data_obj = parse_result(sea_response)

        # Extract description from manifest schema
        description = self._extract_description_from_manifest(manifest_obj)

        # Check for compression
        lz4_compressed = manifest_obj.result_compression == "LZ4_FRAME"

        execute_response = ExecuteResponse(
            command_id=command_id,
            status=status.state,
            description=description,
            has_been_closed_server_side=False,
            lz4_compressed=lz4_compressed,
            is_staging_operation=False,
            arrow_schema_bytes=None,  # to be extracted during fetch phase for ARROW
            result_format=manifest_obj.format,
        )

        return execute_response, result_data_obj, manifest_obj

    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: "Cursor",
        use_cloud_fetch: bool,
        parameters: List,
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
    ) -> Union["ResultSet", None]:
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
                        value=param.value,
                        type=param.type if hasattr(param, "type") else None,
                    )
                )

        format = (
            ResultFormat.ARROW_STREAM if use_cloud_fetch else ResultFormat.JSON_ARRAY
        ).value
        disposition = (
            ResultDisposition.EXTERNAL_LINKS
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
            row_limit=max_rows,
            parameters=sea_parameters if sea_parameters else None,
            result_compression=result_compression,
        )

        response_data = self.http_client._make_request(
            method="POST", path=self.STATEMENT_PATH, data=request.to_dict()
        )
        response = ExecuteStatementResponse.from_dict(response_data)
        statement_id = response.statement_id
        if not statement_id:
            raise ServerOperationError(
                "Failed to execute command: No statement ID returned",
                {
                    "operation-id": None,
                    "diagnostic-info": None,
                },
            )

        command_id = CommandId.from_sea_statement_id(statement_id)

        # Store the command ID in the cursor
        cursor.active_command_id = command_id

        # If async operation, return and let the client poll for results
        if async_op:
            return None

        # For synchronous operation, wait for the statement to complete
        status = response.status
        state = status.state

        # Keep polling until we reach a terminal state
        while state in [CommandState.PENDING, CommandState.RUNNING]:
            time.sleep(0.5)  # add a small delay to avoid excessive API calls
            state = self.get_query_state(command_id)

        if state != CommandState.SUCCEEDED:
            raise ServerOperationError(
                f"Statement execution did not succeed: {status.error.message if status.error else 'Unknown error'}",
                {
                    "operation-id": command_id.to_sea_statement_id(),
                    "diagnostic-info": None,
                },
            )

        return self.get_execution_result(command_id, cursor)

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
        self.http_client._make_request(
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
        self.http_client._make_request(
            method="DELETE",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict(),
        )

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

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        request = GetStatementRequest(statement_id=sea_statement_id)
        response_data = self.http_client._make_request(
            method="GET",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict(),
        )

        # Parse the response
        response = GetStatementResponse.from_dict(response_data)
        return response.status.state

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ) -> "ResultSet":
        """
        Get the result of a command execution.

        Args:
            command_id: Command identifier
            cursor: Cursor executing the command

        Returns:
            ResultSet: A SeaResultSet instance with the execution results

        Raises:
            ValueError: If the command ID is invalid
        """

        if command_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA command ID")

        sea_statement_id = command_id.to_sea_statement_id()

        # Create the request model
        request = GetStatementRequest(statement_id=sea_statement_id)

        # Get the statement result
        response_data = self.http_client._make_request(
            method="GET",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict(),
        )

        # Create and return a SeaResultSet
        from databricks.sql.result_set import SeaResultSet

        # Convert the response to an ExecuteResponse and extract result data
        (
            execute_response,
            result_data,
            manifest,
        ) = self._results_message_to_execute_response(response_data, command_id)

        return SeaResultSet(
            connection=cursor.connection,
            execute_response=execute_response,
            sea_client=self,
            buffer_size_bytes=cursor.buffer_size_bytes,
            arraysize=cursor.arraysize,
            result_data=result_data,
            manifest=manifest,
        )

    # == Metadata Operations ==

    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
    ):
        """Not implemented yet."""
        raise NotImplementedError("get_catalogs is not yet implemented for SEA backend")

    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ):
        """Not implemented yet."""
        raise NotImplementedError("get_schemas is not yet implemented for SEA backend")

    def get_tables(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ):
        """Not implemented yet."""
        raise NotImplementedError("get_tables is not yet implemented for SEA backend")

    def get_columns(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ):
        """Not implemented yet."""
        raise NotImplementedError("get_columns is not yet implemented for SEA backend")
