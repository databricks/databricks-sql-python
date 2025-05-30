import logging
import uuid
import time
from typing import Dict, Tuple, List, Optional, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import SessionId, CommandId, CommandState, BackendType
from databricks.sql.exc import Error, NotSupportedError
from databricks.sql.backend.utils.http_client import CustomHttpClient
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions

from databricks.sql.backend.models import (
    ExecuteStatementRequest,
    GetStatementRequest,
    CancelStatementRequest,
    CloseStatementRequest,
    CreateSessionRequest,
    DeleteSessionRequest,
    StatementParameter,
    ExecuteStatementResponse,
    GetStatementResponse,
    CreateSessionResponse
)

logger = logging.getLogger(__name__)


class SeaDatabricksClient(DatabricksClient):
    """
    Statement Execution API (SEA) implementation of the DatabricksClient interface.

    This implementation provides session management functionality for SEA,
    while other operations raise NotImplementedError.
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
        staging_allowed_local_path: Union[None, str, List[str]] = None,
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
            staging_allowed_local_path: Allowed local paths for staging operations
            **kwargs: Additional keyword arguments
        """
        logger.debug(
            "SEADatabricksClient.__init__(server_hostname=%s, port=%s, http_path=%s)",
            server_hostname,
            port,
            http_path,
        )

        self._staging_allowed_local_path = staging_allowed_local_path
        self._ssl_options = ssl_options
        self._max_download_threads = kwargs.get("max_download_threads", 10)

        # Extract warehouse ID from http_path
        self.warehouse_id = self._extract_warehouse_id(http_path)

        # Initialize HTTP client
        self.http_client = CustomHttpClient(
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

        The warehouse ID is expected to be the last segment of the path when the
        second-to-last segment is either 'warehouses' or 'endpoints'.
        This matches the JDBC implementation which supports both formats.

        Args:
            http_path: The HTTP path from which to extract the warehouse ID

        Returns:
            The extracted warehouse ID

        Raises:
            Error: If the warehouse ID cannot be extracted from the path
        """
        path_parts = http_path.strip("/").split("/")
        warehouse_id = None

        if len(path_parts) >= 3 and path_parts[-2] in ["warehouses", "endpoints"]:
            warehouse_id = path_parts[-1]
            logger.debug(f"Extracted warehouse ID: {warehouse_id} from path: {http_path}")

        if not warehouse_id:
            error_message = (
                f"Could not extract warehouse ID from http_path: {http_path}. "
                f"Expected format: /path/to/warehouses/{{warehouse_id}} or "
                f"/path/to/endpoints/{{warehouse_id}}"
            )
            logger.error(error_message)
            raise ValueError(error_message)

        return warehouse_id

    @property
    def staging_allowed_local_path(self) -> Union[None, str, List[str]]:
        """Get the allowed local paths for staging operations."""
        return self._staging_allowed_local_path

    @property
    def ssl_options(self) -> SSLOptions:
        """Get the SSL options for this client."""
        return self._ssl_options

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
            session_configuration: Optional dictionary of configuration parameters for the session
            catalog: Optional catalog name to use as the initial catalog for the session
            schema: Optional schema name to use as the initial schema for the session

        Returns:
            SessionId: A session identifier object that can be used for subsequent operations

        Raises:
            Error: If the session configuration is invalid
            OperationalError: If there's an error establishing the session
        """
        logger.debug(
            "SEADatabricksClient.open_session(session_configuration=%s, catalog=%s, schema=%s)",
            session_configuration,
            catalog,
            schema,
        )

        request = CreateSessionRequest(
            warehouse_id=self.warehouse_id,
            session_confs=session_configuration,
            catalog=catalog,
            schema=schema
        )

        # Send the request
        response = self.http_client._make_request(
            method="POST", path=self.SESSION_PATH, data=request.to_dict()
        )

        # Parse the response
        session_response = CreateSessionResponse.from_dict(response)
        session_id = session_response.session_id

        if not session_id:
            raise Error("Failed to create session: No session ID returned")

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
        logger.debug("SEADatabricksClient.close_session(session_id=%s)", session_id)

        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")
        sea_session_id = session_id.to_sea_session_id()

        # Create the request model
        request = DeleteSessionRequest(
            warehouse_id=self.warehouse_id,
            session_id=sea_session_id
        )

        # Send the request
        self.http_client._make_request(
            method="DELETE",
            path=self.SESSION_PATH_WITH_ID.format(sea_session_id),
            data=request.to_dict()
        )

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
                sea_parameters.append(StatementParameter(
                    name=param.name,
                    value=param.value,
                    type=param.type if hasattr(param, 'type') else None
                ))
        
        # Create the request model
        request = ExecuteStatementRequest(
            warehouse_id=self.warehouse_id,
            session_id=sea_session_id,
            statement=operation,
            disposition="EXTERNAL_LINKS" if use_cloud_fetch else "INLINE",
            format="ARROW_STREAM" if use_cloud_fetch else "JSON_ARRAY",
            wait_timeout="0s" if async_op else "30s",
            on_wait_timeout="CONTINUE",
            row_limit=max_rows if max_rows > 0 else None,
            byte_limit=max_bytes if max_bytes > 0 else None,
            parameters=sea_parameters if sea_parameters else None
        )
        
        # Execute the statement
        response_data = self.http_client._make_request(
            method="POST",
            path=self.STATEMENT_PATH,
            data=request.to_dict()
        )
        
        # Parse the response
        response = ExecuteStatementResponse.from_dict(response_data)
        
        # Create a command ID from the statement ID
        statement_id = response.statement_id
        if not statement_id:
            raise Error("Failed to execute command: No statement ID returned")
        
        command_id = CommandId.from_sea_statement_id(statement_id)
        
        # Store the command ID in the cursor
        cursor.active_command_id = command_id
        
        # If async operation, return None and let the client poll for results
        if async_op:
            return None
        
        # For synchronous operation, wait for the statement to complete
        # Poll until the statement is done
        status = response.status
        state = status.state
        
        # Keep polling until we reach a terminal state
        while state in ["PENDING", "RUNNING"]:
            # Add a small delay to avoid excessive API calls
            time.sleep(0.5)
            
            # Create the request model
            get_request = GetStatementRequest(
                warehouse_id=self.warehouse_id,
                statement_id=statement_id
            )
            
            # Get the statement status
            poll_response_data = self.http_client._make_request(
                method="GET",
                path=self.STATEMENT_PATH_WITH_ID.format(statement_id),
                data=get_request.to_dict()
            )
            
            # Parse the response
            poll_response = GetStatementResponse.from_dict(poll_response_data)
            status = poll_response.status
            state = status.state
            
            # Check for errors
            if state == "FAILED" and status.error:
                error_message = status.error.message
                raise Error(f"Statement execution failed: {error_message}")
            
            # Check for cancellation
            if state == "CANCELED":
                raise Error("Statement execution was canceled")
        
        # Get the final result
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
        
        # Create the request model
        request = CancelStatementRequest(
            warehouse_id=self.warehouse_id,
            statement_id=sea_statement_id
        )
        
        # Send the cancel request
        self.http_client._make_request(
            method="POST",
            path=self.CANCEL_STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict()
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
        
        # Create the request model
        request = CloseStatementRequest(
            warehouse_id=self.warehouse_id,
            statement_id=sea_statement_id
        )
        
        # Send the close request - SEA uses DELETE for closing statements
        self.http_client._make_request(
            method="DELETE",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict()
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
        
        # Create the request model
        request = GetStatementRequest(
            warehouse_id=self.warehouse_id,
            statement_id=sea_statement_id
        )
        
        # Get the statement status
        response_data = self.http_client._make_request(
            method="GET",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict()
        )
        
        # Parse the response
        response = GetStatementResponse.from_dict(response_data)
        
        # Extract the status
        state = response.status.state
        
        # Map SEA state to CommandState
        return CommandState.from_sea_state(state)

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
        request = GetStatementRequest(
            warehouse_id=self.warehouse_id,
            statement_id=sea_statement_id
        )
        
        # Get the statement result
        response_data = self.http_client._make_request(
            method="GET",
            path=self.STATEMENT_PATH_WITH_ID.format(sea_statement_id),
            data=request.to_dict()
        )
        
        # Create and return a SeaResultSet
        from databricks.sql.backend.sea_result_set import SeaResultSet
        return SeaResultSet(
            connection=cursor.connection,
            sea_response=response_data,
            sea_client=self,
            buffer_size_bytes=cursor.buffer_size_bytes,
            arraysize=cursor.arraysize,
        )

    # == Metadata Operations ==

    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
    ) -> "ResultSet":
        """Get available catalogs by executing 'SHOW CATALOGS'."""
        result = self.execute_command(
            operation="SHOW CATALOGS",
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
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
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> "ResultSet":
        """Get schemas by executing 'SHOW SCHEMAS [IN catalog]'."""
        operation = "SHOW SCHEMAS"
        if catalog_name:
            operation += f" IN `{catalog_name}`"
        if schema_name:
            operation += f" LIKE '{schema_name}'"
            
        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
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
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ) -> "ResultSet":
        """Get tables by executing 'SHOW TABLES [IN catalog.schema]'."""
        operation = "SHOW TABLES"
        
        if catalog_name and schema_name:
            operation += f" IN `{catalog_name}`.`{schema_name}`"
        elif schema_name:
            operation += f" IN `{schema_name}`"
            
        if table_name:
            operation += f" LIKE '{table_name}'"
            
        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result

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
    ) -> "ResultSet":
        """Get columns by executing 'DESCRIBE TABLE [catalog.schema.]table'."""
        if not table_name:
            raise ValueError("Table name is required for get_columns")
            
        operation = "DESCRIBE TABLE "
        
        if catalog_name and schema_name:
            operation += f"`{catalog_name}`.`{schema_name}`."
        elif schema_name:
            operation += f"`{schema_name}`."
            
        operation += f"`{table_name}`"
        
        # Column name filtering will be done client-side
        
        result = self.execute_command(
            operation=operation,
            session_id=session_id,
            max_rows=max_rows,
            max_bytes=max_bytes,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )
        assert result is not None, "execute_command returned None in synchronous mode"
        return result