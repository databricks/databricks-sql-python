"""
Abstract client interface for interacting with Databricks SQL services.

Implementations of this class are responsible for:
- Managing connections to Databricks SQL services
- Handling authentication
- Executing SQL queries and commands
- Retrieving query results
- Fetching metadata about catalogs, schemas, tables, and columns
- Managing error handling and retries
"""

from abc import ABC, abstractmethod
from typing import Dict, Tuple, List, Optional, Any, Union

from databricks.sql.client import Cursor
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.backend.types import SessionId, CommandId
from databricks.sql.utils import ExecuteResponse
from databricks.sql.types import SSLOptions


class DatabricksClient(ABC):
    # == Connection and Session Management ==
    @abstractmethod
    def open_session(
        self,
        session_configuration: Optional[Dict[str, Any]],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> SessionId:
        """
        Opens a new session with the Databricks SQL service.
        
        This method establishes a new session with the server and returns a session
        identifier that can be used for subsequent operations.
        
        Args:
            session_configuration: Optional dictionary of configuration parameters for the session
            catalog: Optional catalog name to use as the initial catalog for the session
            schema: Optional schema name to use as the initial schema for the session
            
        Returns:
            SessionId: A session identifier object that can be used for subsequent operations
            
        Raises:
            Error: If the session configuration is invalid
            OperationalError: If there's an error establishing the session
            InvalidServerResponseError: If the server response is invalid or unexpected
        """
        pass

    @abstractmethod
    def close_session(self, session_id: SessionId) -> None:
        """
        Closes an existing session with the Databricks SQL service.
        
        This method terminates the session identified by the given session ID and
        releases any resources associated with it.
        
        Args:
            session_id: The session identifier returned by open_session()
            
        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error closing the session
        """
        pass

    # == Query Execution, Command Management ==
    @abstractmethod
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
    ) -> Any:
        """
        Executes a SQL command or query within the specified session.
        
        This method sends a SQL command to the server for execution and handles
        the response. It can operate in both synchronous and asynchronous modes.
        
        Args:
            operation: The SQL command or query to execute
            session_id: The session identifier in which to execute the command
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            lz4_compression: Whether to use LZ4 compression for result data
            cursor: The cursor object that will handle the results
            use_cloud_fetch: Whether to use cloud fetch for retrieving large result sets
            parameters: List of parameters to bind to the query
            async_op: Whether to execute the command asynchronously
            enforce_embedded_schema_correctness: Whether to enforce schema correctness
            
        Returns:
            If async_op is False, returns an ExecuteResponse object containing the
            query results and metadata. If async_op is True, returns None and the
            results must be fetched later using get_execution_result().
            
        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error executing the command
            ServerOperationError: If the server encounters an error during execution
        """
        pass

    @abstractmethod
    def cancel_command(self, command_id: CommandId) -> None:
        """
        Cancels a running command or query.
        
        This method attempts to cancel a command that is currently being executed.
        It can be called from a different thread than the one executing the command.
        
        Args:
            command_id: The command identifier to cancel
            
        Raises:
            ValueError: If the command ID is invalid
            OperationalError: If there's an error canceling the command
        """
        pass

    @abstractmethod
    def close_command(self, command_id: CommandId) -> ttypes.TStatus:
        """
        Closes a command and releases associated resources.
        
        This method informs the server that the client is done with the command
        and any resources associated with it can be released.
        
        Args:
            command_id: The command identifier to close
            
        Returns:
            ttypes.TStatus: The status of the close operation
            
        Raises:
            ValueError: If the command ID is invalid
            OperationalError: If there's an error closing the command
        """
        pass

    @abstractmethod
    def get_query_state(self, command_id: CommandId) -> ttypes.TOperationState:
        """
        Gets the current state of a query or command.
        
        This method retrieves the current execution state of a command from the server.
        
        Args:
            command_id: The command identifier to check
            
        Returns:
            ttypes.TOperationState: The current state of the command
            
        Raises:
            ValueError: If the command ID is invalid
            OperationalError: If there's an error retrieving the state
            ServerOperationError: If the command is in an error state
            DatabaseError: If the command has been closed unexpectedly
        """
        pass

    @abstractmethod
    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: Cursor,
    ) -> ExecuteResponse:
        """
        Retrieves the results of a previously executed command.
        
        This method fetches the results of a command that was executed asynchronously
        or retrieves additional results from a command that has more rows available.
        
        Args:
            command_id: The command identifier for which to retrieve results
            cursor: The cursor object that will handle the results
            
        Returns:
            ExecuteResponse: An object containing the query results and metadata
            
        Raises:
            ValueError: If the command ID is invalid
            OperationalError: If there's an error retrieving the results
        """
        pass

    # == Metadata Operations ==
    @abstractmethod
    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
    ) -> ExecuteResponse:
        """
        Retrieves a list of available catalogs.
        
        This method fetches metadata about the catalogs that are available
        in the current session.
        
        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            
        Returns:
            ExecuteResponse: An object containing the catalog metadata
            
        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error retrieving the catalogs
        """
        pass

    @abstractmethod
    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Cursor,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> ExecuteResponse:
        """
        Retrieves a list of available schemas.
        
        This method fetches metadata about the schemas that are available
        in the specified catalog.
        
        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            catalog_name: Optional catalog name to filter schemas
            schema_name: Optional schema pattern to filter schemas by name
            
        Returns:
            ExecuteResponse: An object containing the schema metadata
            
        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error retrieving the schemas
        """
        pass

    @abstractmethod
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
    ) -> ExecuteResponse:
        """
        Retrieves a list of available tables.
        
        This method fetches metadata about the tables that are available
        in the specified catalog and schema.
        
        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            catalog_name: Optional catalog name to filter tables
            schema_name: Optional schema name to filter tables
            table_name: Optional table pattern to filter tables by name
            table_types: Optional list of table types to include (e.g., "TABLE", "VIEW")
            
        Returns:
            ExecuteResponse: An object containing the table metadata
            
        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error retrieving the tables
        """
        pass

    @abstractmethod
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
    ) -> ExecuteResponse:
        """
        Retrieves column metadata for tables.
        
        This method fetches metadata about the columns in the specified
        catalog, schema, and table.
        
        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            catalog_name: Optional catalog name to filter columns
            schema_name: Optional schema name to filter columns
            table_name: Optional table name to filter columns
            column_name: Optional column pattern to filter columns by name
            
        Returns:
            ExecuteResponse: An object containing the column metadata
            
        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error retrieving the columns
        """
        pass

    # == Utility Methods ==
    @abstractmethod
    def handle_to_id(self, session_id: SessionId) -> Any:
        """
        Gets the raw session ID from a SessionId object.
        
        This method extracts the underlying protocol-specific identifier
        from the SessionId abstraction.
        
        Args:
            session_id: The session identifier
            
        Returns:
            The raw session ID as used by the underlying protocol
            
        Raises:
            ValueError: If the session ID is not valid for this client's backend type
        """
        pass

    @abstractmethod
    def handle_to_hex_id(self, session_id: SessionId) -> str:
        """
        Gets a hexadecimal string representation of a session ID.
        
        This method converts the session ID to a human-readable hexadecimal string
        that can be used for logging and debugging.
        
        Args:
            session_id: The session identifier
            
        Returns:
            str: A hexadecimal string representation of the session ID
            
        Raises:
            ValueError: If the session ID is not valid for this client's backend type
        """
        pass

    # Properties related to specific backend features
    @property
    @abstractmethod
    def staging_allowed_local_path(self) -> Union[None, str, List[str]]:
        """
        Gets the local path(s) allowed for staging data.
        
        This property returns the path or paths on the local filesystem that
        are allowed to be used for staging data when uploading to the server.
        
        Returns:
            Union[None, str, List[str]]: The allowed local path(s) or None if staging is not allowed
        """
        pass

    @property
    @abstractmethod
    def ssl_options(self) -> SSLOptions:
        """
        Gets the SSL options used by this client.
        
        This property returns the SSL configuration options that are used
        for secure communication with the server.
        
        Returns:
            SSLOptions: The SSL configuration options
        """
        pass

    @property
    @abstractmethod
    def max_download_threads(self) -> int:
        """
        Gets the maximum number of threads for handling cloud fetch downloads.
        
        This property returns the maximum number of concurrent threads that
        can be used for downloading result data when using cloud fetch.
        
        Returns:
            int: The maximum number of download threads
        """
        pass