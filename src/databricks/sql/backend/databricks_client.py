from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.backend.types import SessionId, CommandId, CommandState


class DatabricksClient(ABC):
    """
    Abstract client interface for interacting with Databricks SQL services.

    Implementations of this class are responsible for:
    - Managing connections to Databricks SQL services
    - Executing SQL queries and commands
    - Retrieving query results
    - Fetching metadata about catalogs, schemas, tables, and columns
    """

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
        row_limit: Optional[int] = None,
    ) -> Union[ResultSet, None]:
        """
        Executes a SQL command or query within the specified session.

        This method sends a SQL command to the server for execution and handles
        the response. It can operate in both synchronous and asynchronous modes.

        Args:
            operation: The SQL command or query to execute
            session_id: The session identifier in which to execute the command
            max_rows: Maximum number of rows to fetch in a single fetch batch
            max_bytes: Maximum number of bytes to fetch in a single fetch batch
            lz4_compression: Whether to use LZ4 compression for result data
            cursor: The cursor object that will handle the results. The command id is set in this cursor.
            use_cloud_fetch: Whether to use cloud fetch for retrieving large result sets
            parameters: List of parameters to bind to the query
            async_op: Whether to execute the command asynchronously
            enforce_embedded_schema_correctness: Whether to enforce schema correctness
            row_limit: Maximum number of rows in the response.

        Returns:
            If async_op is False, returns a ResultSet object containing the
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
    def close_command(self, command_id: CommandId) -> None:
        """
        Closes a command and releases associated resources.

        This method informs the server that the client is done with the command
        and any resources associated with it can be released.

        Args:
            command_id: The command identifier to close

        Raises:
            ValueError: If the command ID is invalid
            OperationalError: If there's an error closing the command
        """
        pass

    @abstractmethod
    def get_query_state(self, command_id: CommandId) -> CommandState:
        """
        Gets the current state of a query or command.

        This method retrieves the current execution state of a command from the server.

        Args:
            command_id: The command identifier to check

        Returns:
            CommandState: The current state of the command

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
    ) -> ResultSet:
        """
        Retrieves the results of a previously executed command.

        This method fetches the results of a command that was executed asynchronously
        or retrieves additional results from a command that has more rows available.

        Args:
            command_id: The command identifier for which to retrieve results
            cursor: The cursor object that will handle the results

        Returns:
            ResultSet: An object containing the query results and metadata

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
    ) -> ResultSet:
        """
        Retrieves a list of available catalogs.

        This method fetches metadata about all catalogs available in the current
        session's context.

        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results

        Returns:
            ResultSet: An object containing the catalog metadata

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
    ) -> ResultSet:
        """
        Retrieves a list of schemas, optionally filtered by catalog and schema name patterns.

        This method fetches metadata about schemas available in the specified catalog
        or all catalogs if no catalog is specified.

        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            catalog_name: Optional catalog name pattern to filter by
            schema_name: Optional schema name pattern to filter by

        Returns:
            ResultSet: An object containing the schema metadata

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
    ) -> ResultSet:
        """
        Retrieves a list of tables, optionally filtered by catalog, schema, table name, and table types.

        This method fetches metadata about tables available in the specified catalog
        and schema, or all catalogs and schemas if not specified.

        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            catalog_name: Optional catalog name pattern to filter by
                if catalog_name is None, we fetch across all catalogs
            schema_name: Optional schema name pattern to filter by
                if schema_name is None, we fetch across all schemas
            table_name: Optional table name pattern to filter by
            table_types: Optional list of table types to filter by (e.g., ['TABLE', 'VIEW'])

        Returns:
            ResultSet: An object containing the table metadata

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
    ) -> ResultSet:
        """
        Retrieves a list of columns, optionally filtered by catalog, schema, table, and column name patterns.

        This method fetches metadata about columns available in the specified table,
        or all tables if not specified.

        Args:
            session_id: The session identifier
            max_rows: Maximum number of rows to fetch in a single batch
            max_bytes: Maximum number of bytes to fetch in a single batch
            cursor: The cursor object that will handle the results
            catalog_name: Optional catalog name pattern to filter by
            schema_name: Optional schema name pattern to filter by
            table_name: Optional table name pattern to filter by
                if table_name is None, we fetch across all tables
            column_name: Optional column name pattern to filter by

        Returns:
            ResultSet: An object containing the column metadata

        Raises:
            ValueError: If the session ID is invalid
            OperationalError: If there's an error retrieving the columns
        """
        pass

    @property
    @abstractmethod
    def max_download_threads(self) -> int:
        """
        Gets the maximum number of download threads for cloud fetch operations.

        Returns:
            int: The maximum number of download threads
        """
        pass
