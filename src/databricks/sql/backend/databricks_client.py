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
from typing import Dict, Tuple, List, Optional, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.client import Cursor

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.backend.types import SessionId, CommandId, CommandState
from databricks.sql.utils import ExecuteResponse
from databricks.sql.types import SSLOptions

# Forward reference for type hints
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.result_set import ResultSet


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
        cursor: "Cursor",
        use_cloud_fetch: bool,
        parameters: List[ttypes.TSparkParameter],
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
    ) -> Union["ResultSet", None]:
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
        pass

    @abstractmethod
    def get_query_state(self, command_id: CommandId) -> CommandState:
        pass

    @abstractmethod
    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ) -> "ResultSet":
        pass

    # == Metadata Operations ==
    @abstractmethod
    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
    ) -> "ResultSet":
        pass

    @abstractmethod
    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> "ResultSet":
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    # == Properties ==
    @property
    @abstractmethod
    def staging_allowed_local_path(self) -> Union[None, str, List[str]]:
        """
        Gets the allowed local paths for staging operations.

        Returns:
            Union[None, str, List[str]]: The allowed local paths for staging operations,
            or None if staging is not allowed
        """
        pass

    @property
    @abstractmethod
    def ssl_options(self) -> SSLOptions:
        """
        Gets the SSL options for this client.

        Returns:
            SSLOptions: The SSL configuration options
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
