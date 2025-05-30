import logging
import uuid
from typing import Dict, Tuple, List, Optional, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.client import Cursor

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import SessionId, CommandId, CommandState, BackendType
from databricks.sql.exc import Error, NotSupportedError
from databricks.sql.backend.utils.http_client import CustomHttpClient
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions

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
            logger.debug(
                f"Extracted warehouse ID: {warehouse_id} from path: {http_path}"
            )

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

        request_data: Dict[str, Any] = {"warehouse_id": self.warehouse_id}
        if session_configuration:
            request_data["session_confs"] = session_configuration
        if catalog:
            request_data["catalog"] = catalog
        if schema:
            request_data["schema"] = schema

        response = self.http_client._make_request(
            method="POST", path=self.SESSION_PATH, data=request_data
        )

        session_id = response.get("session_id")
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

        request_data = {"warehouse_id": self.warehouse_id}

        self.http_client._make_request(
            method="DELETE",
            path=self.SESSION_PATH_WITH_ID.format(sea_session_id),
            data=request_data,
        )

    # == Not Implemented Operations ==
    # These methods will be implemented in future iterations

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
    ):
        """Not implemented yet."""
        raise NotSupportedError(
            "execute_command is not yet implemented for SEA backend"
        )

    def cancel_command(self, command_id: CommandId) -> None:
        """Not implemented yet."""
        raise NotSupportedError("cancel_command is not yet implemented for SEA backend")

    def close_command(self, command_id: CommandId) -> None:
        """Not implemented yet."""
        raise NotSupportedError("close_command is not yet implemented for SEA backend")

    def get_query_state(self, command_id: CommandId) -> CommandState:
        """Not implemented yet."""
        raise NotSupportedError(
            "get_query_state is not yet implemented for SEA backend"
        )

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ):
        """Not implemented yet."""
        raise NotSupportedError(
            "get_execution_result is not yet implemented for SEA backend"
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
        raise NotSupportedError("get_catalogs is not yet implemented for SEA backend")

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
        raise NotSupportedError("get_schemas is not yet implemented for SEA backend")

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
        raise NotSupportedError("get_tables is not yet implemented for SEA backend")

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
        raise NotSupportedError("get_columns is not yet implemented for SEA backend")
