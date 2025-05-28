"""Statement Execution API backend implementation."""

import logging
import re
from typing import Dict, List, Optional, Any, Union, Tuple

from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.auth.http_utils import make_request
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.sea_constants import (
    SESSION_PATH,
    SESSION_PATH_WITH_ID,
)
from databricks.sql.backend.types import (
    SessionId,
    CommandId,
    CommandState,
    BackendType,
)
from databricks.sql.exc import (
    DatabaseError,
    NotSupportedError,
)
from databricks.sql.types import SSLOptions

logger = logging.getLogger(__name__)


class SEADatabricksClient(DatabricksClient):
    """Implementation of DatabricksClient using the Statement Execution API."""
    
    def __init__(
        self,
        server_hostname: str,
        port: int,
        http_path: str,
        http_headers: List[Tuple[str, str]],
        auth_provider: AuthProvider,
        ssl_options: SSLOptions,
        staging_allowed_local_path: Union[None, str, List[str]] = None,
        **kwargs,
    ):
        """Initialize the SEA client.
        
        Args:
            server_hostname: Databricks instance hostname
            port: Port to connect to
            http_path: Base HTTP path for API calls
            http_headers: HTTP headers to include in requests
            auth_provider: Authentication provider
            ssl_options: SSL configuration options
            staging_allowed_local_path: Path(s) allowed for staging operations
        """
        self._staging_allowed_local_path = staging_allowed_local_path
        self._ssl_options = ssl_options
        self._max_download_threads = kwargs.get("max_download_threads", 10)
        
        # Build URI for Thrift HTTP client
        uri = f"https://{server_hostname}:{port}/{http_path.lstrip('/')}"
        
        # Create Thrift HTTP client to reuse its transport layer
        self._thrift_http_client = THttpClient(
            auth_provider=auth_provider,
            uri_or_host=uri,
            ssl_options=ssl_options,
            **kwargs
        )
        
        # Set custom headers
        headers_dict = dict(http_headers) if http_headers else {}
        self._thrift_http_client.setCustomHeaders(headers_dict)
        
        # Extract warehouse ID from http_path
        self._warehouse_id = self._extract_warehouse_id(http_path)
    
    def _extract_warehouse_id(self, http_path: str) -> str:
        """Extract warehouse ID from http_path.
        
        Args:
            http_path: HTTP path for the endpoint
            
        Returns:
            Warehouse ID
        """
        # Try to extract warehouse ID using regex patterns
        # Pattern for endpoints: /sql/1.0/endpoints/1234567890abcdef
        endpoint_match = re.search(r'/endpoints/([^/]+)', http_path)
        if endpoint_match:
            return endpoint_match.group(1)
        
        # Pattern for warehouses: /sql/1.0/warehouses/1234567890abcdef
        warehouse_match = re.search(r'/warehouses/([^/]+)', http_path)
        if warehouse_match:
            return warehouse_match.group(1)
        
        # Pattern for clusters: /sql/protocolv1/o/1234567890123456/1234-123456-slid123
        cluster_match = re.search(r'protocolv1/o/[^/]+/([^/]+)', http_path)
        if cluster_match:
            return cluster_match.group(1)
        
        raise ValueError(f"Could not extract warehouse ID from http_path: {http_path}")
    
    @property
    def staging_allowed_local_path(self) -> Union[None, str, List[str]]:
        """Get the path(s) allowed for staging operations."""
        return self._staging_allowed_local_path
    
    @property
    def ssl_options(self) -> SSLOptions:
        """Get the SSL options."""
        return self._ssl_options
    
    @property
    def max_download_threads(self) -> int:
        """Get the maximum number of download threads."""
        return self._max_download_threads
    
    def open_session(
        self,
        session_configuration: Optional[Dict[str, Any]],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> SessionId:
        """Open a session using the Statement Execution API.
        
        Args:
            session_configuration: Configuration for the session
            catalog: Initial catalog to use
            schema: Initial schema to use
            
        Returns:
            SessionId object representing the session
        """
        # Create session request
        request_data = {
            "warehouse_id": self._warehouse_id
        }
        
        if catalog:
            request_data["catalog"] = catalog
        
        if schema:
            request_data["schema"] = schema
        
        if session_configuration:
            request_data["session_configs"] = session_configuration
        
        try:
            # Make API request using the Thrift HTTP client's connection pool
            response_data = make_request(
                self._thrift_http_client,
                method="POST",
                path=SESSION_PATH,
                data=request_data,
            )
            
            session_id = response_data.get("session_id")
            
            if not session_id:
                raise DatabaseError("Failed to create session: No session ID returned")
            
            # Create and return SessionId object
            return SessionId.from_sea_session_id(session_id)
        
        except Exception as e:
            logger.error("Error opening session: %s", e)
            raise DatabaseError(f"Failed to create session: {str(e)}")
    
    def close_session(self, session_id: SessionId) -> None:
        """Close a session.
        
        Args:
            session_id: ID of the session to close
        """
        sea_session_id = session_id.to_sea_session_id()
        if not sea_session_id:
            raise ValueError("Not a valid SEA session ID")
        
        try:
            # Make API request using the Thrift HTTP client's connection pool
            path = SESSION_PATH_WITH_ID.format(sea_session_id)
            params = {"warehouse_id": self._warehouse_id}
            
            make_request(
                self._thrift_http_client,
                method="DELETE",
                path=path,
                params=params,
            )
        except Exception as e:
            logger.error("Error closing session: %s", e)
            # Don't raise an exception here, as we want to continue cleanup
    
    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: Any,
        use_cloud_fetch: bool,
        parameters: List,
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
    ) -> "ResultSet":
        """Execute a command using the Statement Execution API.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("execute_command is not yet implemented for SEA backend")
    
    def cancel_command(self, command_id: CommandId) -> None:
        """Cancel a command.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("cancel_command is not yet implemented for SEA backend")
    
    def close_command(self, command_id: CommandId) -> None:
        """Close a command.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("close_command is not yet implemented for SEA backend")
    
    def get_query_state(self, command_id: CommandId) -> CommandState:
        """Get the state of a query.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("get_query_state is not yet implemented for SEA backend")
    
    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: Any,
    ) -> Any:
        """Get the result of a command execution.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("get_execution_result is not yet implemented for SEA backend")
    
    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Any,
    ) -> "ResultSet":
        """Get catalogs.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("get_catalogs is not yet implemented for SEA backend")
    
    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Any,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> "ResultSet":
        """Get schemas.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("get_schemas is not yet implemented for SEA backend")
    
    def get_tables(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Any,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ) -> "ResultSet":
        """Get tables.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("get_tables is not yet implemented for SEA backend")
    
    def get_columns(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Any,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> "ResultSet":
        """Get columns.
        
        This method is not yet implemented and will raise NotSupportedError.
        """
        raise NotSupportedError("get_columns is not yet implemented for SEA backend")
    
    def handle_to_id(self, session_id: SessionId) -> Any:
        """Get the raw session ID from a SessionId.
        
        Args:
            session_id: SessionId object
            
        Returns:
            Raw session ID
        """
        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")
        return session_id.guid
    
    def handle_to_hex_id(self, session_id: SessionId) -> str:
        """Get the hex representation of a session ID.
        
        Args:
            session_id: SessionId object
            
        Returns:
            Hex representation of the session ID
        """
        if session_id.backend_type != BackendType.SEA:
            raise ValueError("Not a valid SEA session ID")
        return str(session_id.guid)