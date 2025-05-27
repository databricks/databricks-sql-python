from abc import ABC, abstractmethod
from typing import Dict, Tuple, List, Optional, Any, Union

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.ids import SessionId, CommandId
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
        pass

    @abstractmethod
    def close_session(self, session_id: SessionId) -> None:
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
        cursor: Any,
        use_cloud_fetch: bool,
        parameters: List[ttypes.TSparkParameter],
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
    ) -> "ResultSet":  # Changed return type to ResultSet
        pass

    @abstractmethod
    def cancel_command(self, command_id: CommandId) -> None:
        pass

    @abstractmethod
    def close_command(self, command_id: CommandId) -> ttypes.TStatus:
        pass

    @abstractmethod
    def get_query_state(self, command_id: CommandId) -> ttypes.TOperationState:
        pass

    @abstractmethod
    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: Any,
    ) -> ExecuteResponse:
        pass

    # == Metadata Operations ==
    @abstractmethod
    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Any,
    ) -> Any:
        pass

    @abstractmethod
    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: Any,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Any:
        pass

    @abstractmethod
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
    ) -> Any:
        pass

    @abstractmethod
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
    ) -> Any:
        pass

    # == Utility Methods ==
    @abstractmethod
    def handle_to_id(self, session_id: SessionId) -> Any:
        pass

    @abstractmethod
    def handle_to_hex_id(self, session_id: SessionId) -> str:
        pass

    # Properties related to specific backend features
    @property
    @abstractmethod
    def staging_allowed_local_path(self) -> Union[None, str, List[str]]:
        pass

    @property
    @abstractmethod
    def ssl_options(self) -> SSLOptions:
        pass

    @property
    @abstractmethod
    def max_download_threads(self) -> int:
        pass
