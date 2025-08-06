from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
import logging

from databricks.sql.backend.utils.guid_utils import guid_to_hex_id
from databricks.sql.telemetry.models.enums import StatementType
from databricks.sql.thrift_api.TCLIService import ttypes

logger = logging.getLogger(__name__)


class CommandState(Enum):
    """
    Enum representing the execution state of a command in Databricks SQL.

    This enum maps Thrift operation states to normalized command states,
    providing a consistent interface for tracking command execution status
    across different backend implementations.

    Attributes:
        PENDING: Command is queued or initialized but not yet running
        RUNNING: Command is currently executing
        SUCCEEDED: Command completed successfully
        FAILED: Command failed due to error, timeout, or unknown state
        CLOSED: Command has been closed
        CANCELLED: Command was cancelled before completion
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"

    @classmethod
    def from_thrift_state(
        cls, state: ttypes.TOperationState
    ) -> Optional["CommandState"]:
        """
        Convert a Thrift TOperationState to a normalized CommandState.

        Args:
            state: A TOperationState from the Thrift API representing the current
                  state of an operation

        Returns:
            CommandState: The corresponding normalized command state

        Raises:
            ValueError: If the provided state is not a recognized TOperationState

        State Mappings:
            - INITIALIZED_STATE, PENDING_STATE -> PENDING
            - RUNNING_STATE -> RUNNING
            - FINISHED_STATE -> SUCCEEDED
            - ERROR_STATE, TIMEDOUT_STATE, UKNOWN_STATE -> FAILED
            - CLOSED_STATE -> CLOSED
            - CANCELED_STATE -> CANCELLED
        """

        if state in (
            ttypes.TOperationState.INITIALIZED_STATE,
            ttypes.TOperationState.PENDING_STATE,
        ):
            return cls.PENDING
        elif state == ttypes.TOperationState.RUNNING_STATE:
            return cls.RUNNING
        elif state == ttypes.TOperationState.FINISHED_STATE:
            return cls.SUCCEEDED
        elif state in (
            ttypes.TOperationState.ERROR_STATE,
            ttypes.TOperationState.TIMEDOUT_STATE,
            ttypes.TOperationState.UKNOWN_STATE,
        ):
            return cls.FAILED
        elif state == ttypes.TOperationState.CLOSED_STATE:
            return cls.CLOSED
        elif state == ttypes.TOperationState.CANCELED_STATE:
            return cls.CANCELLED
        else:
            return None

    @classmethod
    def from_sea_state(cls, state: str) -> Optional["CommandState"]:
        """
        Map SEA state string to CommandState enum.
        Args:
            state: SEA state string
        Returns:
            CommandState: The corresponding CommandState enum value
        """
        state_mapping = {
            "PENDING": cls.PENDING,
            "RUNNING": cls.RUNNING,
            "SUCCEEDED": cls.SUCCEEDED,
            "FAILED": cls.FAILED,
            "CLOSED": cls.CLOSED,
            "CANCELED": cls.CANCELLED,
        }

        return state_mapping.get(state, None)


class BackendType(Enum):
    """
    Enum representing the type of backend
    """

    THRIFT = "thrift"
    SEA = "sea"


class SessionId:
    """
    A normalized session identifier that works with both Thrift and SEA backends.

    This class abstracts away the differences between Thrift's TSessionHandle and
    SEA's session ID string, providing a consistent interface for the connector.
    """

    def __init__(
        self,
        backend_type: BackendType,
        guid: Any,
        secret: Optional[Any] = None,
        properties: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a SessionId.

        Args:
            backend_type: The type of backend (THRIFT or SEA)
            guid: The primary identifier for the session
            secret: The secret part of the identifier (only used for Thrift)
            properties: Additional information about the session
        """

        self.backend_type = backend_type
        self.guid = guid
        self.secret = secret
        self.properties = properties or {}

    def __str__(self) -> str:
        """
        Return a string representation of the SessionId.

        For SEA backend, returns the guid.
        For Thrift backend, returns a format like "guid|secret".

        Returns:
            A string representation of the session ID
        """

        if self.backend_type == BackendType.SEA:
            return str(self.guid)
        elif self.backend_type == BackendType.THRIFT:
            secret_hex = (
                guid_to_hex_id(self.secret)
                if isinstance(self.secret, bytes)
                else str(self.secret)
            )
            return f"{self.hex_guid}|{secret_hex}"
        return str(self.guid)

    @classmethod
    def from_thrift_handle(
        cls, session_handle, properties: Optional[Dict[str, Any]] = None
    ):
        """
        Create a SessionId from a Thrift session handle.

        Args:
            session_handle: A TSessionHandle object from the Thrift API

        Returns:
            A SessionId instance
        """

        if session_handle is None:
            return None

        guid_bytes = session_handle.sessionId.guid
        secret_bytes = session_handle.sessionId.secret

        if session_handle.serverProtocolVersion is not None:
            if properties is None:
                properties = {}
            properties["serverProtocolVersion"] = session_handle.serverProtocolVersion

        return cls(BackendType.THRIFT, guid_bytes, secret_bytes, properties)

    @classmethod
    def from_sea_session_id(
        cls, session_id: str, properties: Optional[Dict[str, Any]] = None
    ):
        """
        Create a SessionId from a SEA session ID.

        Args:
            session_id: The SEA session ID string

        Returns:
            A SessionId instance
        """

        return cls(BackendType.SEA, session_id, properties=properties)

    def to_thrift_handle(self):
        """
        Convert this SessionId to a Thrift TSessionHandle.

        Returns:
            A TSessionHandle object or None if this is not a Thrift session ID
        """

        if self.backend_type != BackendType.THRIFT:
            return None

        from databricks.sql.thrift_api.TCLIService import ttypes

        handle_identifier = ttypes.THandleIdentifier(guid=self.guid, secret=self.secret)
        server_protocol_version = self.properties.get("serverProtocolVersion")
        return ttypes.TSessionHandle(
            sessionId=handle_identifier, serverProtocolVersion=server_protocol_version
        )

    def to_sea_session_id(self):
        """
        Get the SEA session ID string.

        Returns:
            The session ID string or None if this is not a SEA session ID
        """

        if self.backend_type != BackendType.SEA:
            return None

        return self.guid

    @property
    def hex_guid(self) -> str:
        """
        Get a hexadecimal string representation of the session ID.

        Returns:
            A hexadecimal string representation
        """

        if isinstance(self.guid, bytes):
            return guid_to_hex_id(self.guid)
        else:
            return str(self.guid)

    @property
    def protocol_version(self):
        """
        Get the server protocol version for this session.

        Returns:
            The server protocol version or None if it does not exist
            It is not expected to exist for SEA sessions.
        """

        return self.properties.get("serverProtocolVersion")


class CommandId:
    """
    A normalized command identifier that works with both Thrift and SEA backends.

    This class abstracts away the differences between Thrift's TOperationHandle and
    SEA's statement ID string, providing a consistent interface for the connector.
    """

    def __init__(
        self,
        backend_type: BackendType,
        guid: Any,
        secret: Optional[Any] = None,
        operation_type: Optional[int] = None,
        has_result_set: bool = False,
        modified_row_count: Optional[int] = None,
    ):
        """
        Initialize a CommandId.

        Args:
            backend_type: The type of backend (THRIFT or SEA)
            guid: The primary identifier for the command
            secret: The secret part of the identifier (only used for Thrift)
            operation_type: The operation type (only used for Thrift)
            has_result_set: Whether the command has a result set
            modified_row_count: The number of rows modified by the command
        """

        self.backend_type = backend_type
        self.guid = guid
        self.secret = secret
        self.operation_type = operation_type
        self.has_result_set = has_result_set
        self.modified_row_count = modified_row_count

    def __str__(self) -> str:
        """
        Return a string representation of the CommandId.

        For SEA backend, returns the guid.
        For Thrift backend, returns a format like "guid|secret".

        Returns:
            A string representation of the command ID
        """

        if self.backend_type == BackendType.SEA:
            return str(self.guid)
        elif self.backend_type == BackendType.THRIFT:
            secret_hex = (
                guid_to_hex_id(self.secret)
                if isinstance(self.secret, bytes)
                else str(self.secret)
            )
            return f"{self.to_hex_guid()}|{secret_hex}"
        return str(self.guid)

    @classmethod
    def from_thrift_handle(cls, operation_handle):
        """
        Create a CommandId from a Thrift operation handle.

        Args:
            operation_handle: A TOperationHandle object from the Thrift API

        Returns:
            A CommandId instance
        """

        if operation_handle is None:
            return None

        guid_bytes = operation_handle.operationId.guid
        secret_bytes = operation_handle.operationId.secret

        return cls(
            BackendType.THRIFT,
            guid_bytes,
            secret_bytes,
            operation_handle.operationType,
            operation_handle.hasResultSet,
            operation_handle.modifiedRowCount,
        )

    @classmethod
    def from_sea_statement_id(cls, statement_id: str):
        """
        Create a CommandId from a SEA statement ID.

        Args:
            statement_id: The SEA statement ID string

        Returns:
            A CommandId instance
        """

        return cls(BackendType.SEA, statement_id)

    def to_thrift_handle(self):
        """
        Convert this CommandId to a Thrift TOperationHandle.

        Returns:
            A TOperationHandle object or None if this is not a Thrift command ID
        """

        if self.backend_type != BackendType.THRIFT:
            return None

        from databricks.sql.thrift_api.TCLIService import ttypes

        handle_identifier = ttypes.THandleIdentifier(guid=self.guid, secret=self.secret)
        return ttypes.TOperationHandle(
            operationId=handle_identifier,
            operationType=self.operation_type,
            hasResultSet=self.has_result_set,
            modifiedRowCount=self.modified_row_count,
        )

    def to_sea_statement_id(self):
        """
        Get the SEA statement ID string.

        Returns:
            The statement ID string or None if this is not a SEA statement ID
        """

        if self.backend_type != BackendType.SEA:
            return None

        return self.guid

    def to_hex_guid(self) -> str:
        """
        Get a hexadecimal string representation of the command ID.

        Returns:
            A hexadecimal string representation
        """

        if isinstance(self.guid, bytes):
            return guid_to_hex_id(self.guid)
        else:
            return str(self.guid)


@dataclass
class ExecuteResponse:
    """Response from executing a SQL command."""

    command_id: CommandId
    status: CommandState
    description: List[Tuple]
    has_been_closed_server_side: bool = False
    lz4_compressed: bool = True
    is_staging_operation: bool = False
    arrow_schema_bytes: Optional[bytes] = None
    result_format: Optional[Any] = None
