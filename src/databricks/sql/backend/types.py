from enum import Enum
from typing import Dict, Optional, Any, Union
import uuid
import logging

logger = logging.getLogger(__name__)


def guid_to_hex_id(guid: bytes) -> str:
    """Return a hexadecimal string instead of bytes

    Example:
        IN   b'\x01\xee\x1d)\xa4\x19\x1d\xb6\xa9\xc0\x8d\xf1\xfe\xbaB\xdd'
        OUT  '01ee1d29-a419-1db6-a9c0-8df1feba42dd'

    If conversion to hexadecimal fails, the original bytes are returned
    """
    try:
        this_uuid = uuid.UUID(bytes=guid)
    except Exception as e:
        logger.debug(f"Unable to convert bytes to UUID: {guid!r} -- {str(e)}")
        return str(guid)
    return str(this_uuid)


class BackendType(Enum):
    """Enum representing the type of backend."""

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
        info: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a SessionId.

        Args:
            backend_type: The type of backend (THRIFT or SEA)
            guid: The primary identifier for the session
            secret: The secret part of the identifier (only used for Thrift)
            info: Additional information about the session
        """
        self.backend_type = backend_type
        self.guid = guid
        self.secret = secret
        self.info = info or {}

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
            return f"{self.to_hex_id()}|{guid_to_hex_id(self.secret) if isinstance(self.secret, bytes) else str(self.secret)}"
        return str(self.guid)

    @classmethod
    def from_thrift_handle(cls, session_handle, info: Optional[Dict[str, Any]] = None):
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
            if info is None:
                info = {}
            info["serverProtocolVersion"] = session_handle.serverProtocolVersion

        return cls(BackendType.THRIFT, guid_bytes, secret_bytes, info)

    @classmethod
    def from_sea_session_id(
        cls, session_id: str, info: Optional[Dict[str, Any]] = None
    ):
        """
        Create a SessionId from a SEA session ID.

        Args:
            session_id: The SEA session ID string

        Returns:
            A SessionId instance
        """
        return cls(BackendType.SEA, session_id, info=info)

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
        server_protocol_version = self.info.get("serverProtocolVersion")
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

    def get_id(self) -> Any:
        """
        Get the ID of the session.
        """
        return self.guid

    def get_hex_id(self) -> str:
        """
        Get a hexadecimal string representation of the session ID.

        Returns:
            A hexadecimal string representation
        """
        if isinstance(self.guid, bytes):
            return guid_to_hex_id(self.guid)
        else:
            return str(self.guid)

    def get_protocol_version(self):
        """
        Get the server protocol version for this session.

        Returns:
            The server protocol version or None if this is not a Thrift session ID
        """
        return self.info.get("serverProtocolVersion")


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
            return f"{self.to_hex_id()}|{guid_to_hex_id(self.secret) if isinstance(self.secret, bytes) else str(self.secret)}"
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

    def to_hex_id(self) -> str:
        """
        Get a hexadecimal string representation of the command ID.

        Returns:
            A hexadecimal string representation
        """
        if isinstance(self.guid, bytes):
            return guid_to_hex_id(self.guid)
        else:
            return str(self.guid)
