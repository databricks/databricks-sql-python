"""Data models for the Statement Execution API."""

from dataclasses import dataclass
from typing import Dict, Optional
from enum import Enum


class Format(str, Enum):
    """Format of the result data."""
    JSON_ARRAY = "JSON_ARRAY"
    ARROW_STREAM = "ARROW_STREAM"


class Disposition(str, Enum):
    """Disposition of the result data."""
    INLINE = "INLINE"
    EXTERNAL_LINKS = "EXTERNAL_LINKS"
    INLINE_OR_EXTERNAL_LINKS = "INLINE_OR_EXTERNAL_LINKS"


class CompressionCodec(str, Enum):
    """Compression codec for the result data."""
    NONE = "NONE"
    SNAPPY = "SNAPPY"
    GZIP = "GZIP"
    LZ4 = "LZ4"


class StatementState(str, Enum):
    """State of a statement execution."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CLOSED = "CLOSED"
    CANCELED = "CANCELED"


@dataclass
class ServiceError:
    """Error information from the service."""
    message: str
    error_code: Optional[str] = None


@dataclass
class StatementStatus:
    """Status of a statement execution."""
    state: StatementState
    error: Optional[ServiceError] = None
    sql_state: Optional[str] = None


@dataclass
class CreateSessionRequest:
    """Request to create a session."""
    warehouse_id: str
    catalog: Optional[str] = None
    schema: Optional[str] = None
    session_configs: Optional[Dict[str, str]] = None


@dataclass
class CreateSessionResponse:
    """Response from creating a session."""
    session_id: str


@dataclass
class DeleteSessionRequest:
    """Request to delete a session."""
    session_id: str
    warehouse_id: str