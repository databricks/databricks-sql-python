"""
Base models for the SEA (Statement Execution API) backend.

These models define the common structures used in SEA API requests and responses.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field

from databricks.sql.backend.types import CommandState


@dataclass
class ServiceError:
    """Error information returned by the SEA API."""

    message: str
    error_code: Optional[str] = None


@dataclass
class StatementStatus:
    """Status information for a statement execution."""

    state: CommandState
    error: Optional[ServiceError] = None
    sql_state: Optional[str] = None


@dataclass
class ExternalLink:
    """External link information for result data."""

    external_link: str
    expiration: str
    chunk_index: int
    byte_count: int = 0
    row_count: int = 0
    row_offset: int = 0
    next_chunk_index: Optional[int] = None
    next_chunk_internal_link: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None


@dataclass
class ChunkInfo:
    """Information about a chunk in the result set."""

    chunk_index: int
    byte_count: int
    row_offset: int
    row_count: int


@dataclass
class ResultData:
    """Result data from a statement execution."""

    data: Optional[List[List[Any]]] = None
    external_links: Optional[List[ExternalLink]] = None
    byte_count: Optional[int] = None
    chunk_index: Optional[int] = None
    next_chunk_index: Optional[int] = None
    next_chunk_internal_link: Optional[str] = None
    row_count: Optional[int] = None
    row_offset: Optional[int] = None
    attachment: Optional[bytes] = None


@dataclass
class ResultManifest:
    """Manifest information for a result set."""

    format: str
    schema: Dict[str, Any]
    total_row_count: int
    total_byte_count: int
    total_chunk_count: int
    truncated: bool = False
    chunks: Optional[List[ChunkInfo]] = None
    result_compression: Optional[str] = None
    is_volume_operation: bool = False
