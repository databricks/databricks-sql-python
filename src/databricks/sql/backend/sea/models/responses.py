"""
Response models for the SEA (Statement Execution API) backend.

These models define the structures used in SEA API responses.
"""

import base64
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from databricks.sql.backend.types import CommandState
from databricks.sql.backend.sea.models.base import (
    StatementStatus,
    ResultManifest,
    ResultData,
    ServiceError,
    ExternalLink,
    ChunkInfo,
)


def _parse_status(data: Dict[str, Any]) -> StatementStatus:
    """Parse status from response data."""
    status_data = data.get("status", {})
    error = None
    if "error" in status_data:
        error_data = status_data["error"]
        error = ServiceError(
            message=error_data.get("message", ""),
            error_code=error_data.get("error_code"),
        )

    state = CommandState.from_sea_state(status_data.get("state", ""))
    if state is None:
        raise ValueError(f"Invalid state: {status_data.get('state', '')}")

    return StatementStatus(
        state=state,
        error=error,
        sql_state=status_data.get("sql_state"),
    )


def _parse_manifest(data: Dict[str, Any]) -> ResultManifest:
    """Parse manifest from response data."""

    manifest_data = data.get("manifest", {})
    chunks = None
    if "chunks" in manifest_data:
        chunks = [
            ChunkInfo(
                chunk_index=chunk.get("chunk_index", 0),
                byte_count=chunk.get("byte_count", 0),
                row_offset=chunk.get("row_offset", 0),
                row_count=chunk.get("row_count", 0),
            )
            for chunk in manifest_data.get("chunks", [])
        ]

    return ResultManifest(
        format=manifest_data.get("format", ""),
        schema=manifest_data.get("schema", {}),
        total_row_count=manifest_data.get("total_row_count", 0),
        total_byte_count=manifest_data.get("total_byte_count", 0),
        total_chunk_count=manifest_data.get("total_chunk_count", 0),
        truncated=manifest_data.get("truncated", False),
        chunks=chunks,
        result_compression=manifest_data.get("result_compression"),
        is_volume_operation=manifest_data.get("is_volume_operation", False),
    )


def _parse_result(data: Dict[str, Any]) -> ResultData:
    """Parse result data from response data."""
    result_data = data.get("result", {})
    external_links = None

    if "external_links" in result_data:
        external_links = []
        for link_data in result_data["external_links"]:
            external_links.append(
                ExternalLink(
                    external_link=link_data.get("external_link", ""),
                    expiration=link_data.get("expiration", ""),
                    chunk_index=link_data.get("chunk_index", 0),
                    byte_count=link_data.get("byte_count", 0),
                    row_count=link_data.get("row_count", 0),
                    row_offset=link_data.get("row_offset", 0),
                    next_chunk_index=link_data.get("next_chunk_index"),
                    next_chunk_internal_link=link_data.get("next_chunk_internal_link"),
                    http_headers=link_data.get("http_headers"),
                )
            )

    # Handle attachment field - decode from base64 if present
    attachment = result_data.get("attachment")
    if attachment is not None:
        attachment = base64.b64decode(attachment)

    return ResultData(
        data=result_data.get("data_array"),
        external_links=external_links,
        byte_count=result_data.get("byte_count"),
        chunk_index=result_data.get("chunk_index"),
        next_chunk_index=result_data.get("next_chunk_index"),
        next_chunk_internal_link=result_data.get("next_chunk_internal_link"),
        row_count=result_data.get("row_count"),
        row_offset=result_data.get("row_offset"),
        attachment=attachment,
    )


@dataclass
class ExecuteStatementResponse:
    """Representation of the response from executing a SQL statement."""

    statement_id: str
    status: StatementStatus
    manifest: ResultManifest
    result: ResultData

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecuteStatementResponse":
        """Create an ExecuteStatementResponse from a dictionary."""
        return cls(
            statement_id=data.get("statement_id", ""),
            status=_parse_status(data),
            manifest=_parse_manifest(data),
            result=_parse_result(data),
        )


@dataclass
class GetStatementResponse:
    """Representation of the response from getting information about a statement."""

    statement_id: str
    status: StatementStatus
    manifest: ResultManifest
    result: ResultData

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetStatementResponse":
        """Create a GetStatementResponse from a dictionary."""
        return cls(
            statement_id=data.get("statement_id", ""),
            status=_parse_status(data),
            manifest=_parse_manifest(data),
            result=_parse_result(data),
        )


@dataclass
class CreateSessionResponse:
    """Representation of the response from creating a new session."""

    session_id: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CreateSessionResponse":
        """Create a CreateSessionResponse from a dictionary."""
        return cls(session_id=data.get("session_id", ""))


@dataclass
class GetChunksResponse:
    """
    Response from getting chunks for a statement.

    The response model can be found in the docs, here:
    https://docs.databricks.com/api/workspace/statementexecution/getstatementresultchunkn
    """

    data: Optional[List[List[Any]]] = None
    external_links: Optional[List[ExternalLink]] = None
    byte_count: Optional[int] = None
    chunk_index: Optional[int] = None
    next_chunk_index: Optional[int] = None
    next_chunk_internal_link: Optional[str] = None
    row_count: Optional[int] = None
    row_offset: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetChunksResponse":
        """Create a GetChunksResponse from a dictionary."""
        result = _parse_result({"result": data})
        return cls(
            data=result.data,
            external_links=result.external_links,
            byte_count=result.byte_count,
            chunk_index=result.chunk_index,
            next_chunk_index=result.next_chunk_index,
            next_chunk_internal_link=result.next_chunk_internal_link,
            row_count=result.row_count,
            row_offset=result.row_offset,
        )
