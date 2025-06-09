"""
Response models for the SEA (Statement Execution API) backend.

These models define the structures used in SEA API responses.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field

from databricks.sql.backend.types import CommandState
from databricks.sql.backend.sea.models.base import (
    StatementStatus,
    ResultManifest,
    ResultData,
    ServiceError,
)


@dataclass
class ExecuteStatementResponse:
    """Response from executing a SQL statement."""

    statement_id: str
    status: StatementStatus
    manifest: Optional[ResultManifest] = None
    result: Optional[ResultData] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecuteStatementResponse":
        """Create an ExecuteStatementResponse from a dictionary."""
        status_data = data.get("status", {})
        error = None
        if "error" in status_data:
            error_data = status_data["error"]
            error = ServiceError(
                message=error_data.get("message", ""),
                error_code=error_data.get("error_code"),
            )

        status = StatementStatus(
            state=CommandState.from_sea_state(status_data.get("state", "")),
            error=error,
            sql_state=status_data.get("sql_state"),
        )

        return cls(
            statement_id=data.get("statement_id", ""),
            status=status,
            manifest=data.get("manifest"),  # We'll parse this more fully if needed
            result=data.get("result"),  # We'll parse this more fully if needed
        )


@dataclass
class GetStatementResponse:
    """Response from getting information about a statement."""

    statement_id: str
    status: StatementStatus
    manifest: Optional[ResultManifest] = None
    result: Optional[ResultData] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetStatementResponse":
        """Create a GetStatementResponse from a dictionary."""
        status_data = data.get("status", {})
        error = None
        if "error" in status_data:
            error_data = status_data["error"]
            error = ServiceError(
                message=error_data.get("message", ""),
                error_code=error_data.get("error_code"),
            )

        status = StatementStatus(
            state=CommandState.from_sea_state(status_data.get("state", "")),
            error=error,
            sql_state=status_data.get("sql_state"),
        )

        return cls(
            statement_id=data.get("statement_id", ""),
            status=status,
            manifest=data.get("manifest"),  # We'll parse this more fully if needed
            result=data.get("result"),  # We'll parse this more fully if needed
        )


@dataclass
class CreateSessionResponse:
    """Response from creating a new session."""

    session_id: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CreateSessionResponse":
        """Create a CreateSessionResponse from a dictionary."""
        return cls(session_id=data.get("session_id", ""))
