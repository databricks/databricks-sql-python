"""
Response models for the SEA (Statement Execution API) backend.

These models define the structures used in SEA API responses.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field

from databricks.sql.backend.models.base import (
    StatementStatus,
    ResultManifest,
    ResultData,
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
        status = StatementStatus(
            state=status_data.get("state", ""),
            error=None
            if "error" not in status_data
            else {
                "message": status_data["error"].get("message", ""),
                "error_code": status_data["error"].get("error_code"),
            },
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
        status = StatementStatus(
            state=status_data.get("state", ""),
            error=None
            if "error" not in status_data
            else {
                "message": status_data["error"].get("message", ""),
                "error_code": status_data["error"].get("error_code"),
            },
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
