"""
Request models for the SEA (Statement Execution API) backend.

These models define the structures used in SEA API requests.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field


@dataclass
class StatementParameter:
    """Representation of a parameter for a SQL statement."""

    name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass
class ExecuteStatementRequest:
    """Representation of a request to execute a SQL statement."""

    session_id: str
    statement: str
    warehouse_id: str
    disposition: str = "EXTERNAL_LINKS"
    format: str = "JSON_ARRAY"
    result_compression: Optional[str] = None
    parameters: Optional[List[StatementParameter]] = None
    wait_timeout: str = "10s"
    on_wait_timeout: str = "CONTINUE"
    row_limit: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        result: Dict[str, Any] = {
            "warehouse_id": self.warehouse_id,
            "session_id": self.session_id,
            "statement": self.statement,
            "disposition": self.disposition,
            "format": self.format,
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": self.on_wait_timeout,
        }

        if self.row_limit is not None and self.row_limit > 0:
            result["row_limit"] = self.row_limit

        if self.result_compression:
            result["result_compression"] = self.result_compression

        if self.parameters:
            result["parameters"] = [
                {
                    "name": param.name,
                    "value": param.value,
                    "type": param.type,
                }
                for param in self.parameters
            ]

        return result


@dataclass
class GetStatementRequest:
    """Representation of a request to get information about a statement."""

    statement_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {"statement_id": self.statement_id}


@dataclass
class CancelStatementRequest:
    """Representation of a request to cancel a statement."""

    statement_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {"statement_id": self.statement_id}


@dataclass
class CloseStatementRequest:
    """Representation of a request to close a statement."""

    statement_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {"statement_id": self.statement_id}


@dataclass
class CreateSessionRequest:
    """Representation of a request to create a new session."""

    warehouse_id: str
    session_confs: Optional[Dict[str, str]] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        result: Dict[str, Any] = {"warehouse_id": self.warehouse_id}

        if self.session_confs:
            result["session_confs"] = self.session_confs

        if self.catalog:
            result["catalog"] = self.catalog

        if self.schema:
            result["schema"] = self.schema

        return result


@dataclass
class DeleteSessionRequest:
    """Representation of a request to delete a session."""

    warehouse_id: str
    session_id: str

    def to_dict(self) -> Dict[str, str]:
        """Convert the request to a dictionary for JSON serialization."""
        return {"warehouse_id": self.warehouse_id, "session_id": self.session_id}
