"""
Request models for the SEA (Statement Execution API) backend.

These models define the structures used in SEA API requests.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field


@dataclass
class StatementParameter:
    """Parameter for a SQL statement."""
    name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass
class ExecuteStatementRequest:
    """Request to execute a SQL statement."""
    warehouse_id: str
    statement: str
    session_id: str
    disposition: str = "EXTERNAL_LINKS"
    format: str = "JSON_ARRAY"
    wait_timeout: str = "10s"
    on_wait_timeout: str = "CONTINUE"
    row_limit: Optional[int] = None
    byte_limit: Optional[int] = None
    parameters: Optional[List[StatementParameter]] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        result = {
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
            
        if self.byte_limit is not None and self.byte_limit > 0:
            result["byte_limit"] = self.byte_limit
            
        if self.catalog:
            result["catalog"] = self.catalog
            
        if self.schema:
            result["schema"] = self.schema
            
        if self.parameters:
            result["parameters"] = [
                {
                    "name": param.name,
                    **({"value": param.value} if param.value is not None else {}),
                    **({"type": param.type} if param.type is not None else {})
                }
                for param in self.parameters
            ]
            
        return result


@dataclass
class GetStatementRequest:
    """Request to get information about a statement."""
    warehouse_id: str
    statement_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {
            "warehouse_id": self.warehouse_id,
            "statement_id": self.statement_id
        }


@dataclass
class CancelStatementRequest:
    """Request to cancel a statement."""
    warehouse_id: str
    statement_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {
            "warehouse_id": self.warehouse_id,
            "statement_id": self.statement_id
        }


@dataclass
class CloseStatementRequest:
    """Request to close a statement."""
    warehouse_id: str
    statement_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {
            "warehouse_id": self.warehouse_id,
            "statement_id": self.statement_id
        }


@dataclass
class CreateSessionRequest:
    """Request to create a new session."""
    warehouse_id: str
    session_confs: Optional[Dict[str, str]] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        result = {"warehouse_id": self.warehouse_id}
        
        if self.session_confs:
            result["session_confs"] = self.session_confs
            
        if self.catalog:
            result["catalog"] = self.catalog
            
        if self.schema:
            result["schema"] = self.schema
            
        return result


@dataclass
class DeleteSessionRequest:
    """Request to delete a session."""
    warehouse_id: str
    session_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert the request to a dictionary for JSON serialization."""
        return {
            "warehouse_id": self.warehouse_id,
            "session_id": self.session_id
        }