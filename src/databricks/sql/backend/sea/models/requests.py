from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class CreateSessionRequest:
    """Request to create a new session."""

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
    """Request to delete a session."""

    warehouse_id: str
    session_id: str

    def to_dict(self) -> Dict[str, str]:
        """Convert the request to a dictionary for JSON serialization."""
        return {"warehouse_id": self.warehouse_id, "session_id": self.session_id}
