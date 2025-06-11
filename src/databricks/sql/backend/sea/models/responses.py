from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class CreateSessionResponse:
    """Representation of the response from creating a new session."""

    session_id: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CreateSessionResponse":
        """Create a CreateSessionResponse from a dictionary."""
        return cls(session_id=data.get("session_id", ""))
