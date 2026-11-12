import json
from enum import Enum
from dataclasses import fields, is_dataclass
from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)

_LEAF_TYPES = frozenset((str, int, float, bool))
_FIELD_NAMES: Dict[type, Tuple[str, ...]] = {}


def _to_json_dict(obj: Any) -> Any:
    """``dataclasses.asdict`` minus None fields, without asdict's deep copies.

    Telemetry serializes one event per statement, and asdict was most of that cost.
    """
    cls = type(obj)
    names = _FIELD_NAMES.get(cls)
    if names is None:
        if not is_dataclass(cls):
            if isinstance(obj, Enum):
                return obj.value
            if isinstance(obj, (list, tuple)):
                return [_to_json_dict(v) for v in obj]
            if isinstance(obj, dict):
                return {k: _to_json_dict(v) for k, v in obj.items()}
            return obj
        names = _FIELD_NAMES[cls] = tuple(f.name for f in fields(cls))
    result: Dict[str, Any] = {}
    for name in names:
        value = getattr(obj, name)
        if value is not None:
            result[name] = value if type(value) in _LEAF_TYPES else _to_json_dict(value)
    return result


class BaseTelemetryClient(ABC):
    """
    Base class for telemetry clients.
    It is used to define the interface for telemetry clients.
    """

    @abstractmethod
    def export_initial_telemetry_log(self, driver_connection_params, user_agent):
        logger.debug("subclass must implement export_initial_telemetry_log")
        pass

    @abstractmethod
    def export_failure_log(self, error_name, error_message):
        logger.debug("subclass must implement export_failure_log")
        pass

    @abstractmethod
    def export_latency_log(self, latency_ms, sql_execution_event, sql_statement_id):
        logger.debug("subclass must implement export_latency_log")
        pass

    @abstractmethod
    def close(self):
        logger.debug("subclass must implement close")
        pass


class JsonSerializableMixin:
    """Mixin class to provide JSON serialization capabilities to dataclasses."""

    def to_json(self) -> str:
        """
        Convert the object to a JSON string, excluding None values.
        Handles Enum serialization and filters out None values from the output.
        """
        if not is_dataclass(self):
            raise TypeError(
                f"{self.__class__.__name__} must be a dataclass to use JsonSerializableMixin"
            )

        return _ENUM_ENCODER.encode(_to_json_dict(self))


class EnumEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle Enum values.
    This is used to convert Enum values to their string representations.
    Default JSON encoder raises a TypeError for Enums.
    """

    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


# Same as json.dumps(..., cls=EnumEncoder) without building an encoder per call.
_ENUM_ENCODER = EnumEncoder()
