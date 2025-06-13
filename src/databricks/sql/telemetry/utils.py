import json
from enum import Enum
from dataclasses import asdict, is_dataclass
from abc import ABC


class JsonSerializableMixin(ABC):
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

        return json.dumps(
            asdict(
                self,
                dict_factory=lambda data: {k: v for k, v in data if v is not None},
            ),
            cls=EnumEncoder,
        )


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
