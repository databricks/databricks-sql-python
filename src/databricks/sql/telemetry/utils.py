import json
from enum import Enum
from dataclasses import asdict


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


def to_json_compact(dataclass_obj):
    """
    Convert a dataclass to JSON string, excluding None values.
    """
    return json.dumps(
        asdict(
            dataclass_obj,
            dict_factory=lambda data: {k: v for k, v in data if v is not None},
        ),
        cls=EnumEncoder,
    )
