import json
from enum import Enum


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
