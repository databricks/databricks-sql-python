"""
Constants for the Statement Execution API (SEA) backend.
"""

from typing import Dict
from enum import Enum

# from https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters
ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP: Dict[str, str] = {
    "ANSI_MODE": "true",
    "ENABLE_PHOTON": "true",
    "LEGACY_TIME_PARSER_POLICY": "Exception",
    "MAX_FILE_PARTITION_BYTES": "128m",
    "READ_ONLY_EXTERNAL_METASTORE": "false",
    "STATEMENT_TIMEOUT": "0",
    "TIMEZONE": "UTC",
    "USE_CACHED_RESULT": "true",
}


class ResultFormat(Enum):
    """Enum for result format values."""

    ARROW_STREAM = "ARROW_STREAM"
    JSON_ARRAY = "JSON_ARRAY"


class ResultDisposition(Enum):
    """Enum for result disposition values."""

    # TODO: add support for hybrid disposition
    EXTERNAL_LINKS = "EXTERNAL_LINKS"
    INLINE = "INLINE"


class ResultCompression(Enum):
    """Enum for result compression values."""

    LZ4_FRAME = "LZ4_FRAME"
    NONE = None


class WaitTimeout(Enum):
    """Enum for wait timeout values."""

    ASYNC = "0s"
    SYNC = "10s"
