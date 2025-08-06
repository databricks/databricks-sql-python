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

    HYBRID = "INLINE_OR_EXTERNAL_LINKS"
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


class MetadataCommands(Enum):
    """SQL commands used in the SEA backend.

    These constants are used for metadata operations and other SQL queries
    to ensure consistency and avoid string literal duplication.
    """

    SHOW_CATALOGS = "SHOW CATALOGS"
    SHOW_SCHEMAS = "SHOW SCHEMAS IN {}"
    SHOW_TABLES = "SHOW TABLES IN {}"
    SHOW_TABLES_ALL_CATALOGS = "SHOW TABLES IN ALL CATALOGS"
    SHOW_COLUMNS = "SHOW COLUMNS IN CATALOG {}"

    LIKE_PATTERN = " LIKE '{}'"
    SCHEMA_LIKE_PATTERN = " SCHEMA" + LIKE_PATTERN
    TABLE_LIKE_PATTERN = " TABLE" + LIKE_PATTERN

    CATALOG_SPECIFIC = "CATALOG {}"
