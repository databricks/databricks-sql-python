"""
Constants for the Statement Execution API (SEA) backend.
"""

from typing import Dict

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
