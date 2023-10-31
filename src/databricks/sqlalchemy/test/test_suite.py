"""
The order of these imports is important. Test cases are imported first from SQLAlchemy,
then are overridden by our local skip markers in _regression, _unsupported, and _future.
"""


def start_protocol_patch():
    """See tests/test_parameterized_queries.py for more information about this patch."""
    from unittest.mock import patch

    native_support_patcher = patch(
        "databricks.sql.client.Connection.server_parameterized_queries_enabled",
        return_value=True,
    )
    native_support_patcher.start()


start_protocol_patch()

# type: ignore
# fmt: off
from sqlalchemy.testing.suite import *
from databricks.sqlalchemy.test._regression import *
from databricks.sqlalchemy.test._unsupported import *
from databricks.sqlalchemy.test._future import *
from databricks.sqlalchemy.test._extra import TinyIntegerTest, DateTimeTZTestCustom
