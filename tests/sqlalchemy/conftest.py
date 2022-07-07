from sqlalchemy.dialects import registry
import pytest

registry.register("databricks.thrift", "databricks.sqlalchemy", "DatabricksDialect")
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *