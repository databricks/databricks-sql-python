from unittest.mock import patch

import pytest

from databricks.sql.common import ssl_context_cache


@pytest.fixture(autouse=True)
def clear_ssl_context_cache():
    # Tests patch the stdlib/urllib3 context builders; never serve another test's context.
    ssl_context_cache.clear()
    yield
    ssl_context_cache.clear()


@pytest.fixture(autouse=True)
def session_feature_flags():
    # Unit connections must not contact connector-service. Cache/lifecycle tests
    # exercise the real reader with a mocked HTTP client.
    with patch("databricks.sql.session.FeatureFlagsContextFactory") as factory:
        factory.get_instance.return_value.get_bool.return_value = False
        yield factory
