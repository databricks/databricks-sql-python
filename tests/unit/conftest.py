from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def session_feature_flags():
    # Unit connections must not contact connector-service. Cache/lifecycle tests
    # exercise the real reader with a mocked HTTP client.
    with patch("databricks.sql.session.FeatureFlagsContextFactory") as factory:
        factory.get_instance.return_value.get_bool.return_value = False
        yield factory
