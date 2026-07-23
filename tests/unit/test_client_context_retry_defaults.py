"""
Unit tests for ClientContext / build_client_context retry-policy defaults.

Regression tests for issue #709: CloudFetch downloads go through
UnifiedHttpClient, which builds its DatabricksRetryPolicy from ClientContext.
When the user does not override the retry settings, those defaults must match
the connector-wide defaults used by the Thrift and SEA backends (30 attempts /
900.0s), which are anchored to the ODBC/JDBC drivers (see
thrift_backend.py: "900s attempts-duration lines up w ODBC/JDBC drivers").
Previously ClientContext fell back to 5 attempts / 300.0s, cutting cloudfetch
retries off far earlier than the rest of the connector.
"""

from databricks.sql.auth.common import ClientContext
from databricks.sql.utils import build_client_context


class TestClientContextRetryDefaults:
    def test_default_max_retry_duration_is_900(self):
        """With no override, retry duration defaults to 900.0s (not 300.0)."""
        context = build_client_context("test.databricks.com", "test-version")
        assert context.retry_stop_after_attempts_duration == 900.0

    def test_default_max_retry_count_is_30(self):
        """With no override, retry attempt count defaults to 30 (not 5)."""
        context = build_client_context("test.databricks.com", "test-version")
        assert context.retry_stop_after_attempts_count == 30

    def test_user_override_still_honored(self):
        """Explicit user overrides are propagated unchanged."""
        context = build_client_context(
            "test.databricks.com",
            "test-version",
            _retry_stop_after_attempts_duration=120.0,
            _retry_stop_after_attempts_count=7,
        )
        assert context.retry_stop_after_attempts_duration == 120.0
        assert context.retry_stop_after_attempts_count == 7
