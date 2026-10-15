from unittest.mock import MagicMock, patch

import pytest

from databricks import sql
from databricks.sql.backend.types import BackendType, SessionId
from databricks.sql.common.feature_flag import FeatureFlagsContextFactory
from databricks.sql.telemetry.telemetry_client import TelemetryHelper


@pytest.mark.parametrize("use_sea", [False, True])
@pytest.mark.parametrize("oauth", [False, True])
def test_flags_available_with_telemetry_disabled(session_feature_flags, use_sea, oauth):
    session_feature_flags.get_instance.side_effect = (
        FeatureFlagsContextFactory.get_instance
    )
    http = MagicMock()
    http.request.return_value = MagicMock(
        status=200, data=b'{"flags":[{"name":"sampleLimit","value":"42"}]}'
    )
    auth = MagicMock()
    auth.add_headers.side_effect = lambda headers: headers.update(
        Authorization="Bearer test-token"
    )
    backend = MagicMock()
    backend.open_session.return_value = SessionId(BackendType.THRIFT, b"1", b"2")

    def create_backend(**kwargs):
        http.request.assert_not_called()
        session_feature_flags.get_instance.assert_not_called()
        assert kwargs["auth_provider"] is auth
        return backend

    backend_name = "SeaDatabricksClient" if use_sea else "ThriftDatabricksClient"
    try:
        with patch("databricks.sql.client.UnifiedHttpClient", return_value=http), patch(
            "databricks.sql.session.get_python_sql_connector_auth_provider",
            return_value=auth,
        ) as auth_factory, patch(
            f"databricks.sql.session.{backend_name}", side_effect=create_backend
        ):
            conn = sql.connect(
                "flags.example",
                "/sql/1.0/warehouses/test?o=123",
                enable_telemetry=False,
                use_sea=use_sea,
                **(
                    {"auth_type": "databricks-oauth"}
                    if oauth
                    else {"access_token": "test-token"}
                ),
            )
            try:
                assert conn.telemetry_enabled is False
                http.request.assert_not_called()
                session_feature_flags.get_instance.assert_not_called()
                assert conn.session.feature_flags.get_int32("sampleLimit") == 42
                assert (
                    http.request.call_args.kwargs["headers"]["Authorization"]
                    == "Bearer test-token"
                )
                assert (
                    http.request.call_args.kwargs["headers"]["x-databricks-org-id"]
                    == "123"
                )
                conn.enable_telemetry = True
                assert TelemetryHelper.is_telemetry_enabled(conn) is False
                http.request.assert_called_once()
                auth_factory.assert_called_once()
                session_feature_flags.get_instance.assert_called_once()
            finally:
                conn.close()
    finally:
        FeatureFlagsContextFactory.remove_instance(
            "flags.example", {"x-databricks-org-id": "123"}
        )


def test_failed_flag_fetch_does_not_block_session(session_feature_flags):
    session_feature_flags.get_instance.side_effect = (
        FeatureFlagsContextFactory.get_instance
    )
    http = MagicMock()
    http.request.side_effect = OSError("connector-service unavailable")
    try:
        with patch("databricks.sql.client.UnifiedHttpClient", return_value=http), patch(
            "databricks.sql.session.ThriftDatabricksClient"
        ) as backend:
            backend.return_value.open_session.return_value = SessionId(
                BackendType.THRIFT, b"1", b"2"
            )
            with sql.connect(
                "flags.example", "/test", access_token="test", enable_telemetry=False
            ) as conn:
                backend.return_value.open_session.assert_called_once()
                http.request.assert_not_called()
                assert conn.session.feature_flags.get_bool("missing") is False
                http.request.assert_called_once()
    finally:
        FeatureFlagsContextFactory.remove_instance("flags.example")
