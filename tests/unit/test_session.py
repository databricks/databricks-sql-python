import pytest
from unittest.mock import patch, MagicMock, Mock, PropertyMock
from contextlib import contextmanager
import gc
import importlib.metadata as ilm
import jwt
import socket
import sys
import threading
import types

from databricks.sql.thrift_api.TCLIService.ttypes import (
    TOpenSessionResp,
    TSessionHandle,
    THandleIdentifier,
)
from databricks.sql.backend.types import SessionId, BackendType
from databricks.sql.common.agent import KNOWN_AGENTS
from databricks.sql.session import _kernel_endpoint, _check_oauth_origin

import databricks.sql
from databricks.sql.exc import (
    ReydenThriftUnsupportedError,
    DatabaseError,
    OperationalError,
)
from databricks.sql.session import Session


class TestSession:
    """
    Unit tests for Session functionality
    """

    PACKAGE_NAME = "databricks.sql"
    DUMMY_CONNECTION_ARGS = {
        "server_hostname": "foo",
        "http_path": "dummy_path",
        "access_token": "tok",
        "enable_telemetry": False,
    }

    @pytest.mark.parametrize(
        "backend_kwargs, backend_name",
        [
            ({}, "ThriftDatabricksClient"),
            ({"use_kernel": False}, "ThriftDatabricksClient"),
            ({"use_sea": True}, "SeaDatabricksClient"),
        ],
    )
    @pytest.mark.parametrize("proxy_uri", [None, "http://proxy.example.com:8081"])
    def test_python_backends_keep_http_client(
        self, backend_kwargs, backend_name, proxy_uri
    ):
        with patch(f"{self.PACKAGE_NAME}.session.{backend_name}") as backend, patch(
            f"{self.PACKAGE_NAME}.client.UnifiedHttpClient"
        ) as http_client, patch(
            f"{self.PACKAGE_NAME}.client.TelemetryClientFactory.get_telemetry_client"
        ) as telemetry:
            http_client.return_value.using_proxy.return_value = proxy_uri is not None
            http_client.return_value.proxy_uri = proxy_uri
            conn = databricks.sql.connect(
                **self.DUMMY_CONNECTION_ARGS, **backend_kwargs
            )
            try:
                http_client.assert_called_once()
                assert conn.http_client is http_client.return_value
                assert conn.session.http_client is conn.http_client
                assert backend.call_args.kwargs["http_client"] is conn.http_client
                params = telemetry.return_value.export_initial_telemetry_log.call_args.kwargs[
                    "driver_connection_params"
                ]
                assert params.use_proxy is (proxy_uri is not None)
                assert params.use_system_proxy is (proxy_uri is not None)
                if proxy_uri:
                    assert params.proxy_host_info.host_url == "proxy.example.com"
                    assert params.proxy_host_info.port == 8081
                else:
                    assert params.proxy_host_info is None
            finally:
                conn.close()
            backend.return_value.close_session.assert_called_once()
            http_client.return_value.close.assert_called_once()

    def test_conflicting_kernel_and_sea_flags_preserve_error(self):
        with patch(
            f"{self.PACKAGE_NAME}.client.UnifiedHttpClient",
            side_effect=AssertionError(
                "Explicit kernel must not create a Python client"
            ),
        ) as http_client, pytest.raises(ValueError, match="mutually exclusive"):
            databricks.sql.connect(
                **self.DUMMY_CONNECTION_ARGS, use_kernel=True, use_sea=True
            )
        http_client.assert_not_called()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_close_uses_the_correct_session_id(self, mock_client_class):
        instance = mock_client_class.return_value

        # Create a mock SessionId that will be returned by open_session
        mock_session_id = SessionId(BackendType.THRIFT, b"\x22", b"\x33")
        instance.open_session.return_value = mock_session_id

        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        connection.close()

        # Check that close_session was called with the correct SessionId
        close_session_call_args = instance.close_session.call_args[0][0]
        assert close_session_call_args.guid == b"\x22"
        assert close_session_call_args.secret == b"\x33"

    @patch("%s.client.UnifiedHttpClient" % PACKAGE_NAME)
    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_auth_args(self, mock_client_class, _mock_http_client):
        # Test that the following auth args work:
        # token = foo,
        # token = None, _tls_client_cert_file = something, _use_cert_as_auth = True
        connection_args = [
            {
                "server_hostname": "foo",
                "http_path": None,
                "access_token": "tok",
                "enable_telemetry": False,
            },
            {
                "server_hostname": "foo",
                "http_path": None,
                "_tls_client_cert_file": "something",
                "_use_cert_as_auth": True,
                "access_token": None,
                "enable_telemetry": False,
            },
        ]

        for args in connection_args:
            connection = databricks.sql.connect(**args)
            call_kwargs = mock_client_class.call_args[1]
            assert args["server_hostname"] == call_kwargs["server_hostname"]
            assert args["http_path"] == call_kwargs["http_path"]
            connection.close()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_http_header_passthrough(self, mock_client_class):
        http_headers = [("foo", "bar")]
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS, http_headers=http_headers)

        call_kwargs = mock_client_class.call_args[1]
        assert ("foo", "bar") in call_kwargs["http_headers"]

    @patch("%s.client.UnifiedHttpClient" % PACKAGE_NAME)
    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_tls_arg_passthrough(self, mock_client_class, mock_http_client):
        databricks.sql.connect(
            **self.DUMMY_CONNECTION_ARGS,
            _tls_verify_hostname="hostname",
            _tls_trusted_ca_file="trusted ca file",
            _tls_client_cert_file="trusted client cert",
            _tls_client_cert_key_file="trusted client cert",
            _tls_client_cert_key_password="key password",
        )

        kwargs = mock_client_class.call_args[1]
        assert kwargs["_tls_verify_hostname"] == "hostname"
        assert kwargs["_tls_trusted_ca_file"] == "trusted ca file"
        assert kwargs["_tls_client_cert_file"] == "trusted client cert"
        assert kwargs["_tls_client_cert_key_file"] == "trusted client cert"
        assert kwargs["_tls_client_cert_key_password"] == "key password"

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_useragent_header(self, mock_client_class, monkeypatch):
        for env_var, _ in KNOWN_AGENTS:
            monkeypatch.delenv(env_var, raising=False)
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)

        call_kwargs = mock_client_class.call_args[1]
        http_headers = call_kwargs["http_headers"]
        user_agent_header = (
            "User-Agent",
            "{}/{}".format(databricks.sql.USER_AGENT_NAME, databricks.sql.__version__),
        )
        assert user_agent_header in http_headers

        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS, user_agent_entry="foobar")
        user_agent_header_with_entry = (
            "User-Agent",
            "{}/{} ({})".format(
                databricks.sql.USER_AGENT_NAME, databricks.sql.__version__, "foobar"
            ),
        )
        call_kwargs = mock_client_class.call_args[1]
        http_headers = call_kwargs["http_headers"]
        assert user_agent_header_with_entry in http_headers

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_context_manager_closes_connection(self, mock_client_class):
        instance = mock_client_class.return_value

        # Create a mock SessionId that will be returned by open_session
        mock_session_id = SessionId(BackendType.THRIFT, b"\x22", b"\x33")
        instance.open_session.return_value = mock_session_id

        with databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS) as connection:
            pass

        # Check that close_session was called with the correct SessionId
        close_session_call_args = instance.close_session.call_args[0][0]
        assert close_session_call_args.guid == b"\x22"
        assert close_session_call_args.secret == b"\x33"

        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        connection.close = Mock()
        try:
            with pytest.raises(KeyboardInterrupt):
                with connection:
                    raise KeyboardInterrupt("Simulated interrupt")
        finally:
            connection.close.assert_called()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_max_number_of_retries_passthrough(self, mock_client_class):
        databricks.sql.connect(
            _retry_stop_after_attempts_count=54, **self.DUMMY_CONNECTION_ARGS
        )

        assert mock_client_class.call_args[1]["_retry_stop_after_attempts_count"] == 54

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_socket_timeout_passthrough(self, mock_client_class):
        databricks.sql.connect(_socket_timeout=234, **self.DUMMY_CONNECTION_ARGS)
        assert mock_client_class.call_args[1]["_socket_timeout"] == 234

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_configuration_passthrough(self, mock_client_class):
        mock_session_config = {
            "ANSI_MODE": "FALSE",
            "QUERY_TAGS": "team:engineering,project:data-pipeline",
        }
        databricks.sql.connect(
            session_configuration=mock_session_config, **self.DUMMY_CONNECTION_ARGS
        )

        call_kwargs = mock_client_class.return_value.open_session.call_args[1]
        assert call_kwargs["session_configuration"] == mock_session_config

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_enable_metric_view_metadata_parameter(self, mock_client_class):
        """Test that enable_metric_view_metadata parameter sets the correct session configuration."""
        databricks.sql.connect(
            enable_metric_view_metadata=True, **self.DUMMY_CONNECTION_ARGS
        )

        call_kwargs = mock_client_class.return_value.open_session.call_args[1]
        expected_config = {"spark.sql.thriftserver.metadata.metricview.enabled": "true"}
        assert call_kwargs["session_configuration"] == expected_config

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_initial_namespace_passthrough(self, mock_client_class):
        mock_cat = Mock()
        mock_schem = Mock()
        databricks.sql.connect(
            **self.DUMMY_CONNECTION_ARGS, catalog=mock_cat, schema=mock_schem
        )

        call_kwargs = mock_client_class.return_value.open_session.call_args[1]
        assert call_kwargs["catalog"] == mock_cat
        assert call_kwargs["schema"] == mock_schem

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_finalizer_closes_abandoned_connection(self, mock_client_class):
        instance = mock_client_class.return_value

        mock_session_id = SessionId(BackendType.THRIFT, b"\x22", b"\x33")
        instance.open_session.return_value = mock_session_id

        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)

        # not strictly necessary as the refcount is 0, but just to be sure
        gc.collect()

        # Check that close_session was called with the correct SessionId
        close_session_call_args = instance.close_session.call_args[0][0]
        assert close_session_call_args.guid == b"\x22"
        assert close_session_call_args.secret == b"\x33"

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_query_tags_dict_sets_session_config(self, mock_client_class):
        databricks.sql.connect(
            query_tags={"team": "data-eng", "project": "etl"},
            **self.DUMMY_CONNECTION_ARGS,
        )

        call_kwargs = mock_client_class.return_value.open_session.call_args[1]
        assert (
            call_kwargs["session_configuration"]["QUERY_TAGS"]
            == "team:data-eng,project:etl"
        )

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_query_tags_dict_takes_precedence_over_session_config(
        self, mock_client_class
    ):
        databricks.sql.connect(
            query_tags={"team": "new-team"},
            session_configuration={"QUERY_TAGS": "team:old-team,other:value"},
            **self.DUMMY_CONNECTION_ARGS,
        )

        call_kwargs = mock_client_class.return_value.open_session.call_args[1]
        assert call_kwargs["session_configuration"]["QUERY_TAGS"] == "team:new-team"


class TestSpogHeaders:
    """Unit tests for SPOG header extraction from http_path."""

    def test_extracts_org_id_from_query_param(self):
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=6051921418418893", []
        )
        assert result == {"x-databricks-org-id": "6051921418418893"}

    def test_no_query_param_returns_empty(self):
        result = Session._extract_spog_headers("/sql/1.0/warehouses/abc123", [])
        assert result == {}

    def test_no_o_param_returns_empty(self):
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?other=value", []
        )
        assert result == {}

    def test_empty_http_path_returns_empty(self):
        result = Session._extract_spog_headers("", [])
        assert result == {}

    def test_none_http_path_returns_empty(self):
        result = Session._extract_spog_headers(None, [])
        assert result == {}

    def test_explicit_header_takes_precedence(self):
        existing = [("x-databricks-org-id", "explicit-value")]
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=6051921418418893", existing
        )
        assert result == {}

    def test_explicit_header_takes_precedence_case_insensitively(self):
        existing = [("X-Databricks-Org-Id", "explicit-value")]
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=6051921418418893", existing
        )
        assert result == {}

    def test_multiple_query_params(self):
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=12345&extra=val", []
        )
        assert result == {"x-databricks-org-id": "12345"}

    def test_non_numeric_query_param_returns_empty(self):
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=abc123", []
        )
        assert result == {}

    def test_control_char_query_param_returns_empty(self):
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=123%0D%0AX-Injected:%20yes", []
        )
        assert result == {}

    def test_empty_query_param_returns_empty(self):
        result = Session._extract_spog_headers("/sql/1.0/warehouses/abc123?o=", [])
        assert result == {}

    def test_extracts_org_id_from_cluster_path_segment(self):
        # All-purpose-compute path embeds workspace ID in /o/<wsid>/<cluster>.
        # Without ?o=, the driver must still set x-databricks-org-id so that
        # telemetry and other non-Thrift requests route to the right workspace
        # on SPOG hosts.
        result = Session._extract_spog_headers(
            "sql/protocolv1/o/6051921418418893/0528-220959-uzmcn1qt", []
        )
        assert result == {"x-databricks-org-id": "6051921418418893"}

    def test_extracts_org_id_from_cluster_path_with_leading_slash(self):
        result = Session._extract_spog_headers(
            "/sql/protocolv1/o/6051921418418893/0528-220959-uzmcn1qt", []
        )
        assert result == {"x-databricks-org-id": "6051921418418893"}

    def test_query_param_wins_over_cluster_path_segment(self):
        # When both forms are present, ?o= takes precedence.
        result = Session._extract_spog_headers(
            "sql/protocolv1/o/111/0528-220959-uzmcn1qt?o=222", []
        )
        assert result == {"x-databricks-org-id": "222"}

    def test_explicit_header_wins_over_cluster_path_segment(self):
        existing = [("x-databricks-org-id", "from-caller")]
        result = Session._extract_spog_headers(
            "sql/protocolv1/o/111/0528-220959-uzmcn1qt", existing
        )
        assert result == {}

    def test_nested_cluster_path_prefix_returns_empty(self):
        result = Session._extract_spog_headers(
            "evil/sql/protocolv1/o/999/0528-220959-uzmcn1qt", []
        )
        assert result == {}

    def test_incomplete_cluster_path_returns_empty(self):
        result = Session._extract_spog_headers("sql/protocolv1/o/999/", [])
        assert result == {}

    def test_warehouse_path_without_query_param_returns_empty(self):
        # Regression guard: the new cluster-path regex must not accidentally
        # match warehouse paths (which never embed the workspace ID).
        result = Session._extract_spog_headers("/sql/1.0/warehouses/abc123", [])
        assert result == {}


class TestKernelAuthProviderBypass:
    """Regression guards for the use_kernel auth-provider handling.

    On use_kernel=True the connector must NOT build its own OAuth
    provider — doing so eagerly runs the U2M browser flow / M2M token
    exchange at connect() time (before use_kernel is even consulted),
    which both opens a browser and races the kernel's own auth. The
    kernel owns auth from the raw kwargs instead. See session.py.

    These exercise ``Session.__init__``'s provider-selection logic
    directly: ``_create_backend`` is stubbed to a no-op so the kernel
    client (and its ``import databricks_sql_kernel``) is never touched,
    keeping the tests independent of whether the Rust wheel is installed
    in the unit-test job. We assert on the resulting ``session.auth_provider``
    and that the connector's provider builder was not called.
    """

    PACKAGE = "databricks.sql"

    def _build_session(self, **extra):
        """Construct a Session with use_kernel=True and a stubbed
        backend, returning (session, mock_get_provider)."""
        with patch(
            "%s.session.Session._create_backend" % self.PACKAGE
        ) as mock_backend, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ) as mock_get_provider:
            mock_backend.return_value = MagicMock()
            sess = Session(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                http_client=MagicMock(),
                http_headers=[],
                use_kernel=True,
                enable_telemetry=False,
                **extra,
            )
            return sess, mock_get_provider

    def test_use_kernel_m2m_does_not_build_connector_provider(
        self, session_feature_flags
    ):
        sess, mock_get_provider = self._build_session(
            oauth_client_id="sp-uuid", oauth_client_secret="shh"
        )
        # The connector's provider builder (which would fire the eager
        # OAuth flow) must never be called on the kernel path...
        mock_get_provider.assert_not_called()
        # ...and with no access_token, auth_provider is None (M2M
        # resolves in-kernel from the raw kwargs).
        assert sess.auth_provider is None
        assert sess.feature_flags is None
        session_feature_flags.get_instance.assert_not_called()

    def test_use_kernel_pat_builds_minimal_access_token_provider(
        self, session_feature_flags
    ):
        from databricks.sql.auth.authenticators import AccessTokenAuthProvider

        sess, mock_get_provider = self._build_session(access_token="dapi-xyz")
        mock_get_provider.assert_not_called()
        # PAT path: a minimal AccessTokenAuthProvider, not the
        # federation-wrapped connector provider.
        assert isinstance(sess.auth_provider, AccessTokenAuthProvider)
        assert sess.feature_flags is None
        session_feature_flags.get_instance.assert_not_called()


class TestKernelTransportOptionsThreading:
    """The connector's retry and socket timeout kwargs must be forwarded
    on the use_kernel path. Captures the kwargs session.py passes by
    patching ``KernelDatabricksClient`` and inspecting its call args.

    Patching ``KernelDatabricksClient`` requires importing
    ``databricks.sql.backend.kernel.client``, which imports pyarrow at
    module load — so this test is skipped when pyarrow is absent (the
    no-pyarrow CI tier), matching the other kernel tests. The Rust wheel
    is still faked via sys.modules so the kernel extension itself isn't
    needed.
    """

    PACKAGE = "databricks.sql"

    def test_retry_and_socket_timeout_threaded_into_kernel_client(self):
        import sys
        import types

        pytest.importorskip(
            "pyarrow",
            reason="kernel client module imports pyarrow at load",
        )

        # The lazy ``from databricks.sql.backend.kernel.client import
        # KernelDatabricksClient`` triggers ``import databricks_sql_kernel``
        # at module load; the unit-test job has no Rust wheel, so inject
        # a fake module (scoped via patch.dict) before connect() runs.
        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()

        # Patch the kernel client class (imported lazily inside
        # _create_backend) and the provider builder; capture the kwargs
        # session.py passes to the kernel client.
        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel_client, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ):
            instance = mock_kernel_client.return_value
            instance.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                access_token="dapi-xyz",
                enable_telemetry=False,
                _retry_delay_min=2.0,
                _retry_delay_max=90.0,
                _retry_stop_after_attempts_count=10,
                _retry_stop_after_attempts_duration=600.0,
                _socket_timeout=12.5,
                _pool_maxsize=41,
                use_cloud_fetch=False,
                max_download_threads=4,
                enable_geospatial_support=False,
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                opts = kwargs["retry_options"]
                assert opts["retry_delay_min"] == 2.0
                assert opts["retry_delay_max"] == 90.0
                assert opts["retry_stop_after_attempts_count"] == 10
                assert opts["retry_stop_after_attempts_duration"] == 600.0
                assert kwargs["request_timeout_secs"] == 12.5
                assert kwargs["max_connections"] == 41
                assert kwargs["use_cloud_fetch"] is False
                assert kwargs["max_download_threads"] == 4
                assert kwargs["enable_geospatial_support"] is False
            finally:
                conn.close()

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                access_token="dapi-xyz",
                enable_telemetry=False,
                _pool_maxsize=0,
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                assert kwargs["max_connections"] is None
                assert kwargs["max_download_threads"] == 16
            finally:
                conn.close()

    def test_azure_sp_m2m_kwargs_threaded_into_kernel_auth_options(self):
        # The Azure SP credentials a user passes to connect() must reach the
        # kernel auth bridge via auth_options; without this threading the
        # azure-sp-m2m path would fail at session-open with "requires
        # azure_client_id". Guards the session.py -> kernel_auth_options map.
        import sys
        import types

        pytest.importorskip(
            "pyarrow",
            reason="kernel client module imports pyarrow at load",
        )

        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()

        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel_client, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ):
            instance = mock_kernel_client.return_value
            instance.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                auth_type="azure-sp-m2m",
                azure_client_id="azure-sp",
                azure_client_secret="azure-secret",
                azure_tenant_id="tenant-123",
                azure_workspace_resource_id="/subscriptions/s/rg/w",
                enable_telemetry=False,
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                opts = kwargs["auth_options"]
                assert opts["auth_type"] == "azure-sp-m2m"
                assert opts["azure_client_id"] == "azure-sp"
                assert opts["azure_client_secret"] == "azure-secret"
                assert opts["azure_tenant_id"] == "tenant-123"
                assert opts["azure_workspace_resource_id"] == "/subscriptions/s/rg/w"
            finally:
                conn.close()

    def test_oauth_token_cache_enabled_threaded_into_kernel_auth_options(self):
        # oauth_token_cache_enabled must reach the kernel auth bridge via
        # auth_options; without this session.py mapping line the feature
        # would silently regress to always-disabled (the safe default masks
        # the failure). Guards the session.py -> kernel_auth_options map.
        import sys
        import types

        pytest.importorskip(
            "pyarrow",
            reason="kernel client module imports pyarrow at load",
        )

        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()

        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel_client, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ):
            instance = mock_kernel_client.return_value
            instance.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                auth_type="databricks-oauth",
                oauth_token_cache_enabled=True,
                enable_telemetry=False,
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                opts = kwargs["auth_options"]
                assert opts["oauth_token_cache_enabled"] is True
            finally:
                conn.close()


class TestKernelTelemetryOptionsThreading:
    """The kernel path must forward telemetry options from connect()
    into ``KernelDatabricksClient`` so phase-7 PyO3 Session kwargs can
    be populated before the kernel opens its session."""

    PACKAGE = "databricks.sql"

    def test_telemetry_kwargs_threaded_into_kernel_client(self):
        import sys
        import types

        pytest.importorskip(
            "pyarrow",
            reason="kernel client module imports pyarrow at load",
        )

        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()

        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel_client, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ):
            instance = mock_kernel_client.return_value
            instance.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                access_token="dapi-xyz",
                enable_telemetry=True,
                force_enable_telemetry=False,
                telemetry_batch_size=17,
                telemetry_flush_interval_ms=300_000,
                _telemetry_circuit_breaker_enabled=False,
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                opts = kwargs["telemetry_options"]
                assert opts["enable_telemetry"] is True
                assert opts["telemetry_batch_size"] == 17
                assert opts["telemetry_flush_interval_ms"] == 300_000
                assert opts["telemetry_circuit_breaker_enabled"] is False
            finally:
                conn.close()

    def test_telemetry_enabled_defaults_none_for_kernel_client(self):
        import sys
        import types

        pytest.importorskip(
            "pyarrow",
            reason="kernel client module imports pyarrow at load",
        )

        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()

        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel_client, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ):
            instance = mock_kernel_client.return_value
            instance.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                access_token="dapi-xyz",
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                opts = kwargs["telemetry_options"]
                assert opts["enable_telemetry"] is None
                assert opts["telemetry_flush_interval_ms"] is None
            finally:
                conn.close()


class TestKernelUserAgentForwarding:
    """user_agent_entry must reach the kernel on the use_kernel path —
    session.py folds it into the composed User-Agent and includes it in
    all_headers, which is passed to the kernel client as http_headers.
    Guards against a regression where session.py stops folding it under
    use_kernel=True (which would silently drop partner attribution)."""

    PACKAGE = "databricks.sql"

    def test_user_agent_entry_reaches_kernel_client_http_headers(self):
        import sys
        import types

        pytest.importorskip(
            "pyarrow", reason="kernel client module imports pyarrow at load"
        )

        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()

        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel_client, patch(
            "%s.session.get_python_sql_connector_auth_provider" % self.PACKAGE
        ):
            instance = mock_kernel_client.return_value
            instance.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )

            conn = databricks.sql.connect(
                server_hostname="foo",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                access_token="dapi-xyz",
                enable_telemetry=False,
                user_agent_entry="my-partner-app",
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                # http_headers carries a User-Agent that embeds the entry.
                headers = dict(kwargs["http_headers"])
                ua = headers.get("User-Agent", "")
                assert "my-partner-app" in ua, f"UA was {ua!r}"
            finally:
                conn.close()


@pytest.mark.realkernel
class TestUseKernelRoutesThroughRealWheel:
    """No-network proof that ``sql.connect(use_kernel=True)`` actually
    routes through the REAL databricks-sql-kernel wheel — not a stub and
    not a fallback to Thrift.

    This is the unit-side complement to the live e2e suite: it does not
    need a warehouse (only the network boundary ``open_session`` is
    mocked), but unlike the other kernel unit tests it does NOT fake the
    wheel — the real ``KernelDatabricksClient`` is instantiated and its
    ``_kernel_session`` is built from the real ``databricks_sql_kernel``
    ``Session``. Skips only when the real wheel is genuinely absent
    (e.g. the no-kernel CI tier); it must never silently pass when the
    wheel is present.
    """

    def _real_kernel_or_skip(self):
        import importlib.metadata as ilm

        try:
            ilm.version("databricks-sql-kernel")
        except ilm.PackageNotFoundError:
            pytest.skip("databricks-sql-kernel wheel not installed")
        mod = __import__("databricks_sql_kernel")
        if not getattr(mod, "__file__", None):
            pytest.fail(
                "databricks-sql-kernel is installed but sys.modules holds a "
                "stub (no __file__) — a unit-test fake is shadowing the real "
                "wheel; this routing test would not exercise the real kernel."
            )

    def test_connect_use_kernel_instantiates_real_kernel_backend(self):
        self._real_kernel_or_skip()

        from databricks.sql.backend.kernel.client import KernelDatabricksClient

        # Mock only the network boundary: the real KernelDatabricksClient
        # is constructed (building a real databricks_sql_kernel Session),
        # but open_session() doesn't hit the wire.
        with patch.object(
            KernelDatabricksClient,
            "open_session",
            return_value=SessionId(BackendType.SEA, "sess-id", None),
        ):
            conn = databricks.sql.connect(
                server_hostname="foo.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                use_kernel=True,
                access_token="dapi-xyz",
                enable_telemetry=False,
            )
            try:
                # The active backend is the REAL kernel client class.
                assert isinstance(conn.session.backend, KernelDatabricksClient), (
                    "use_kernel=True did not route through the real "
                    f"KernelDatabricksClient; got "
                    f"{type(conn.session.backend).__name__}"
                )
            finally:
                conn.close()

    def test_enable_geospatial_support_matches_real_kernel_signature(self):
        self._real_kernel_or_skip()

        from databricks.sql.backend.kernel.client import (
            _kernel_geospatial_kwargs,
            _kernel_session_accepts_kwarg,
        )
        from databricks.sql.exc import NotSupportedError

        # The ordinary kernel unit-test tier installs the latest published
        # wheel, which may lag the KERNEL_REV source pin while the matching
        # kernel change is still in flight. Validate the compatibility error
        # in that case; once the wheel includes the typed option, validate
        # both values against its real PyO3 signature.
        if not _kernel_session_accepts_kwarg("enable_geospatial_support"):
            with pytest.raises(NotSupportedError, match="newer databricks-sql-kernel"):
                _kernel_geospatial_kwargs(True)
            return

        assert _kernel_geospatial_kwargs(True) == {"enable_geospatial_support": True}
        assert _kernel_geospatial_kwargs(False) == {"enable_geospatial_support": False}


class TestReydenThriftFallback:
    """Transparent auto-recovery from a Reyden / Real-Time warehouse rejecting
    the legacy Thrift protocol (SQLSTATE KP001) onto the kernel backend."""

    PACKAGE = "databricks.sql"
    HOST = "reyden.example.com"
    WAREHOUSE_PATH = "/sql/1.0/warehouses/wh-reyden"

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        from databricks.sql.backend import reyden_warehouse_cache

        reyden_warehouse_cache.clear_cache()
        yield
        reyden_warehouse_cache.clear_cache()

    @contextmanager
    def _fake_kernel(self):
        """Fake the Rust wheel and patch KernelDatabricksClient so open_session
        returns a valid SessionId; yields the mock for call assertions."""
        import sys
        import types

        pytest.importorskip(
            "pyarrow", reason="kernel client module imports pyarrow at load"
        )
        fake = types.ModuleType("databricks_sql_kernel")
        fake.KernelError = type("KernelError", (Exception,), {})
        fake.Session = MagicMock()
        with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
            "databricks.sql.backend.kernel.client.KernelDatabricksClient"
        ) as mock_kernel:
            mock_kernel.return_value.open_session.return_value = SessionId(
                BackendType.SEA, "sess-id", None
            )
            yield mock_kernel

    @staticmethod
    def _reject():
        return ReydenThriftUnsupportedError(
            "Lakehouse/RT is not supported for Thrift protocol"
        )

    def _connect(self, **overrides):
        args = dict(
            server_hostname=self.HOST,
            http_path=self.WAREHOUSE_PATH,
            access_token="tok",
            enable_telemetry=False,
        )
        args.update(overrides)
        return databricks.sql.connect(**args)

    @pytest.mark.parametrize("route", ["explicit", "recovery", "cached"])
    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_kernel_http_client_lifecycle(self, mock_thrift, route):
        from databricks.sql.backend import reyden_warehouse_cache
        from databricks.sql.result_set import ResultSet

        explicit = route == "explicit"
        mock_thrift.return_value.open_session.side_effect = self._reject()
        if route == "cached":
            reyden_warehouse_cache.mark_reyden(self.HOST, "wh-reyden")

        with self._fake_kernel() as kernel, patch(
            f"{self.PACKAGE}.client.UnifiedHttpClient"
        ) as http_client, patch(
            f"{self.PACKAGE}.client.TelemetryClientFactory.get_telemetry_client"
        ) as telemetry:
            if explicit:
                http_client.side_effect = AssertionError(
                    "Explicit kernel must not create a Python client"
                )
            http_client.return_value.using_proxy.return_value = True
            http_client.return_value.proxy_uri = "http://proxy.example.com:8081"
            result = Mock(spec=ResultSet, is_staging_operation=False)
            result.fetchall.return_value = [(1,)]
            kernel.return_value.execute_command.return_value = result
            conn = self._connect(
                **({"use_kernel": True} if explicit else {}),
                enable_telemetry=True,
                force_enable_telemetry=True,
            )
            try:
                assert conn.session.use_kernel is True
                assert conn.telemetry_enabled is False
                assert conn.http_client is (
                    None if explicit else http_client.return_value
                )
                assert conn.session.http_client is conn.http_client
                assert kernel.call_args.kwargs["http_client"] is conn.http_client
                params = telemetry.return_value.export_initial_telemetry_log.call_args.kwargs[
                    "driver_connection_params"
                ]
                assert params.use_proxy is (not explicit)
                assert params.use_system_proxy is (not explicit)
                if explicit:
                    assert params.proxy_host_info is None
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    assert cursor.fetchall() == [(1,)]
                kernel.return_value.execute_command.assert_called_once()
            finally:
                conn.close()
            kernel.return_value.close_session.assert_called_once()
            if explicit:
                http_client.assert_not_called()
            else:
                http_client.assert_called_once()
                http_client.return_value.close.assert_called_once()
            assert mock_thrift.return_value.open_session.call_count == (
                1 if route == "recovery" else 0
            )

    @pytest.mark.parametrize("failure_stage", ["backend", "open"])
    def test_explicit_kernel_failure_preserves_error_and_telemetry(self, failure_stage):
        with self._fake_kernel() as kernel, patch(
            f"{self.PACKAGE}.client.UnifiedHttpClient",
            side_effect=AssertionError(
                "Explicit kernel must not create a Python client"
            ),
        ) as http_client, patch(
            f"{self.PACKAGE}.client.TelemetryClientFactory.connection_failure_log"
        ) as failure_log:
            if failure_stage == "backend":
                error = ImportError("kernel package unavailable")
                kernel.side_effect = error
            else:
                error = OperationalError("kernel could not open session")
                kernel.return_value.open_session.side_effect = error
            with pytest.raises(type(error)) as excinfo:
                self._connect(use_kernel=True, enable_telemetry=True)
            assert excinfo.value is error
            http_client.assert_not_called()
            failure_log.assert_called_once()
            assert failure_log.call_args.kwargs["enable_telemetry"] is (
                failure_stage == "backend"
            )

    @pytest.mark.parametrize("route", ["explicit", "recovery", "cached"])
    @pytest.mark.parametrize(
        "geospatial_kwargs, expected",
        [
            pytest.param({}, False, id="default"),
            pytest.param({"enable_geospatial_support": False}, False, id="disabled"),
            pytest.param({"enable_geospatial_support": True}, True, id="enabled"),
        ],
    )
    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_kernel_routing_preserves_geospatial_representation(
        self, mock_thrift, route, geospatial_kwargs, expected
    ):
        from databricks.sql.backend import reyden_warehouse_cache

        mock_thrift.return_value.open_session.side_effect = self._reject()
        if route == "cached":
            reyden_warehouse_cache.mark_reyden(self.HOST, "wh-reyden")
        if route == "explicit":
            geospatial_kwargs = {**geospatial_kwargs, "use_kernel": True}

        with self._fake_kernel() as mock_kernel:
            conn = self._connect(**geospatial_kwargs)
            try:
                assert conn.session.use_kernel is True
                assert (
                    mock_kernel.call_args.kwargs["enable_geospatial_support"]
                    is expected
                )
                if route == "recovery":
                    mock_thrift.return_value.open_session.assert_called_once()
                else:
                    mock_thrift.return_value.open_session.assert_not_called()
            finally:
                conn.close()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_thrift_rejection_recovers_onto_kernel(self, mock_thrift):
        mock_thrift.return_value.open_session.side_effect = self._reject()
        with self._fake_kernel() as mock_kernel:
            conn = self._connect()
            try:
                assert conn.session.use_kernel is True
                mock_kernel.return_value.open_session.assert_called_once()
            finally:
                conn.close()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_known_reyden_warehouse_skips_thrift(self, mock_thrift):
        from databricks.sql.backend import reyden_warehouse_cache

        # A previously observed rejection is remembered — casing of the host in
        # the cache key must not matter.
        reyden_warehouse_cache.mark_reyden(self.HOST.upper(), "wh-reyden")
        with self._fake_kernel() as mock_kernel:
            conn = self._connect()
            try:
                assert conn.session.use_kernel is True
                mock_kernel.return_value.open_session.assert_called_once()
                # Pre-check must short-circuit before any Thrift OpenSession.
                mock_thrift.return_value.open_session.assert_not_called()
            finally:
                conn.close()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_rejection_marks_cache_for_next_connect(self, mock_thrift):
        from databricks.sql.backend import reyden_warehouse_cache

        mock_thrift.return_value.open_session.side_effect = self._reject()
        with self._fake_kernel():
            conn = self._connect()
            conn.close()
        assert reyden_warehouse_cache.is_known_reyden(self.HOST, "wh-reyden")

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_non_reyden_thrift_error_not_recovered(self, mock_thrift):
        mock_thrift.return_value.open_session.side_effect = DatabaseError(
            "some unrelated server error"
        )
        with pytest.raises(DatabaseError) as excinfo:
            self._connect()
        assert not isinstance(excinfo.value, ReydenThriftUnsupportedError)

    @patch("%s.session.SeaDatabricksClient" % PACKAGE)
    def test_explicit_use_sea_not_recovered(self, mock_sea):
        # An explicit backend choice is always honored — even the (contrived)
        # case of SEA surfacing the marker must not trigger Thrift→kernel
        # recovery.
        mock_sea.return_value.open_session.side_effect = self._reject()
        with pytest.raises(ReydenThriftUnsupportedError):
            self._connect(use_sea=True)

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_kernel_retry_failure_chains_both_errors(self, mock_thrift):
        mock_thrift.return_value.open_session.side_effect = self._reject()
        with self._fake_kernel() as mock_kernel:
            mock_kernel.return_value.open_session.side_effect = OperationalError(
                "kernel could not open session"
            )
            with pytest.raises(OperationalError) as excinfo:
                self._connect()
        # Kernel failure is surfaced as primary; the Thrift rejection is
        # preserved in the chain for diagnosis.
        assert isinstance(excinfo.value.__cause__, ReydenThriftUnsupportedError)

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_recovered_kernel_failure_suppresses_wrapper_telemetry(self, mock_thrift):
        # A connection that recovered onto the kernel and then failed there must
        # NOT emit the wrapper's connection-failure log — the kernel owns
        # telemetry for kernel connections. Guards against reading the original
        # (Thrift) kwargs instead of the session that actually failed.
        mock_thrift.return_value.open_session.side_effect = self._reject()
        with self._fake_kernel() as mock_kernel, patch(
            "databricks.sql.client.TelemetryClientFactory.connection_failure_log"
        ) as mock_fail_log:
            mock_kernel.return_value.open_session.side_effect = OperationalError(
                "kernel boom"
            )
            # enable_telemetry=True so only the kernel-suppression logic can flip
            # it off — proving the fix rather than the user's opt-out.
            with pytest.raises(OperationalError):
                self._connect(enable_telemetry=True)
        mock_fail_log.assert_called_once()
        assert mock_fail_log.call_args.kwargs["enable_telemetry"] is False

    @patch("%s.session.get_python_sql_connector_auth_provider" % PACKAGE)
    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_oauth_default_recovery_injects_databricks_oauth_auth_type(
        self, mock_thrift, mock_provider
    ):
        # A bare connection (no access_token, no auth_type) defaults to OAuth
        # U2M on the Thrift path. The kernel path has no such default and would
        # reject auth_type=None, so recovery must inject databricks-oauth to
        # mirror the Thrift default. (The provider builder is patched so the
        # token-less Thrift attempt doesn't build a real OAuth provider, which
        # would hit the network at construction.)
        mock_thrift.return_value.open_session.side_effect = self._reject()
        with self._fake_kernel() as mock_kernel:
            conn = self._connect(access_token=None)
            try:
                _, kwargs = mock_kernel.call_args
                assert kwargs["auth_options"]["auth_type"] == "databricks-oauth"
            finally:
                conn.close()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE)
    def test_pat_recovery_does_not_inject_auth_type(self, mock_thrift):
        # With a credential shape present (here a PAT) the kernel routes on it
        # regardless of auth_type, so recovery must NOT inject databricks-oauth:
        # forcing it alongside other credentials could change kernel routing or
        # trip the auth bridge's ambiguity guards.
        mock_thrift.return_value.open_session.side_effect = self._reject()
        with self._fake_kernel() as mock_kernel:
            conn = self._connect()  # access_token="tok"
            try:
                _, kwargs = mock_kernel.call_args
                assert kwargs["auth_options"].get("auth_type") is None
            finally:
                conn.close()


# ---- kernel _port / _connection_uri resolution (use_kernel=True) ----
# The kernel applies _port (host:port folding, IPv6 brackets, embedded-port rules, and
# credential/path/query checks), so these tests cover only what the driver resolves:
# the host, the port and the path that reach the kernel.

_P = "/sql/1.0/warehouses/abc"


@pytest.mark.parametrize(
    "server, kwargs, expected_port",
    [
        # A non-default _port is passed as port=; server_hostname is passed unchanged.
        ("h.io", {"_port": 8443}, 8443),
        ("h.io", {"_port": "8443"}, 8443),
        ("h.io", {"_port": 1}, 1),
        ("h.io", {"_port": 65535}, 65535),
        ("[::1]", {"_port": 8443}, 8443),
        ("h.io:8443", {"_port": 8443}, 8443),
        ("h.io/x", {"_port": 8443}, 8443),
        ("", {"_port": 8443}, 8443),
        # Unset, falsy and the default 443 pass no port, so the host is untouched.
        ("h.io", {}, None),
        ("h.io", {"_port": None}, None),
        ("h.io", {"_port": ""}, None),
        ("h.io", {"_port": "  "}, None),
        ("h.io", {"_port": False}, None),
        ("h.io", {"_port": 0}, None),
        ("h.io", {"_port": "0"}, None),
        ("h.io", {"_port": 443}, None),
        ("h.io", {"_port": "443"}, None),
        ("h.io:0", {}, None),
    ],
)
def test_kernel_endpoint_passes_host_unchanged_and_port_separately(
    server, kwargs, expected_port
):
    assert _kernel_endpoint(server, _P, kwargs) == (server, _P, expected_port)


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # _connection_uri gives the host (with any port), the path and the query. It wins
        # over _port, which is not passed.
        (
            {"_connection_uri": "https://d.io:8443/sql/1.0/warehouses/xyz"},
            ("https://d.io:8443", "/sql/1.0/warehouses/xyz", None),
        ),
        (
            {"_connection_uri": "https://h.io/sql/x?o=123"},
            ("https://h.io", "/sql/x?o=123", None),
        ),
        (
            {"_connection_uri": "http://h.io:8080/sql/x"},
            ("http://h.io:8080", "/sql/x", None),
        ),
        (
            {"_connection_uri": "https://d.io:9999/p", "_port": 8443},
            ("https://d.io:9999", "/p", None),
        ),
    ],
)
def test_connection_uri_resolves_host_and_path_and_passes_no_port(kwargs, expected):
    assert _kernel_endpoint("h.io", _P, kwargs) == expected


def test_connection_uri_only_resolves_host_and_path():
    assert _kernel_endpoint("", "", {"_connection_uri": "https://h.io/sql/x"}) == (
        "https://h.io",
        "/sql/x",
        None,
    )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        # The driver parses _port as an int in 1..65535 (falsy means the default).
        ({"_port": -1}, "Invalid _port"),
        ({"_port": 65536}, "Invalid _port"),
        ({"_port": "abc"}, "Invalid _port"),
        ({"_port": True}, "Invalid _port"),
        ({"_port": 8443.5}, "Invalid _port"),
        # _connection_uri must be http(s) with a host and a warehouse path, no credentials.
        ({"_connection_uri": "d.io/sql/x"}, "Invalid _connection_uri"),
        ({"_connection_uri": "ftp://h.io/sql/x"}, "Invalid _connection_uri"),
        ({"_connection_uri": "https://h.io:8443"}, "Invalid _connection_uri"),
        ({"_connection_uri": "https://h.io:8443/"}, "Invalid _connection_uri"),
        ({"_connection_uri": "https://h.io:notaport/sql/x"}, "Invalid _connection_uri"),
        ({"_connection_uri": "https://h.io:0/sql/x"}, "Invalid _connection_uri"),
        ({"_connection_uri": "https://u:p@h.io/sql/x"}, "Invalid _connection_uri"),
        ({"_connection_uri": "https:///sql/x"}, "Invalid _connection_uri"),
    ],
)
def test_kernel_endpoint_rejects_driver_owned_inputs(kwargs, message):
    with pytest.raises(ValueError, match=message):
        _kernel_endpoint("h", _P, kwargs)


@pytest.mark.parametrize(
    "server, port",
    [
        ("h.io:8443", 443),
        ("h.io", 8443),
        ("h.io/x", 8443),
        ("h.io", ""),
    ],
)
@patch("databricks.sql.session.ThriftDatabricksClient")
def test_thrift_receives_host_and_port_unchanged(mock_client_class, server, port):
    # Thrift is unchanged: the host and _port reach ThriftDatabricksClient verbatim, and
    # ThriftDatabricksClient applies its own `port or 443` to the URL. The kernel-only
    # port handling above does not apply here.
    connection = databricks.sql.connect(
        server_hostname=server,
        http_path=_P,
        access_token="tok",
        enable_telemetry=False,
        _port=port,
    )
    call_kwargs = mock_client_class.call_args.kwargs
    assert call_kwargs["server_hostname"] == server
    assert call_kwargs["port"] == port
    connection.close()


@pytest.mark.parametrize(
    "server, kwargs, uri, should_raise",
    [
        # AC-P4: with OAuth, the _connection_uri origin must equal https://server_hostname.
        (
            "h.io",
            {"oauth_client_id": "c", "oauth_client_secret": "s"},
            "https://evil.io/sql/x",
            True,
        ),
        ("h.io", {"auth_type": "azure-sp-m2m"}, "https://evil.io/sql/x", True),
        ("h.io", {"oauth_jwt_key_file": "/k.pem"}, "https://evil.io/sql/x", True),
        ("h.io", {"auth_type": "databricks-oauth"}, "http://h.io/sql/x", True),
        ("h.io", {"auth_type": "databricks-oauth"}, "https://h.io:8443/sql/x", True),
        ("h.io", {"auth_type": "databricks-oauth"}, "https://H.io/sql/x", False),
        (
            "h.io:8443",
            {"auth_type": "databricks-oauth"},
            "https://h.io:8443/sql/x",
            False,
        ),
        ("[::1]", {"auth_type": "databricks-oauth"}, "https://[::1]/sql/x", False),
        (
            "h.io",
            {"access_token": "dapi"},
            "https://evil.io/sql/x",
            False,
        ),  # PAT is not guarded
        # The auth bridge routes access_token with no M2M/JWT credentials to PAT,
        # even when auth_type is databricks-oauth; azure-sp-m2m and M2M/JWT stay guarded.
        (
            "h.io",
            {"auth_type": "databricks-oauth", "access_token": "dapi"},
            "https://evil.io/sql/x",
            False,
        ),
        (
            "h.io",
            {"auth_type": "azure-sp-m2m", "access_token": "dapi"},
            "https://evil.io/sql/x",
            True,
        ),
        # The kernel auth bridge rejects a custom credentials_provider with NotSupportedError,
        # so the origin guard does not classify it as OAuth.
        (
            "h.io",
            {"credentials_provider": object()},
            "https://evil.io/sql/x",
            False,
        ),
        (
            "h.io",
            {"oauth_jwt_key_file": "/k.pem", "access_token": "dapi"},
            "https://evil.io/sql/x",
            True,
        ),
        (
            "h.io",
            {
                "oauth_client_id": "c",
                "oauth_client_secret": "s",
                "access_token": "dapi",
            },
            "https://evil.io/sql/x",
            True,
        ),
        (
            "",
            {"auth_type": "databricks-oauth"},
            "https://h.io/sql/x",
            False,
        ),  # URI-only
        (
            "h.io",
            {"auth_type": "databricks-oauth", "_port": 8443},
            None,
            False,
        ),  # documented, not guarded
        # A server_hostname with no comparable origin is an origin mismatch, not a
        # "malformed host" error.
        ("::1", {"auth_type": "databricks-oauth"}, "https://h.io/sql/x", True),
        ("h.io:", {"auth_type": "databricks-oauth"}, "https://h.io/sql/x", True),
        (
            "user:pass@h.io",
            {"auth_type": "databricks-oauth"},
            "https://h.io/sql/x",
            True,
        ),
    ],
)
def test_oauth_origin_guard(server, kwargs, uri, should_raise):
    kwargs = dict(kwargs)
    if uri is not None:
        kwargs["_connection_uri"] = uri
    kernel_host = _kernel_endpoint(server, _P, kwargs)[0]
    if should_raise:
        with pytest.raises(ValueError, match="requires the _connection_uri origin"):
            _check_oauth_origin(server, kernel_host, kwargs)
    else:
        _check_oauth_origin(server, kernel_host, kwargs)


def _jwt(issuer):
    # Unverified on purpose: the guard only reads the iss claim, so the key is a placeholder.
    return jwt.encode({"iss": issuer, "sub": "user"}, "k" * 32, algorithm="HS256")


@pytest.mark.parametrize(
    "server, uri, token, should_raise",
    [
        # A foreign-issuer JWT is exchanged at the kernel's connection host, so a
        # mismatched _connection_uri origin must fail before any request.
        (
            "h.io",
            "https://evil.io/sql/x",
            _jwt("https://login.microsoftonline.com/tenant/v2.0"),
            True,
        ),
        # The same foreign JWT is fine when the _connection_uri origin matches.
        (
            "h.io",
            "https://h.io/sql/x",
            _jwt("https://login.microsoftonline.com/tenant/v2.0"),
            False,
        ),
        # An issuer on the server host is not exchanged (Thrift parity), so it passes.
        ("h.io", "https://h.io/sql/x", _jwt("https://h.io/oidc"), False),
        # A PAT is not a JWT and is not guarded.
        ("h.io", "https://evil.io/sql/x", "dapi-xyz", False),
    ],
)
def test_foreign_issuer_access_token_origin_guard(server, uri, token, should_raise):
    kwargs = {"access_token": token, "_connection_uri": uri}
    kernel_host = _kernel_endpoint(server, _P, kwargs)[0]
    if should_raise:
        with pytest.raises(ValueError, match="requires the _connection_uri origin"):
            _check_oauth_origin(server, kernel_host, kwargs)
    else:
        _check_oauth_origin(server, kernel_host, kwargs)


@pytest.mark.parametrize(
    "uri, should_raise",
    [
        # identity_federation_client_id makes the kernel exchange the token at the
        # connection host, so a mismatched origin must fail even for an opaque token.
        ("https://evil.io/sql/x", True),
        ("https://h.io/sql/x", False),
    ],
)
def test_federation_client_id_access_token_origin_guard(uri, should_raise):
    kwargs = {
        "access_token": "dapi-xyz",
        "identity_federation_client_id": "sp-client",
        "_connection_uri": uri,
    }
    kernel_host = _kernel_endpoint("h.io", _P, kwargs)[0]
    if should_raise:
        with pytest.raises(ValueError, match="requires the _connection_uri origin"):
            _check_oauth_origin("h.io", kernel_host, kwargs)
    else:
        _check_oauth_origin("h.io", kernel_host, kwargs)


def test_foreign_issuer_access_token_without_connection_uri_is_unaffected():
    # No _connection_uri: the kernel host is the server_hostname, so there is no origin to check.
    kwargs = {"access_token": _jwt("https://login.microsoftonline.com/tenant/v2.0")}
    _check_oauth_origin("h.io", "h.io", kwargs)


@pytest.mark.parametrize(
    "uri, secret",
    [
        ("https://user:secret@h.io/sql/x", "secret"),
        ("https://h.io:8443?token=secret", "secret"),
        # "@" after the host with no path: the URI is rejected, and the "@" must not leak.
        ("https://h.io?x=a@secret", "secret"),
        # Percent-encoded "@" hides the userinfo marker from a literal "@" check.
        ("https://user%3Asecret%40h.io", "secret"),
        # A "/" or "?" inside the password must not cut the authority short.
        ("https://user:sec/ret@h.io/sql/x", "sec/ret"),
        ("https://user:sec?ret@h.io/sql/x", "sec?ret"),
        # A "/" inside the password, with "@" after it in the path: netloc stops at "/".
        ("https://user:secret/foo@h.io/sql/x", "secret"),
        # A fullwidth solidus makes urlsplit's NFKC check raise with the raw netloc in its text.
        ("https://user:secret@h.io／sql/x", "secret"),
        # Scheme-less, with "://" inside the query: the text before it is not a scheme.
        ("h.io/sql/x?token=secret&redirect=https://example.com", "secret"),
        # A non-http(s) prefix before "://" is not shown either.
        ("token=secret://h.io/sql/x", "secret"),
    ],
)
def test_invalid_connection_uri_message_omits_credentials(uri, secret):
    with pytest.raises(ValueError, match="Invalid _connection_uri") as exc:
        _kernel_endpoint("h", _P, {"_connection_uri": uri})
    assert secret not in str(exc.value)
    # The secret must not survive in a chained cause or context either.
    assert exc.value.__cause__ is None
    assert exc.value.__context__ is None or secret not in str(exc.value.__context__)


def test_oauth_origin_message_omits_uri_query():
    kwargs = {
        "auth_type": "databricks-oauth",
        "_connection_uri": "https://evil.io/sql/x?token=secret",
    }
    kernel_host = _kernel_endpoint("h.io", _P, kwargs)[0]
    with pytest.raises(ValueError, match="requires the _connection_uri origin") as exc:
        _check_oauth_origin("h.io", kernel_host, kwargs)
    assert "secret" not in str(exc.value)
    assert "https://evil.io" in str(exc.value)


@pytest.mark.parametrize(
    "conn_kwargs, expected_host, expected_path, expected_port",
    [
        # _port reaches the kernel client as port=; server_hostname is unchanged.
        (
            {"server_hostname": "h.io", "http_path": _P, "_port": 8443},
            "h.io",
            _P,
            8443,
        ),
        (
            {"server_hostname": "h.io", "http_path": _P, "_port": "8443"},
            "h.io",
            _P,
            8443,
        ),
        # Unset, falsy and 443 send no port.
        ({"server_hostname": "h.io", "http_path": _P}, "h.io", _P, None),
        ({"server_hostname": "h.io", "http_path": _P, "_port": 443}, "h.io", _P, None),
        ({"server_hostname": "h.io", "http_path": _P, "_port": 0}, "h.io", _P, None),
        # _connection_uri supplies the host (with its port) and the path; no port.
        (
            {
                "server_hostname": "h.io",
                "http_path": _P,
                "_connection_uri": f"https://d.io:9999{_P}",
                "_port": 8443,
            },
            "https://d.io:9999",
            _P,
            None,
        ),
        (
            {
                "server_hostname": "",
                "http_path": "",
                "_connection_uri": f"https://d.io:9999{_P}",
            },
            "https://d.io:9999",
            _P,
            None,
        ),
    ],
)
def test_resolved_kernel_host_and_path_reach_kernel_client(
    fake_kernel, conn_kwargs, expected_host, expected_path, expected_port
):
    client, _ = fake_kernel
    databricks.sql.connect(
        use_kernel=True,
        access_token="dapi-xyz",
        enable_telemetry=False,
        **conn_kwargs,
    ).close()
    kwargs = client.call_args.kwargs
    assert kwargs["server_hostname"] == expected_host
    assert kwargs["http_path"] == expected_path
    assert kwargs["port"] == expected_port


@pytest.fixture
def fake_kernel():
    """Fake the kernel wheel and client so connect() runs without a warehouse."""
    pytest.importorskip(
        "pyarrow", reason="kernel client module imports pyarrow at load"
    )
    fake = types.ModuleType("databricks_sql_kernel")
    fake.KernelError = type("KernelError", (Exception,), {})
    fake.Session = MagicMock()
    with patch.dict(sys.modules, {"databricks_sql_kernel": fake}), patch(
        "databricks.sql.backend.kernel.client.KernelDatabricksClient"
    ) as client, patch("databricks.sql.session.get_python_sql_connector_auth_provider"):
        client.return_value.open_session.return_value = SessionId(
            BackendType.SEA, "sess-id", None
        )
        yield client, fake


def test_oauth_guard_refuses_before_kernel_client_is_built(fake_kernel):
    client, fake = fake_kernel
    with pytest.raises(ValueError, match="requires the _connection_uri origin"):
        databricks.sql.connect(
            server_hostname="h.io",
            http_path=_P,
            use_kernel=True,
            enable_telemetry=False,
            oauth_client_id="c",
            oauth_client_secret="s",
            _connection_uri="https://evil.io/sql/x",
        )
    client.assert_not_called()
    fake.Session.assert_not_called()


@pytest.mark.parametrize(
    "hp, uri, expected_org",
    [
        ("/sql/1.0/warehouses/abc?o=111", "https://d.io/sql/x?o=222", "222"),
        ("/sql/1.0/warehouses/abc?o=111", "https://d.io/sql/x", None),
        ("/sql/1.0/warehouses/abc", "https://d.io/sql/x?o=222", "222"),
        ("/sql/1.0/warehouses/abc?o=111", None, "111"),
    ],
)
def test_spog_org_id_follows_resolved_path(fake_kernel, hp, uri, expected_org):
    client, _ = fake_kernel
    extra = {"_connection_uri": uri} if uri else {}
    databricks.sql.connect(
        server_hostname="h.io",
        http_path=hp,
        access_token="dapi",
        use_kernel=True,
        enable_telemetry=False,
        **extra,
    ).close()
    assert (
        dict(client.call_args.kwargs["http_headers"]).get("x-databricks-org-id")
        == expected_org
    )


@pytest.mark.realkernel
@pytest.mark.parametrize(
    "build",
    [
        lambda p: {"server_hostname": "127.0.0.1", "http_path": _P, "_port": p},
        lambda p: {
            "server_hostname": "",
            "http_path": "",
            "_connection_uri": f"https://127.0.0.1:{p}{_P}",
        },
    ],
    ids=["port", "uri-only"],
)
def test_kernel_dials_over_tls(build):
    """Loopback dial on the real kernel wheel: a TCP listener on 127.0.0.1 must receive the
    kernel's TLS handshake (first byte 0x16) at the requested port. Nothing completes the
    handshake, so connect() is expected to fail; only the dial is asserted."""
    try:
        ilm.version("databricks-sql-kernel")
    except ilm.PackageNotFoundError:
        pytest.skip("databricks-sql-kernel wheel not installed")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(60)
    port = listener.getsockname()[1]
    received = {}

    def accept_one():
        try:
            sock, _ = listener.accept()
            sock.settimeout(10)
            received["first_byte"] = sock.recv(1)
            sock.close()
        except OSError as exc:
            received["error"] = repr(exc)
        finally:
            listener.close()

    thread = threading.Thread(target=accept_one, daemon=True)
    thread.start()
    try:
        databricks.sql.connect(
            use_kernel=True,
            access_token="dapi-xyz",
            enable_telemetry=False,
            _retry_stop_after_attempts_count=1,
            **build(port),
        ).close()
    except Exception:
        pass  # expected: nothing behind the listener completes TLS
    thread.join(timeout=70)
    assert "error" not in received, received
    assert received.get("first_byte") == b"\x16", received


@pytest.mark.realkernel
def test_kernel_embedded_port_wins_over_explicit_port():
    """Loopback on the real kernel wheel: server_hostname 'h:P1' with an explicit _port P2
    dials P1 (TLS first byte 0x16) and never P2."""
    try:
        ilm.version("databricks-sql-kernel")
    except ilm.PackageNotFoundError:
        pytest.skip("databricks-sql-kernel wheel not installed")

    def listen():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        sock.settimeout(15)
        return sock

    embedded, explicit = listen(), listen()
    embedded_port = embedded.getsockname()[1]
    explicit_port = explicit.getsockname()[1]
    received = {}

    def accept_one(name, sock):
        try:
            conn, _ = sock.accept()
            conn.settimeout(10)
            received[name] = conn.recv(1)
            conn.close()
        except OSError as exc:
            received[name + "_error"] = repr(exc)
        finally:
            sock.close()

    threads = [
        threading.Thread(target=accept_one, args=("embedded", embedded), daemon=True),
        threading.Thread(target=accept_one, args=("explicit", explicit), daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        databricks.sql.connect(
            server_hostname=f"127.0.0.1:{embedded_port}",
            http_path=_P,
            use_kernel=True,
            access_token="dapi-xyz",
            enable_telemetry=False,
            _retry_stop_after_attempts_count=1,
            _port=explicit_port,
        ).close()
    except Exception:
        pass  # expected: nothing behind the listener completes TLS
    for thread in threads:
        thread.join(timeout=70)
    assert received.get("embedded") == b"\x16", received
    assert "explicit" not in received, received
    assert "explicit_error" in received, received
