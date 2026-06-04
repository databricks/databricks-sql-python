import pytest
from unittest.mock import patch, MagicMock, Mock, PropertyMock
import gc

from databricks.sql.thrift_api.TCLIService.ttypes import (
    TOpenSessionResp,
    TSessionHandle,
    THandleIdentifier,
)
from databricks.sql.backend.types import SessionId, BackendType
from databricks.sql.common.agent import KNOWN_AGENTS
from databricks.sql.session import Session

import databricks.sql


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

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_auth_args(self, mock_client_class):
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
            _tls_client_cert_key_file="trusted client cert",
            _tls_client_cert_key_password="key password",
        )

        kwargs = mock_client_class.call_args[1]
        assert kwargs["_tls_verify_hostname"] == "hostname"
        assert kwargs["_tls_trusted_ca_file"] == "trusted ca file"
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
        result = Session._extract_spog_headers(
            "/sql/1.0/warehouses/abc123?o=", []
        )
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

    def test_use_kernel_m2m_does_not_build_connector_provider(self):
        sess, mock_get_provider = self._build_session(
            oauth_client_id="sp-uuid", oauth_client_secret="shh"
        )
        # The connector's provider builder (which would fire the eager
        # OAuth flow) must never be called on the kernel path...
        mock_get_provider.assert_not_called()
        # ...and with no access_token, auth_provider is None (M2M
        # resolves in-kernel from the raw kwargs).
        assert sess.auth_provider is None

    def test_use_kernel_pat_builds_minimal_access_token_provider(self):
        from databricks.sql.auth.authenticators import AccessTokenAuthProvider

        sess, mock_get_provider = self._build_session(access_token="dapi-xyz")
        mock_get_provider.assert_not_called()
        # PAT path: a minimal AccessTokenAuthProvider, not the
        # federation-wrapped connector provider.
        assert isinstance(sess.auth_provider, AccessTokenAuthProvider)


class TestKernelRetryOptionsThreading:
    """The connector's ``_retry_*`` kwargs must be forwarded into the
    kernel client's ``retry_options`` on the use_kernel path (the kernel
    owns the retry loop). Captures the kwargs session.py passes by
    patching ``KernelDatabricksClient`` and inspecting its call args.

    Patching ``KernelDatabricksClient`` requires importing
    ``databricks.sql.backend.kernel.client``, which imports pyarrow at
    module load — so this test is skipped when pyarrow is absent (the
    no-pyarrow CI tier), matching the other kernel tests. The Rust wheel
    is still faked via sys.modules so the kernel extension itself isn't
    needed.
    """

    PACKAGE = "databricks.sql"

    def test_retry_kwargs_threaded_into_kernel_client(self):
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
            )
            try:
                _, kwargs = mock_kernel_client.call_args
                opts = kwargs["retry_options"]
                assert opts["retry_delay_min"] == 2.0
                assert opts["retry_delay_max"] == 90.0
                assert opts["retry_stop_after_attempts_count"] == 10
                assert opts["retry_stop_after_attempts_duration"] == 600.0
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
