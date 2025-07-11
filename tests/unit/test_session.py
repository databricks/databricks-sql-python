import pytest
from unittest.mock import patch, MagicMock, Mock, PropertyMock
import gc

from databricks.sql.thrift_api.TCLIService.ttypes import (
    TOpenSessionResp,
    TSessionHandle,
    THandleIdentifier,
)
from databricks.sql.backend.types import SessionId, BackendType

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
            },
            {
                "server_hostname": "foo",
                "http_path": None,
                "_tls_client_cert_file": "something",
                "_use_cert_as_auth": True,
                "access_token": None,
            },
        ]

        for args in connection_args:
            connection = databricks.sql.connect(**args)
            host, port, http_path, *_ = mock_client_class.call_args[0]
            assert args["server_hostname"] == host
            assert args["http_path"] == http_path
            connection.close()

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_http_header_passthrough(self, mock_client_class):
        http_headers = [("foo", "bar")]
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS, http_headers=http_headers)

        call_args = mock_client_class.call_args[0][3]
        assert ("foo", "bar") in call_args

    @patch("%s.session.ThriftDatabricksClient" % PACKAGE_NAME)
    def test_tls_arg_passthrough(self, mock_client_class):
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
    def test_useragent_header(self, mock_client_class):
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)

        http_headers = mock_client_class.call_args[0][3]
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
        http_headers = mock_client_class.call_args[0][3]
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
        mock_session_config = Mock()
        databricks.sql.connect(
            session_configuration=mock_session_config, **self.DUMMY_CONNECTION_ARGS
        )

        call_kwargs = mock_client_class.return_value.open_session.call_args[1]
        assert call_kwargs["session_configuration"] == mock_session_config

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
