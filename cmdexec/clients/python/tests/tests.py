import re
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
import itertools
from decimal import Decimal
from datetime import datetime

import databricks.sql
import databricks.sql.client as client
from databricks.sql import InterfaceError, DatabaseError, Error

from test_fetches import FetchTests
from test_thrift_backend import ThriftBackendTestSuite
from test_arrow_queue import ArrowQueueSuite


class ClientTestSuite(unittest.TestCase):
    """
    Unit tests for isolated client behaviour. See
    qa/test/cmdexec/python/suites/simple_connection_test.py for integration tests that
    interact with the server.
    """

    PACKAGE_NAME = "databricks.sql"
    DUMMY_CONNECTION_ARGS = {
        "server_hostname": "foo",
        "http_path": "dummy_path",
        "access_token": "tok",
    }

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_close_uses_the_correct_session_id(self, mock_client_class):
        instance = mock_client_class.return_value
        instance.open_session.return_value = b'\x22'

        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        connection.close()

        # Check the close session request has an id of x22
        close_session_id = instance.close_session.call_args[0][0]
        self.assertEqual(close_session_id, b'\x22')

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_auth_args(self, mock_client_class):
        # Test that the following auth args work:
        # token = foo,
        # token = None, _username = foo, _password = bar
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
                "_username": "foo",
                "_password": "bar",
                "access_token": None,
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
            host, port, http_path, _ = mock_client_class.call_args[0]
            self.assertEqual(args["server_hostname"], host)
            self.assertEqual(args["http_path"], http_path)
            connection.close()

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_metadata_passthrough(self, mock_client_class):
        metadata = [("foo", "bar")]
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS, metadata=metadata)

        http_headers = mock_client_class.call_args[0][3]
        self.assertIn(("foo", "bar"), http_headers)

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_authtoken_passthrough(self, mock_client_class):
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)

        headers = mock_client_class.call_args[0][3]

        self.assertIn(("Authorization", "Bearer tok"), headers)

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_tls_arg_passthrough(self, mock_client_class):
        databricks.sql.connect(
            **self.DUMMY_CONNECTION_ARGS,
            _tls_verify_hostname="hostname",
            _tls_trusted_ca_file="trusted ca file",
            _tls_client_cert_key_file="trusted client cert",
            _tls_client_cert_key_password="key password",
        )

        kwargs = mock_client_class.call_args[1]
        self.assertEqual(kwargs["_tls_verify_hostname"], "hostname")
        self.assertEqual(kwargs["_tls_trusted_ca_file"], "trusted ca file")
        self.assertEqual(kwargs["_tls_client_cert_key_file"], "trusted client cert")
        self.assertEqual(kwargs["_tls_client_cert_key_password"], "key password")

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_useragent_header(self, mock_client_class):
        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)

        http_headers = mock_client_class.call_args[0][3]
        user_agent_header = ("User-Agent", "{}/{}".format(databricks.sql.USER_AGENT_NAME,
                                                          databricks.sql.__version__))
        self.assertIn(user_agent_header, http_headers)

        databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS, _user_agent_entry="foobar")
        user_agent_header_with_entry = ("User-Agent", "{}/{} ({})".format(
            databricks.sql.USER_AGENT_NAME, databricks.sql.__version__, "foobar"))
        http_headers = mock_client_class.call_args[0][3]
        self.assertIn(user_agent_header_with_entry, http_headers)

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    @patch("%s.client.ResultSet" % PACKAGE_NAME)
    def test_closing_connection_closes_commands(self, mock_result_set_class, mock_client_class):
        # Test once with has_been_closed_server side, once without
        for closed in (True, False):
            with self.subTest(closed=closed):
                mock_result_set_class.return_value = Mock()
                connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
                cursor = connection.cursor()
                cursor.execute("SELECT 1;")
                connection.close()

                self.assertTrue(mock_result_set_class.return_value.has_been_closed_server_side)
                mock_result_set_class.return_value.close.assert_called_once_with()

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_cant_open_cursor_on_closed_connection(self, mock_client_class):
        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        self.assertTrue(connection.open)
        connection.close()
        self.assertFalse(connection.open)
        with self.assertRaises(Error) as cm:
            connection.cursor()
        self.assertIn("closed", str(cm.exception))

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    @patch("%s.client.Cursor" % PACKAGE_NAME)
    def test_arraysize_buffer_size_passthrough(self, mock_cursor_class, mock_client_class):
        connection = databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS)
        connection.cursor(arraysize=999, buffer_size_bytes=1234)
        kwargs = mock_cursor_class.call_args[1]

        self.assertEqual(kwargs["arraysize"], 999)
        self.assertEqual(kwargs["result_buffer_size_bytes"], 1234)

    def test_closing_result_set_with_closed_connection_soft_closes_commands(self):
        mock_connection = Mock()
        mock_backend = Mock()
        result_set = client.ResultSet(
            connection=mock_connection, thrift_backend=mock_backend, execute_response=Mock())
        mock_connection.open = False

        result_set.close()

        self.assertFalse(mock_backend.close_command.called)
        self.assertTrue(result_set.has_been_closed_server_side)

    def test_closing_result_set_hard_closes_commands(self):
        mock_results_response = Mock()
        mock_results_response.has_been_closed_server_side = False
        mock_connection = Mock()
        mock_thrift_backend = Mock()
        mock_connection.open = True
        result_set = client.ResultSet(mock_connection, mock_results_response, mock_thrift_backend)

        result_set.close()

        mock_thrift_backend.close_command.assert_called_once_with(
            mock_results_response.command_handle)

    @patch("%s.client.ResultSet" % PACKAGE_NAME)
    def test_executing_multiple_commands_uses_the_most_recent_command(self, mock_result_set_class):
        mock_result_sets = [Mock(), Mock()]
        mock_result_set_class.side_effect = mock_result_sets

        cursor = client.Cursor(Mock(), Mock())
        cursor.execute("SELECT 1;")
        cursor.execute("SELECT 1;")

        mock_result_sets[0].close.assert_called_once_with()
        mock_result_sets[1].close.assert_not_called()

        cursor.fetchall()

        mock_result_sets[0].fetchall.assert_not_called()
        mock_result_sets[1].fetchall.assert_called_once_with()

    def test_closed_cursor_doesnt_allow_operations(self):
        cursor = client.Cursor(Mock(), Mock())
        cursor.close()

        with self.assertRaises(Error) as e:
            cursor.execute("SELECT 1;")
            self.assertIn("closed", e.msg)

        with self.assertRaises(Error) as e:
            cursor.fetchall()
            self.assertIn("closed", e.msg)

    def test_negative_fetch_throws_exception(self):
        result_set = client.ResultSet(Mock(), Mock(), Mock())

        with self.assertRaises(ValueError) as e:
            result_set.fetchmany(-1)

    def test_context_manager_closes_cursor(self):
        mock_close = Mock()
        with client.Cursor(Mock(), Mock()) as cursor:
            cursor.close = mock_close
        mock_close.assert_called_once_with()

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_context_manager_closes_connection(self, mock_client_class):
        instance = mock_client_class.return_value
        instance.open_session.return_value = b'\x22'

        with databricks.sql.connect(**self.DUMMY_CONNECTION_ARGS) as connection:
            pass

        # Check the close session request has an id of x22
        close_session_id = instance.close_session.call_args[0][0]
        self.assertEqual(close_session_id, b'\x22')

    def dict_product(self, dicts):
        """
        Generate cartesion product of values in input dictionary, outputting a dictionary
        for each combination.
        >>> list(dict_product(dict(number=[1,2], character='ab')))
        [{'character': 'a', 'number': 1},
        {'character': 'a', 'number': 2},
        {'character': 'b', 'number': 1},
        {'character': 'b', 'number': 2}]
        """
        return (dict(zip(dicts.keys(), x)) for x in itertools.product(*dicts.values()))

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_get_schemas_parameters_passed_to_thrift_backend(self, mock_thrift_backend):
        req_args_combinations = self.dict_product(
            dict(
                catalog_name=["NOT_SET", None, "catalog_pattern"],
                schema_name=["NOT_SET", None, "schema_pattern"]))

        for req_args in req_args_combinations:
            req_args = {k: v for k, v in req_args.items() if v != "NOT_SET"}
            with self.subTest(req_args=req_args):
                mock_thrift_backend = Mock()

                cursor = client.Cursor(Mock(), mock_thrift_backend)
                cursor.schemas(**req_args)

                call_args = mock_thrift_backend.get_schemas.call_args[1]
                for k, v in req_args.items():
                    self.assertEqual(v, call_args[k])

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_get_tables_parameters_passed_to_thrift_backend(self, mock_thrift_backend):
        req_args_combinations = self.dict_product(
            dict(
                catalog_name=["NOT_SET", None, "catalog_pattern"],
                schema_name=["NOT_SET", None, "schema_pattern"],
                table_name=["NOT_SET", None, "table_pattern"],
                table_types=["NOT_SET", [], ["type1", "type2"]]))

        for req_args in req_args_combinations:
            req_args = {k: v for k, v in req_args.items() if v != "NOT_SET"}
            with self.subTest(req_args=req_args):
                mock_thrift_backend = Mock()

                cursor = client.Cursor(Mock(), mock_thrift_backend)
                cursor.tables(**req_args)

                call_args = mock_thrift_backend.get_tables.call_args[1]
                for k, v in req_args.items():
                    self.assertEqual(v, call_args[k])

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_get_columns_parameters_passed_to_thrift_backend(self, mock_thrift_backend):
        req_args_combinations = self.dict_product(
            dict(
                catalog_name=["NOT_SET", None, "catalog_pattern"],
                schema_name=["NOT_SET", None, "schema_pattern"],
                table_name=["NOT_SET", None, "table_pattern"],
                column_name=["NOT_SET", None, "column_pattern"]))

        for req_args in req_args_combinations:
            req_args = {k: v for k, v in req_args.items() if v != "NOT_SET"}
            with self.subTest(req_args=req_args):
                mock_thrift_backend = Mock()

                cursor = client.Cursor(Mock(), mock_thrift_backend)
                cursor.columns(**req_args)

                call_args = mock_thrift_backend.get_columns.call_args[1]
                for k, v in req_args.items():
                    self.assertEqual(v, call_args[k])

    def test_cancel_command_calls_the_backend(self):
        mock_thrift_backend = Mock()
        cursor = client.Cursor(Mock(), mock_thrift_backend)
        mock_op_handle = Mock()
        cursor.active_op_handle = mock_op_handle
        cursor.cancel()
        self.assertTrue(mock_thrift_backend.cancel_command.called_with(mock_op_handle))

    @patch("databricks.sql.client.logger")
    def test_cancel_command_will_issue_warning_for_cancel_with_no_executing_command(
            self, logger_instance):
        mock_thrift_backend = Mock()
        cursor = client.Cursor(Mock(), mock_thrift_backend)
        cursor.cancel()

        self.assertTrue(logger_instance.warning.called)
        self.assertFalse(mock_thrift_backend.cancel_command.called)

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_max_number_of_retries_passthrough(self, mock_client_class):
        databricks.sql.connect(_retry_stop_after_attempts_count=54, **self.DUMMY_CONNECTION_ARGS)

        self.assertEqual(mock_client_class.call_args[1]["_retry_stop_after_attempts_count"], 54)

    def test_version_is_canonical(self):
        version = databricks.sql.__version__
        canonical_version_re = r'^([1-9][0-9]*!)?(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*((a|b|rc)' \
                               r'(0|[1-9][0-9]*))?(\.post(0|[1-9][0-9]*))?(\.dev(0|[1-9][0-9]*))?$'
        self.assertIsNotNone(re.match(canonical_version_re, version))

    @patch("%s.client.ThriftBackend" % PACKAGE_NAME)
    def test_configuration_passthrough(self, mock_client_class):
        mock_session_config = Mock()
        databricks.sql.connect(
            session_configuration=mock_session_config, **self.DUMMY_CONNECTION_ARGS)

        self.assertEqual(mock_client_class.return_value.open_session.call_args[0][0],
                         mock_session_config)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    loader = unittest.TestLoader()
    test_classes = [ClientTestSuite, FetchTests, ThriftBackendTestSuite, ArrowQueueSuite]
    suites_list = []
    for test_class in test_classes:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    suite = unittest.TestSuite(suites_list)
    test_result = unittest.TextTestRunner().run(suite)

    if len(test_result.errors) != 0 or len(test_result.failures) != 0:
        sys.exit(1)
