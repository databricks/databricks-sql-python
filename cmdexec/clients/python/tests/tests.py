import sys
import unittest
from unittest.mock import patch, MagicMock, Mock

import cmdexec.clients.python.command_exec_client as command_exec_client
import cmdexec.clients.python.api.messages_pb2 as command_pb2
from cmdexec.clients.python.errors import InterfaceError, DatabaseError, Error
from cmdexec.clients.python.tests.test_fetches import FetchTests


class SimpleTests(unittest.TestCase):
    """
    Unit tests for isolated client behaviour. See
    qa/test/cmdexec/python/suites/simple_connection_test.py for integration tests that
    interact with the server.
    """

    def test_missing_params_throws_interface_exception(self):
        bad_connection_args = [
            {
                "HOST": 'host'
            },
            {
                "host": 'host',
                "PORT": 1
            },
            {},
        ]

        for args in bad_connection_args:
            with self.assertRaises(InterfaceError) as ie:
                command_exec_client.connect(**args)
                self.assertIn("HOST and PORT", ie.message)

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    def test_close_uses_the_correct_session_id(self, mock_client_class):
        instance = mock_client_class.return_value
        mock_response = Mock()
        mock_response.id = b'\x22'
        instance.make_request.return_value = mock_response
        good_connection_args = {"HOST": 1, "PORT": 1}

        connection = command_exec_client.connect(**good_connection_args)
        connection.close()

        # Check the close session request has an id of x22
        _, close_session_request = instance.make_request.call_args[0]
        self.assertEqual(close_session_request.id, mock_response.id)

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    @patch("cmdexec.clients.python.command_exec_client.ResultSet")
    def test_closing_connection_closes_commands(self, mock_result_set_class, mock_client_class):
        # Test once with has_been_closed_server side, once without
        for closed in (True, False):
            with self.subTest(closed=closed):
                instance = mock_client_class.return_value
                mock_response = Mock()
                mock_response.id = b'\x22'
                instance.make_request.return_value = mock_response
                instance.stub.CloseCommand = Mock()
                mock_response.status.state = command_pb2.SUCCESS
                mock_response.closed = closed
                mock_result_set = Mock()
                mock_result_set_class.return_value = mock_result_set

                good_connection_args = {"HOST": 1, "PORT": 1}
                connection = command_exec_client.connect(**good_connection_args)
                cursor = connection.cursor()
                cursor.execute("SELECT 1;")
                connection.close()

                self.assertTrue(mock_result_set.has_been_closed_server_side)
                mock_result_set.close.assert_called_once_with()

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    def test_cant_open_cursor_on_closed_connection(self, mock_client_class):
        instance = mock_client_class.return_value
        mock_response = Mock()
        mock_response.id = b'\x22'
        instance.make_request.return_value = mock_response
        good_connection_args = {"HOST": 1, "PORT": 1}
        connection = command_exec_client.connect(**good_connection_args)
        self.assertTrue(connection.open)
        connection.close()
        self.assertFalse(connection.open)
        with self.assertRaises(Error) as e:
            cursor = connection.cursor()
            self.assertIn("closed", e.msg)

    @patch("pyarrow.ipc.open_stream")
    def test_closing_result_set_with_closed_connection_soft_closes_commands(
            self, pyarrow_ipc_open_stream):
        mock_connection = Mock()
        mock_response = Mock()
        mock_response.id = b'\x22'
        mock_connection.base_client.make_request.return_value = mock_response
        result_set = command_exec_client.ResultSet(
            mock_connection,
            b'\x10',
            command_pb2.SUCCESS,
            False,
            arrow_ipc_stream=Mock(),
            num_valid_rows=0,
            has_more_rows=False)
        mock_connection.open = False

        result_set.close()

        with self.assertRaises(AssertionError):
            mock_connection.base_client.make_request.assert_called_with(
                mock_connection.base_client.stub.CloseCommand,
                command_pb2.CloseCommandRequest(id=b'\x10'))

    @patch("pyarrow.ipc.open_stream")
    def test_closing_result_set_hard_closes_commands(self, pyarrow_ipc_open_stream):
        mock_connection = Mock()
        mock_response = Mock()
        mock_response.id = b'\x22'
        mock_response.results.start_row_offset = 0
        mock_connection.base_client.make_request.return_value = mock_response
        result_set = command_exec_client.ResultSet(
            mock_connection, b'\x10', command_pb2.SUCCESS, False, has_more_rows=False)
        mock_connection.open = True

        result_set.close()

        mock_connection.base_client.make_request.assert_called_with(
            mock_connection.base_client.stub.CloseCommand,
            command_pb2.CloseCommandRequest(id=b'\x10'))

    @patch("cmdexec.clients.python.command_exec_client.ResultSet")
    def test_executing_multiple_commands_uses_the_most_recent_command(self, mock_result_set_class):
        mock_client = Mock()
        mock_response = Mock()
        mock_connection = Mock()
        mock_response.id = b'\x22'
        mock_response.status.state = command_pb2.SUCCESS
        mock_client.make_request.return_value = mock_response
        mock_connection.session_id = b'\x33'
        mock_connection.base_client = mock_client
        mock_result_sets = [Mock(), Mock()]
        mock_result_set_class.side_effect = mock_result_sets

        cursor = command_exec_client.Cursor(mock_connection)
        cursor.execute("SELECT 1;")
        cursor.execute("SELECT 1;")

        mock_result_sets[0].close.assert_called_once_with()
        mock_result_sets[1].close.assert_not_called()

        cursor.fetchall()

        mock_result_sets[0].fetchall.assert_not_called()
        mock_result_sets[1].fetchall.assert_called_once_with()

    def test_closed_cursor_doesnt_allow_operations(self):
        mock_connection = Mock()
        mock_response = Mock()
        mock_response.id = b'\x22'
        mock_response.status.state = command_pb2.SUCCESS
        mock_connection.base_client.make_request.return_value = mock_response

        cursor = command_exec_client.Cursor(mock_connection)
        cursor.close()

        with self.assertRaises(Error) as e:
            cursor.execute("SELECT 1;")
            self.assertIn("closed", e.msg)

        with self.assertRaises(Error) as e:
            cursor.fetchall()
            self.assertIn("closed", e.msg)

    @patch("pyarrow.ipc.open_stream")
    def test_negative_fetch_throws_exception(self, pyarrow_ipc_open_stream_mock):
        mock_connection = Mock()
        mock_response = Mock()
        mock_response.id = b'\x22'
        mock_response.results.start_row_offset = 0
        mock_response.status.state = command_pb2.SUCCESS
        mock_connection.base_client.make_request.return_value = mock_response

        result_set = command_exec_client.ResultSet(
            mock_connection, b'\x22', command_pb2.SUCCESS, Mock(), has_more_rows=False)

        with self.assertRaises(ValueError) as e:
            result_set.fetchmany(-1)

    def test_context_manager_closes_cursor(self):
        mock_close = Mock()
        with command_exec_client.Cursor(Mock()) as cursor:
            cursor.close = mock_close
        mock_close.assert_called_once_with()

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    def test_context_manager_closes_connection(self, mock_client_class):
        instance = mock_client_class.return_value
        mock_response = Mock()
        mock_response.id = b'\x22'
        instance.make_request.return_value = mock_response
        good_connection_args = {"HOST": 1, "PORT": 1}
        mock_close = Mock()

        with command_exec_client.connect(**good_connection_args) as connection:
            connection.close = mock_close
        mock_close.assert_called_once_with()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    loader = unittest.TestLoader()
    test_classes = [SimpleTests, FetchTests]
    suites_list = []
    for test_class in test_classes:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    suite = unittest.TestSuite(suites_list)
    test_result = unittest.TextTestRunner().run(suite)

    if len(test_result.errors) != 0 or len(test_result.failures) != 0:
        sys.exit(1)
