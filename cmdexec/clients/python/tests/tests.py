import grpc

import unittest
from unittest.mock import patch, MagicMock, Mock

import cmdexec.clients.python.command_exec_client
from cmdexec.clients.python.errors import InterfaceError, OperationalError


class TestConnection(unittest.TestCase):
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
                cmdexec.clients.python.command_exec_client.connect(**args)
                self.assertIn("HOST and PORT", ie.message)

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    def test_rpc_error_will_throw_operational_exception_during_connection(self, mock_client_class):
        instance = mock_client_class.return_value
        instance.make_request.side_effect = grpc.RpcError
        good_connection_args = {"HOST": 1, "PORT": 1}

        with self.assertRaises(OperationalError):
            cmdexec.clients.python.command_exec_client.connect(**good_connection_args)

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    def test_rpc_error_will_throw_operational_exception_during_connection_close(
            self, mock_client_class):
        instance = mock_client_class.return_value
        mock_response = Mock()
        mock_response.id = b'\x22'
        instance.make_request.side_effect = [mock_response, grpc.RpcError]
        good_connection_args = {"HOST": 1, "PORT": 1}

        connection = cmdexec.clients.python.command_exec_client.connect(**good_connection_args)

        with self.assertRaises(OperationalError):
            connection.close()

    @patch("cmdexec.clients.python.command_exec_client.CmdExecBaseHttpClient")
    def test_close_uses_the_correct_session_id(self, mock_client_class):
        instance = mock_client_class.return_value
        mock_response = Mock()
        mock_response.id = b'\x22'
        instance.make_request.return_value = mock_response
        good_connection_args = {"HOST": 1, "PORT": 1}

        connection = cmdexec.clients.python.command_exec_client.connect(**good_connection_args)
        connection.close()

        # Check the close session request has an id of x22
        _, close_session_request = instance.make_request.call_args[0]
        self.assertEqual(close_session_request.id, mock_response.id)


if __name__ == '__main__':
    unittest.main()
