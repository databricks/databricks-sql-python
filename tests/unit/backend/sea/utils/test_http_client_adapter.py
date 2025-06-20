import unittest
from unittest.mock import MagicMock, patch

from databricks.sql.auth.retry import CommandType
from databricks.sql.backend.sea.utils.http_client_adapter import SeaHttpClientAdapter


class TestSeaHttpClientAdapter(unittest.TestCase):
    def setUp(self):
        self.mock_thrift_client = MagicMock()
        self.adapter = SeaHttpClientAdapter(thrift_client=self.mock_thrift_client)

    def test_determine_command_type(self):
        """Test the command type determination logic."""
        # Test statement execution
        self.assertEqual(
            self.adapter._determine_command_type("/api/2.0/sql/statements", "POST"),
            CommandType.EXECUTE_STATEMENT,
        )

        # Test get operation status
        self.assertEqual(
            self.adapter._determine_command_type("/api/2.0/sql/statements/123", "GET"),
            CommandType.GET_OPERATION_STATUS,
        )

        # Test cancel operation
        self.assertEqual(
            self.adapter._determine_command_type(
                "/api/2.0/sql/statements/123/cancel", "POST"
            ),
            CommandType.CLOSE_OPERATION,
        )

        # Test close operation
        self.assertEqual(
            self.adapter._determine_command_type(
                "/api/2.0/sql/statements/123", "DELETE"
            ),
            CommandType.CLOSE_OPERATION,
        )

        # Test close session
        self.assertEqual(
            self.adapter._determine_command_type("/api/2.0/sql/sessions/123", "DELETE"),
            CommandType.CLOSE_SESSION,
        )

        # Test other operations
        self.assertEqual(
            self.adapter._determine_command_type("/api/2.0/sql/sessions", "POST"),
            CommandType.OTHER,
        )

    def test_get_sets_command_type_and_starts_timer(self):
        """Test that GET method sets command type and starts retry timer."""
        self.adapter.get("/api/2.0/sql/statements/123")

        # Verify command type was set
        self.mock_thrift_client.set_retry_command_type.assert_called_once_with(
            CommandType.GET_OPERATION_STATUS
        )

        # Verify timer was started
        self.mock_thrift_client.startRetryTimer.assert_called_once()

        # Verify request was made
        self.mock_thrift_client.make_rest_request.assert_called_once_with(
            "GET", "/api/2.0/sql/statements/123", params=None, headers=None
        )

    def test_post_sets_command_type_and_starts_timer(self):
        """Test that POST method sets command type and starts retry timer."""
        data = {"key": "value"}
        self.adapter.post("/api/2.0/sql/statements", data=data)

        # Verify command type was set
        self.mock_thrift_client.set_retry_command_type.assert_called_once_with(
            CommandType.EXECUTE_STATEMENT
        )

        # Verify timer was started
        self.mock_thrift_client.startRetryTimer.assert_called_once()

        # Verify request was made
        self.mock_thrift_client.make_rest_request.assert_called_once_with(
            "POST", "/api/2.0/sql/statements", data=data, params=None, headers=None
        )

    def test_delete_sets_command_type_and_starts_timer(self):
        """Test that DELETE method sets command type and starts retry timer."""
        self.adapter.delete("/api/2.0/sql/sessions/123")

        # Verify command type was set
        self.mock_thrift_client.set_retry_command_type.assert_called_once_with(
            CommandType.CLOSE_SESSION
        )

        # Verify timer was started
        self.mock_thrift_client.startRetryTimer.assert_called_once()

        # Verify request was made
        self.mock_thrift_client.make_rest_request.assert_called_once_with(
            "DELETE", "/api/2.0/sql/sessions/123", data=None, params=None, headers=None
        )


if __name__ == "__main__":
    unittest.main()
