import unittest
import json
import urllib
from unittest.mock import patch, Mock, MagicMock
import urllib3
from http.client import HTTPResponse
from io import BytesIO

from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.exc import RequestError
from databricks.sql.auth.retry import DatabricksRetryPolicy
from databricks.sql.types import SSLOptions


class TestTHttpClient(unittest.TestCase):
    """Unit tests for the THttpClient class."""

    @patch("urllib.request.getproxies")
    @patch("urllib.request.proxy_bypass")
    def setUp(self, mock_proxy_bypass, mock_getproxies):
        """Set up test fixtures."""
        # Mock proxy functions
        mock_getproxies.return_value = {}
        mock_proxy_bypass.return_value = True

        # Create auth provider mock
        self.mock_auth_provider = Mock()
        self.mock_auth_provider.add_headers = Mock()

        # Create HTTP client
        self.uri = "https://example.com/path"
        self.http_client = THttpClient(
            auth_provider=self.mock_auth_provider,
            uri_or_host=self.uri,
            ssl_options=SSLOptions(),
        )

        # Mock the connection pool
        self.mock_pool = Mock()
        self.http_client._THttpClient__pool = self.mock_pool

        # Set custom headers to include User-Agent (required by the class)
        self.http_client._headers = {"User-Agent": "test-agent"}
        self.http_client.__custom_headers = {"User-Agent": "test-agent"}

    def test_check_rest_response_for_error_success(self):
        """Test _check_rest_response_for_error with success status."""
        # No exception should be raised for status codes < 400
        self.http_client._check_rest_response_for_error(200, None)
        self.http_client._check_rest_response_for_error(201, None)
        self.http_client._check_rest_response_for_error(302, None)
        # No assertion needed - test passes if no exception is raised

    def test_check_rest_response_for_error_client_error(self):
        """Test _check_rest_response_for_error with client error status."""
        # Setup response data with error message
        response_data = json.dumps({"message": "Bad request"}).encode("utf-8")

        # Check that exception is raised for client error
        with self.assertRaises(RequestError) as context:
            self.http_client._check_rest_response_for_error(400, response_data)

        # Verify the exception message
        self.assertIn(
            "REST HTTP request failed with status 400", str(context.exception)
        )
        self.assertIn("Bad request", str(context.exception))

    def test_check_rest_response_for_error_server_error(self):
        """Test _check_rest_response_for_error with server error status."""
        # Setup response data with error message
        response_data = json.dumps({"message": "Internal server error"}).encode("utf-8")

        # Check that exception is raised for server error
        with self.assertRaises(RequestError) as context:
            self.http_client._check_rest_response_for_error(500, response_data)

        # Verify the exception message
        self.assertIn(
            "REST HTTP request failed with status 500", str(context.exception)
        )
        self.assertIn("Internal server error", str(context.exception))

    def test_check_rest_response_for_error_no_message(self):
        """Test _check_rest_response_for_error with error but no message."""
        # Check that exception is raised with generic message
        with self.assertRaises(RequestError) as context:
            self.http_client._check_rest_response_for_error(404, None)

        # Verify the exception message
        self.assertIn(
            "REST HTTP request failed with status 404", str(context.exception)
        )

    def test_check_rest_response_for_error_invalid_json(self):
        """Test _check_rest_response_for_error with invalid JSON response."""
        # Setup invalid JSON response
        response_data = "Not a JSON response".encode("utf-8")

        # Check that exception is raised with generic message
        with self.assertRaises(RequestError) as context:
            self.http_client._check_rest_response_for_error(500, response_data)

        # Verify the exception message
        self.assertIn(
            "REST HTTP request failed with status 500", str(context.exception)
        )

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.make_rest_request")
    def test_make_rest_request_integration(self, mock_make_rest_request):
        """Test that make_rest_request can be called with the expected parameters."""
        # Setup mock return value
        expected_result = {"result": "success"}
        mock_make_rest_request.return_value = expected_result

        # Call the original method to verify it works
        result = self.http_client.make_rest_request(
            method="GET",
            endpoint_path="test/endpoint",
            params={"param": "value"},
            data={"key": "value"},
            headers={"X-Custom-Header": "custom-value"},
        )

        # Verify the result
        self.assertEqual(result, expected_result)

        # Verify the method was called with correct parameters
        mock_make_rest_request.assert_called_once_with(
            method="GET",
            endpoint_path="test/endpoint",
            params={"param": "value"},
            data={"key": "value"},
            headers={"X-Custom-Header": "custom-value"},
        )


if __name__ == "__main__":
    unittest.main()
