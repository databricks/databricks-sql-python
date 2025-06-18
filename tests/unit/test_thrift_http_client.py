import unittest
import json
import urllib
from unittest.mock import patch, Mock, MagicMock, PropertyMock, call
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

        # Set timeout
        self.http_client._THttpClient__timeout = None

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

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    @patch(
        "databricks.sql.auth.thrift_http_client.THttpClient._check_rest_response_for_error"
    )
    def test_make_rest_request_success(self, mock_check_error, mock_open, mock_is_open):
        """Test the make_rest_request method with a successful response."""
        # Setup mocks
        mock_is_open.return_value = False  # To trigger open() call

        # Create a mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.reason = "OK"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.data = json.dumps({"result": "success"}).encode("utf-8")

        # Configure the mock pool to return our mock response
        self.mock_pool.request.return_value = mock_response

        # Call the method under test
        result = self.http_client.make_rest_request(
            method="GET", endpoint_path="test/endpoint", params={"param": "value"}
        )

        # Verify the result
        self.assertEqual(result, {"result": "success"})

        # Verify open was called
        mock_open.assert_called_once()

        # Verify the request was made with correct parameters
        self.mock_pool.request.assert_called_once()

        # Check URL contains the parameters
        args, kwargs = self.mock_pool.request.call_args
        self.assertIn("test/endpoint?param=value", kwargs["url"])

        # Verify error check was called
        mock_check_error.assert_called_once_with(200, mock_response.data)

        # Verify auth headers were added
        self.mock_auth_provider.add_headers.assert_called_once()

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    @patch(
        "databricks.sql.auth.thrift_http_client.THttpClient._check_rest_response_for_error"
    )
    def test_make_rest_request_with_data(
        self, mock_check_error, mock_open, mock_is_open
    ):
        """Test the make_rest_request method with data payload."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Create a mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.reason = "OK"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.data = json.dumps({"result": "success"}).encode("utf-8")

        # Configure the mock pool to return our mock response
        self.mock_pool.request.return_value = mock_response

        # Call the method under test with data
        data = {"key": "value"}
        result = self.http_client.make_rest_request(
            method="POST", endpoint_path="test/endpoint", data=data
        )

        # Verify the result
        self.assertEqual(result, {"result": "success"})

        # Verify open was not called (connection already open)
        mock_open.assert_not_called()

        # Verify the request was made with correct parameters
        self.mock_pool.request.assert_called_once()

        # Check body contains the JSON data
        args, kwargs = self.mock_pool.request.call_args
        self.assertEqual(kwargs["body"], json.dumps(data).encode("utf-8"))

        # Verify error check was called
        mock_check_error.assert_called_once_with(200, mock_response.data)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    @patch(
        "databricks.sql.auth.thrift_http_client.THttpClient._check_rest_response_for_error"
    )
    def test_make_rest_request_with_custom_headers(
        self, mock_check_error, mock_open, mock_is_open
    ):
        """Test the make_rest_request method with custom headers."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Create a mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.reason = "OK"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.data = json.dumps({"result": "success"}).encode("utf-8")

        # Configure the mock pool to return our mock response
        self.mock_pool.request.return_value = mock_response

        # Call the method under test with custom headers
        custom_headers = {"X-Custom-Header": "custom-value"}
        result = self.http_client.make_rest_request(
            method="GET", endpoint_path="test/endpoint", headers=custom_headers
        )

        # Verify the result
        self.assertEqual(result, {"result": "success"})

        # Verify the request was made with correct headers
        self.mock_pool.request.assert_called_once()

        # Check headers contain the custom header
        args, kwargs = self.mock_pool.request.call_args
        headers = kwargs["headers"]
        self.assertIn("X-Custom-Header", headers)
        self.assertEqual(headers["X-Custom-Header"], "custom-value")
        self.assertEqual(headers["Content-Type"], "application/json")

        # Verify error check was called
        mock_check_error.assert_called_once_with(200, mock_response.data)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    def test_make_rest_request_http_error(self, mock_open, mock_is_open):
        """Test the make_rest_request method with an HTTP error."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Configure the mock pool to raise an HTTP error
        http_error = urllib3.exceptions.HTTPError("HTTP Error")
        self.mock_pool.request.side_effect = http_error

        # Call the method under test and expect an exception
        with self.assertRaises(RequestError) as context:
            self.http_client.make_rest_request(
                method="GET", endpoint_path="test/endpoint"
            )

        # Verify the exception message
        self.assertIn("REST HTTP request failed", str(context.exception))
        self.assertIn("HTTP Error", str(context.exception))

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    @patch(
        "databricks.sql.auth.thrift_http_client.THttpClient._check_rest_response_for_error"
    )
    def test_make_rest_request_empty_response(
        self, mock_check_error, mock_open, mock_is_open
    ):
        """Test the make_rest_request method with an empty response."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Create a mock response with empty data
        mock_response = Mock()
        mock_response.status = 204  # No Content
        mock_response.reason = "No Content"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.data = None  # Empty response

        # Configure the mock pool to return our mock response
        self.mock_pool.request.return_value = mock_response

        # Call the method under test
        result = self.http_client.make_rest_request(
            method="DELETE", endpoint_path="test/endpoint/123"
        )

        # Verify the result is an empty dict
        self.assertEqual(result, {})

        # Verify error check was called with None data
        mock_check_error.assert_called_once_with(204, None)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    def test_make_rest_request_no_response(self, mock_open, mock_is_open):
        """Test the make_rest_request method with no response."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Configure the mock pool to return None
        self.mock_pool.request.return_value = None

        # Call the method under test and expect an exception
        with self.assertRaises(ValueError) as context:
            self.http_client.make_rest_request(
                method="GET", endpoint_path="test/endpoint"
            )

        # Verify the exception message
        self.assertEqual(str(context.exception), "No response received from server")

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    @patch(
        "databricks.sql.auth.thrift_http_client.THttpClient._check_rest_response_for_error"
    )
    def test_make_rest_request_with_retry_policy(
        self, mock_check_error, mock_open, mock_is_open
    ):
        """Test the make_rest_request method with a retry policy."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Create a mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.reason = "OK"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.data = json.dumps({"result": "success"}).encode("utf-8")

        # Configure the mock pool to return our mock response
        self.mock_pool.request.return_value = mock_response

        # Create a retry policy mock
        mock_retry_policy = Mock(spec=DatabricksRetryPolicy)

        # Set the retry policy on the client
        self.http_client.retry_policy = mock_retry_policy

        # Call the method under test
        result = self.http_client.make_rest_request(
            method="GET", endpoint_path="test/endpoint"
        )

        # Verify the result
        self.assertEqual(result, {"result": "success"})

        # Verify the request was made with the retry policy
        self.mock_pool.request.assert_called_once()

        # Check retries parameter
        args, kwargs = self.mock_pool.request.call_args
        self.assertEqual(kwargs["retries"], mock_retry_policy)

    @patch("databricks.sql.auth.thrift_http_client.THttpClient.isOpen")
    @patch("databricks.sql.auth.thrift_http_client.THttpClient.open")
    @patch(
        "databricks.sql.auth.thrift_http_client.THttpClient._check_rest_response_for_error"
    )
    def test_make_rest_request_invalid_json_response(
        self, mock_check_error, mock_open, mock_is_open
    ):
        """Test the make_rest_request method with invalid JSON response."""
        # Setup mocks
        mock_is_open.return_value = True  # Connection is already open

        # Create a mock response with invalid JSON
        mock_response = Mock()
        mock_response.status = 200
        mock_response.reason = "OK"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.data = "Not a valid JSON".encode("utf-8")

        # Configure the mock pool to return our mock response
        self.mock_pool.request.return_value = mock_response

        # Call the method under test and expect a JSON decode error
        with self.assertRaises(json.JSONDecodeError):
            self.http_client.make_rest_request(
                method="GET", endpoint_path="test/endpoint"
            )

        # Verify error check was called before the JSON parsing
        mock_check_error.assert_called_once_with(200, mock_response.data)


if __name__ == "__main__":
    unittest.main()
