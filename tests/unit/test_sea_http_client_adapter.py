"""
Tests for the SEA HTTP client adapter.

This module contains tests for the SeaHttpClientAdapter class, which provides
an adapter for using ThriftHttpClient with the SEA API.
"""

import pytest
from unittest.mock import patch, MagicMock

from databricks.sql.backend.sea.utils.http_client_adapter import SeaHttpClientAdapter
from databricks.sql.auth.retry import CommandType


class TestSeaHttpClientAdapter:
    """Test suite for the SeaHttpClientAdapter class."""

    @pytest.fixture
    def mock_thrift_client(self):
        """Create a mock ThriftHttpClient."""
        mock_client = MagicMock()
        mock_client.make_rest_request.return_value = {}
        return mock_client

    @pytest.fixture
    def adapter(self, mock_thrift_client):
        """Create a SeaHttpClientAdapter instance with a mock ThriftHttpClient."""
        return SeaHttpClientAdapter(thrift_client=mock_thrift_client)

    def test_determine_command_type(self, adapter):
        """Test the command type determination logic."""
        # Test statement execution
        assert (
            adapter._determine_command_type("/api/2.0/sql/statements", "POST")
            == CommandType.EXECUTE_STATEMENT
        )

        # Test get operation status
        assert (
            adapter._determine_command_type("/api/2.0/sql/statements/123", "GET")
            == CommandType.GET_OPERATION_STATUS
        )

        # Test cancel operation
        assert (
            adapter._determine_command_type(
                "/api/2.0/sql/statements/123/cancel", "POST"
            )
            == CommandType.CLOSE_OPERATION
        )

        # Test close operation
        assert (
            adapter._determine_command_type("/api/2.0/sql/statements/123", "DELETE")
            == CommandType.CLOSE_OPERATION
        )

        # Test close session
        assert (
            adapter._determine_command_type("/api/2.0/sql/sessions/123", "DELETE")
            == CommandType.CLOSE_SESSION
        )

        # Test other operations
        assert (
            adapter._determine_command_type("/api/2.0/sql/sessions", "POST")
            == CommandType.OTHER
        )
        assert (
            adapter._determine_command_type("/api/2.0/sql/other", "GET")
            == CommandType.OTHER
        )

    def test_http_methods_set_command_type(self, adapter, mock_thrift_client):
        """Test that HTTP methods set the command type and start the retry timer."""
        # Test GET method
        adapter.get("/api/2.0/sql/statements/123")
        mock_thrift_client.set_retry_command_type.assert_called_with(
            CommandType.GET_OPERATION_STATUS
        )
        mock_thrift_client.startRetryTimer.assert_called_once()
        mock_thrift_client.make_rest_request.assert_called_with(
            "GET", "/api/2.0/sql/statements/123", params=None, headers=None
        )

        # Reset mocks
        mock_thrift_client.reset_mock()

        # Test POST method
        adapter.post("/api/2.0/sql/statements")
        mock_thrift_client.set_retry_command_type.assert_called_with(
            CommandType.EXECUTE_STATEMENT
        )
        mock_thrift_client.startRetryTimer.assert_called_once()
        mock_thrift_client.make_rest_request.assert_called_with(
            "POST", "/api/2.0/sql/statements", data=None, params=None, headers=None
        )

        # Reset mocks
        mock_thrift_client.reset_mock()

        # Test DELETE method
        adapter.delete("/api/2.0/sql/statements/123")
        mock_thrift_client.set_retry_command_type.assert_called_with(
            CommandType.CLOSE_OPERATION
        )
        mock_thrift_client.startRetryTimer.assert_called_once()
        mock_thrift_client.make_rest_request.assert_called_with(
            "DELETE",
            "/api/2.0/sql/statements/123",
            data=None,
            params=None,
            headers=None,
        )

    def test_http_methods_with_parameters(self, adapter, mock_thrift_client):
        """Test HTTP methods with parameters."""
        # Test GET with parameters
        params = {"param1": "value1"}
        headers = {"header1": "value1"}
        adapter.get("/api/2.0/sql/statements/123", params=params, headers=headers)
        mock_thrift_client.make_rest_request.assert_called_with(
            "GET", "/api/2.0/sql/statements/123", params=params, headers=headers
        )

        # Reset mocks
        mock_thrift_client.reset_mock()

        # Test POST with data and parameters
        data = {"key": "value"}
        adapter.post(
            "/api/2.0/sql/statements", data=data, params=params, headers=headers
        )
        mock_thrift_client.make_rest_request.assert_called_with(
            "POST", "/api/2.0/sql/statements", data=data, params=params, headers=headers
        )

        # Reset mocks
        mock_thrift_client.reset_mock()

        # Test DELETE with data and parameters
        adapter.delete(
            "/api/2.0/sql/statements/123", data=data, params=params, headers=headers
        )
        mock_thrift_client.make_rest_request.assert_called_with(
            "DELETE",
            "/api/2.0/sql/statements/123",
            data=data,
            params=params,
            headers=headers,
        )
