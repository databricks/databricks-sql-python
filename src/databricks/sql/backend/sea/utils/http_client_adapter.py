"""
HTTP client adapter for the Statement Execution API (SEA).

This module provides an adapter that uses the ThriftHttpClient for HTTP operations
but provides a simplified interface for HTTP methods.
"""

import logging
from typing import Dict, Optional, Any

from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.auth.retry import CommandType

logger = logging.getLogger(__name__)


class SeaHttpClientAdapter:
    """
    Adapter for using ThriftHttpClient with SEA API.

    This class provides a simplified interface for HTTP methods while using
    ThriftHttpClient for the actual HTTP operations.
    """

    # SEA API paths
    BASE_PATH = "/api/2.0/sql/"

    def __init__(
        self,
        thrift_client: THttpClient,
    ):
        """
        Initialize the SEA HTTP client adapter.

        Args:
            thrift_client: ThriftHttpClient instance to use for HTTP operations
        """
        self.thrift_client = thrift_client

    def _determine_command_type(
        self, path: str, method: str, data: Optional[Dict[str, Any]] = None
    ) -> CommandType:
        """
        Determine the CommandType based on the request path and method.

        Args:
            path: API endpoint path
            method: HTTP method (GET, POST, DELETE)
            data: Request payload data

        Returns:
            CommandType: The appropriate CommandType enum value
        """
        # Extract the base path component (e.g., "sessions", "statements")
        path_parts = path.strip("/").split("/")
        base_path = path_parts[-1] if path_parts else ""

        # Check for specific operations based on path and method
        if "statements" in path:
            if method == "POST" and "cancel" in path:
                return CommandType.CLOSE_OPERATION
            elif method == "POST" and "cancel" not in path:
                return CommandType.EXECUTE_STATEMENT
            elif method == "GET":
                return CommandType.GET_OPERATION_STATUS
            elif method == "DELETE":
                return CommandType.CLOSE_OPERATION
        elif "sessions" in path:
            if method == "POST":
                # Creating a new session
                return CommandType.OTHER
            elif method == "DELETE":
                return CommandType.CLOSE_SESSION

        # Default for any other operations
        return CommandType.OTHER

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method for GET requests with retry support.

        Args:
            path: API endpoint path
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data parsed from JSON
        """
        # Set the command type for retry policy
        command_type = self._determine_command_type(path, "GET", None)
        self.thrift_client.set_retry_command_type(command_type)

        return self.thrift_client.make_rest_request(
            "GET", path, params=params, headers=headers
        )

    def post(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method for POST requests with retry support.

        Args:
            path: API endpoint path
            data: Request payload data
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data parsed from JSON
        """
        # Set the command type for retry policy
        command_type = self._determine_command_type(path, "POST", data)
        self.thrift_client.set_retry_command_type(command_type)

        response = self.thrift_client.make_rest_request(
            "POST", path, data=data, params=params, headers=headers
        )
        return response

    def delete(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method for DELETE requests with retry support.

        Args:
            path: API endpoint path
            data: Request payload data
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data parsed from JSON
        """
        # Set the command type for retry policy
        command_type = self._determine_command_type(path, "DELETE", data)
        self.thrift_client.set_retry_command_type(command_type)

        return self.thrift_client.make_rest_request(
            "DELETE", path, data=data, params=params, headers=headers
        )
