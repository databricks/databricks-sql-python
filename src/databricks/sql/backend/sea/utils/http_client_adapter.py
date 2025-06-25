"""
HTTP client adapter for the Statement Execution API (SEA).

This module provides an adapter that uses the ThriftHttpClient for HTTP operations
but provides a simplified interface for HTTP methods.
"""

import logging
from typing import Dict, Optional, Any

from databricks.sql.auth.thrift_http_client import THttpClient

logger = logging.getLogger(__name__)


class SeaHttpClientAdapter:
    """
    Adapter for using ThriftHttpClient with SEA API.

    This class provides a simplified interface for HTTP methods while using
    ThriftHttpClient for the actual HTTP operations.
    
    Note: Retry logic is now handled in SeaDatabricksClient.make_request()
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

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method for GET requests (DEPRECATED - use SeaDatabricksClient.make_request).

        Args:
            path: API endpoint path
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data parsed from JSON
        """
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
        Convenience method for POST requests (DEPRECATED - use SeaDatabricksClient.make_request).

        Args:
            path: API endpoint path
            data: Request payload data
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data parsed from JSON
        """
        return self.thrift_client.make_rest_request(
            "POST", path, data=data, params=params, headers=headers
        )

    def delete(
        self,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method for DELETE requests (DEPRECATED - use SeaDatabricksClient.make_request).

        Args:
            path: API endpoint path
            data: Request payload data
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data parsed from JSON
        """
        return self.thrift_client.make_rest_request(
            "DELETE", path, data=data, params=params, headers=headers
        )
