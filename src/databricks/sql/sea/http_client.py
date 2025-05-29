import json
import logging
import requests
from typing import Dict, Any, Optional, Union, List
from urllib.parse import urljoin

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.types import SSLOptions

logger = logging.getLogger(__name__)

class SEAHttpClient:
    """
    HTTP client for Statement Execution API (SEA).
    
    This client handles the HTTP communication with the SEA endpoints,
    including authentication, request formatting, and response parsing.
    """
    
    def __init__(
        self,
        server_hostname: str,
        port: int,
        http_path: str,
        http_headers: List[tuple],
        auth_provider: AuthProvider,
        ssl_options: SSLOptions,
        **kwargs
    ):
        """
        Initialize the SEA HTTP client.
        
        Args:
            server_hostname: Hostname of the Databricks server
            port: Port number for the connection
            http_path: HTTP path for the connection
            http_headers: List of HTTP headers to include in requests
            auth_provider: Authentication provider
            ssl_options: SSL configuration options
            **kwargs: Additional keyword arguments
        """
        self.server_hostname = server_hostname
        self.port = port
        self.http_path = http_path
        self.auth_provider = auth_provider
        self.ssl_options = ssl_options
        
        # Base URL for API requests
        self.base_url = f"https://{server_hostname}:{port}"
        
        # Convert headers list to dictionary
        self.headers = dict(http_headers)
        self.headers.update({"Content-Type": "application/json"})
        
        # Session retry configuration
        self.max_retries = kwargs.get("_retry_stop_after_attempts_count", 30)
        
        # Create a session for connection pooling
        self.session = requests.Session()
        
        # Configure SSL verification
        if ssl_options.tls_verify:
            self.session.verify = ssl_options.tls_trusted_ca_file or True
        else:
            self.session.verify = False
            
        # Configure client certificates if provided
        if ssl_options.tls_client_cert_file:
            client_cert = ssl_options.tls_client_cert_file
            client_key = ssl_options.tls_client_cert_key_file
            client_key_password = ssl_options.tls_client_cert_key_password
            
            if client_key:
                self.session.cert = (client_cert, client_key)
            else:
                self.session.cert = client_cert
                
            if client_key_password:
                # Note: requests doesn't directly support key passwords
                # This would require more complex handling with libraries like pyOpenSSL
                logger.warning("Client key password provided but not supported by requests library")
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers from the auth provider."""
        headers = {}
        self.auth_provider.add_headers(headers)
        return headers
    
    def _make_request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make an HTTP request to the SEA endpoint.
        
        Args:
            method: HTTP method (GET, POST, DELETE)
            path: API endpoint path
            data: Request payload data
            
        Returns:
            Dict[str, Any]: Response data parsed from JSON
            
        Raises:
            RequestError: If the request fails
        """
        url = urljoin(self.base_url, path)
        headers = {**self.headers, **self._get_auth_headers()}
        
        # Log request details (without sensitive information)
        logger.debug(f"Making {method} request to {url}")
        logger.debug(f"Headers: {[k for k in headers.keys()]}")
        if data:
            # Don't log sensitive data like access tokens
            safe_data = {k: v for k, v in data.items() if k not in ["access_token", "token"]}
            logger.debug(f"Request data: {safe_data}")
        
        try:
            if method.upper() == "GET":
                response = self.session.get(url, headers=headers, params=data)
            elif method.upper() == "POST":
                response = self.session.post(url, headers=headers, json=data)
            elif method.upper() == "DELETE":
                # For DELETE requests, use params for data (query parameters)
                response = self.session.delete(url, headers=headers, params=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Log response details
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            # Parse JSON response
            if response.content:
                result = response.json()
                # Log response content (but limit it for large responses)
                content_str = json.dumps(result)
                if len(content_str) > 1000:
                    logger.debug(f"Response content (truncated): {content_str[:1000]}...")
                else:
                    logger.debug(f"Response content: {content_str}")
                return result
            return {}
            
        except requests.exceptions.RequestException as e:
            # Handle request errors
            error_message = f"SEA HTTP request failed: {str(e)}"
            logger.error(error_message)
            
            # Extract error details from response if available
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_details = e.response.json()
                    error_message = f"{error_message}: {error_details.get('message', '')}"
                    logger.error(f"Response status: {e.response.status_code}, Error details: {error_details}")
                except (ValueError, KeyError):
                    # If we can't parse the JSON, just log the raw content
                    logger.error(f"Response status: {e.response.status_code}, Raw content: {e.response.content}")
                    pass
                
            # Re-raise as a RequestError
            from databricks.sql.exc import RequestError
            raise RequestError(error_message, e)