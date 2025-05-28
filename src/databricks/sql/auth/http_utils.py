import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def extract_connection_pool(thrift_http_client):
    """
    Extract the connection pool from a Thrift HTTP client.
    
    Args:
        thrift_http_client: A THttpClient instance
        
    Returns:
        The underlying connection pool
    """
    return thrift_http_client._THttpClient__pool

def make_request(
    thrift_http_client,
    method: str,
    path: str,
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
):
    """
    Make an HTTP request using a Thrift HTTP client's connection pool.
    
    Args:
        thrift_http_client: A THttpClient instance
        method: HTTP method (GET, POST, DELETE, etc.)
        path: Path to append to the base URL
        data: Request body data as a dictionary
        params: URL parameters as a dictionary
        
    Returns:
        Parsed JSON response
    """
    # Access the underlying connection pool
    pool = extract_connection_pool(thrift_http_client)
    
    # Get the base URI from the Thrift client
    scheme = thrift_http_client.scheme
    host = thrift_http_client.host
    port = thrift_http_client.port
    
    # Build the full URL
    base_url = f"{scheme}://{host}"
    if port:
        base_url += f":{port}"
    url = f"{path.lstrip('/')}"
    
    # Prepare headers
    headers = {}
    if hasattr(thrift_http_client, '_headers') and thrift_http_client._headers:
        headers.update(thrift_http_client._headers)
    headers["Content-Type"] = "application/json"
    
    # Make the request
    logger.debug("Making %s request to %s/%s", method, base_url, url)
    
    response = pool.request(
        method=method,
        url=url,
        body=json.dumps(data).encode('utf-8') if data else None,
        headers=headers,
        fields=params
    )
    
    # Check for errors
    if response.status >= 400:
        error_message = response.data.decode('utf-8')
        logger.error("HTTP error %s: %s", response.status, error_message)
        raise Exception(f"HTTP error {response.status}: {error_message}")
    
    # Parse and return the response
    if response.data:
        return json.loads(response.data.decode('utf-8'))
    return None