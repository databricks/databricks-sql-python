"""
Telemetry push client interface and implementations.

This module provides an interface for telemetry push clients with two implementations:
1. TelemetryPushClient - Direct HTTP client implementation
2. CircuitBreakerTelemetryPushClient - Circuit breaker wrapper implementation
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

try:
    from urllib3 import BaseHTTPResponse
except ImportError:
    from urllib3 import HTTPResponse as BaseHTTPResponse
from pybreaker import CircuitBreakerError

from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.common.http import HttpMethod
from databricks.sql.exc import TelemetryRateLimitError, RequestError
from databricks.sql.telemetry.circuit_breaker_manager import (
    CircuitBreakerManager,
    is_circuit_breaker_error,
)

logger = logging.getLogger(__name__)


class ITelemetryPushClient(ABC):
    """Interface for telemetry push clients."""

    @abstractmethod
    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> BaseHTTPResponse:
        """Make an HTTP request."""
        pass


class TelemetryPushClient(ITelemetryPushClient):
    """Direct HTTP client implementation for telemetry requests."""

    def __init__(self, http_client: UnifiedHttpClient):
        """
        Initialize the telemetry push client.

        Args:
            http_client: The underlying HTTP client
        """
        self._http_client = http_client
        logger.debug("TelemetryPushClient initialized")

    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> BaseHTTPResponse:
        """Make an HTTP request using the underlying HTTP client."""
        return self._http_client.request(method, url, headers, **kwargs)


class CircuitBreakerTelemetryPushClient(ITelemetryPushClient):
    """Circuit breaker wrapper implementation for telemetry requests."""

    def __init__(self, delegate: ITelemetryPushClient, host: str):
        """
        Initialize the circuit breaker telemetry push client.

        Args:
            delegate: The underlying telemetry push client to wrap
            host: The hostname for circuit breaker identification
        """
        self._delegate = delegate
        self._host = host

        # Get circuit breaker for this host (creates if doesn't exist)
        self._circuit_breaker = CircuitBreakerManager.get_circuit_breaker(host)

        logger.debug(
            "CircuitBreakerTelemetryPushClient initialized for host %s",
            host,
        )

    def _create_mock_success_response(self) -> BaseHTTPResponse:
        """
        Create a mock success response for when circuit breaker is open.
        
        This allows telemetry to fail silently without raising exceptions.
        """
        from unittest.mock import Mock
        mock_response = Mock(spec=BaseHTTPResponse)
        mock_response.status = 200
        mock_response.data = b'{"numProtoSuccess": 0, "errors": []}'
        return mock_response

    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> BaseHTTPResponse:
        """
        Make an HTTP request with circuit breaker protection.
        
        Circuit breaker only opens for 429/503 responses (rate limiting).
        If circuit breaker is open, silently drops the telemetry request.
        Other errors fail silently without triggering circuit breaker.
        """
        
        def _make_request_and_check_status():
            """
            Inner function that makes the request and checks response status.
            
            Raises TelemetryRateLimitError ONLY for 429/503 so circuit breaker counts them as failures.
            For all other errors, returns mock success response so circuit breaker does NOT count them.
            
            This ensures circuit breaker only opens for rate limiting, not for network errors,
            timeouts, or server errors.
            """
            try:
                response = self._delegate.request(method, url, headers, **kwargs)
                
                # Check for rate limiting or service unavailable in successful response
                # (case where urllib3 returns response without exhausting retries)
                if response.status in [429, 503]:
                    logger.warning(
                        "Telemetry endpoint returned %d for host %s, triggering circuit breaker",
                        response.status,
                        self._host
                    )
                    raise TelemetryRateLimitError(
                        f"Telemetry endpoint rate limited or unavailable: {response.status}"
                    )
                
                return response
                
            except Exception as e:
                # Don't catch TelemetryRateLimitError - let it propagate to circuit breaker
                if isinstance(e, TelemetryRateLimitError):
                    raise
                
                # Check if it's a RequestError with rate limiting status code (exhausted retries)
                if isinstance(e, RequestError):
                    http_code = e.context.get("http-code") if hasattr(e, "context") and e.context else None
                    
                    if http_code in [429, 503]:
                        logger.warning(
                            "Telemetry retries exhausted with status %d for host %s, triggering circuit breaker",
                            http_code,
                            self._host
                        )
                        raise TelemetryRateLimitError(
                            f"Telemetry rate limited after retries: {http_code}"
                        )
                
                # NOT rate limiting (500 errors, network errors, timeouts, etc.)
                # Return mock success response so circuit breaker does NOT see this as a failure
                logger.debug(
                    "Non-rate-limit telemetry error for host %s: %s, failing silently",
                    self._host,
                    e
                )
                return self._create_mock_success_response()
        
        try:
            # Use circuit breaker to protect the request
            # The inner function will raise TelemetryRateLimitError for 429/503
            # which the circuit breaker will count as a failure
            return self._circuit_breaker.call(_make_request_and_check_status)
            
        except Exception as e:
            # All telemetry errors are consumed and return mock success
            # Log appropriate message based on exception type
            if isinstance(e, CircuitBreakerError):
                logger.debug(
                    "Circuit breaker is open for host %s, dropping telemetry request",
                    self._host,
                )
            elif isinstance(e, TelemetryRateLimitError):
                logger.debug(
                    "Telemetry rate limited for host %s (already counted by circuit breaker): %s",
                    self._host,
                    e
                )
            else:
                logger.debug("Unexpected telemetry error for host %s: %s, failing silently", self._host, e)
            
            return self._create_mock_success_response()

