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
from databricks.sql.exc import (
    TelemetryRateLimitError,
    TelemetryNonRateLimitError,
    RequestError,
)
from databricks.sql.telemetry.circuit_breaker_manager import CircuitBreakerManager

logger = logging.getLogger(__name__)


class ITelemetryPushClient(ABC):
    """Interface for telemetry push clients."""

    @abstractmethod
    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
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
        **kwargs,
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

    def _make_request_and_check_status(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]],
        **kwargs,
    ) -> BaseHTTPResponse:
        """
        Make the request and check response status.

        Raises TelemetryRateLimitError for 429/503 (circuit breaker counts these).
        Wraps other errors in TelemetryNonRateLimitError (circuit breaker excludes these).

        Args:
            method: HTTP method
            url: Request URL
            headers: Request headers
            **kwargs: Additional request parameters

        Returns:
            HTTP response

        Raises:
            TelemetryRateLimitError: For 429/503 status codes (circuit breaker counts)
            TelemetryNonRateLimitError: For other errors (circuit breaker excludes)
        """
        try:
            response = self._delegate.request(method, url, headers, **kwargs)

            # Check for rate limiting or service unavailable
            if response.status in [429, 503]:
                logger.debug(
                    "Telemetry endpoint returned %d for host %s, triggering circuit breaker",
                    response.status,
                    self._host,
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
                http_code = (
                    e.context.get("http-code")
                    if hasattr(e, "context") and e.context
                    else None
                )

                if http_code in [429, 503]:
                    logger.debug(
                        "Telemetry retries exhausted with status %d for host %s, triggering circuit breaker",
                        http_code,
                        self._host,
                    )
                    raise TelemetryRateLimitError(
                        f"Telemetry rate limited after retries: {http_code}"
                    )

            # NOT rate limiting (500 errors, network errors, timeouts, etc.)
            # Wrap in TelemetryNonRateLimitError so circuit breaker excludes it
            logger.debug(
                "Non-rate-limit telemetry error for host %s: %s, wrapping to exclude from circuit breaker",
                self._host,
                e,
            )
            raise TelemetryNonRateLimitError(e) from e

    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> BaseHTTPResponse:
        """
        Make an HTTP request with circuit breaker protection.

        Circuit breaker only opens for TelemetryRateLimitError (429/503 responses).
        Other errors are wrapped in TelemetryNonRateLimitError and excluded from circuit breaker.
        All exceptions propagate to caller (TelemetryClient callback handles them).
        """
        try:
            # Use circuit breaker to protect the request
            # TelemetryRateLimitError will trigger circuit breaker
            # TelemetryNonRateLimitError is excluded from circuit breaker
            return self._circuit_breaker.call(
                self._make_request_and_check_status,
                method,
                url,
                headers,
                **kwargs,
            )

        except TelemetryNonRateLimitError as e:
            # Unwrap and re-raise original exception
            # Circuit breaker didn't count this, but caller should handle it
            logger.debug(
                "Non-rate-limit telemetry error for host %s, re-raising original: %s",
                self._host,
                e.original_exception,
            )
            raise e.original_exception from e
        # All other exceptions (TelemetryRateLimitError, CircuitBreakerError) propagate as-is
