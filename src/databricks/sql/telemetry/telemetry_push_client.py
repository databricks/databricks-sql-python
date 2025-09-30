"""
Telemetry push client interface and implementations.

This module provides an interface for telemetry push clients with two implementations:
1. TelemetryPushClient - Direct HTTP client implementation
2. CircuitBreakerTelemetryPushClient - Circuit breaker wrapper implementation
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from contextlib import contextmanager

try:
    from urllib3 import BaseHTTPResponse
except ImportError:
    from urllib3 import HTTPResponse as BaseHTTPResponse
from pybreaker import CircuitBreakerError

from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.common.http import HttpMethod
from databricks.sql.telemetry.circuit_breaker_manager import (
    CircuitBreakerConfig,
    CircuitBreakerManager,
    is_circuit_breaker_error,
    CIRCUIT_BREAKER_STATE_OPEN,
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

    @abstractmethod
    @contextmanager
    def request_context(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """Context manager for making HTTP requests."""
        pass

    @abstractmethod
    def get_circuit_breaker_state(self) -> str:
        """Get the current state of the circuit breaker."""
        pass

    @abstractmethod
    def is_circuit_breaker_open(self) -> bool:
        """Check if the circuit breaker is currently open."""
        pass

    @abstractmethod
    def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker to closed state."""
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

    @contextmanager
    def request_context(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """Context manager for making HTTP requests."""
        with self._http_client.request_context(
            method, url, headers, **kwargs
        ) as response:
            yield response

    def get_circuit_breaker_state(self) -> str:
        """Circuit breaker is not available in direct implementation."""
        return "not_available"

    def is_circuit_breaker_open(self) -> bool:
        """Circuit breaker is not available in direct implementation."""
        return False

    def reset_circuit_breaker(self) -> None:
        """Circuit breaker is not available in direct implementation."""
        pass


class CircuitBreakerTelemetryPushClient(ITelemetryPushClient):
    """Circuit breaker wrapper implementation for telemetry requests."""

    def __init__(
        self, delegate: ITelemetryPushClient, host: str, config: CircuitBreakerConfig
    ):
        """
        Initialize the circuit breaker telemetry push client.

        Args:
            delegate: The underlying telemetry push client to wrap
            host: The hostname for circuit breaker identification
            config: Circuit breaker configuration
        """
        self._delegate = delegate
        self._host = host
        self._config = config

        # Initialize circuit breaker manager with config
        CircuitBreakerManager.initialize(config)

        # Get circuit breaker for this host
        self._circuit_breaker = CircuitBreakerManager.get_circuit_breaker(host)

        logger.debug(
            "CircuitBreakerTelemetryPushClient initialized for host %s with config: %s",
            host,
            config,
        )

    def request(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> BaseHTTPResponse:
        """Make an HTTP request with circuit breaker protection."""
        try:
            # Use circuit breaker to protect the request
            return self._circuit_breaker.call(
                lambda: self._delegate.request(method, url, headers, **kwargs)
            )
        except CircuitBreakerError as e:
            logger.warning(
                "Circuit breaker is open for host %s, blocking telemetry request to %s: %s",
                self._host,
                url,
                e,
            )
            raise
        except Exception as e:
            # Re-raise non-circuit breaker exceptions
            logger.debug("Telemetry request failed for host %s: %s", self._host, e)
            raise

    @contextmanager
    def request_context(
        self,
        method: HttpMethod,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """Context manager for making HTTP requests with circuit breaker protection."""
        try:
            # Use circuit breaker to protect the request
            def _make_request():
                with self._delegate.request_context(
                    method, url, headers, **kwargs
                ) as response:
                    return response

            response = self._circuit_breaker.call(_make_request)
            yield response
        except CircuitBreakerError as e:
            logger.warning(
                "Circuit breaker is open for host %s, blocking telemetry request to %s: %s",
                self._host,
                url,
                e,
            )
            raise
        except Exception as e:
            # Re-raise non-circuit breaker exceptions
            logger.debug("Telemetry request failed for host %s: %s", self._host, e)
            raise

    def get_circuit_breaker_state(self) -> str:
        """Get the current state of the circuit breaker."""
        return CircuitBreakerManager.get_circuit_breaker_state(self._host)

    def is_circuit_breaker_open(self) -> bool:
        """Check if the circuit breaker is currently open."""
        return self.get_circuit_breaker_state() == CIRCUIT_BREAKER_STATE_OPEN

    def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker to closed state."""
        CircuitBreakerManager.reset_circuit_breaker(self._host)
