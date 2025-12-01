"""
Circuit breaker implementation for telemetry requests.

This module provides circuit breaker functionality to prevent telemetry failures
from impacting the main SQL operations. It uses pybreaker library to implement
the circuit breaker pattern.
"""

import logging
import threading
from typing import Dict

import pybreaker
from pybreaker import CircuitBreaker, CircuitBreakerError, CircuitBreakerListener

from databricks.sql.exc import TelemetryNonRateLimitError

logger = logging.getLogger(__name__)

# Circuit Breaker Constants
MINIMUM_CALLS = 20  # Number of failures before circuit opens
RESET_TIMEOUT = 30  # Seconds to wait before trying to close circuit
NAME_PREFIX = "telemetry-circuit-breaker"

# Circuit Breaker State Constants (used in logging)
CIRCUIT_BREAKER_STATE_OPEN = "open"
CIRCUIT_BREAKER_STATE_CLOSED = "closed"
CIRCUIT_BREAKER_STATE_HALF_OPEN = "half-open"

# Logging Message Constants
LOG_CIRCUIT_BREAKER_STATE_CHANGED = "Circuit breaker state changed from %s to %s for %s"
LOG_CIRCUIT_BREAKER_OPENED = (
    "Circuit breaker opened for %s - telemetry requests will be blocked"
)
LOG_CIRCUIT_BREAKER_CLOSED = (
    "Circuit breaker closed for %s - telemetry requests will be allowed"
)
LOG_CIRCUIT_BREAKER_HALF_OPEN = (
    "Circuit breaker half-open for %s - testing telemetry requests"
)


class CircuitBreakerStateListener(CircuitBreakerListener):
    """Listener for circuit breaker state changes."""

    def before_call(self, cb: CircuitBreaker, func, *args, **kwargs) -> None:
        """Called before the circuit breaker calls a function."""
        pass

    def failure(self, cb: CircuitBreaker, exc: BaseException) -> None:
        """Called when a function called by the circuit breaker fails."""
        pass

    def success(self, cb: CircuitBreaker) -> None:
        """Called when a function called by the circuit breaker succeeds."""
        pass

    def state_change(self, cb: CircuitBreaker, old_state, new_state) -> None:
        """Called when the circuit breaker state changes."""
        old_state_name = old_state.name if old_state else "None"
        new_state_name = new_state.name if new_state else "None"

        logger.debug(
            LOG_CIRCUIT_BREAKER_STATE_CHANGED, old_state_name, new_state_name, cb.name
        )

        if new_state_name == CIRCUIT_BREAKER_STATE_OPEN:
            logger.debug(LOG_CIRCUIT_BREAKER_OPENED, cb.name)
        elif new_state_name == CIRCUIT_BREAKER_STATE_CLOSED:
            logger.debug(LOG_CIRCUIT_BREAKER_CLOSED, cb.name)
        elif new_state_name == CIRCUIT_BREAKER_STATE_HALF_OPEN:
            logger.debug(LOG_CIRCUIT_BREAKER_HALF_OPEN, cb.name)


class CircuitBreakerManager:
    """
    Manages circuit breaker instances for telemetry requests.

    Creates and caches circuit breaker instances per host to ensure telemetry
    failures don't impact main SQL operations.
    """

    _instances: Dict[str, CircuitBreaker] = {}
    _lock = threading.RLock()

    @classmethod
    def get_circuit_breaker(cls, host: str) -> CircuitBreaker:
        """
        Get or create a circuit breaker instance for the specified host.

        Args:
            host: The hostname for which to get the circuit breaker

        Returns:
            CircuitBreaker instance for the host
        """
        with cls._lock:
            if host not in cls._instances:
                breaker = CircuitBreaker(
                    fail_max=MINIMUM_CALLS,
                    reset_timeout=RESET_TIMEOUT,
                    name=f"{NAME_PREFIX}-{host}",
                    exclude=[
                        TelemetryNonRateLimitError
                    ],  # Don't count these as failures
                )
                # Add state change listener for logging
                breaker.add_listener(CircuitBreakerStateListener())
                cls._instances[host] = breaker
                logger.debug("Created circuit breaker for host: %s", host)

            return cls._instances[host]
