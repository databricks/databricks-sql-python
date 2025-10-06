"""
Circuit breaker implementation for telemetry requests.

This module provides circuit breaker functionality to prevent telemetry failures
from impacting the main SQL operations. It uses pybreaker library to implement
the circuit breaker pattern with configurable thresholds and timeouts.
"""

import logging
import threading
from typing import Dict, Optional, Any
from dataclasses import dataclass

import pybreaker
from pybreaker import CircuitBreaker, CircuitBreakerError, CircuitBreakerListener

logger = logging.getLogger(__name__)

# Circuit Breaker Configuration Constants
MINIMUM_CALLS = 20
RESET_TIMEOUT = 30
CIRCUIT_BREAKER_NAME = "telemetry-circuit-breaker"

# Circuit Breaker State Constants
CIRCUIT_BREAKER_STATE_OPEN = "open"
CIRCUIT_BREAKER_STATE_CLOSED = "closed"
CIRCUIT_BREAKER_STATE_HALF_OPEN = "half-open"
CIRCUIT_BREAKER_STATE_DISABLED = "disabled"

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

        logger.info(
            LOG_CIRCUIT_BREAKER_STATE_CHANGED, old_state_name, new_state_name, cb.name
        )

        if new_state_name == CIRCUIT_BREAKER_STATE_OPEN:
            logger.warning(LOG_CIRCUIT_BREAKER_OPENED, cb.name)
        elif new_state_name == CIRCUIT_BREAKER_STATE_CLOSED:
            logger.info(LOG_CIRCUIT_BREAKER_CLOSED, cb.name)
        elif new_state_name == CIRCUIT_BREAKER_STATE_HALF_OPEN:
            logger.info(LOG_CIRCUIT_BREAKER_HALF_OPEN, cb.name)


class CircuitBreakerManager:
    """
    Manages circuit breaker instances for telemetry requests.

    This class provides a singleton pattern to manage circuit breaker instances
    per host, ensuring that telemetry failures don't impact main SQL operations.

    Circuit breaker configuration is fixed and cannot be overridden.
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
                cls._instances[host] = cls._create_circuit_breaker(host)
                logger.debug("Created circuit breaker for host: %s", host)

            return cls._instances[host]

    @classmethod
    def _create_circuit_breaker(cls, host: str) -> CircuitBreaker:
        """
        Create a new circuit breaker instance for the specified host.

        Args:
            host: The hostname for the circuit breaker

        Returns:
            New CircuitBreaker instance
        """
        # Create circuit breaker with fixed configuration
        breaker = CircuitBreaker(
            fail_max=MINIMUM_CALLS,
            reset_timeout=RESET_TIMEOUT,
            name=f"{CIRCUIT_BREAKER_NAME}-{host}",
        )
        breaker.add_listener(CircuitBreakerStateListener())

        return breaker


def is_circuit_breaker_error(exception: Exception) -> bool:
    """
    Check if an exception is a circuit breaker error.

    Args:
        exception: The exception to check

    Returns:
        True if the exception is a circuit breaker error
    """
    return isinstance(exception, CircuitBreakerError)
