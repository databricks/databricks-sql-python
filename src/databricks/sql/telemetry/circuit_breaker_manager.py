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
DEFAULT_FAILURE_THRESHOLD = 0.5
DEFAULT_MINIMUM_CALLS = 20
DEFAULT_TIMEOUT = 30
DEFAULT_RESET_TIMEOUT = 30
DEFAULT_EXPECTED_EXCEPTION = (Exception,)
DEFAULT_NAME = "telemetry-circuit-breaker"

# Circuit Breaker State Constants
CIRCUIT_BREAKER_STATE_OPEN = "open"
CIRCUIT_BREAKER_STATE_CLOSED = "closed"
CIRCUIT_BREAKER_STATE_HALF_OPEN = "half-open"
CIRCUIT_BREAKER_STATE_DISABLED = "disabled"
CIRCUIT_BREAKER_STATE_NOT_INITIALIZED = "not_initialized"

# Logging Message Constants
LOG_CIRCUIT_BREAKER_STATE_CHANGED = "Circuit breaker state changed from %s to %s for %s"
LOG_CIRCUIT_BREAKER_OPENED = "Circuit breaker opened for %s - telemetry requests will be blocked"
LOG_CIRCUIT_BREAKER_CLOSED = "Circuit breaker closed for %s - telemetry requests will be allowed"
LOG_CIRCUIT_BREAKER_HALF_OPEN = "Circuit breaker half-open for %s - testing telemetry requests"


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
            LOG_CIRCUIT_BREAKER_STATE_CHANGED,
            old_state_name, new_state_name, cb.name
        )
        
        if new_state_name == CIRCUIT_BREAKER_STATE_OPEN:
            logger.warning(
                LOG_CIRCUIT_BREAKER_OPENED,
                cb.name
            )
        elif new_state_name == CIRCUIT_BREAKER_STATE_CLOSED:
            logger.info(
                LOG_CIRCUIT_BREAKER_CLOSED,
                cb.name
            )
        elif new_state_name == CIRCUIT_BREAKER_STATE_HALF_OPEN:
            logger.info(
                LOG_CIRCUIT_BREAKER_HALF_OPEN,
                cb.name
            )


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior.
    
    This class is immutable to prevent modification of circuit breaker settings.
    All configuration values are set to constants defined at the module level.
    """
    
    # Failure threshold percentage (0.0 to 1.0)
    failure_threshold: float = DEFAULT_FAILURE_THRESHOLD
    
    # Minimum number of calls before circuit can open
    minimum_calls: int = DEFAULT_MINIMUM_CALLS
    
    # Time window for counting failures (in seconds)
    timeout: int = DEFAULT_TIMEOUT
    
    # Time to wait before trying to close circuit (in seconds)
    reset_timeout: int = DEFAULT_RESET_TIMEOUT
    
    # Expected exception types that should trigger circuit breaker
    expected_exception: tuple = DEFAULT_EXPECTED_EXCEPTION
    
    # Name for the circuit breaker (for logging)
    name: str = DEFAULT_NAME


class CircuitBreakerManager:
    """
    Manages circuit breaker instances for telemetry requests.
    
    This class provides a singleton pattern to manage circuit breaker instances
    per host, ensuring that telemetry failures don't impact main SQL operations.
    """
    
    _instances: Dict[str, CircuitBreaker] = {}
    _lock = threading.RLock()
    _config: Optional[CircuitBreakerConfig] = None
    
    @classmethod
    def initialize(cls, config: CircuitBreakerConfig) -> None:
        """
        Initialize the circuit breaker manager with configuration.
        
        Args:
            config: Circuit breaker configuration
        """
        with cls._lock:
            cls._config = config
            logger.debug("CircuitBreakerManager initialized with config: %s", config)
    
    @classmethod
    def get_circuit_breaker(cls, host: str) -> CircuitBreaker:
        """
        Get or create a circuit breaker instance for the specified host.
        
        Args:
            host: The hostname for which to get the circuit breaker
            
        Returns:
            CircuitBreaker instance for the host
        """
        if not cls._config:
            # Return a no-op circuit breaker if not initialized
            return cls._create_noop_circuit_breaker()
        
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
        config = cls._config
        
        # Create circuit breaker with configuration
        breaker = CircuitBreaker(
            fail_max=config.minimum_calls,  # Number of failures before circuit opens
            reset_timeout=config.reset_timeout,
            name=f"{config.name}-{host}"
        )
        
        # Add state change listeners for logging
        breaker.add_listener(CircuitBreakerStateListener())
        
        return breaker
    
    @classmethod
    def _create_noop_circuit_breaker(cls) -> CircuitBreaker:
        """
        Create a no-op circuit breaker that always allows calls.
        
        Returns:
            CircuitBreaker that never opens
        """
        # Create a circuit breaker with very high thresholds so it never opens
        breaker = CircuitBreaker(
            fail_max=1000000,  # Very high threshold
            reset_timeout=1,   # Short reset time
            name="noop-circuit-breaker"
        )
        breaker.failure_threshold = 1.0  # 100% failure threshold
        return breaker
    
    
    @classmethod
    def get_circuit_breaker_state(cls, host: str) -> str:
        """
        Get the current state of the circuit breaker for a host.
        
        Args:
            host: The hostname
            
        Returns:
            Current state of the circuit breaker
        """
        if not cls._config:
            return CIRCUIT_BREAKER_STATE_DISABLED
        
        with cls._lock:
            if host not in cls._instances:
                return CIRCUIT_BREAKER_STATE_NOT_INITIALIZED
            
            breaker = cls._instances[host]
            return breaker.current_state
    
    @classmethod
    def reset_circuit_breaker(cls, host: str) -> None:
        """
        Reset the circuit breaker for a host to closed state.
        
        Args:
            host: The hostname
        """
        with cls._lock:
            if host in cls._instances:
                # pybreaker doesn't have a reset method, we need to recreate the breaker
                del cls._instances[host]
                logger.info("Reset circuit breaker for host: %s", host)
    
    @classmethod
    def clear_circuit_breaker(cls, host: str) -> None:
        """
        Remove the circuit breaker instance for a host.
        
        Args:
            host: The hostname
        """
        with cls._lock:
            if host in cls._instances:
                del cls._instances[host]
                logger.debug("Cleared circuit breaker for host: %s", host)
    
    @classmethod
    def clear_all_circuit_breakers(cls) -> None:
        """Clear all circuit breaker instances."""
        with cls._lock:
            cls._instances.clear()
            logger.debug("Cleared all circuit breakers")


def is_circuit_breaker_error(exception: Exception) -> bool:
    """
    Check if an exception is a circuit breaker error.
    
    Args:
        exception: The exception to check
        
    Returns:
        True if the exception is a circuit breaker error
    """
    return isinstance(exception, CircuitBreakerError)
