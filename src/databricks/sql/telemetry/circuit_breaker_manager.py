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
from pybreaker import CircuitBreaker, CircuitBreakerError

logger = logging.getLogger(__name__)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    
    # Failure threshold percentage (0.0 to 1.0)
    failure_threshold: float = 0.5
    
    # Minimum number of calls before circuit can open
    minimum_calls: int = 20
    
    # Time window for counting failures (in seconds)
    timeout: int = 30
    
    # Time to wait before trying to close circuit (in seconds)
    reset_timeout: int = 30
    
    # Expected exception types that should trigger circuit breaker
    expected_exception: tuple = (Exception,)
    
    # Name for the circuit breaker (for logging)
    name: str = "telemetry-circuit-breaker"


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
            fail_max=config.minimum_calls,
            reset_timeout=config.reset_timeout,
            name=f"{config.name}-{host}"
        )
        
        # Set failure threshold
        breaker.failure_threshold = config.failure_threshold
        
        # Add state change listeners for logging
        breaker.add_listener(cls._on_state_change)
        
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
    def _on_state_change(cls, old_state: str, new_state: str, breaker: CircuitBreaker) -> None:
        """
        Handle circuit breaker state changes.
        
        Args:
            old_state: Previous state of the circuit breaker
            new_state: New state of the circuit breaker
            breaker: The circuit breaker instance
        """
        logger.info(
            "Circuit breaker state changed from %s to %s for %s",
            old_state, new_state, breaker.name
        )
        
        if new_state == "open":
            logger.warning(
                "Circuit breaker opened for %s - telemetry requests will be blocked",
                breaker.name
            )
        elif new_state == "closed":
            logger.info(
                "Circuit breaker closed for %s - telemetry requests will be allowed",
                breaker.name
            )
        elif new_state == "half-open":
            logger.info(
                "Circuit breaker half-open for %s - testing telemetry requests",
                breaker.name
            )
    
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
            return "disabled"
        
        with cls._lock:
            if host not in cls._instances:
                return "not_initialized"
            
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
