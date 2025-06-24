"""
Unit tests for DatabricksRetryPolicy to verify retry decisions independent of HTTP adapter.
"""
import time
import pytest
from databricks.sql.auth.retry import (
    DatabricksRetryPolicy,
    CommandType,
)
from databricks.sql.exc import MaxRetryDurationError


def make_policy(**overrides):
    # Base parameters: small counts and durations for quick tests
    params = dict(
        delay_min=0.1,
        delay_max=0.5,
        stop_after_attempts_count=3,
        stop_after_attempts_duration=1.0,
        delay_default=0.1,
        force_dangerous_codes=[],
    )
    params.update(overrides)
    return DatabricksRetryPolicy(**params)


def test_retry_for_429_and_503_execute_statement():
    policy = make_policy()
    policy.command_type = CommandType.EXECUTE_STATEMENT
    # 429 is in status_forcelist
    can_retry_429, _ = policy.should_retry("POST", 429)
    assert can_retry_429
    # 503 is in status_forcelist
    can_retry_503, _ = policy.should_retry("POST", 503)
    assert can_retry_503


def test_no_retry_for_502_execute_statement_by_default():
    policy = make_policy()
    policy.command_type = CommandType.EXECUTE_STATEMENT
    can_retry_502, _ = policy.should_retry("POST", 502)
    assert not can_retry_502


def test_retry_for_502_when_forced():
    policy = make_policy(force_dangerous_codes=[502])
    policy.command_type = CommandType.EXECUTE_STATEMENT
    can_retry_502, _ = policy.should_retry("POST", 502)
    assert can_retry_502


def test_no_retry_for_get_method():
    policy = make_policy()
    policy.command_type = CommandType.EXECUTE_STATEMENT
    # GET should not retry
    can_retry_get, _ = policy.should_retry("GET", 503)
    assert not can_retry_get


def test_max_retry_duration_error():
    policy = make_policy(stop_after_attempts_duration=0.2)
    policy.start_retry_timer()
    time.sleep(0.25)
    # Even with a small backoff, check_proposed_wait should raise
    with pytest.raises(MaxRetryDurationError):
        policy.check_proposed_wait(0.1)
