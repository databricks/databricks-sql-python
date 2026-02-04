import time
from unittest.mock import patch, call
import pytest
from urllib3 import HTTPResponse
from databricks.sql.auth.retry import DatabricksRetryPolicy, RequestHistory, CommandType
from urllib3.exceptions import MaxRetryError


class TestRetry:
    @pytest.fixture()
    def retry_policy(self) -> DatabricksRetryPolicy:
        return DatabricksRetryPolicy(
            delay_min=1,
            delay_max=30,
            stop_after_attempts_count=3,
            stop_after_attempts_duration=900,
            delay_default=2,
            force_dangerous_codes=[],
        )

    @pytest.fixture()
    def error_history(self) -> RequestHistory:
        return RequestHistory(
            method="POST", url=None, error=None, status=503, redirect_location=None
        )

    def calculate_backoff_time(self, attempt, delay_min, delay_max):
        exponential_backoff_time = (2**attempt) * delay_min
        return min(exponential_backoff_time, delay_max)

    @patch("time.sleep")
    def test_sleep__no_retry_after(self, t_mock, retry_policy, error_history):
        retry_policy._retry_start_time = time.time()
        retry_policy.history = [error_history, error_history]
        retry_policy.sleep(HTTPResponse(status=503))

        expected_backoff_time = max(
            self.calculate_backoff_time(
                0, retry_policy.delay_min, retry_policy.delay_max
            ),
            retry_policy.delay_max,
        )
        t_mock.assert_called_with(expected_backoff_time)

    @patch("time.sleep")
    def test_sleep__no_retry_after_header__multiple_retries(self, t_mock, retry_policy):
        num_attempts = retry_policy.stop_after_attempts_count

        retry_policy._retry_start_time = time.time()
        retry_policy.command_type = CommandType.OTHER

        for attempt in range(num_attempts):
            retry_policy.sleep(HTTPResponse(status=503))
            # Internally urllib3 calls the increment function generating a new instance for every retry
            retry_policy = retry_policy.increment()

        expected_backoff_times = []
        for attempt in range(num_attempts):
            expected_backoff_times.append(
                max(
                    self.calculate_backoff_time(
                        attempt, retry_policy.delay_min, retry_policy.delay_max
                    ),
                    retry_policy.delay_max,
                )
            )

        # Asserts if the sleep value was called in the expected order
        t_mock.assert_has_calls(
            [call(expected_time) for expected_time in expected_backoff_times]
        )

    @patch("time.sleep")
    def test_excessive_retry_attempts_error(self, t_mock, retry_policy):
        # Attempting more than stop_after_attempt_count
        num_attempts = retry_policy.stop_after_attempts_count + 1

        retry_policy._retry_start_time = time.time()
        retry_policy.command_type = CommandType.OTHER

        with pytest.raises(MaxRetryError):
            for attempt in range(num_attempts):
                retry_policy.sleep(HTTPResponse(status=503))
                # Internally urllib3 calls the increment function generating a new instance for every retry
                retry_policy = retry_policy.increment()

    def test_404_does_not_retry_for_any_command_type(self, retry_policy):
        """Test that 404 never retries for any CommandType"""
        retry_policy._retry_start_time = time.time()

        # Test for each CommandType
        for command_type in CommandType:
            retry_policy.command_type = command_type
            should_retry, msg = retry_policy.should_retry("POST", 404)

            assert should_retry is False, f"404 should not retry for {command_type}"
            assert "404" in msg or "NOT_FOUND" in msg
