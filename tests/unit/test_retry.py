import time
from unittest.mock import patch, call
import pytest
from urllib3 import HTTPResponse
from databricks.sql.auth.retry import DatabricksRetryPolicy, RequestHistory, CommandType
from urllib3.exceptions import MaxRetryError


class TestRetry:
    def _make_retry_policy(self, **overrides) -> DatabricksRetryPolicy:
        defaults = dict(
            delay_min=1,
            delay_max=30,
            stop_after_attempts_count=3,
            stop_after_attempts_duration=900,
            delay_default=2,
            force_dangerous_codes=[],
        )
        defaults.update(overrides)
        return DatabricksRetryPolicy(**defaults)

    @pytest.fixture()
    def retry_policy(self) -> DatabricksRetryPolicy:
        return self._make_retry_policy()

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

    def test_respect_server_retry_after__retries_with_retry_after(self):
        """429 + Retry-After header → should retry"""
        policy = self._make_retry_policy(respect_server_retry_after_header=True)
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.OTHER
        should_retry, msg = policy.should_retry("POST", 429, has_retry_after=True)
        assert should_retry is True

    def test_respect_server_retry_after__no_retry_without_retry_after(self):
        """429 without Retry-After header → no retry"""
        policy = self._make_retry_policy(respect_server_retry_after_header=True)
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.OTHER
        should_retry, msg = policy.should_retry("POST", 429, has_retry_after=False)
        assert should_retry is False
        assert "respect_server_retry_after_header" in msg

    def test_respect_server_retry_after__no_retry_503_without_header(self):
        """503 without Retry-After header → no retry"""
        policy = self._make_retry_policy(respect_server_retry_after_header=True)
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.OTHER
        should_retry, msg = policy.should_retry("POST", 503, has_retry_after=False)
        assert should_retry is False
        assert "respect_server_retry_after_header" in msg

    def test_respect_server_retry_after__overrides_dangerous_codes(self):
        """force_dangerous_codes=[500] + no Retry-After → no retry in respect_server_retry_after_header mode"""
        policy = self._make_retry_policy(
            force_dangerous_codes=[500], respect_server_retry_after_header=True
        )
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.EXECUTE_STATEMENT
        should_retry, msg = policy.should_retry("POST", 500, has_retry_after=False)
        assert should_retry is False
        assert "respect_server_retry_after_header" in msg

    def test_respect_server_retry_after__non_retryable_codes_unaffected(self):
        """401/403/501 still don't retry even with Retry-After header"""
        policy = self._make_retry_policy(respect_server_retry_after_header=True)
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.OTHER
        for code in [401, 403, 501]:
            should_retry, msg = policy.should_retry(
                "POST", code, has_retry_after=True
            )
            assert should_retry is False, f"Code {code} should never retry"

    def test_default_mode_unchanged(self, retry_policy):
        """respect_server_retry_after_header=False preserves existing behavior — 429 retries without header"""
        retry_policy._retry_start_time = time.time()
        retry_policy.command_type = CommandType.OTHER
        should_retry, msg = retry_policy.should_retry(
            "POST", 429, has_retry_after=False
        )
        assert should_retry is True

    def test_respect_server_retry_after__survives_new(self):
        """urllib3 calls .new() between retries to create a fresh policy instance.
        Verify that respect_server_retry_after_header is carried over and still enforced."""
        policy = self._make_retry_policy(respect_server_retry_after_header=True)
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.OTHER
        new_policy = policy.new()
        assert new_policy.respect_server_retry_after_header is True
        # The new instance should still block retries without Retry-After
        should_retry, msg = new_policy.should_retry("POST", 429, has_retry_after=False)
        assert should_retry is False
        assert "respect_server_retry_after_header" in msg

    def test_respect_server_retry_after__execute_statement_with_retry_after(self):
        """EXECUTE_STATEMENT + 429 + Retry-After header → retry"""
        policy = self._make_retry_policy(respect_server_retry_after_header=True)
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.EXECUTE_STATEMENT
        should_retry, msg = policy.should_retry("POST", 429, has_retry_after=True)
        assert should_retry is True

