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

    @pytest.fixture()
    def server_directed_retry_policy(self) -> DatabricksRetryPolicy:
        return DatabricksRetryPolicy(
            delay_min=1,
            delay_max=30,
            stop_after_attempts_count=3,
            stop_after_attempts_duration=900,
            delay_default=2,
            force_dangerous_codes=[],
            server_directed_only=True,
        )

    def test_server_directed_only__retries_with_retry_after(
        self, server_directed_retry_policy
    ):
        """429 + Retry-After header → should retry"""
        server_directed_retry_policy._retry_start_time = time.time()
        server_directed_retry_policy.command_type = CommandType.OTHER
        should_retry, msg = server_directed_retry_policy.should_retry(
            "POST", 429, has_retry_after=True
        )
        assert should_retry is True

    def test_server_directed_only__no_retry_without_retry_after(
        self, server_directed_retry_policy
    ):
        """429 without Retry-After header → no retry"""
        server_directed_retry_policy._retry_start_time = time.time()
        server_directed_retry_policy.command_type = CommandType.OTHER
        should_retry, msg = server_directed_retry_policy.should_retry(
            "POST", 429, has_retry_after=False
        )
        assert should_retry is False
        assert "server_directed_only" in msg

    def test_server_directed_only__no_retry_503_without_header(
        self, server_directed_retry_policy
    ):
        """503 without Retry-After header → no retry"""
        server_directed_retry_policy._retry_start_time = time.time()
        server_directed_retry_policy.command_type = CommandType.OTHER
        should_retry, msg = server_directed_retry_policy.should_retry(
            "POST", 503, has_retry_after=False
        )
        assert should_retry is False
        assert "server_directed_only" in msg

    def test_server_directed_only__overrides_dangerous_codes(self):
        """force_dangerous_codes=[500] + no Retry-After → no retry in server_directed_only mode"""
        policy = DatabricksRetryPolicy(
            delay_min=1,
            delay_max=30,
            stop_after_attempts_count=3,
            stop_after_attempts_duration=900,
            delay_default=2,
            force_dangerous_codes=[500],
            server_directed_only=True,
        )
        policy._retry_start_time = time.time()
        policy.command_type = CommandType.EXECUTE_STATEMENT
        should_retry, msg = policy.should_retry("POST", 500, has_retry_after=False)
        assert should_retry is False
        assert "server_directed_only" in msg

    def test_server_directed_only__non_retryable_codes_unaffected(
        self, server_directed_retry_policy
    ):
        """401/403/501 still don't retry even with Retry-After header"""
        server_directed_retry_policy._retry_start_time = time.time()
        server_directed_retry_policy.command_type = CommandType.OTHER
        for code in [401, 403, 501]:
            should_retry, msg = server_directed_retry_policy.should_retry(
                "POST", code, has_retry_after=True
            )
            assert should_retry is False, f"Code {code} should never retry"

    def test_default_mode_unchanged(self, retry_policy):
        """server_directed_only=False preserves existing behavior — 429 retries without header"""
        retry_policy._retry_start_time = time.time()
        retry_policy.command_type = CommandType.OTHER
        should_retry, msg = retry_policy.should_retry(
            "POST", 429, has_retry_after=False
        )
        assert should_retry is True

    def test_server_directed_only__survives_new(self, server_directed_retry_policy):
        """urllib3 calls .new() between retries to create a fresh policy instance.
        Verify that server_directed_only is carried over and still enforced."""
        server_directed_retry_policy._retry_start_time = time.time()
        server_directed_retry_policy.command_type = CommandType.OTHER
        new_policy = server_directed_retry_policy.new()
        assert new_policy.server_directed_only is True
        # The new instance should still block retries without Retry-After
        should_retry, msg = new_policy.should_retry("POST", 429, has_retry_after=False)
        assert should_retry is False
        assert "server_directed_only" in msg

    def test_server_directed_only__execute_statement_with_retry_after(
        self, server_directed_retry_policy
    ):
        """EXECUTE_STATEMENT + 429 + Retry-After header → retry"""
        server_directed_retry_policy._retry_start_time = time.time()
        server_directed_retry_policy.command_type = CommandType.EXECUTE_STATEMENT
        should_retry, msg = server_directed_retry_policy.should_retry(
            "POST", 429, has_retry_after=True
        )
        assert should_retry is True

    def test_404_does_not_retry_for_any_command_type(self, retry_policy):
        """Test that 404 never retries for any CommandType"""
        retry_policy._retry_start_time = time.time()

        # Test for each CommandType
        for command_type in CommandType:
            retry_policy.command_type = command_type
            should_retry, msg = retry_policy.should_retry("POST", 404)

            assert should_retry is False, f"404 should not retry for {command_type}"
            assert "404" in msg or "NOT_FOUND" in msg
