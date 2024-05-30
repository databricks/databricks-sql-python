from os import error
import time
from unittest.mock import Mock, patch
import pytest
from requests import Request
from urllib3 import HTTPResponse
from databricks.sql.auth.retry import DatabricksRetryPolicy, RequestHistory


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

    @patch("time.sleep")
    def test_sleep__no_retry_after(self, t_mock, retry_policy, error_history):
        retry_policy._retry_start_time = time.time()
        retry_policy.history = [error_history, error_history]
        retry_policy.sleep(HTTPResponse(status=503))
        t_mock.assert_called_with(2)

    @patch("time.sleep")
    def test_sleep__retry_after_is_binding(self, t_mock, retry_policy, error_history):
        retry_policy._retry_start_time = time.time()
        retry_policy.history = [error_history, error_history]
        retry_policy.sleep(HTTPResponse(status=503, headers={"Retry-After": "3"}))
        t_mock.assert_called_with(3)

    @patch("time.sleep")
    def test_sleep__retry_after_present_but_not_binding(self, t_mock, retry_policy, error_history):
        retry_policy._retry_start_time = time.time()
        retry_policy.history = [error_history, error_history]
        retry_policy.sleep(HTTPResponse(status=503, headers={"Retry-After": "1"}))
        t_mock.assert_called_with(2)

    @patch("time.sleep")
    def test_sleep__retry_after_surpassed(self, t_mock, retry_policy, error_history):
        retry_policy._retry_start_time = time.time()
        retry_policy.history = [error_history, error_history, error_history]
        retry_policy.sleep(HTTPResponse(status=503, headers={"Retry-After": "3"}))
        t_mock.assert_called_with(4)
