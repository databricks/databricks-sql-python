from contextlib import contextmanager
from typing import List
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from urllib3.exceptions import MaxRetryError

from databricks.sql.auth.retry import DatabricksRetryPolicy
from databricks.sql.exc import (
    MaxRetryDurationError,
    NonRecoverableNetworkError,
    RequestError,
    SessionAlreadyClosedError,
    UnsafeToRetryError,
)


class Client429ResponseMixin:
    def test_client_should_retry_automatically_when_getting_429(self):
        with self.cursor() as cursor:
            for _ in range(10):
                cursor.execute("SELECT 1")
                rows = cursor.fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], 1)

    def test_client_should_not_retry_429_if_RateLimitRetry_is_0(self):
        with self.assertRaises(self.error_type) as cm:
            with self.cursor(self.conf_to_disable_rate_limit_retries) as cursor:
                for _ in range(10):
                    cursor.execute("SELECT 1")
                    rows = cursor.fetchall()
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0][0], 1)
        expected = (
            "Maximum rate of 1 requests per SECOND has been exceeded. "
            "Please reduce the rate of requests and try again after 1 seconds."
        )
        exception_str = str(cm.exception)

        # FIXME (Ali Smesseim, 7-Jul-2020): ODBC driver does not always return the
        #  X-Thriftserver-Error-Message as-is. Re-enable once Simba resolves this flakiness.
        #  Simba support ticket: https://magnitudesoftware.force.com/support/5001S000018RlaD
        # self.assertIn(expected, exception_str)


class Client503ResponseMixin:
    def test_wait_cluster_startup(self):
        with self.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()

    def _test_retry_disabled_with_message(self, error_msg_substring, exception_type):
        with self.assertRaises(exception_type) as cm:
            with self.connection(self.conf_to_disable_temporarily_unavailable_retries):
                pass
        self.assertIn(error_msg_substring, str(cm.exception))


@contextmanager
def mocked_server_response(status: int = 200, headers: dict = {}):
    """Context manager for patching urllib3 responses"""

    # When mocking mocking a BaseHTTPResponse for urllib3 the mock must include
    #   1. A status code
    #   2. A headers dict
    #   3. mock.get_redirect_location() return falsy

    # `msg` is included for testing when urllib3~=1.0.0 is installed
    mock_response = MagicMock(headers=headers, msg=headers, status=status)
    mock_response.get_redirect_location.return_value = False

    with patch("urllib3.connectionpool.HTTPSConnectionPool._get_conn") as getconn_mock:
        getconn_mock.return_value.getresponse.return_value = mock_response
        try:
            yield getconn_mock
        finally:
            pass


@contextmanager
def mock_sequential_server_responses(responses: List[dict]):
    """Same as the mocked_server_response context manager but it will yield
    the provided responses in the order received

    `responses` should be a list of dictionaries containing these members:
        - status: int
        - headers: dict
    """

    mock_responses = []

    # Each resp should have these members:

    for resp in responses:
        _mock = MagicMock(
            headers=resp["headers"], msg=resp["headers"], status=resp["status"]
        )
        _mock.get_redirect_location.return_value = False
        mock_responses.append(_mock)

    with patch("urllib3.connectionpool.HTTPSConnectionPool._get_conn") as getconn_mock:
        getconn_mock.return_value.getresponse.side_effect = mock_responses
        try:
            yield getconn_mock
        finally:
            pass


class PySQLRetryTestsMixin:
    """Home for retry tests where we patch urllib to return different codes and monitor that it tries to retry"""

    # For testing purposes
    _retry_policy = {
        "_enable_v3_retries": True,
        "_retry_delay_min": 0.1,
        "_retry_delay_max": 5,
        "_retry_stop_after_attempts_count": 5,
        "_retry_stop_after_attempts_duration": 10,
        "_retry_delay_default": 0.5,
    }

    def test_retry_urllib3_settings_are_honored(self):
        """Databricks overrides some of urllib3's configuration. This tests confirms that what configuration
        we DON'T override is preserved in urllib3's internals
        """

        urllib3_config = {"connect": 10, "read": 11, "redirect": 12}
        rp = DatabricksRetryPolicy(
            delay_min=0.1,
            delay_max=10.0,
            stop_after_attempts_count=10,
            stop_after_attempts_duration=10.0,
            delay_default=1.0,
            force_dangerous_codes=[],
            urllib3_kwargs=urllib3_config,
        )

        assert rp.connect == 10
        assert rp.read == 11
        assert rp.redirect == 12

    def test_oserror_retries(self):
        """If a network error occurs during make_request, the request is retried according to policy"""
        with patch(
            "urllib3.connectionpool.HTTPSConnectionPool._validate_conn",
        ) as mock_validate_conn:
            mock_validate_conn.side_effect = OSError("Some arbitrary network error")
            with self.assertRaises(MaxRetryError) as cm:
                with self.connection(extra_params=self._retry_policy) as conn:
                    pass

            assert mock_validate_conn.call_count == 6

    def test_retry_max_count_not_exceeded(self):
        """GIVEN the max_attempts_count is 5
        WHEN the server sends nothing but 429 responses
        THEN the connector issues six request (original plus five retries)
            before raising an exception
        """
        with mocked_server_response(status=404) as mock_obj:
            with self.assertRaises(MaxRetryError) as cm:
                with self.connection(extra_params=self._retry_policy) as conn:
                    pass
            assert mock_obj.return_value.getresponse.call_count == 6

    def test_retry_max_duration_not_exceeded(self):
        """GIVEN the max attempt duration of 10 seconds
        WHEN the server sends a Retry-After header of 60 seconds
        THEN the connector raises a MaxRetryDurationError
        """
        with mocked_server_response(status=429, headers={"Retry-After": "60"}):
            with self.assertRaises(RequestError) as cm:
                with self.connection(extra_params=self._retry_policy) as conn:
                    pass
            assert isinstance(cm.exception.args[1], MaxRetryDurationError)

    def test_retry_abort_non_recoverable_error(self):
        """GIVEN the server returns a code 501
        WHEN the connector receives this response
        THEN nothing is retried and an exception is raised
        """

        # Code 501 is a Not Implemented error
        with mocked_server_response(status=501):
            with self.assertRaises(RequestError) as cm:
                with self.connection(extra_params=self._retry_policy) as conn:
                    pass
                assert isinstance(cm.exception.args[1], NonRecoverableNetworkError)

    def test_retry_abort_unsafe_execute_statement_retry_condition(self):
        """GIVEN the server sends a code other than 429 or 503
        WHEN the connector sent an ExecuteStatement command
        THEN nothing is retried because it's idempotent
        """
        with self.connection(extra_params=self._retry_policy) as conn:
            with conn.cursor() as cursor:
                # Code 502 is a Bad Gateway, which we commonly see in production under heavy load
                with mocked_server_response(status=502):
                    with self.assertRaises(RequestError) as cm:
                        cursor.execute("Not a real query")
                        assert isinstance(cm.exception.args[1], UnsafeToRetryError)

    def test_retry_dangerous_codes(self):
        """GIVEN the server sends a dangerous code and the user forced this to be retryable
        WHEN the connector sent an ExecuteStatement command
        THEN the command is retried
        """

        # These http codes are not retried by default
        # For some applications, idempotency is not important so we give users a way to force retries anyway
        DANGEROUS_CODES = [502, 504, 400]

        additional_settings = {
            "_retry_dangerous_codes": DANGEROUS_CODES,
            "_retry_stop_after_attempts_count": 1,
        }

        # Prove that these codes are not retried by default
        with self.connection(extra_params={**self._retry_policy}) as conn:
            with conn.cursor() as cursor:
                for dangerous_code in DANGEROUS_CODES:
                    with mocked_server_response(status=dangerous_code) as mock_obj:
                        with self.assertRaises(RequestError) as cm:
                            cursor.execute("Not a real query")
                            assert isinstance(cm.exception.args[1], UnsafeToRetryError)

        # Prove that these codes are retried if forced by the user
        with self.connection(
            extra_params={**self._retry_policy, **additional_settings}
        ) as conn:
            with conn.cursor() as cursor:
                for dangerous_code in DANGEROUS_CODES:
                    with mocked_server_response(status=dangerous_code) as mock_obj:
                        with pytest.raises(MaxRetryError) as cm:
                            cursor.execute("Not a real query")

    def test_retry_safe_execute_statement_retry_condition(self):
        """GIVEN the server sends either code 429 or 503
        WHEN the connector sent an ExecuteStatement command
        THEN the request is retried because these are idempotent
        """

        responses = [
            {"status": 429, "headers": {"Retry-After": "1"}},
            {"status": 503, "headers": {}},
        ]

        with self.connection(
            extra_params={**self._retry_policy, "_retry_stop_after_attempts_count": 1}
        ) as conn:
            with conn.cursor() as cursor:
                # Code 502 is a Bad Gateway, which we commonly see in production under heavy load
                with mock_sequential_server_responses(responses) as mock_obj:
                    with pytest.raises(MaxRetryError):
                        cursor.execute("This query never reaches the server")
                    assert mock_obj.return_value.getresponse.call_count == 2

    def test_retry_abort_close_session_on_404(self):
        """GIVEN the connector sends a CloseSession command
        WHEN server sends a 404 (which is normally retried)
        THEN nothing is retried because 404 means the session already closed
        """

        # First response is a Bad Gateway -> Result is the command actually goes through
        # Second response is a 404 because the session is no longer found
        responses = [
            {"status": 502, "headers": {"Retry-After": "1"}},
            {"status": 404, "headers": {}},
        ]

        with self.connection(extra_params={**self._retry_policy}) as conn:
            with mock_sequential_server_responses(responses):
                with self.assertLogs(
                    "databricks.sql",
                    level="INFO",
                ) as cm:
                    conn.close()
                    expected_message_was_found = False
                    for log in cm.output:
                        if expected_message_was_found:
                            break
                        target = "Session was closed by a prior request"
                        expected_message_was_found = target in log
            self.assertTrue(
                expected_message_was_found, "Did not find expected log messages"
            )

    def test_retry_abort_close_operation_on_404(self):
        """GIVEN the connector sends a CancelOperation command
        WHEN server sends a 404 (which is normally retried)
        THEN nothing is retried because 404 means the operation was already canceled
        """

        # First response is a Bad Gateway -> Result is the command actually goes through
        # Second response is a 404 because the session is no longer found
        responses = [
            {"status": 502, "headers": {"Retry-After": "1"}},
            {"status": 404, "headers": {}},
        ]

        with self.connection(extra_params={**self._retry_policy}) as conn:
            with conn.cursor() as curs:
                with patch(
                    "databricks.sql.utils.ExecuteResponse.has_been_closed_server_side",
                    new_callable=PropertyMock,
                    return_value=False,
                ):
                    # This call guarantees we have an open cursor at the server
                    curs.execute("SELECT 1")
                    with mock_sequential_server_responses(responses):
                        with self.assertLogs(
                            "databricks.sql",
                            level="INFO",
                        ) as cm:
                            curs.close()
                        expected_message_was_found = False
                        for log in cm.output:
                            if expected_message_was_found:
                                break
                            target = "Operation was canceled by a prior request"
                            expected_message_was_found = target in log
                self.assertTrue(
                    expected_message_was_found, "Did not find expected log messages"
                )
