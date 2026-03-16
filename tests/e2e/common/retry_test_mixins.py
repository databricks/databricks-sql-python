from contextlib import contextmanager
import time
from typing import Optional, List
from unittest.mock import MagicMock, PropertyMock, patch
import io

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
    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_client_should_retry_automatically_when_getting_429(self, extra_params):
        with self.cursor(extra_params) as cursor:
            for _ in range(10):
                cursor.execute("SELECT 1")
                rows = cursor.fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], 1)

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_client_should_not_retry_429_if_RateLimitRetry_is_0(self, extra_params):
        with pytest.raises(self.error_type) as cm:
            extra_params = {**extra_params, **self.conf_to_disable_rate_limit_retries}
            with self.cursor(extra_params) as cursor:
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
    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_wait_cluster_startup(self, extra_params):
        with self.cursor(extra_params) as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def _test_retry_disabled_with_message(
        self, error_msg_substring, exception_type, extra_params
    ):
        with pytest.raises(exception_type) as cm:
            with self.connection(
                self.conf_to_disable_temporarily_unavailable_retries, extra_params
            ):
                pass
        assert error_msg_substring in str(cm.exception)


class SimpleHttpResponse:
    """A simple HTTP response mock that works with both urllib3 v1.x and v2.x"""
    
    def __init__(self, status: int, headers: dict, redirect_location: Optional[str] = None):
        # Import the correct HTTP message type that urllib3 v1.x expects
        try:
            from http.client import HTTPMessage
        except ImportError:
            from httplib import HTTPMessage
            
        self.status = status
        # Create proper HTTPMessage for urllib3 v1.x compatibility  
        self.headers = HTTPMessage()
        for key, value in headers.items():
            self.headers[key] = str(value)
        self.msg = self.headers  # For urllib3~=1.0.0 compatibility
        self.reason = "Mocked Response"
        self.version = 11
        self.length = 0
        self.length_remaining = 0
        self._redirect_location = redirect_location
        self._body = b""
        self._fp = io.BytesIO(self._body)
        self._url = "https://example.com"
        
    def get_redirect_location(self, *args, **kwargs):
        """Return the redirect location or False"""
        return False if self._redirect_location is None else self._redirect_location
        
    def read(self, amt=None):
        """Mock read method for file-like behavior"""
        return self._body
        
    def close(self):
        """Mock close method"""
        pass
        
    def drain_conn(self):
        """Mock drain_conn method for urllib3 v2.x"""
        pass
        
    def isclosed(self):
        """Mock isclosed method for urllib3 v1.x"""
        return False
        
    def release_conn(self):
        """Mock release_conn method for thrift HTTP client"""
        pass
        
    @property
    def data(self):
        """Mock data property for urllib3 v2.x"""
        return self._body
        
    @property
    def url(self):
        """Mock url property"""
        return self._url
        
    @url.setter
    def url(self, value):
        """Mock url setter"""
        self._url = value


@contextmanager
def mocked_server_response(
    status: int = 200, headers: dict = {}, redirect_location: Optional[str] = None
):
    """Context manager for patching urllib3 responses with version compatibility"""
    
    mock_response = SimpleHttpResponse(status, headers, redirect_location)

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
        - redirect_location: str
    """

    mock_responses = [
        SimpleHttpResponse(
            status=resp["status"],
            headers=resp["headers"],
            redirect_location=resp["redirect_location"]
        )
        for resp in responses
    ]

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
        "_retry_delay_min": 0.1,
        "_retry_delay_max": 3,
        "_retry_stop_after_attempts_count": 5,
        "_retry_stop_after_attempts_duration": 30,
        "_retry_delay_default": 0.5,
    }

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient._send_telemetry")
    def test_retry_urllib3_settings_are_honored(
        self, mock_send_telemetry, extra_params
    ):
        """Databricks overrides some of urllib3's configuration. This tests confirms that what configuration
        we DON'T override is preserved in urllib3's internals
        """

        urllib3_config = {"connect": 10, "read": 11, "redirect": 12}
        rp = DatabricksRetryPolicy(
            delay_min=0.1,
            delay_max=3,
            stop_after_attempts_count=10,
            stop_after_attempts_duration=10.0,
            delay_default=1.0,
            force_dangerous_codes=[],
            urllib3_kwargs=urllib3_config,
        )

        assert rp.connect == 10
        assert rp.read == 11
        assert rp.redirect == 12

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient._send_telemetry")
    def test_oserror_retries(self, mock_send_telemetry, extra_params):
        """If a network error occurs during make_request, the request is retried according to policy"""
        with patch(
            "urllib3.connectionpool.HTTPSConnectionPool._validate_conn",
        ) as mock_validate_conn:
            mock_validate_conn.side_effect = OSError("Some arbitrary network error")
            with pytest.raises(MaxRetryError) as cm:
                extra_params = {**extra_params, **self._retry_policy}
                with self.connection(extra_params=extra_params) as conn:
                    pass

            assert mock_validate_conn.call_count == 6

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient._send_telemetry")
    def test_retry_max_count_not_exceeded(self, mock_send_telemetry, extra_params):
        """GIVEN the max_attempts_count is 5
        WHEN the server sends nothing but 429 responses
        THEN the connector issues six request (original plus five retries)
            before raising an exception
        """
        with mocked_server_response(status=429, headers={"Retry-After": "0"}) as mock_obj:
            with pytest.raises(MaxRetryError) as cm:
                extra_params = {**extra_params, **self._retry_policy}
                with self.connection(extra_params=extra_params) as conn:
                    pass
            assert mock_obj.return_value.getresponse.call_count == 6

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient._send_telemetry")
    def test_retry_exponential_backoff(self, mock_send_telemetry, extra_params):
        """GIVEN the retry policy is configured for reasonable exponential backoff
        WHEN the server sends nothing but 429 responses with retry-afters
        THEN the connector will use those retry-afters values as floor
        """
        retry_policy = self._retry_policy.copy()
        retry_policy["_retry_delay_min"] = 1

        time_start = time.time()
        with mocked_server_response(
            status=429, headers={"Retry-After": "8"}
        ) as mock_obj:
            with pytest.raises(RequestError) as cm:
                extra_params = {**extra_params, **retry_policy}
                with self.connection(extra_params=extra_params) as conn:
                    pass

            duration = time.time() - time_start
            assert isinstance(cm.value.args[1], MaxRetryDurationError)

            # With setting delay_min to 1, the expected retry delays should be:
            # 8, 8, 8, 8
            # The first 3 retries are allowed, the 4th retry puts the total duration over the limit
            # of 30 seconds
            assert mock_obj.return_value.getresponse.call_count == 4
            assert duration > 24

            # Should be less than 26, but this is a safe margin for CI/CD slowness
            assert duration < 30

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_max_duration_not_exceeded(self, extra_params):
        """GIVEN the max attempt duration of 10 seconds
        WHEN the server sends a Retry-After header of 60 seconds
        THEN the connector raises a MaxRetryDurationError
        """
        with mocked_server_response(status=429, headers={"Retry-After": "60"}):
            with pytest.raises(RequestError) as cm:
                extra_params = {**extra_params, **self._retry_policy}
                with self.connection(extra_params=extra_params) as conn:
                    pass
            assert isinstance(cm.value.args[1], MaxRetryDurationError)

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_abort_non_recoverable_error(self, extra_params):
        """GIVEN the server returns a code 501
        WHEN the connector receives this response
        THEN nothing is retried and an exception is raised
        """

        # Code 501 is a Not Implemented error
        with mocked_server_response(status=501):
            with pytest.raises(RequestError) as cm:
                extra_params = {**extra_params, **self._retry_policy}
                with self.connection(extra_params=extra_params) as conn:
                    pass
                assert isinstance(cm.value.args[1], NonRecoverableNetworkError)

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_abort_unsafe_execute_statement_retry_condition(self, extra_params):
        """GIVEN the server sends a code other than 429 or 503
        WHEN the connector sent an ExecuteStatement command
        THEN nothing is retried because it's idempotent
        """
        extra_params = {**extra_params, **self._retry_policy}
        with self.connection(extra_params=extra_params) as conn:
            with conn.cursor() as cursor:
                # Code 502 is a Bad Gateway, which we commonly see in production under heavy load
                with mocked_server_response(status=502):
                    with pytest.raises(RequestError) as cm:
                        cursor.execute("Not a real query")
                        assert isinstance(cm.value.args[1], UnsafeToRetryError)

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_dangerous_codes(self, extra_params):
        """GIVEN the server sends a dangerous code and the user forced this to be retryable
        WHEN the connector sent an ExecuteStatement command
        THEN the command is retried
        """

        # These http codes are not retried by default
        # For some applications, idempotency is not important so we give users a way to force retries anyway
        DANGEROUS_CODES = [502, 504]

        additional_settings = {
            "_retry_dangerous_codes": DANGEROUS_CODES,
            "_retry_stop_after_attempts_count": 1,
        }

        # Prove that these codes are not retried by default
        extra_params = {**extra_params, **self._retry_policy}
        with self.connection(extra_params=extra_params) as conn:
            with conn.cursor() as cursor:
                for dangerous_code in DANGEROUS_CODES:
                    with mocked_server_response(status=dangerous_code):
                        with pytest.raises(RequestError) as cm:
                            cursor.execute("Not a real query")
                            assert isinstance(cm.value.args[1], UnsafeToRetryError)

        # Prove that these codes are retried if forced by the user
        with self.connection(
            extra_params={**extra_params, **self._retry_policy, **additional_settings}
        ) as conn:
            with conn.cursor() as cursor:
                for dangerous_code in DANGEROUS_CODES:
                    with mocked_server_response(status=dangerous_code):
                        with pytest.raises(MaxRetryError) as cm:
                            cursor.execute("Not a real query")

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_safe_execute_statement_retry_condition(self, extra_params):
        """GIVEN the server sends either code 429 or 503
        WHEN the connector sent an ExecuteStatement command
        THEN the request is retried because these are idempotent
        """

        responses = [
            {"status": 429, "headers": {"Retry-After": "1"}, "redirect_location": None},
            {"status": 503, "headers": {}, "redirect_location": None},
        ]

        with self.connection(
            extra_params={
                **extra_params,
                **self._retry_policy,
                "_retry_stop_after_attempts_count": 1,
            }
        ) as conn:
            with conn.cursor() as cursor:
                # Code 502 is a Bad Gateway, which we commonly see in production under heavy load
                with mock_sequential_server_responses(responses) as mock_obj:
                    with pytest.raises(MaxRetryError):
                        cursor.execute("This query never reaches the server")
                    assert mock_obj.return_value.getresponse.call_count == 2

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_abort_close_session_on_404(self, extra_params, caplog):
        """GIVEN the connector sends a CloseSession command
        WHEN server sends a 404 (which is not retried since commit 41b28159)
        THEN nothing is retried because 404 is globally non-retryable
        """

        # With the idempotency-based retry refactor, 404 is now globally non-retryable
        # regardless of command type. The close() method catches RequestError and proceeds.
        responses = [
            {"status": 404, "headers": {}, "redirect_location": None},
        ]

        extra_params = {**extra_params, **self._retry_policy}
        with self.connection(extra_params=extra_params) as conn:
            with mock_sequential_server_responses(responses):
                # Should not raise an exception, the error is caught internally
                conn.close()

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_abort_close_operation_on_404(self, extra_params, caplog):
        """GIVEN the connector sends a CancelOperation command
        WHEN server sends a 404 (which is not retried since commit 41b28159)
        THEN nothing is retried because 404 is globally non-retryable
        """

        # With the idempotency-based retry refactor, 404 is now globally non-retryable
        # regardless of command type. The close() method catches RequestError and proceeds.
        responses = [
            {"status": 404, "headers": {}, "redirect_location": None},
        ]

        extra_params = {**extra_params, **self._retry_policy}
        with self.connection(extra_params=extra_params) as conn:
            with conn.cursor() as curs:
                with patch(
                    "databricks.sql.backend.types.ExecuteResponse.has_been_closed_server_side",
                    new_callable=PropertyMock,
                    return_value=False,
                ):
                    # This call guarantees we have an open cursor at the server
                    curs.execute("SELECT 1")
                    with mock_sequential_server_responses(responses):
                        # Should not raise an exception, the error is caught internally
                        curs.close()

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient._send_telemetry")
    def test_3xx_redirect_codes_are_not_retried(
        self, mock_send_telemetry, extra_params
    ):
        """GIVEN the connector is configured with a custom max_redirects
        WHEN the DatabricksRetryPolicy receives a 302 redirect
        THEN the connector does not retry since 3xx codes are not retried per policy
        """

        max_redirects, expected_call_count = 1, 1

        # Code 302 is a redirect, but 3xx codes are not retried per policy
        # Note: We don't set redirect_location because that would cause urllib3 v2.x 
        # to follow redirects internally, bypassing our retry policy test
        with mocked_server_response(
            status=302, redirect_location=None
        ) as mock_obj:
            with pytest.raises(RequestError):  # Should get RequestError, not MaxRetryError
                with self.connection(
                    extra_params={
                        **extra_params,
                        **self._retry_policy,
                        "_retry_max_redirects": max_redirects,
                    }
                ):
                    pass
            # Total call count should be 1 (original only, no retries for 3xx codes)
            assert mock_obj.return_value.getresponse.call_count == expected_call_count

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    @patch("databricks.sql.telemetry.telemetry_client.TelemetryClient._send_telemetry")
    def test_3xx_codes_not_retried_regardless_of_max_redirects_setting(
        self, mock_send_telemetry, extra_params
    ):
        """GIVEN the connector is configured without a custom max_redirects
        WHEN the DatabricksRetryPolicy receives a 302 redirect
        THEN the connector does not retry since 3xx codes are not retried per policy

        This test confirms that 3xx codes (including redirects) are not retried
        according to the DatabricksRetryPolicy regardless of redirect settings.
        """
        # Code 302 is a redirect, but 3xx codes are not retried per policy
        # Note: We don't set redirect_location because that would cause urllib3 v2.x 
        # to follow redirects internally, bypassing our retry policy test
        with mocked_server_response(
            status=302, redirect_location=None
        ) as mock_obj:
            with pytest.raises(RequestError):  # Should get RequestError, not MaxRetryError
                with self.connection(
                    extra_params={
                        **extra_params,
                        **self._retry_policy,
                    }
                ):
                    pass

            # Total call count should be 1 (original only, no retries for 3xx codes)
            assert mock_obj.return_value.getresponse.call_count == 1

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_3xx_codes_stop_request_immediately_no_retry_attempts(
        self, extra_params
    ):
        # Since 3xx codes are not retried per policy, we only ever see the first 302 response
        responses = [
            {"status": 302, "headers": {}, "redirect_location": "/foo.bar"},
            {"status": 500, "headers": {}, "redirect_location": None},  # Never reached
        ]

        additional_settings = {
            "_retry_max_redirects": 1,
            "_retry_stop_after_attempts_count": 2,
        }

        with pytest.raises(RequestError) as cm:
            with mock_sequential_server_responses(responses):
                with self.connection(
                    extra_params={
                        **extra_params,
                        **self._retry_policy,
                        **additional_settings,
                    }
                ):
                    pass

        # The error should be the result of the 302, since 3xx codes are not retried
        assert "too many redirects" not in str(cm.value.message)
        assert "Error during request to server" in str(cm.value.message)

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_max_redirects_exceeds_max_attempts_count_warns_user(
        self, extra_params, caplog
    ):
        with self.connection(
            extra_params={
                **extra_params,
                **self._retry_policy,
                **{
                    "_retry_max_redirects": 100,
                    "_retry_stop_after_attempts_count": 1,
                },
            }
        ):
            assert "it will have no affect!" in caplog.text

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_retry_legacy_behavior_warns_user(self, extra_params, caplog):
        with self.connection(
            extra_params={
                **extra_params,
                **self._retry_policy,
                "_enable_v3_retries": False,
            }
        ):
            assert (
                "Legacy retry behavior is enabled for this connection." in caplog.text
            )

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_403_not_retried(self, extra_params):
        """GIVEN the server returns a code 403
        WHEN the connector receives this response
        THEN nothing is retried and an exception is raised
        """

        # Code 403 is a Forbidden error
        with mocked_server_response(status=403):
            with pytest.raises(RequestError) as cm:
                extra_params = {**extra_params, **self._retry_policy}
                with self.connection(extra_params=extra_params) as conn:
                    pass
                assert isinstance(cm.value.args[1], NonRecoverableNetworkError)

    @pytest.mark.parametrize(
        "extra_params",
        [
            {},
            {"use_sea": True},
        ],
    )
    def test_401_not_retried(self, extra_params):
        """GIVEN the server returns a code 401
        WHEN the connector receives this response
        THEN nothing is retried and an exception is raised
        """

        # Code 401 is an Unauthorized error
        with mocked_server_response(status=401):
            with pytest.raises(RequestError) as cm:
                extra_params = {**extra_params, **self._retry_policy}
                with self.connection(extra_params=extra_params):
                    pass
                assert isinstance(cm.value.args[1], NonRecoverableNetworkError)
