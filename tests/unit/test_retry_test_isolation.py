from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from unittest.mock import MagicMock, patch

import pytest
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

from tests.e2e.common.retry_test_mixins import (
    SimpleHttpResponse,
    _mock_pool_method,
    mocked_server_response,
    mock_sequential_server_responses,
)


@contextmanager
def _offline_pool():
    connection = MagicMock()
    connection.sock = None
    connection.close.return_value = None
    connection.getresponse.side_effect = lambda: SimpleHttpResponse(200, {})
    with HTTPSConnectionPool("example.invalid") as pool, patch.object(
        pool, "_new_conn", return_value=connection
    ):
        yield pool


@pytest.fixture
def pending_telemetry():
    ready, release = Event(), Event()

    def send(pool):
        ready.set()
        assert release.wait(timeout=5)
        return pool.urlopen("POST", "/telemetry-ext", retries=False)

    with _offline_pool() as pool, ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(send, pool)
        assert ready.wait(timeout=5)
        try:
            yield future, release
        finally:
            release.set()


@pytest.mark.parametrize("sequential", [False, True])
def test_background_telemetry_does_not_consume_retry_responses(
    pending_telemetry, sequential
):
    responses = [
        {"status": 429, "headers": {"Retry-After": "0"}, "redirect_location": None},
        {"status": 503, "headers": {}, "redirect_location": None},
    ]
    mock_response = (
        mock_sequential_server_responses(responses)
        if sequential
        else mocked_server_response(status=429, headers={"Retry-After": "0"})
    )
    future, release = pending_telemetry
    with _offline_pool() as pool, mock_response as mocked_connection:
        release.set()
        assert future.result(timeout=5).status == 200
        mocked_connection.assert_not_called()

        with pytest.raises(MaxRetryError):
            pool.urlopen(
                "POST",
                "/sql",
                retries=Retry(
                    total=1, status_forcelist=[429, 503], allowed_methods=None
                ),
            )
        assert mocked_connection.return_value.getresponse.call_count == 2


def test_background_telemetry_does_not_receive_retry_errors(pending_telemetry):
    future, release = pending_telemetry
    with _offline_pool() as pool, _mock_pool_method("_validate_conn") as validate:
        validate.side_effect = OSError("Some arbitrary network error")
        release.set()
        assert future.result(timeout=5).status == 200
        validate.assert_not_called()

        with pytest.raises(MaxRetryError):
            pool.urlopen("POST", "/sql", retries=Retry(total=1, allowed_methods=None))
        assert validate.call_count == 2


def test_mocked_connections_do_not_leak_into_a_shared_pool():
    ready, release = Event(), Event()
    responses = [
        {"status": 429, "headers": {"Retry-After": "0"}, "redirect_location": None},
        {"status": 503, "headers": {}, "redirect_location": None},
    ]
    with _offline_pool() as pool, ThreadPoolExecutor(max_workers=1) as executor:
        connection = pool._new_conn.return_value

        def new_connection():
            # Hold the real pool's slot while the test sends mocked requests.
            ready.set()
            assert release.wait(timeout=5)
            return connection

        def send():
            return [
                pool.urlopen("POST", "/telemetry-ext", retries=False).status
                for _ in range(2)
            ]

        pool._new_conn.side_effect = new_connection
        future = executor.submit(send)
        assert ready.wait(timeout=5)
        try:
            with mock_sequential_server_responses(responses) as mocked_connection:
                with pytest.raises(MaxRetryError):
                    pool.urlopen(
                        "POST",
                        "/sql",
                        retries=Retry(
                            total=1, status_forcelist=[429, 503], allowed_methods=None
                        ),
                    )
                release.set()
                assert future.result(timeout=5) == [200, 200]
                assert mocked_connection.return_value.getresponse.call_count == 2
        finally:
            release.set()
