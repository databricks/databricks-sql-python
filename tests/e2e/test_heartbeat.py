"""
End-to-end tests for the Thrift result-set heartbeat.

Requires a real warehouse and the standard env vars:
    DATABRICKS_SERVER_HOSTNAME
    DATABRICKS_HTTP_PATH
    DATABRICKS_TOKEN

Skipped automatically when any of those are not set. Runs in seconds, not
the full 30-min idle window — we use a tiny heartbeat_interval_seconds to
verify the loop is alive, rather than waiting for actual handle eviction.
"""

import os
import time
from contextlib import contextmanager

import pytest

import databricks.sql as sql


pytestmark = pytest.mark.skipif(
    not (
        os.getenv("DATABRICKS_SERVER_HOSTNAME")
        and os.getenv("DATABRICKS_HTTP_PATH")
        and os.getenv("DATABRICKS_TOKEN")
    ),
    reason="DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN not set",
)


@contextmanager
def _connect(**extra):
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        use_cloud_fetch=False,
        **extra,
    )
    try:
        yield conn
    finally:
        conn.close()


def _prime_for_more_rows(cursor):
    """
    Execute a query and drain the direct-results batch so that subsequent
    fetches must round-trip to the server. Guarantees has_more_rows=True
    on the active ResultSet — the path the heartbeat protects.
    """
    cursor.arraysize = 10
    cursor.execute("SELECT id FROM range(0, 100000)")
    first = cursor.fetchmany(10)
    assert len(first) == 10
    sanity = cursor.fetchmany(10)
    assert len(sanity) == 10


class TestThriftHeartbeat:
    def test_heartbeat_polls_during_idle(self):
        with _connect(enable_heartbeat=True, heartbeat_interval_seconds=2) as conn:
            with conn.cursor() as cursor:
                _prime_for_more_rows(cursor)

                rs = cursor.active_result_set
                mgr = rs._heartbeat_manager
                assert mgr is not None, (
                    "heartbeat manager should be constructed when " "has_more_rows=True"
                )
                assert mgr._thread is not None and mgr._thread.is_alive()

                # Wait > 2× interval so the loop has issued multiple polls.
                time.sleep(5)

                assert mgr._poll_count >= 2, (
                    f"expected >= 2 heartbeat polls in 5s at interval=2s, "
                    f"got {mgr._poll_count}"
                )

                # Pull more rows; the heartbeat should still be alive while
                # the server has data left.
                cursor.fetchmany(10)
                assert mgr._thread.is_alive()

    def test_heartbeat_disabled_skips_manager(self):
        with _connect(enable_heartbeat=False) as conn:
            with conn.cursor() as cursor:
                cursor.arraysize = 10
                cursor.execute("SELECT id FROM range(0, 100000)")
                cursor.fetchmany(10)
                rs = cursor.active_result_set
                assert rs._heartbeat_manager is None

    def test_heartbeat_stops_on_close(self):
        with _connect(enable_heartbeat=True, heartbeat_interval_seconds=2) as conn:
            with conn.cursor() as cursor:
                _prime_for_more_rows(cursor)
                rs = cursor.active_result_set
                mgr = rs._heartbeat_manager
                assert mgr is not None and mgr._thread.is_alive()

                rs.close()

                # stop() joins with timeout=5s, so the thread must be dead
                # by the time close() returns.
                assert not mgr._thread.is_alive()
                assert rs._heartbeat_manager is None
