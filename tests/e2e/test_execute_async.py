from tests.e2e.test_driver import PySQLPytestTestCase

from databricks.sql.ae import (
    AsyncExecutionStatus,
    AsyncExecutionException,
    AsyncExecution,
)
import pytest
import time

import threading

BASE_LONG_QUERY = """
SELECT SUM(A.id - B.id)
FROM range({val}) A CROSS JOIN range({val}) B 
GROUP BY (A.id - B.id)
"""
GT_ONE_MINUTE_VALUE = 100000000

# Arrived at this value through some manual testing on a serverless SQL warehouse
# The goal here is to have a query that takes longer than five seconds (therefore bypassing directResults)
# but not so long that I can't attempt to fetch its results in a reasonable amount of time
GT_FIVE_SECONDS_VALUE = 90000

LONG_RUNNING_QUERY = BASE_LONG_QUERY.format(val=GT_ONE_MINUTE_VALUE)
LONG_ISH_QUERY = BASE_LONG_QUERY.format(val=GT_FIVE_SECONDS_VALUE)

# This query should always return in < 5 seconds and therefore should be a direct result
DIRECT_RESULTS_QUERY = "select :param `col`"


class TestExecuteAsync(PySQLPytestTestCase):
    @pytest.fixture
    def long_running_ae(self, scope="function") -> AsyncExecution:
        """Start a long-running query so we can make assertions about it."""
        with self.connection() as conn:
            ae = conn.execute_async(LONG_RUNNING_QUERY)
            yield ae

            # cancellation is idempotent
            ae.cancel()

    @pytest.fixture
    def long_ish_ae(self, scope="function") -> AsyncExecution:
        """Start a long-running query so we can make assertions about it."""
        with self.connection() as conn:
            ae = conn.execute_async(LONG_ISH_QUERY)
            yield ae

    def test_execute_async(self):
        """This is a WIP test of the basic API defined in PECO-1263"""
        # This is taken directly from the design doc

        with self.connection() as conn:
            ae = conn.execute_async(DIRECT_RESULTS_QUERY, {"param": 1})
            while ae.is_running:
                ae.poll_for_status()
                time.sleep(1)

            result = ae.get_result().fetchone()

        assert result.col == 1

    def test_direct_results_query_canary(self):
        """This test verifies that on the current endpoint, the DIRECT_RESULTS_QUERY returned a thrift operation state
        other than FINISHED_STATE. If this test fails, it means the SQL warehouse got slower at executing this query
        """

        with self.connection() as conn:
            ae = conn.execute_async(DIRECT_RESULTS_QUERY, {"param": 1})
            assert not ae.is_running

    def test_cancel_running_query(self, long_running_ae: AsyncExecution):
        long_running_ae.cancel()
        assert long_running_ae.status == AsyncExecutionStatus.CANCELED

    def test_cant_get_results_while_running(self, long_running_ae: AsyncExecution):
        with pytest.raises(AsyncExecutionException, match="Query is still running"):
            long_running_ae.get_result()

    def test_cant_get_results_after_cancel(self, long_running_ae: AsyncExecution):
        long_running_ae.cancel()
        with pytest.raises(AsyncExecutionException, match="Query was canceled"):
            long_running_ae.get_result()

    def test_get_async_execution_can_check_status(self, long_running_ae: AsyncExecution):
        query_id, query_secret = str(long_running_ae.query_id), str(
            long_running_ae.query_secret
        )

        with self.connection() as conn:
            ae = conn.get_async_execution(query_id, query_secret)
            assert ae.is_running

    def test_get_async_execution_can_cancel_across_threads(self, long_running_ae: AsyncExecution):
        query_id, query_secret = str(long_running_ae.query_id), str(
            long_running_ae.query_secret
        )

        def cancel_query_in_separate_thread(query_id, query_secret):
            with self.connection() as conn:
                ae = conn.get_async_execution(query_id, query_secret)
                ae.cancel()

        threading.Thread(
            target=cancel_query_in_separate_thread, args=(query_id, query_secret)
        ).start()

        time.sleep(5)

        long_running_ae.poll_for_status()
        assert long_running_ae.status == AsyncExecutionStatus.CANCELED

    def test_long_ish_query_canary(self, long_ish_ae: AsyncExecution):
        """This test verifies that on the current endpoint, the LONG_ISH_QUERY requires
        at least one poll_for_status call before it is finished. If this test fails, it means
        the SQL warehouse got faster at executing this query and we should increment the value
        of GT_FIVE_SECONDS_VALUE

        It would be easier to do this if Databricks SQL had a SLEEP() function :/
        """

        poll_count = 0
        while long_ish_ae.is_running:
            time.sleep(1)
            long_ish_ae.poll_for_status()
            poll_count += 1

        assert poll_count > 0

    def test_get_async_execution_and_get_results_without_direct_results(
        self, long_ish_ae: AsyncExecution
    ):
        while long_ish_ae.is_running:
            time.sleep(1)
            long_ish_ae.poll_for_status()

        result = long_ish_ae.get_result().fetchone()
        assert len(result) == 1

    def test_get_async_execution_with_bogus_query_id(self):

        with self.connection() as conn:
            with pytest.raises(AsyncExecutionException, match="Query not found"):
                ae = conn.get_async_execution("bedc786d-64da-45d4-99da-5d3603525803", "ba469f82-cf3f-454e-b575-f4dcd58dd692")

    def test_get_async_execution_with_badly_formed_query_id(self):
        with self.connection() as conn:
            with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
                ae = conn.get_async_execution("foo", "bar")

    def test_serialize(self, long_running_ae: AsyncExecution):
        serialized = long_running_ae.serialize()
        query_id, query_secret = serialized.split(":")

        with self.connection() as conn:
            ae = conn.get_async_execution(query_id, query_secret)
            assert ae.is_running

    def test_get_async_execution_no_results_when_direct_results_were_sent(self):
        """It remains to be seen whether results can be fetched repeatedly from a "picked up" execution.
        """

        with self.connection() as conn:
            ae = conn.execute_async(DIRECT_RESULTS_QUERY, {"param": 1})
            query_id, query_secret = ae.serialize().split(":")
            ae.get_result()

        with self.connection() as conn:
            with pytest.raises(AsyncExecutionException, match="Query not found"):
                ae_late = conn.get_async_execution(query_id, query_secret)

    def test_get_async_execution_and_fetch_results(self, long_ish_ae: AsyncExecution):
        """This tests currently _fails_ because of how result fetching is factored.

        Currently, thrift_backend.py can't fetch results unless it has a TExecuteStatementResp object.
        But with async executions, we don't have the original TExecuteStatementResp. So we'll need to build
        a way to "fake" this until we can refactor thrift_backend.py to be more testable.
        """

        query_id, query_secret = long_ish_ae.serialize().split(":")

        with self.connection() as conn:
            ae = conn.get_async_execution(query_id, query_secret)

            while ae.is_running:
                time.sleep(1)
                ae.poll_for_status()

            result = ae.get_result().fetchone()

            assert len(result) == 1

        