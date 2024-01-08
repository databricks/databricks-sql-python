from tests.e2e.test_driver import PySQLPytestTestCase

from databricks.sql.ae import (
    AsyncExecutionStatus,
    AsyncExecutionException,
    AsyncExecution,
)
import pytest
import time

LONG_RUNNING_QUERY = """
SELECT SUM(A.id - B.id)
FROM range(1000000000) A CROSS JOIN range(100000000) B 
GROUP BY (A.id - B.id)
"""


class TestExecuteAsync(PySQLPytestTestCase):
    @pytest.fixture
    def long_running_ae(self, scope="function") -> AsyncExecution:
        """Start a long-running query so we can make assertions about it."""
        with self.connection() as conn:
            ae = conn.execute_async(LONG_RUNNING_QUERY)
            yield ae

            # cancellation is idempotent
            ae.cancel()

    def test_basic_api(self):
        """This is a WIP test of the basic API defined in PECO-1263"""
        # This is taken directly from the design doc

        with self.connection() as conn:
            ae = conn.execute_async("select :param `col`", {"param": 1})
            while ae.is_running:
                ae.poll_for_status()
                time.sleep(1)

            result = ae.get_result().fetchone()

        assert result.col == 1

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


    def test_staging_operation(self):
        """We need to test what happens with a staging operation since this query won't have a result set
        that user needs. It could be sufficient to notify users that they shouldn't use this API for staging/volumes
        queries...
        """
        assert False
