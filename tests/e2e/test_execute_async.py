from tests.e2e.test_driver import PySQLPytestTestCase

from databricks.sql.ae import AsyncExecutionStatus as AsyncExecutionStatus

import time

LONG_RUNNING_QUERY =  """
SELECT SUM(A.id - B.id)
FROM range(1000000000) A CROSS JOIN range(100000000) B 
GROUP BY (A.id - B.id)
"""

class TestExecuteAsync(PySQLPytestTestCase):

    def test_basic_api(self):
        """This is a WIP test of the basic API defined in PECO-1263
        """
        # This is taken directly from the design doc

        with self.connection() as conn:
            ae = conn.execute_async("select :param `col`", {"param": 1})
            while ae.is_running:
                ae.get_result_or_status()
                time.sleep(1)
            
            result = ae.get_result_or_status().fetchone()

        assert result.col == 1

    def test_cancel_running_query(self):
        """Start a long-running query and cancel it
        """

        with self.connection() as conn:
            ae = conn.execute_async(LONG_RUNNING_QUERY)
            time.sleep(2)
            ae.cancel()

            status = ae.get_result_or_status()

        assert ae.status == AsyncExecutionStatus.CANCELED

    

    def test_staging_operation(self):
        """We need to test what happens with a staging operation since this query won't have a result set
        that user needs. It could be sufficient to notify users that they shouldn't use this API for staging/volumes
        queries...
        """
        assert False