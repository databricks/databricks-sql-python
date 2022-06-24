import unittest
from unittest.mock import Mock

import pyarrow as pa
import uuid
import time
import pytest

import databricks.sql.client as client
from databricks.sql.utils import ExecuteResponse, ArrowQueue


class FetchBenchmarkTests(unittest.TestCase):
    """
    Micro benchmark test for Arrow result handling.
    ( Not included in regular tests started with tests.py )
    """

    @staticmethod
    def make_arrow_table(n_cols, n_rows):
        schema = pa.schema({"col%s" % i: pa.string() for i in range(n_cols)})
        cols = [[str(uuid.uuid4()) for row in range(n_rows)] for col in range(n_cols)]
        return pa.Table.from_pydict(dict(zip(schema.names, cols)), schema=schema)

    @staticmethod
    def make_dummy_result_set_from_initial_results(arrow_table):
        arrow_queue = ArrowQueue(arrow_table, arrow_table.num_rows, 0)
        rs = client.ResultSet(
            connection=None,
            thrift_backend=None,
            execute_response=ExecuteResponse(
                status=None,
                has_been_closed_server_side=True,
                has_more_rows=False,
                description=Mock(),
                command_handle=None,
                arrow_queue=arrow_queue,
                arrow_schema=arrow_table.schema))
        rs.description = [(f'col{col_id}', 'string', None, None, None, None, None)
                          for col_id in range(arrow_table.num_columns)]
        return rs

    @pytest.mark.skip(reason="Test has not been updated for latest connector API (June 2022)")
    def test_benchmark_fetchall(self):
        print("preparing dummy arrow table")
        arrow_table = FetchBenchmarkTests.make_arrow_table(10, 25000)
        benchmark_seconds = 30
        print(f"running test for: {benchmark_seconds} sec.")

        start_time = time.time()
        count = 0
        while time.time() < start_time + benchmark_seconds:
            dummy_result_set = self.make_dummy_result_set_from_initial_results(arrow_table)
            res = dummy_result_set.fetchall()
            for _ in res:
                pass

            count += 1
        print(f"Executed query {count} times, in {time.time() - start_time} seconds")


if __name__ == '__main__':
    unittest.main()
