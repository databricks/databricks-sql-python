import unittest
import pytest
from unittest.mock import Mock

try:
    import pyarrow as pa
except ImportError:
    pa = None

import databricks.sql.client as client
from databricks.sql.backend.types import ExecuteResponse
from databricks.sql.utils import ArrowQueue
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.result_set import ThriftResultSet


@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
class FetchTests(unittest.TestCase):
    """
    Unit tests for checking the fetch logic.
    """

    @staticmethod
    def make_arrow_table(batch):
        n_cols = len(batch[0]) if batch else 0
        schema = pa.schema({"col%s" % i: pa.uint32() for i in range(n_cols)})
        cols = [[batch[row][col] for row in range(len(batch))] for col in range(n_cols)]
        return schema, pa.Table.from_pydict(
            dict(zip(schema.names, cols)), schema=schema
        )

    @staticmethod
    def make_arrow_queue(batch):
        _, table = FetchTests.make_arrow_table(batch)
        queue = ArrowQueue(table, len(batch))
        return queue

    @staticmethod
    def make_dummy_result_set_from_initial_results(initial_results):
        # If the initial results have been set, then we should never try and fetch more
        schema, arrow_table = FetchTests.make_arrow_table(initial_results)
        arrow_queue = ArrowQueue(arrow_table, len(initial_results), 0)

        # Create a mock backend that will return the queue when _fill_results_buffer is called
        mock_thrift_backend = Mock(spec=ThriftDatabricksClient)
        mock_thrift_backend.fetch_results.return_value = (arrow_queue, False, 0)

        num_cols = len(initial_results[0]) if initial_results else 0
        description = [
            (f"col{col_id}", "integer", None, None, None, None, None)
            for col_id in range(num_cols)
        ]

        rs = ThriftResultSet(
            connection=Mock(),
            execute_response=ExecuteResponse(
                command_id=None,
                status=None,
                has_been_closed_server_side=True,
                description=description,
                lz4_compressed=True,
                is_staging_operation=False,
            ),
            thrift_client=mock_thrift_backend,
            t_row_set=None,
        )
        return rs

    @staticmethod
    def make_dummy_result_set_from_batch_list(batch_list):
        batch_index = 0

        def fetch_results(
            command_id,
            max_rows,
            max_bytes,
            expected_row_start_offset,
            lz4_compressed,
            arrow_schema_bytes,
            description,
            use_cloud_fetch=True,
            chunk_id=0,
        ):
            nonlocal batch_index
            results = FetchTests.make_arrow_queue(batch_list[batch_index])
            batch_index += 1

            return results, batch_index < len(batch_list), 0

        mock_thrift_backend = Mock(spec=ThriftDatabricksClient)
        mock_thrift_backend.fetch_results = fetch_results
        num_cols = len(batch_list[0][0]) if batch_list and batch_list[0] else 0

        description = [
            (f"col{col_id}", "integer", None, None, None, None, None)
            for col_id in range(num_cols)
        ]

        rs = ThriftResultSet(
            connection=Mock(),
            execute_response=ExecuteResponse(
                command_id=None,
                status=None,
                has_been_closed_server_side=False,
                description=description,
                lz4_compressed=True,
                is_staging_operation=False,
            ),
            thrift_client=mock_thrift_backend,
        )
        return rs

    def assertEqualRowValues(self, actual, expected):
        self.assertEqual(len(actual) if actual else 0, len(expected) if expected else 0)
        for act, exp in zip(actual, expected):
            self.assertSequenceEqual(act, exp)

    def test_fetchmany_with_initial_results(self):
        # Fetch all in one go
        initial_results_1 = [
            [1],
            [2],
            [3],
        ]  # This is a list of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_1
        )
        self.assertEqualRowValues(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch in small amounts
        initial_results_2 = [[1], [2], [3], [4]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_2
        )
        self.assertEqualRowValues(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqualRowValues(dummy_result_set.fetchmany(2), [[2], [3]])
        self.assertEqualRowValues(dummy_result_set.fetchmany(1), [[4]])

        # Fetch too many
        initial_results_3 = [[2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_3
        )
        self.assertEqualRowValues(dummy_result_set.fetchmany(5), [[2], [3]])

        # Empty results
        initial_results_4 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_4
        )
        self.assertEqualRowValues(dummy_result_set.fetchmany(0), [])

    def test_fetch_many_without_initial_results(self):
        # Fetch all in one go; single batch
        batch_list_1 = [
            [[1], [2], [3]]
        ]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqualRowValues(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch all in one go; multiple batches
        batch_list_2 = [[[1], [2]], [[3]]]  # This is a list of two batches of rows
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqualRowValues(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch in small amounts; single batch
        batch_list_3 = [[[1], [2], [3]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_3)
        self.assertEqualRowValues(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqualRowValues(dummy_result_set.fetchmany(2), [[2], [3]])

        # Fetch in small amounts; multiple batches
        batch_list_4 = [[[1], [2]], [[3], [4], [5]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_4)
        self.assertEqualRowValues(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqualRowValues(dummy_result_set.fetchmany(3), [[2], [3], [4]])
        self.assertEqualRowValues(dummy_result_set.fetchmany(2), [[5]])

        # Fetch too many; single batch
        batch_list_5 = [[[1], [2], [3], [4]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_5)
        self.assertEqualRowValues(dummy_result_set.fetchmany(6), [[1], [2], [3], [4]])

        # Fetch too many; multiple batches
        batch_list_6 = [[[1]], [[2], [3], [4]], [[5], [6]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_6)
        self.assertEqualRowValues(
            dummy_result_set.fetchmany(100), [[1], [2], [3], [4], [5], [6]]
        )

        # Fetch 0; 1 empty batch
        batch_list_7 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_7)
        self.assertEqualRowValues(dummy_result_set.fetchmany(0), [])

        # Fetch 0; lots of batches
        batch_list_8 = [[[1], [2]], [[3]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_8)
        self.assertEqualRowValues(dummy_result_set.fetchmany(0), [])

    def test_fetchall_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_1
        )
        self.assertEqualRowValues(dummy_result_set.fetchall(), [[1], [2], [3]])

    def test_fetchall_without_initial_results(self):
        # Fetch all, single batch
        batch_list_1 = [
            [[1], [2], [3]]
        ]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqualRowValues(dummy_result_set.fetchall(), [[1], [2], [3]])

        # Fetch all, multiple batches
        batch_list_2 = [[[1], [2]], [[3]], [[4], [5], [6]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqualRowValues(
            dummy_result_set.fetchall(), [[1], [2], [3], [4], [5], [6]]
        )

        batch_list_3 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_3)
        self.assertEqualRowValues(dummy_result_set.fetchall(), [])

    def test_fetchmany_fetchall_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_1
        )
        self.assertEqualRowValues(dummy_result_set.fetchmany(2), [[1], [2]])
        self.assertEqualRowValues(dummy_result_set.fetchall(), [[3]])

    def test_fetchmany_fetchall_without_initial_results(self):
        batch_list_1 = [
            [[1], [2], [3]]
        ]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqualRowValues(dummy_result_set.fetchmany(2), [[1], [2]])
        self.assertEqualRowValues(dummy_result_set.fetchall(), [[3]])

        batch_list_2 = [[[1], [2]], [[3], [4]], [[5], [6], [7]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqualRowValues(dummy_result_set.fetchmany(3), [[1], [2], [3]])
        self.assertEqualRowValues(dummy_result_set.fetchall(), [[4], [5], [6], [7]])

    def test_fetchone_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(
            initial_results_1
        )
        self.assertSequenceEqual(dummy_result_set.fetchone(), [1])
        self.assertSequenceEqual(dummy_result_set.fetchone(), [2])
        self.assertSequenceEqual(dummy_result_set.fetchone(), [3])
        self.assertEqual(dummy_result_set.fetchone(), None)

    def test_fetchone_without_initial_results(self):
        batch_list_1 = [[[1], [2]], [[3]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertSequenceEqual(dummy_result_set.fetchone(), [1])
        self.assertSequenceEqual(dummy_result_set.fetchone(), [2])
        self.assertSequenceEqual(dummy_result_set.fetchone(), [3])
        self.assertEqual(dummy_result_set.fetchone(), None)

        batch_list_2 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqual(dummy_result_set.fetchone(), None)


if __name__ == "__main__":
    unittest.main()
