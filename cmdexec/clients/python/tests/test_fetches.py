import unittest
from unittest.mock import MagicMock
from collections import deque

import pyarrow as pa

import databricks.sql.client as client


class FetchTests(unittest.TestCase):
    """
    Unit tests for checking the fetch logic. See
    qa/test/cmdexec/python/suites/simple_connection_test.py for integration tests that
    interact with the server.
    """

    @staticmethod
    def make_arrow_table(batch):
        n_cols = len(batch[0]) if batch else 0
        schema = pa.schema({"col%s" % i: pa.uint32() for i in range(n_cols)})
        cols = [[batch[row][col] for row in range(len(batch))] for col in range(n_cols)]
        return pa.Table.from_pydict(dict(zip(schema.names, cols)), schema=schema)

    @staticmethod
    def make_arrow_ipc_stream(batch):
        table = FetchTests.make_arrow_table(batch)
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        return sink.getvalue()

    @staticmethod
    def make_arrow_queue(batch):
        table = FetchTests.make_arrow_table(batch)
        queue = client.ArrowQueue(table, len(batch), 0)
        return queue

    @staticmethod
    def make_dummy_result_set_from_initial_results(initial_results):
        # If the initial results have been set, then we should never try and fetch more
        arrow_ipc_stream = FetchTests.make_arrow_ipc_stream(initial_results)
        return client.ResultSet(
            connection=None,
            command_id=None,
            status=None,
            has_been_closed_server_side=True,
            has_more_rows=False,
            arrow_ipc_stream=arrow_ipc_stream,
            num_valid_rows=len(initial_results),
            schema_message=MagicMock())

    @staticmethod
    def make_dummy_result_set_from_batch_list(batch_list):
        batch_index = 0

        class SemiFakeResultSet(client.ResultSet):
            def _fetch_and_deserialize_results(self):
                nonlocal batch_index
                results = FetchTests.make_arrow_queue(batch_list[batch_index])
                batch_index += 1

                return results, batch_index < len(batch_list), \
                       [('id', 'integer', None, None, None, None, None)]

        return SemiFakeResultSet(None, None, None, False, False)

    def test_fetchmany_with_initial_results(self):
        # Fetch all in one go
        initial_results_1 = [[1], [2], [3]]  # This is a list of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_1)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch in small amounts
        initial_results_2 = [[1], [2], [3], [4]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_2)
        self.assertEqual(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqual(dummy_result_set.fetchmany(2), [[2], [3]])
        self.assertEqual(dummy_result_set.fetchmany(1), [[4]])

        # Fetch too many
        initial_results_3 = [[2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_3)
        self.assertEqual(dummy_result_set.fetchmany(5), [[2], [3]])

        # Empty results
        initial_results_4 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_4)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

    def test_fetch_many_without_initial_results(self):
        # Fetch all in one go; single batch
        batch_list_1 = [[[1], [2], [3]]]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch all in one go; multiple batches
        batch_list_2 = [[[1], [2]], [[3]]]  # This is a list of two batches of rows
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch in small amounts; single batch
        batch_list_3 = [[[1], [2], [3]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_3)
        self.assertEqual(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqual(dummy_result_set.fetchmany(2), [[2], [3]])

        # Fetch in small amounts; multiple batches
        batch_list_4 = [[[1], [2]], [[3], [4], [5]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_4)
        self.assertEqual(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqual(dummy_result_set.fetchmany(3), [[2], [3], [4]])
        self.assertEqual(dummy_result_set.fetchmany(2), [[5]])

        # Fetch too many; single batch
        batch_list_5 = [[[1], [2], [3], [4]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_5)
        self.assertEqual(dummy_result_set.fetchmany(6), [[1], [2], [3], [4]])

        # Fetch too many; multiple batches
        batch_list_6 = [[[1]], [[2], [3], [4]], [[5], [6]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_6)
        self.assertEqual(dummy_result_set.fetchmany(100), [[1], [2], [3], [4], [5], [6]])

        # Fetch 0; 1 empty batch
        batch_list_7 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_7)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

        # Fetch 0; lots of batches
        batch_list_8 = [[[1], [2]], [[3]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_8)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

    def test_fetchall_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_1)
        self.assertEqual(dummy_result_set.fetchall(), [[1], [2], [3]])

    def test_fetchall_without_initial_results(self):
        # Fetch all, single batch
        batch_list_1 = [[[1], [2], [3]]]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqual(dummy_result_set.fetchall(), [[1], [2], [3]])

        # Fetch all, multiple batches
        batch_list_2 = [[[1], [2]], [[3]], [[4], [5], [6]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqual(dummy_result_set.fetchall(), [[1], [2], [3], [4], [5], [6]])

        batch_list_3 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_3)
        self.assertEqual(dummy_result_set.fetchall(), [])

    def test_fetchmany_fetchall_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_1)
        self.assertEqual(dummy_result_set.fetchmany(2), [[1], [2]])
        self.assertEqual(dummy_result_set.fetchall(), [[3]])

    def test_fetchmany_fetchall_without_initial_results(self):
        batch_list_1 = [[[1], [2], [3]]]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqual(dummy_result_set.fetchmany(2), [[1], [2]])
        self.assertEqual(dummy_result_set.fetchall(), [[3]])

        batch_list_2 = [[[1], [2]], [[3], [4]], [[5], [6], [7]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])
        self.assertEqual(dummy_result_set.fetchall(), [[4], [5], [6], [7]])

    def test_fetchone_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set_from_initial_results(initial_results_1)
        self.assertEqual(dummy_result_set.fetchone(), [1])
        self.assertEqual(dummy_result_set.fetchone(), [2])
        self.assertEqual(dummy_result_set.fetchone(), [3])
        self.assertEqual(dummy_result_set.fetchone(), None)

    def test_fetchone_without_initial_results(self):
        batch_list_1 = [[[1], [2]], [[3]]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_1)
        self.assertEqual(dummy_result_set.fetchone(), [1])
        self.assertEqual(dummy_result_set.fetchone(), [2])
        self.assertEqual(dummy_result_set.fetchone(), [3])
        self.assertEqual(dummy_result_set.fetchone(), None)

        batch_list_2 = [[]]
        dummy_result_set = self.make_dummy_result_set_from_batch_list(batch_list_2)
        self.assertEqual(dummy_result_set.fetchone(), None)


if __name__ == '__main__':
    unittest.main()
