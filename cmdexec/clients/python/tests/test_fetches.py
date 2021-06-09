import unittest
from collections import deque

import cmdexec.clients.python.command_exec_client as command_exec_client


class FetchTests(unittest.TestCase):
    """
    Unit tests for checking the fetch logic. See
    qa/test/cmdexec/python/suites/simple_connection_test.py for integration tests that
    interact with the server.
    """

    def make_dummy_result_set(self, initial_results=None, batch_list=None):
        if initial_results is not None:
            # If the initial results have been set, then we should never try and fetch more
            result_set = command_exec_client.ResultSet(None, None, None, True)
            result_set.has_more_rows = False
            result_set.results = deque(initial_results)
            return result_set
        elif batch_list is not None:
            batch_index = 0

            class SemiFakeResultSet(command_exec_client.ResultSet):
                def _fetch_and_deserialize_results(self):
                    nonlocal batch_index
                    results = deque(batch_list[batch_index])
                    batch_index += 1

                    return results, batch_index < len(batch_list)

            return SemiFakeResultSet(None, None, None, False)

    def test_fetchmany_with_initial_results(self):
        # Fetch all in one go
        initial_results_1 = [[1], [2], [3]]  # This is a list of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_1)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch in small amounts
        initial_results_2 = [[1], [2], [3], [4]]
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_2)
        self.assertEqual(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqual(dummy_result_set.fetchmany(2), [[2], [3]])
        self.assertEqual(dummy_result_set.fetchmany(1), [[4]])

        # Fetch too many
        initial_results_3 = [[2], [3]]
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_3)
        self.assertEqual(dummy_result_set.fetchmany(5), [[2], [3]])

        # Empty results
        initial_results_4 = []
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_4)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

    def test_fetch_many_without_initial_results(self):
        # Fetch all in one go; single batch
        batch_list_1 = [[[1], [2], [3]]]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_1)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch all in one go; multiple batches
        batch_list_2 = [[[1], [2]], [[3]]]  # This is a list of two batches of rows
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_2)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])

        # Fetch in small amounts; single batch
        batch_list_3 = [[[1], [2], [3]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_3)
        self.assertEqual(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqual(dummy_result_set.fetchmany(2), [[2], [3]])

        # Fetch in small amounts; multiple batches
        batch_list_4 = [[[1], [2]], [[3], [4], [5]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_4)
        self.assertEqual(dummy_result_set.fetchmany(1), [[1]])
        self.assertEqual(dummy_result_set.fetchmany(3), [[2], [3], [4]])
        self.assertEqual(dummy_result_set.fetchmany(2), [[5]])

        # Fetch too many; single batch
        batch_list_5 = [[[1], [2], [3], [4]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_5)
        self.assertEqual(dummy_result_set.fetchmany(6), [[1], [2], [3], [4]])

        # Fetch too many; multiple batches
        batch_list_6 = [[[1]], [[2], [3], [4]], [[5], [6]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_6)
        self.assertEqual(dummy_result_set.fetchmany(100), [[1], [2], [3], [4], [5], [6]])

        # Fetch 0; 1 empty batch
        batch_list_7 = [[]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_7)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

        # Fetch 0; lots of batches
        batch_list_8 = [[[1], [2]], [[3]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_8)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

        # Fetch 0; 0 batches
        batch_list_9 = []
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_9)
        self.assertEqual(dummy_result_set.fetchmany(0), [])

    def test_fetchall_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_1)
        self.assertEqual(dummy_result_set.fetchall(), [[1], [2], [3]])

        initial_results_2 = []
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_2)
        self.assertEqual(dummy_result_set.fetchall(), [])

    def test_fetchall_without_initial_results(self):
        # Fetch all, single batch
        batch_list_1 = [[[1], [2], [3]]]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_1)
        self.assertEqual(dummy_result_set.fetchall(), [[1], [2], [3]])

        # Fetch all, multiple batches
        batch_list_2 = [[[1], [2]], [[3]], [[4], [5], [6]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_2)
        self.assertEqual(dummy_result_set.fetchall(), [[1], [2], [3], [4], [5], [6]])

        batch_list_3 = [[]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_3)
        self.assertEqual(dummy_result_set.fetchall(), [])

    def test_fetchmany_fetchall_with_initial_results(self):
        initial_results_1 = [[1], [2], [3]]
        dummy_result_set = self.make_dummy_result_set(initial_results=initial_results_1)
        self.assertEqual(dummy_result_set.fetchmany(2), [[1], [2]])
        self.assertEqual(dummy_result_set.fetchall(), [[3]])

    def test_fetchmany_fetchall_without_initial_results(self):
        batch_list_1 = [[[1], [2], [3]]]  # This is a list of one batch of rows, each row with 1 col
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_1)
        self.assertEqual(dummy_result_set.fetchmany(2), [[1], [2]])
        self.assertEqual(dummy_result_set.fetchall(), [[3]])

        batch_list_2 = [[[1], [2]], [[3], [4]], [[5], [6], [7]]]
        dummy_result_set = self.make_dummy_result_set(batch_list=batch_list_2)
        self.assertEqual(dummy_result_set.fetchmany(3), [[1], [2], [3]])
        self.assertEqual(dummy_result_set.fetchall(), [[4], [5], [6], [7]])


if __name__ == '__main__':
    unittest.main()
