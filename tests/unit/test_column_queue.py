import pytest
from databricks.sql.utils import ColumnQueue


class TestColumnQueueSuite:
    @pytest.fixture(scope="function")
    def setup(self):
        columnar_table = [[0, 3, 6, 9], [1, 4, 7, 10], [2, 5, 8, 11]]
        column_names = [f"col_{i}" for i in range(len(columnar_table))]
        return ColumnQueue(columnar_table, column_names)

    def test_fetchmany_respects_n_rows(self, setup):
        column_queue = setup
        assert column_queue.next_n_rows(2) == [[0, 3], [1, 4], [2, 5]]
        assert column_queue.next_n_rows(2) == [[6, 9], [7, 10], [8, 11]]

    def test_fetch_remaining_rows_respects_n_rows(self, setup):
        column_queue = setup
        assert column_queue.next_n_rows(2) == [[0, 3], [1, 4], [2, 5]]
        assert column_queue.remaining_rows() == [[6, 9], [7, 10], [8, 11]]
