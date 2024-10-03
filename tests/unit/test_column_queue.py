from databricks.sql.utils import ColumnQueue, ColumnTable


class TestColumnQueueSuite:
    @staticmethod
    def make_column_table(table):
        n_cols = len(table) if table else 0
        return ColumnTable(table, [f"col_{i}" for i in range(n_cols)])

    def test_fetchmany_respects_n_rows(self):
        column_table = self.make_column_table([[0, 3, 6, 9], [1, 4, 7, 10], [2, 5, 8, 11]])
        column_queue = ColumnQueue(column_table)

        assert column_queue.next_n_rows(2) == column_table.slice(0, 2)
        assert column_queue.next_n_rows(2) == column_table.slice(2, 2)

    def test_fetch_remaining_rows_respects_n_rows(self):
        column_table = self.make_column_table([[0, 3, 6, 9], [1, 4, 7, 10], [2, 5, 8, 11]])
        column_queue = ColumnQueue(column_table)

        assert column_queue.next_n_rows(2) == column_table.slice(0, 2)
        assert column_queue.remaining_rows() == column_table.slice(2, 2)
