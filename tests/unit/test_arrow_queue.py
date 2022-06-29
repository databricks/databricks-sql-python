import unittest

import pyarrow as pa

from databricks.sql.utils import ArrowQueue


class ArrowQueueSuite(unittest.TestCase):
    @staticmethod
    def make_arrow_table(batch):
        n_cols = len(batch[0]) if batch else 0
        schema = pa.schema({"col%s" % i: pa.uint32() for i in range(n_cols)})
        cols = [[batch[row][col] for row in range(len(batch))] for col in range(n_cols)]
        return pa.Table.from_pydict(dict(zip(schema.names, cols)), schema=schema)

    def test_fetchmany_respects_n_rows(self):
        arrow_table = self.make_arrow_table([[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]])
        aq = ArrowQueue(arrow_table, 3)
        self.assertEqual(aq.next_n_rows(2), self.make_arrow_table([[0, 1, 2], [3, 4, 5]]))
        self.assertEqual(aq.next_n_rows(2), self.make_arrow_table([[6, 7, 8]]))

    def test_fetch_remaining_rows_respects_n_rows(self):
        arrow_table = self.make_arrow_table([[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]])
        aq = ArrowQueue(arrow_table, 3)
        self.assertEqual(aq.next_n_rows(1), self.make_arrow_table([[0, 1, 2]]))
        self.assertEqual(aq.remaining_rows(), self.make_arrow_table([[3, 4, 5], [6, 7, 8]]))
