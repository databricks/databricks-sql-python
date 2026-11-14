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
from databricks.sql.exc import InternalError


class _StubArrowQueue:
    """Minimal queue that hands back a pre-built pyarrow.Table once.

    Used to inject a schemaless / wrong-schema placeholder that the real
    ArrowQueue would never produce — this is what CloudFetchQueue emits
    when ``self.table is None`` and ``schema_bytes`` is missing.
    """

    def __init__(self, table):
        self._table = table
        self._consumed = False

    def _take(self):
        if self._consumed:
            return self._table.slice(0, 0)
        self._consumed = True
        return self._table

    def next_n_rows(self, num_rows):
        return self._take()

    def remaining_rows(self):
        return self._take()

    def close(self):
        pass


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
    def make_dummy_result_set_from_batch_list(batch_list, offsets=None):
        """``offsets``, if given, records ``expected_row_start_offset`` per refill."""
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
            if offsets is not None:
                offsets.append(expected_row_start_offset)
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

    @staticmethod
    def make_dummy_result_set_from_queue_list(queue_list, description=None):
        """Like make_dummy_result_set_from_batch_list but yields pre-built queues.

        Lets tests inject queues whose returned tables have an arbitrary schema
        (or no schema at all) — needed to reproduce the CloudFetch placeholder
        case that ``ArrowQueue`` would never produce.
        """
        queue_index = 0

        def fetch_results(**_):
            nonlocal queue_index
            q = queue_list[queue_index]
            queue_index += 1
            return q, queue_index < len(queue_list), 0

        mock_thrift_backend = Mock(spec=ThriftDatabricksClient)
        mock_thrift_backend.fetch_results = fetch_results

        rs = ThriftResultSet(
            connection=Mock(),
            execute_response=ExecuteResponse(
                command_id=None,
                status=None,
                has_been_closed_server_side=False,
                description=description or [],
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

    def test_mixed_fetches_across_pages_keep_order_and_refill_offsets(self):
        # Issue #981: rows parked in the fetchone() buffer must not skew
        # ``_next_row_index``, which Thrift sends as ``expected_row_start_offset``
        # on every refill. Pages are 3 rows; fetchone() refills 2 rows at a time,
        # so buffer refills and server page boundaries interleave.
        batch_list = [[[0], [1], [2]], [[3], [4], [5]], [[6], [7], [8]], [[9]]]
        offsets = []
        rs = self.make_dummy_result_set_from_batch_list(batch_list, offsets=offsets)
        rs.arraysize = 2
        self.assertEqual(offsets, [0])

        self.assertSequenceEqual(rs.fetchone(), [0])
        self.assertEqual(rs.rownumber, 1)

        # Drains the buffered row, the rest of page 0, then refills page 1.
        table = rs.fetchmany_arrow(3)
        self.assertEqual(table.column(0).to_pylist(), [1, 2, 3])
        self.assertEqual(rs.rownumber, 4)
        self.assertEqual(offsets, [0, 3])

        self.assertSequenceEqual(rs.fetchone(), [4])
        self.assertSequenceEqual(rs.fetchone(), [5])
        self.assertEqual(rs.rownumber, 6)
        self.assertEqual(offsets, [0, 3])

        # Buffer empty and page 1 exhausted: the refill crosses into page 2.
        self.assertSequenceEqual(rs.fetchone(), [6])
        self.assertEqual(rs.rownumber, 7)
        self.assertEqual(offsets, [0, 3, 6])

        # Drains the buffered row [7], the rest of page 2, then page 3.
        self.assertEqualRowValues(rs.fetchall(), [[7], [8], [9]])
        self.assertEqual(rs.rownumber, 10)
        self.assertEqual(offsets, [0, 3, 6, 9])

        self.assertIsNone(rs.fetchone())
        self.assertEqual(rs.rownumber, 10)
        self.assertEqual(offsets, [0, 3, 6, 9])

    def test_close_drops_fetchone_row_buffer(self):
        # Rows parked in the fetchone() buffer must not survive close(). The warm
        # refill pulls every row into the buffer, so the queue itself is empty and
        # any row returned after close() could only come from a stale buffer.
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS

        for fetch_after_close in (
            lambda rs: rs.fetchall(),
            lambda rs: rs.fetchmany_arrow(10).to_pylist(),
        ):
            rs = self.make_dummy_result_set_from_initial_results([[1], [2], [3], [4]])
            rs._fetchone_refill_rows = _FETCHONE_BUFFER_MAX_ROWS
            self.assertSequenceEqual(rs.fetchone(), [1])
            self.assertEqual(rs._row_buffer_count(), 3)
            rs.close()
            self.assertEqual(rs._row_buffer_count(), 0)
            self.assertEqual(fetch_after_close(rs), [])
            self.assertIsNone(rs.fetchone())

    def test_close_clears_lost_rows_error(self):
        # A result set poisoned by a lost fetchone() refill must behave like any
        # other closed result set: post-close fetches return nothing, not raise.
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS

        rs = self.make_dummy_result_set_from_initial_results([[1], [2], [3]])
        # Warm refill pulls every row into the buffer, so the queue is empty.
        rs._fetchone_refill_rows = _FETCHONE_BUFFER_MAX_ROWS
        self.assertSequenceEqual(rs.fetchone(), [1])
        rs._lost_rows_error = "lost"
        with self.assertRaises(InternalError):
            rs.fetchone()
        rs.close()
        self.assertIsNone(rs._lost_rows_error)
        self.assertIsNone(rs.fetchone())
        self.assertEqual(rs.fetchall(), [])

    def test_fetchone_refill_does_not_fetch_extra_pages(self):
        # A refill must not make extra round-trips to fill ``arraysize`` when
        # server pages are smaller (e.g. byte-limited): it serves what the current
        # queue holds and only fetches a new page once the queue is empty.
        batch_list = [[[0], [1]], [[2], [3]], [[4]]]
        offsets = []
        rs = self.make_dummy_result_set_from_batch_list(batch_list, offsets=offsets)
        rs.arraysize = 100
        self.assertEqual(offsets, [0])

        self.assertSequenceEqual(rs.fetchone(), [0])
        self.assertEqual(offsets, [0])
        self.assertSequenceEqual(rs.fetchone(), [1])
        self.assertEqual(offsets, [0])
        self.assertSequenceEqual(rs.fetchone(), [2])
        self.assertEqual(offsets, [0, 2])
        self.assertEqual(rs.rownumber, 3)
        self.assertEqualRowValues(rs.fetchall(), [[3], [4]])
        self.assertEqual(offsets, [0, 2, 4])
        self.assertIsNone(rs.fetchone())
        self.assertEqual(rs.rownumber, 5)

    def test_cloudfetch_fetchone_refill_capped_to_current_chunk(self):
        # The fetchone() refill is capped at the rows left in the current CloudFetch
        # chunk, so one refill never spans several files. (This only bounds the
        # batch size; the mock queue does not model advancing to the next file.)
        from databricks.sql.utils import ThriftCloudFetchQueue

        _, chunk = self.make_arrow_table([[i] for i in range(10)])
        queue = Mock(spec=ThriftCloudFetchQueue)
        queue.table = chunk
        queue.table_row_index = 8
        queue.next_n_rows.side_effect = lambda n: chunk.slice(8, n)
        rs = self.make_dummy_result_set_from_queue_list(
            [queue],
            description=[("col0", "integer", None, None, None, None, None)],
        )
        rs.arraysize = 100
        # Skip the adaptive refill ramp so the chunk cap, not the ramp, bounds it.
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS

        rs._fetchone_refill_rows = _FETCHONE_BUFFER_MAX_ROWS

        self.assertSequenceEqual(rs.fetchone(), [8])
        queue.next_n_rows.assert_called_once_with(2)
        self.assertSequenceEqual(rs.fetchone(), [9])
        self.assertEqual(rs.rownumber, 2)

    # Regression tests for fetchmany_arrow / fetchall_arrow handling of
    # the schemaless CloudFetch placeholder.
    def test_fetchall_arrow_drops_mismatched_empty_placeholder(self):
        # First fetch_results() call hands back a 0-row placeholder whose
        # schema does not match the real chunks. The second call
        # hands back real data.
        placeholder = pa.Table.from_pydict(
            {"stale_col": []}, schema=pa.schema({"stale_col": pa.string()})
        )
        _, real_table = self.make_arrow_table([[1], [2], [3]])
        rs = self.make_dummy_result_set_from_queue_list(
            [_StubArrowQueue(placeholder), _StubArrowQueue(real_table)],
            description=[("col0", "integer", None, None, None, None, None)],
        )

        result = rs.fetchall_arrow()

        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.schema.names, ["col0"])
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])

    def test_fetchall_arrow_all_empty_returns_zero_row_table(self):
        # Every queue call returns the schemaless placeholder — the
        # call site should fall back to zero_row_table without crashing.
        placeholder = pa.Table.from_pydict({})
        rs = self.make_dummy_result_set_from_queue_list(
            [_StubArrowQueue(placeholder)],
        )

        result = rs.fetchall_arrow()

        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 0)

    def test_fetchmany_arrow_drops_mismatched_empty_placeholder(self):
        # See ``test_fetchall_arrow_drops_mismatched_empty_placeholder``.
        placeholder = pa.Table.from_pydict(
            {"stale_col": []}, schema=pa.schema({"stale_col": pa.string()})
        )
        _, real_table = self.make_arrow_table([[1], [2], [3]])
        rs = self.make_dummy_result_set_from_queue_list(
            [_StubArrowQueue(placeholder), _StubArrowQueue(real_table)],
            description=[("col0", "integer", None, None, None, None, None)],
        )

        result = rs.fetchmany_arrow(3)

        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.schema.names, ["col0"])
        self.assertEqual(result.column(0).to_pylist(), [1, 2, 3])

    def test_fetchmany_arrow_all_empty_returns_zero_row_table(self):
        placeholder = pa.Table.from_pydict({})
        rs = self.make_dummy_result_set_from_queue_list(
            [_StubArrowQueue(placeholder)],
        )

        result = rs.fetchmany_arrow(10)

        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 0)


class ColumnarFetchoneBufferTests(unittest.TestCase):
    """Issue #981: fetchone/iteration on the Thrift ColumnQueue path must convert
    rows in batches (not one row at a time) while keeping order and rownumber."""

    @staticmethod
    def make_columnar_result_set(n_rows, arraysize, warm=True):
        """Build a result set over ``n_rows`` columnar rows.

        With ``warm`` (the default) the adaptive fetchone() refill ramp is skipped,
        so every refill is ``min(arraysize, _FETCHONE_BUFFER_MAX_ROWS)`` rows and the
        batch-boundary assertions below stay simple. Pass ``warm=False`` to test
        the ramp itself.
        """
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS
        from databricks.sql.utils import ColumnQueue, ColumnTable

        table = ColumnTable([list(range(n_rows))], ["col0"])
        mock_thrift_backend = Mock(spec=ThriftDatabricksClient)
        mock_thrift_backend.fetch_results.return_value = (ColumnQueue(table), False, 0)
        rs = ThriftResultSet(
            connection=Mock(),
            execute_response=ExecuteResponse(
                command_id=None,
                status=None,
                has_been_closed_server_side=True,
                description=[("col0", "integer", None, None, None, None, None)],
                lz4_compressed=False,
                is_staging_operation=False,
            ),
            thrift_client=mock_thrift_backend,
            arraysize=arraysize,
        )
        if warm:
            rs._fetchone_refill_rows = _FETCHONE_BUFFER_MAX_ROWS
        return rs

    @staticmethod
    def _count_conversions():
        from unittest.mock import patch

        return patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=ThriftResultSet._convert_columnar_table,
        )

    def test_iteration_converts_in_batches(self):
        rs = self.make_columnar_result_set(100, arraysize=10)
        with self._count_conversions() as m:
            rows = [r[0] for r in rs]
        self.assertEqual(rows, list(range(100)))
        self.assertLessEqual(m.call_count, 100 // 10 + 2)

    def test_fetchone_loop_converts_in_batches(self):
        rs = self.make_columnar_result_set(100, arraysize=10)
        rows = []
        with self._count_conversions() as m:
            while True:
                row = rs.fetchone()
                if row is None:
                    break
                rows.append(row[0])
        self.assertEqual(rows, list(range(100)))
        self.assertLessEqual(m.call_count, 100 // 10 + 2)

    def test_fetchone_refill_is_capped_for_large_arraysize(self):
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS

        n_rows = _FETCHONE_BUFFER_MAX_ROWS * 3
        rs = self.make_columnar_result_set(n_rows, arraysize=100000)
        self.assertEqual(rs.fetchone()[0], 0)
        self.assertEqual(rs._row_buffer_count(), _FETCHONE_BUFFER_MAX_ROWS - 1)
        self.assertEqual(rs.rownumber, 1)
        self.assertEqual([r[0] for r in rs.fetchall()], list(range(1, n_rows)))
        self.assertEqual(rs.rownumber, n_rows)

    def test_first_fetchone_converts_a_single_row(self):
        # A lone fetchone() (SQLAlchemy .first()/.scalar(), EXISTS probes) must not
        # pull and convert a whole batch: the first refill is a single row.
        rs = self.make_columnar_result_set(5000, arraysize=100000, warm=False)
        with self._count_conversions() as m:
            self.assertEqual(rs.fetchone()[0], 0)
        self.assertEqual(m.call_count, 1)
        self.assertEqual(m.call_args[0][1].num_rows, 1)
        self.assertEqual(rs._row_buffer_count(), 0)
        self.assertEqual(rs.rownumber, 1)

    def test_fetchone_refill_size_ramps_up_to_cap(self):
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS

        n_rows = 1 + 10 + 100 + _FETCHONE_BUFFER_MAX_ROWS * 2
        rs = self.make_columnar_result_set(n_rows, arraysize=100000, warm=False)
        with self._count_conversions() as m:
            rows = [r[0] for r in rs]
        self.assertEqual(rows, list(range(n_rows)))
        sizes = [c[0][1].num_rows for c in m.call_args_list]
        self.assertEqual(
            sizes[:5],
            [1, 10, 100, _FETCHONE_BUFFER_MAX_ROWS, _FETCHONE_BUFFER_MAX_ROWS],
        )
        self.assertEqual(rs.rownumber, n_rows)

    def test_fetchone_refill_ramp_is_capped_by_arraysize(self):
        rs = self.make_columnar_result_set(10, arraysize=4, warm=False)
        with self._count_conversions() as m:
            rows = [r[0] for r in rs]
        self.assertEqual(rows, list(range(10)))
        sizes = [c[0][1].num_rows for c in m.call_args_list]
        self.assertEqual(sizes[:4], [1, 4, 4, 1])

    def test_mixed_fetches_keep_order_and_rownumber(self):
        rs = self.make_columnar_result_set(10, arraysize=4)
        self.assertEqual(rs.fetchone()[0], 0)
        self.assertEqual(rs.fetchone()[0], 1)
        self.assertEqual(rs.rownumber, 2)
        self.assertEqual([r[0] for r in rs.fetchmany(3)], [2, 3, 4])
        self.assertEqual(rs.rownumber, 5)
        self.assertEqual(rs.fetchone()[0], 5)
        self.assertEqual(rs.rownumber, 6)
        self.assertEqual([r[0] for r in rs.fetchall()], [6, 7, 8, 9])
        self.assertEqual(rs.rownumber, 10)
        self.assertIsNone(rs.fetchone())

    def test_fetchone_convert_failure_loses_only_the_bad_row(self):
        from unittest.mock import patch

        real_convert = ThriftResultSet._convert_columnar_table

        def convert_failing_on_2(self_, table):
            rows = real_convert(self_, table)
            if any(r[0] == 2 for r in rows):
                raise ValueError("bad value")
            return rows

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert_failing_on_2,
        ), self.assertLogs("databricks.sql.result_set", level="WARNING") as logs:
            self.assertEqual(rs.fetchone()[0], 0)
            self.assertEqual(rs.fetchone()[0], 1)
            with self.assertRaises(ValueError):
                rs.fetchone()
            # Only the failing row is lost; the rest of the batch is still served.
            self.assertEqual(rs.rownumber, 3)
            self.assertEqual(rs.fetchone()[0], 3)
            self.assertEqual([r[0] for r in rs.fetchall()], list(range(4, 10)))
        self.assertTrue(any("1 of 4 row(s)" in m for m in logs.output))
        self.assertEqual(rs.rownumber, 10)

    def _patch_convert_failing_on_2(self):
        from unittest.mock import patch

        real_convert = ThriftResultSet._convert_columnar_table

        def convert_failing_on_2(self_, table):
            rows = real_convert(self_, table)
            if any(r[0] == 2 for r in rows):
                raise ValueError("bad value")
            return rows

        return patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert_failing_on_2,
        )

    def test_fetchmany_over_failed_buffered_row_keeps_rows_after_it(self):
        rs = self.make_columnar_result_set(10, arraysize=4)
        with self._patch_convert_failing_on_2(), self.assertLogs(
            "databricks.sql.result_set", level="WARNING"
        ):
            # Buffer now holds rows 1, 2 (failed), 3.
            self.assertEqual(rs.fetchone()[0], 0)
            with self.assertRaises(ValueError):
                rs.fetchmany(3)
            # Consumed up to and including the failed row; row 3 stays buffered.
            self.assertEqual(rs.rownumber, 3)
            self.assertEqual(rs._row_buffer_count(), 1)
            self.assertEqual([r[0] for r in rs.fetchmany(3)], [3, 4, 5])
            self.assertEqual([r[0] for r in rs.fetchall()], [6, 7, 8, 9])
        self.assertEqual(rs.rownumber, 10)

    def test_fetchall_over_failed_buffered_row_keeps_rows_after_it(self):
        rs = self.make_columnar_result_set(10, arraysize=4)
        with self._patch_convert_failing_on_2(), self.assertLogs(
            "databricks.sql.result_set", level="WARNING"
        ):
            self.assertEqual(rs.fetchone()[0], 0)
            with self.assertRaises(ValueError):
                rs.fetchall()
            self.assertEqual(rs.rownumber, 3)
            self.assertEqual([r[0] for r in rs.fetchall()], list(range(3, 10)))
        self.assertEqual(rs.rownumber, 10)

    def test_fetchone_convert_failure_on_every_row_reraises_per_row(self):
        from unittest.mock import patch

        rs = self.make_columnar_result_set(6, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            side_effect=ValueError("bad value"),
        ):
            for expected_rownumber in range(1, 5):
                with self.assertRaises(ValueError):
                    rs.fetchone()
                self.assertEqual(rs.rownumber, expected_rownumber)
        self.assertEqual(rs._row_buffer_count(), 0)
        self.assertEqual(rs.fetchone()[0], 4)

    def test_fetchone_convert_failure_warns_once_per_result_set(self):
        import logging
        from unittest.mock import patch

        rs = self.make_columnar_result_set(12, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            side_effect=ValueError("bad value"),
        ), self.assertLogs("databricks.sql.result_set", level="DEBUG") as logs:
            # One 4-row refill, then (ramp reset after an all-failing batch) eight
            # 1-row refills, every row failing the same way.
            for _ in range(12):
                with self.assertRaises(ValueError):
                    rs.fetchone()
        summaries = [
            r for r in logs.records if "failed to convert" in r.getMessage()
        ]
        self.assertEqual(len(summaries), 9)
        self.assertEqual(
            [r.levelno for r in summaries],
            [logging.WARNING] + [logging.DEBUG] * 8,
        )
        # The error message may quote result data, so it must stay out of WARNING.
        for r in logs.records:
            if r.levelno >= logging.WARNING:
                self.assertNotIn("bad value", r.getMessage())
                self.assertIn("ValueError", r.getMessage())

    def test_fetchone_all_failing_batch_resets_refill_ramp(self):
        from unittest.mock import patch

        calls = []

        def convert(self_, table):
            calls.append(table.num_rows)
            raise ValueError("systemic failure")

        rs = self.make_columnar_result_set(30, arraysize=10)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ), self.assertLogs("databricks.sql.result_set", level="DEBUG"):
            for _ in range(15):
                with self.assertRaises(ValueError):
                    rs.fetchone()
        # First full refill is bisected (2 * 10 - 1 conversions); after that every
        # refill is a single row costing exactly one conversion.
        self.assertEqual(len(calls), 2 * 10 - 1 + 5)
        self.assertEqual(calls[-5:], [1] * 5)
        self.assertEqual(rs.rownumber, 15)

    def test_fetchone_memory_error_is_not_bisected(self):
        from unittest.mock import patch

        calls = []

        def convert(self_, table):
            calls.append(table.num_rows)
            raise MemoryError()

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ):
            with self.assertRaises(MemoryError):
                rs.fetchone()
        self.assertEqual(calls, [4])
        self.assertEqual(rs._row_buffer_count(), 0)
        # The lost refill chunk must not be skipped silently on later fetches.
        with self.assertRaises(InternalError):
            rs.fetchone()

    def test_fetchone_memory_error_during_bisection_stops_bisecting(self):
        from unittest.mock import patch

        calls = []

        def convert(self_, table):
            calls.append(table.num_rows)
            if len(calls) == 1:
                raise ValueError("bad value")
            raise MemoryError()

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ):
            with self.assertRaises(MemoryError):
                rs.fetchone()
        # The full batch fails with a data error, the first half hits MemoryError:
        # bisection stops there instead of converting more slices.
        self.assertEqual(calls, [4, 2])
        self.assertEqual(rs._row_buffer_count(), 0)
        # The lost refill chunk must not be skipped silently on later fetches.
        with self.assertRaises(InternalError):
            rs.fetchone()

    def test_fetchone_interrupted_conversion_poisons_later_fetches(self):
        from unittest.mock import patch

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            side_effect=KeyboardInterrupt(),
        ):
            with self.assertRaises(KeyboardInterrupt):
                rs.fetchone()
        # Conversion works again, but the interrupted chunk is gone: every fetch
        # must raise instead of resuming from the next chunk with a gap.
        for fetch in (
            rs.fetchone,
            lambda: rs.fetchmany(2),
            rs.fetchall,
            lambda: rs.fetchmany_arrow(2),
            rs.fetchall_arrow,
        ):
            with self.assertRaises(InternalError) as ctx:
                fetch()
            self.assertIn("4 row(s) were lost", str(ctx.exception))

    def test_fetchone_successful_refill_does_not_poison(self):
        rs = self.make_columnar_result_set(10, arraysize=4)
        rows = [rs.fetchone() for _ in range(3)]
        self.assertEqual([r[0] for r in rows], [0, 1, 2])
        self.assertIsNone(rs._lost_rows_error)
        self.assertEqual([r[0] for r in rs.fetchall()], list(range(3, 10)))

    def test_fetchone_convert_failure_on_every_row_bisects_to_single_rows(self):
        from unittest.mock import patch

        shared_error = ValueError("batch failure")
        calls = []

        def convert(self_, table):
            calls.append(table.num_rows)
            # The converter re-raises one shared exception instance for every row.
            raise shared_error

        rs = self.make_columnar_result_set(50, arraysize=20)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ), self.assertLogs("databricks.sql.result_set", level="WARNING") as logs:
            tb_depths = []
            for i in range(20):
                with self.assertRaises(ValueError) as ctx:
                    rs.fetchone()
                self.assertIs(ctx.exception, shared_error)
                tb, depth = ctx.exception.__traceback__, 0
                while tb is not None:
                    tb, depth = tb.tb_next, depth + 1
                tb_depths.append(depth)
                self.assertEqual(rs.rownumber, i + 1)
            # Re-raising a shared error must not grow its traceback per row.
            self.assertEqual(len(set(tb_depths)), 1)
        # Bisection of an all-failing batch of 20 tries every node of the tree once:
        # 2 * 20 - 1 conversions (including the initial batch attempt).
        self.assertEqual(calls[0], 20)
        self.assertEqual(len(calls), 2 * 20 - 1)
        self.assertEqual(sum(1 for c in calls if c == 1), 20)
        self.assertTrue(any("20 of 20 row(s)" in m for m in logs.output))
        # Only the exception type is logged at WARNING; the message (which may quote
        # result data) is kept out of it.
        self.assertTrue(any("(ValueError)" in m for m in logs.output))
        self.assertFalse(any("batch failure" in m for m in logs.output))
        self.assertEqual(rs.fetchone()[0], 20)

    def test_fetchone_all_failing_max_batch_conversion_count_is_bounded(self):
        """Document the worst-case cost of bisecting a full all-failing refill.

        When every row fails (a systemic converter / type problem), bisection tries
        every node of the tree: exactly ``2n - 1`` conversions for an ``n``-row
        refill, i.e. about 2x the one-conversion-per-row cost of fetchone() before
        batching. Short-circuiting on "same exception type in both halves" is
        deliberately not done, since it would drop good rows that share the error
        type (see test_fetchone_same_message_bad_rows_do_not_drop_later_good_rows).
        """
        from unittest.mock import patch
        from databricks.sql.result_set import _FETCHONE_BUFFER_MAX_ROWS

        n = _FETCHONE_BUFFER_MAX_ROWS
        calls = []

        def convert(self_, table):
            calls.append(table.num_rows)
            raise ValueError("systemic failure")

        rs = self.make_columnar_result_set(n, arraysize=n)
        # Skip the adaptive refill ramp so the first refill is a full batch.
        rs._fetchone_refill_rows = n
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ), self.assertLogs("databricks.sql.result_set", level="WARNING"):
            with self.assertRaises(ValueError):
                rs.fetchone()
        self.assertEqual(calls[0], n)
        self.assertEqual(len(calls), 2 * n - 1)
        # Total rows converted across all attempts is O(n log n), not O(n^2).
        self.assertLessEqual(sum(calls), n * (n - 1).bit_length() + n)
        self.assertEqual(rs._row_buffer_count(), n - 1)

    def test_fetchone_failed_fetch_does_not_advance_refill_ramp(self):
        from unittest.mock import patch

        rs = self.make_columnar_result_set(100, arraysize=100, warm=False)
        initial = rs._fetchone_refill_rows
        real_next_n_rows = rs.results.next_n_rows
        with patch.object(
            rs.results,
            "next_n_rows",
            side_effect=[RuntimeError("transient"), real_next_n_rows],
        ):
            with self.assertRaises(RuntimeError):
                rs.fetchone()
        # The failed fetch must not grow the ramp, so a retry refills the same size.
        self.assertEqual(rs._fetchone_refill_rows, initial)
        sizes = []
        with patch.object(
            rs.results,
            "next_n_rows",
            side_effect=lambda n: sizes.append(n) or real_next_n_rows(n),
        ):
            self.assertEqual(rs.fetchone()[0], 0)
        self.assertEqual(sizes, [initial])

    def test_fetchone_same_message_bad_rows_do_not_drop_later_good_rows(self):
        from unittest.mock import patch

        real_convert = ThriftResultSet._convert_columnar_table
        # Enough leading bad rows, all failing with the same message as the batch
        # (e.g. "year 0 is out of range"), to look like a systematic failure.
        n_bad = 8

        def convert(self_, table):
            rows = real_convert(self_, table)
            if any(r[0] < n_bad for r in rows):
                raise ValueError("year 0 is out of range")
            return rows

        rs = self.make_columnar_result_set(20, arraysize=20)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ), self.assertLogs("databricks.sql.result_set", level="WARNING") as logs:
            for _ in range(n_bad):
                with self.assertRaises(ValueError):
                    rs.fetchone()
            # Every row after the bad run still converts and is served.
            self.assertEqual([r[0] for r in rs.fetchall()], list(range(n_bad, 20)))
        self.assertTrue(any("{} of 20 row(s)".format(n_bad) in m for m in logs.output))
        self.assertEqual(rs.rownumber, 20)

    def test_fetchone_leading_bad_rows_do_not_drop_later_good_rows(self):
        from unittest.mock import patch

        real_convert = ThriftResultSet._convert_columnar_table
        n_bad = 6

        def convert(self_, table):
            rows = real_convert(self_, table)
            bad = [r[0] for r in rows if r[0] < n_bad]
            if bad:
                # Each bad row fails with its own message, unlike the batch error.
                raise ValueError("bad value in row {}".format(bad[0]))
            return rows

        rs = self.make_columnar_result_set(20, arraysize=20)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ), self.assertLogs("databricks.sql.result_set", level="WARNING") as logs:
            for i in range(n_bad):
                with self.assertRaises(ValueError) as ctx:
                    rs.fetchone()
                self.assertEqual(str(ctx.exception), "bad value in row {}".format(i))
            # Rows after the leading bad run still convert and are served.
            self.assertEqual([r[0] for r in rs.fetchall()], list(range(n_bad, 20)))
        self.assertTrue(any("{} of 20 row(s)".format(n_bad) in m for m in logs.output))
        self.assertFalse(any("rest not retried" in m for m in logs.output))
        self.assertEqual(rs.rownumber, 20)

    def test_fetchone_raw_converted_count_mismatch_fails_loudly(self):
        from unittest.mock import patch
        from databricks.sql.exc import InternalError

        real_convert = ThriftResultSet._convert_columnar_table

        def convert_dropping_a_row(self_, table):
            return real_convert(self_, table)[:-1]

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert_dropping_a_row,
        ):
            with self.assertRaises(InternalError):
                rs.fetchone()

    def test_fetchone_empty_conversion_of_nonempty_chunk_fails_loudly(self):
        from unittest.mock import patch
        from databricks.sql.exc import InternalError

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            return_value=[],
        ):
            with self.assertRaises(InternalError):
                rs.fetchone()

    def test_fetchone_unknown_raw_chunk_type_fails_loudly(self):
        from unittest.mock import patch
        from databricks.sql.exc import InternalError

        class _NoRowCount:
            pass

        rs = self.make_columnar_result_set(10, arraysize=4)
        with patch.object(
            ThriftResultSet,
            "_fetch_available_from_queue",
            autospec=True,
            return_value=_NoRowCount(),
        ), patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            return_value=[("x",)],
        ):
            with self.assertRaises(InternalError) as ctx:
                rs.fetchone()
        self.assertIn("_NoRowCount", str(ctx.exception))

    @pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
    def test_fetchone_then_fetchall_arrow_keeps_order(self):
        rs = self.make_columnar_result_set(10, arraysize=4)
        self.assertEqual(rs.fetchone()[0], 0)
        self.assertEqual(rs.rownumber, 1)
        table = rs.fetchall_arrow()
        self.assertEqual(table.column(0).to_pylist(), list(range(1, 10)))
        self.assertEqual(rs.rownumber, 10)


if __name__ == "__main__":
    unittest.main()
