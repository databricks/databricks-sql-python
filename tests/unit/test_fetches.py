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
    def make_columnar_result_set(n_rows, arraysize):
        from databricks.sql.utils import ColumnQueue, ColumnTable

        table = ColumnTable([list(range(n_rows))], ["col0"])
        mock_thrift_backend = Mock(spec=ThriftDatabricksClient)
        mock_thrift_backend.fetch_results.return_value = (ColumnQueue(table), False, 0)
        return ThriftResultSet(
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

    def test_fetchone_systematic_convert_failure_stops_per_row_retries(self):
        from unittest.mock import patch
        from databricks.sql.result_set import _FETCHONE_FALLBACK_PROBE_ROWS

        batch_error = ValueError("batch failure")
        calls = []

        def convert(self_, table):
            calls.append(table.num_rows)
            if len(calls) == 1:
                raise batch_error
            raise ValueError("row failure")

        rs = self.make_columnar_result_set(50, arraysize=20)
        with patch.object(
            ThriftResultSet,
            "_convert_columnar_table",
            autospec=True,
            side_effect=convert,
        ), self.assertLogs("databricks.sql.result_set", level="WARNING") as logs:
            for i in range(20):
                with self.assertRaises(ValueError) as ctx:
                    rs.fetchone()
                if i >= _FETCHONE_FALLBACK_PROBE_ROWS:
                    self.assertIs(ctx.exception, batch_error)
                self.assertEqual(rs.rownumber, i + 1)
        # One batch attempt plus only the probe rows were converted individually.
        self.assertEqual(len(calls), 1 + _FETCHONE_FALLBACK_PROBE_ROWS)
        self.assertTrue(any("20 of 20 row(s)" in m for m in logs.output))
        self.assertTrue(any("batch failure" in m for m in logs.output))
        self.assertEqual(rs.fetchone()[0], 20)

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
