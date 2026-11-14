import unittest
import pytest
from unittest.mock import Mock, patch

try:
    import pyarrow as pa
except ImportError:
    pa = None

import databricks.sql.client as client
from databricks.sql.backend.types import ExecuteResponse
from databricks.sql.utils import ArrowQueue, ThriftCloudFetchQueue
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.result_set import ThriftResultSet
from databricks.sql.thrift_api.TCLIService import ttypes


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


class CloudFetchBatchSizeTests(unittest.TestCase):
    """maxBytes sent by ThriftResultSet._fill_results_buffer."""

    MIB = 2**20
    BUFFER = 100 * MIB

    @staticmethod
    def _link(bytes_num):
        return ttypes.TSparkArrowResultLink(
            fileLink="https://example.com/file",
            expiryTime=0,
            startRowOffset=0,
            rowCount=1,
            bytesNum=bytes_num,
        )

    @classmethod
    def _cloudfetch_queue(cls, *link_sizes):
        queue = Mock(spec=ThriftCloudFetchQueue)
        queue.result_links = [cls._link(b) for b in link_sizes]
        return queue

    def _make_result_set(
        self,
        initial_link_sizes=None,
        fetched_queues=(),
        max_download_threads=10,
        buffer_size_bytes=BUFFER,
    ):
        """Returns the result set and the kwargs of every fetch_results call."""
        calls = []
        queues = list(fetched_queues)

        def fetch_results(**kwargs):
            calls.append(kwargs)
            queue = queues.pop(0)
            return queue, bool(queues), len(getattr(queue, "result_links", []))

        backend = Mock(spec=ThriftDatabricksClient)
        backend.fetch_results = fetch_results
        t_row_set = None
        if initial_link_sizes is not None:
            t_row_set = ttypes.TRowSet(
                startRowOffset=0,
                rows=[],
                resultLinks=[self._link(b) for b in initial_link_sizes],
            )
        with patch(
            "databricks.sql.utils.ThriftResultSetQueueFactory.build_queue",
            return_value=Mock(),
        ):
            rs = ThriftResultSet(
                connection=Mock(),
                execute_response=ExecuteResponse(
                    command_id=Mock(),
                    status=None,
                    has_been_closed_server_side=False,
                    description=[],
                    result_format=ttypes.TSparkRowSetType.URL_BASED_SET,
                ),
                thrift_client=backend,
                buffer_size_bytes=buffer_size_bytes,
                arraysize=1000,
                t_row_set=t_row_set,
                max_download_threads=max_download_threads,
            )
        return rs, calls

    def test_requests_one_link_per_download_thread(self):
        rs, calls = self._make_result_set(
            initial_link_sizes=[20 * self.MIB, 20 * self.MIB, 5 * self.MIB],
            fetched_queues=[
                self._cloudfetch_queue(21 * self.MIB),
                self._cloudfetch_queue(),
            ],
        )

        rs._fill_results_buffer()
        rs._fill_results_buffer()

        self.assertEqual(
            [c["max_bytes"] for c in calls], [200 * self.MIB, 210 * self.MIB]
        )
        self.assertEqual([c["max_rows"] for c in calls], [1000, 1000])

    def test_scales_with_max_download_threads(self):
        rs, calls = self._make_result_set(
            initial_link_sizes=[20 * self.MIB],
            fetched_queues=[self._cloudfetch_queue()],
            max_download_threads=15,
        )

        rs._fill_results_buffer()

        self.assertEqual(calls[0]["max_bytes"], 300 * self.MIB)

    def test_caps_at_four_buffers(self):
        rs, calls = self._make_result_set(
            initial_link_sizes=[20 * self.MIB],
            fetched_queues=[self._cloudfetch_queue()],
            max_download_threads=50,
        )

        rs._fill_results_buffer()

        self.assertEqual(calls[0]["max_bytes"], 4 * self.BUFFER)

    def test_links_without_size_keep_buffer_size(self):
        rs, calls = self._make_result_set(
            initial_link_sizes=[None],
            fetched_queues=[self._cloudfetch_queue(None), self._cloudfetch_queue()],
        )

        rs._fill_results_buffer()
        rs._fill_results_buffer()

        self.assertEqual([c["max_bytes"] for c in calls], [self.BUFFER, self.BUFFER])

    def test_small_buffer_size_caps_batch(self):
        rs, calls = self._make_result_set(
            initial_link_sizes=[20 * self.MIB],
            fetched_queues=[self._cloudfetch_queue()],
            buffer_size_bytes=10 * self.MIB,
        )

        rs._fill_results_buffer()

        self.assertEqual(calls[0]["max_bytes"], 40 * self.MIB)

    def test_never_requests_less_than_buffer_size(self):
        rs, calls = self._make_result_set(
            initial_link_sizes=[20 * self.MIB],
            fetched_queues=[self._cloudfetch_queue()],
            max_download_threads=2,
        )

        rs._fill_results_buffer()

        self.assertEqual(calls[0]["max_bytes"], self.BUFFER)

    def test_inline_results_keep_buffer_size(self):
        # No initial rows: the constructor fetches the first batch itself.
        rs, calls = self._make_result_set(
            fetched_queues=[Mock(spec=ArrowQueue), Mock(spec=ArrowQueue)]
        )

        rs._fill_results_buffer()

        self.assertEqual([c["max_bytes"] for c in calls], [self.BUFFER, self.BUFFER])

    def test_first_fetch_without_links_uses_buffer_size(self):
        rs, calls = self._make_result_set(
            fetched_queues=[
                self._cloudfetch_queue(20 * self.MIB),
                self._cloudfetch_queue(),
            ]
        )

        rs._fill_results_buffer()

        self.assertEqual([c["max_bytes"] for c in calls], [self.BUFFER, 200 * self.MIB])


if __name__ == "__main__":
    unittest.main()
