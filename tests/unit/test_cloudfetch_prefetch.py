"""CloudFetch link prefetching across batches and cancel-on-close."""

import threading
import unittest
from concurrent.futures import Future
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, patch

import pytest
from urllib3.exceptions import ProtocolError

try:
    import pyarrow as pa
except ImportError:
    pa = None

import databricks.sql.cloudfetch.download_manager as download_manager
import databricks.sql.cloudfetch.downloader as downloader
from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.backend.types import ExecuteResponse
from databricks.sql.client import Cursor
from databricks.sql.cloudfetch.downloader import DownloadedFile
from databricks.sql.exc import DataError, OperationalError, RequestError
from databricks.sql.result_set import ThriftResultSet
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.utils import ArrowQueue, ThriftCloudFetchQueue

MIB = 2**20


def _link(start, rows=10, size=20 * MIB):
    return ttypes.TSparkArrowResultLink(
        fileLink=f"https://example.com/{start}",
        expiryTime=2**62,
        startRowOffset=start,
        rowCount=rows,
        bytesNum=size,
    )


class _InstantHandler:
    """Stands in for ResultSetDownloadHandler: "downloads" a link immediately."""

    def __init__(self, link, **_):
        self.link = link

    def run(self):
        return DownloadedFile(b"", self.link.startRowOffset, self.link.rowCount)


class _ArrowFileHandler(_InstantHandler):
    """Returns an Arrow IPC stream with ids startRowOffset..startRowOffset+rowCount-1."""

    def run(self):
        start, rows = self.link.startRowOffset, self.link.rowCount
        table = pa.table({"id": pa.array(range(start, start + rows), pa.int64())})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return DownloadedFile(sink.getvalue().to_pybytes(), start, rows)


class DownloadManagerPrefetchTests(unittest.TestCase):
    def _manager(self, links, max_download_threads):
        return download_manager.ResultFileDownloadManager(
            links,
            max_download_threads,
            lz4_compressed=False,
            ssl_options=SSLOptions(),
            session_id_hex=None,
            statement_id="s",
            chunk_id=0,
            http_client=MagicMock(),
        )

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_refills_from_link_source_whenever_a_slot_frees(self):
        manager = self._manager([_link(0), _link(10)], max_download_threads=4)
        replies = [
            ([_link(20), _link(30), _link(40)], True),
            ([_link(50)], True),
            ([], False),
        ]
        free_slots = []

        def source(free):
            free_slots.append(free)
            self.assertLessEqual(len(manager._download_tasks), 4)
            return replies.pop(0)

        manager.set_link_source(source)
        offsets = []
        while True:
            file = manager.get_next_downloaded_file(len(offsets) * 10)
            if file is None:
                break
            self.assertLessEqual(len(manager._download_tasks), 4)
            offsets.append(file.start_row_offset)

        self.assertEqual(offsets, [0, 10, 20, 30, 40, 50])
        # Asked only when a slot was free and no link was pending
        self.assertEqual(free_slots, [2, 1, 1])

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_empty_reply_is_retried_on_the_next_call_not_in_a_loop(self):
        manager = self._manager([_link(0)], max_download_threads=2)
        source = Mock(side_effect=[([], True), ([_link(10)], False)])
        manager.set_link_source(source)

        self.assertEqual(manager.get_next_downloaded_file(0).start_row_offset, 0)
        self.assertEqual(source.call_count, 1)
        self.assertEqual(manager.get_next_downloaded_file(10).start_row_offset, 10)
        self.assertIsNone(manager.get_next_downloaded_file(20))
        self.assertEqual(source.call_count, 2)

    def test_shutdown_cancels_downloads_and_stops_fetching_links(self):
        manager = self._manager([_link(0), _link(10)], max_download_threads=1)
        manager.set_link_source(Mock())
        with patch.object(
            manager._thread_pool, "shutdown", wraps=manager._thread_pool.shutdown
        ) as shutdown:
            manager._shutdown_manager()

        self.assertTrue(manager._cancelled.is_set())
        self.assertIsNone(manager._link_source)
        self.assertEqual(manager._pending_links, [])
        shutdown.assert_called_once_with(wait=False, cancel_futures=True)

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_reply_with_only_empty_links_is_retried_on_the_next_call(self):
        manager = self._manager([_link(0)], max_download_threads=2)
        source = Mock(side_effect=[([_link(10, rows=0)], True), ([_link(10)], False)])
        manager.set_link_source(source)

        self.assertEqual(manager.get_next_downloaded_file(0).start_row_offset, 0)
        self.assertEqual(source.call_count, 1)
        self.assertEqual(manager.get_next_downloaded_file(10).start_row_offset, 10)
        self.assertIsNone(manager.get_next_downloaded_file(20))

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_a_failed_link_fetch_fails_every_later_read(self):
        manager = self._manager([_link(0)], max_download_threads=2)
        error = RuntimeError("FetchResults failed")
        source = Mock(side_effect=[error, ([_link(10)], False)])
        manager.set_link_source(source)

        with self.assertRaises(RuntimeError) as raised:
            manager.get_next_downloaded_file(0)
        self.assertIs(raised.exception, error)
        # Downloads nobody can read any more are stopped
        self.assertTrue(manager._cancelled.is_set())
        # Later reads raise a fresh error that points at the original one
        for _ in range(2):
            with self.assertRaises(OperationalError) as raised:
                manager.get_next_downloaded_file(0)
            self.assertIs(raised.exception.__cause__, error)
        self.assertEqual(source.call_count, 1)

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_shutdown_during_a_link_fetch_ends_the_stream_cleanly(self):
        manager = self._manager([_link(0)], max_download_threads=2)

        def source(free):
            manager._shutdown_manager()
            return [_link(10)], True

        manager.set_link_source(source)

        self.assertIsNone(manager.get_next_downloaded_file(0))
        self.assertEqual(manager._pending_links, [])

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_cancel_after_the_whole_result_arrived_still_returns_it(self):
        manager = self._manager([_link(0), _link(10)], max_download_threads=2)
        self.assertEqual(manager.get_next_downloaded_file(0).start_row_offset, 0)
        manager._download_tasks[0].result()  # the last file finished downloading

        manager.cancel()

        self.assertTrue(manager._cancelled.is_set())
        self.assertEqual(manager.get_next_downloaded_file(10).start_row_offset, 10)
        self.assertIsNone(manager.get_next_downloaded_file(20))

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_cancel_with_links_not_yet_downloaded_fails_the_read(self):
        manager = self._manager(
            [_link(0), _link(10), _link(20)], max_download_threads=1
        )
        self.assertEqual(manager.get_next_downloaded_file(0).start_row_offset, 0)

        manager.cancel()

        with self.assertRaises(OperationalError):
            manager.get_next_downloaded_file(10)

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_cancel_during_a_link_fetch_fails_once_the_downloaded_files_run_out(
        self,
    ):
        manager = self._manager([_link(0)], max_download_threads=2)

        def source(free):
            manager._download_tasks[0].result()  # the first file is downloaded
            manager.cancel()  # Cursor.cancel() running on another thread
            return [_link(10)], True

        manager.set_link_source(source)

        self.assertEqual(manager.get_next_downloaded_file(0).start_row_offset, 0)
        with self.assertRaises(OperationalError):
            manager.get_next_downloaded_file(10)

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_link_whose_submit_raced_with_cancel_still_counts_as_missing(self):
        manager = self._manager([_link(0)], max_download_threads=2)
        manager.cancel()

        manager._submit_pending_links()  # submit fails: the pool is shut down

        self.assertEqual(manager._download_tasks, [])
        self.assertEqual(len(manager._pending_links), 1)
        with self.assertRaises(OperationalError):
            manager.get_next_downloaded_file(0)

    def test_download_cancelled_before_it_started_fails_the_read(self):
        manager = self._manager([], max_download_threads=1)
        future = Future()
        future.cancel()
        manager._download_tasks.append(future)

        with self.assertRaises(OperationalError):
            manager.get_next_downloaded_file(0)

    @patch.object(download_manager, "ResultSetDownloadHandler", _InstantHandler)
    def test_link_source_is_refused_after_shutdown(self):
        # No schedulable links: the first request already shuts the manager down
        manager = self._manager([_link(0, rows=0)], max_download_threads=2)
        self.assertIsNone(manager.get_next_downloaded_file(0))

        self.assertFalse(manager.set_link_source(Mock()))
        self.assertIsNone(manager._link_source)

    def test_handlers_share_the_managers_cancel_event(self):
        manager = self._manager([_link(0)], max_download_threads=1)
        with patch.object(download_manager, "ResultSetDownloadHandler") as handler:
            manager._schedule_downloads()
        self.assertIs(handler.call_args.kwargs["cancel_event"], manager._cancelled)
        manager._shutdown_manager()


class DownloaderCancelTests(unittest.TestCase):
    class _Response:
        """Streams the given chunks; a chunk that is an exception is raised instead."""

        status = 200

        def __init__(self, chunks, before_chunk=None):
            self.chunks = chunks
            self.before_chunk = before_chunk or (lambda i: None)
            self.read = 0
            self.released = False

        def stream(self, amt):
            for i, chunk in enumerate(self.chunks):
                self.before_chunk(i)
                if isinstance(chunk, Exception):
                    raise chunk
                self.read += 1
                yield chunk

        def release_conn(self):
            self.released = True

    @staticmethod
    @contextmanager
    def _request_context(response):
        # Like UnifiedHttpClient.request_context: errors in the block become RequestError
        try:
            yield response
        except Exception as e:
            raise RequestError(f"HTTP request error: {e}")

    def _http_client(self, *responses):
        http_client = MagicMock()
        pending = list(responses)
        http_client.request_context.side_effect = lambda **_: self._request_context(
            pending.pop(0)
        )
        return http_client

    def _handler(self, http_client, event, lz4_compressed=True):
        return downloader.ResultSetDownloadHandler(
            downloader.DownloadableResultSettings(is_lz4_compressed=lz4_compressed),
            _link(0),
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=None,
            statement_id="s",
            http_client=http_client,
            cancel_event=event,
        )

    def test_cancelled_before_the_request_skips_the_get(self):
        http_client = MagicMock()
        event = threading.Event()
        event.set()

        with self.assertRaises(OperationalError):
            self._handler(http_client, event).run()
        http_client.request_context.assert_not_called()

    def test_body_is_read_in_chunks_and_the_connection_released(self):
        response = self._Response([b"ab", b"cd"])
        http_client = self._http_client(response)

        file = self._handler(http_client, threading.Event(), False).run()

        self.assertEqual(file.file_bytes, b"abcd")
        self.assertTrue(response.released)
        self.assertIs(
            http_client.request_context.call_args.kwargs["preload_content"], False
        )

    def test_cancel_mid_body_stops_reading_and_skips_decompression(self):
        event = threading.Event()
        response = self._Response(
            [b"a", b"b", b"c", b"d"], before_chunk=lambda i: i == 1 and event.set()
        )
        with patch.object(
            downloader.ResultSetDownloadHandler, "_decompress_data"
        ) as decompress:
            with self.assertRaises(OperationalError) as raised:
                self._handler(self._http_client(response), event).run()

        # Raised outside the HTTP client, so it is not wrapped (or logged) as an HTTP error
        self.assertNotIsInstance(raised.exception, RequestError)
        self.assertEqual(response.read, 2)
        self.assertFalse(response.released)
        decompress.assert_not_called()

    @staticmethod
    def _unset_event():
        """A cancel event that is never set and records the backoff waits."""
        return Mock(is_set=Mock(return_value=False))

    def test_interrupted_body_is_retried(self):
        broken = self._Response([b"ab", ProtocolError("connection reset")])
        http_client = self._http_client(broken, self._Response([b"ab", b"cd"]))
        event = self._unset_event()

        file = self._handler(http_client, event, False).run()

        self.assertEqual(file.file_bytes, b"abcd")
        self.assertEqual(http_client.request_context.call_count, 2)
        event.wait.assert_called_once_with(0.5)

    def test_no_retry_after_a_cancel(self):
        event = threading.Event()
        broken = self._Response(
            [ProtocolError("connection reset")], before_chunk=lambda i: event.set()
        )
        http_client = self._http_client(broken, self._Response([b"ab"]))

        with self.assertRaises(OperationalError):
            self._handler(http_client, event, False).run()
        self.assertEqual(http_client.request_context.call_count, 1)

    def test_body_errors_give_up_after_three_attempts(self):
        http_client = self._http_client(
            *[self._Response([ProtocolError("connection reset")]) for _ in range(3)]
        )
        event = self._unset_event()

        # The last failure surfaces as the client's RequestError, as on main when
        # urllib3 runs out of retries
        with self.assertRaises(RequestError):
            self._handler(http_client, event, False).run()
        self.assertEqual(http_client.request_context.call_count, 3)
        self.assertEqual([c.args[0] for c in event.wait.call_args_list], [0.5, 1.0])


class ResultSetPrefetchTests(unittest.TestCase):
    BUFFER = 100 * MIB

    def _result_set(self, initial_links, prefetch=True, has_more_rows=True, queue=None):
        backend = Mock(spec=ThriftDatabricksClient)
        if queue is None:
            queue = Mock(spec=ThriftCloudFetchQueue)
            queue.result_links = initial_links
            queue.download_manager = Mock()
        t_row_set = ttypes.TRowSet(startRowOffset=0, rows=[], resultLinks=initial_links)
        with patch(
            "databricks.sql.utils.ThriftResultSetQueueFactory.build_queue",
            return_value=queue,
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
                buffer_size_bytes=self.BUFFER,
                arraysize=1000,
                t_row_set=t_row_set,
                max_download_threads=10,
                has_more_rows=has_more_rows,
                cloudfetch_prefetch=prefetch,
            )
        return rs, backend, queue

    def test_hands_link_fetching_to_the_queue(self):
        rs, backend, queue = self._result_set([_link(0, 100), _link(100, 100)])

        queue.download_manager.set_link_source.assert_called_once_with(
            rs._fetch_more_links
        )
        self.assertFalse(rs.has_more_rows)
        backend.fetch_results.assert_not_called()

    def test_link_source_asks_for_free_slots_plus_half_a_pool(self):
        rs, backend, queue = self._result_set([_link(0, 100), _link(100, 100)])
        source = queue.download_manager.set_link_source.call_args.args[0]
        backend.fetch_result_links.side_effect = [
            ([_link(200, 50)], True),
            ([_link(250, 50)], False),
        ]

        self.assertEqual(source(3), ([_link(200, 50)], True))
        self.assertEqual(source(1), ([_link(250, 50)], False))

        first, second = backend.fetch_result_links.call_args_list
        self.assertEqual(first.kwargs["max_bytes"], 8 * 20 * MIB)
        self.assertEqual(first.kwargs["expected_row_start_offset"], 200)
        self.assertEqual(first.kwargs["max_rows"], 1000)
        self.assertEqual(second.kwargs["max_bytes"], 6 * 20 * MIB)
        self.assertEqual(second.kwargs["expected_row_start_offset"], 250)

    def test_link_source_budget_stays_between_one_and_four_buffers(self):
        rs, backend, queue = self._result_set([_link(0, size=60 * MIB)])
        source = queue.download_manager.set_link_source.call_args.args[0]
        backend.fetch_result_links.return_value = ([], True)

        source(10)
        rs._max_link_bytes = MIB
        source(1)

        budgets = [
            c.kwargs["max_bytes"] for c in backend.fetch_result_links.call_args_list
        ]
        self.assertEqual(budgets, [4 * self.BUFFER, self.BUFFER])

    def test_kill_switch_keeps_per_batch_fetching(self):
        rs, backend, queue = self._result_set([_link(0)], prefetch=False)

        queue.download_manager.set_link_source.assert_not_called()
        self.assertTrue(rs.has_more_rows)

    def test_shut_down_queue_keeps_per_batch_fetching(self):
        queue = Mock(spec=ThriftCloudFetchQueue)
        queue.result_links = [_link(0)]
        queue.download_manager = Mock()
        queue.download_manager.set_link_source.return_value = False

        rs, _, _ = self._result_set([_link(0)], queue=queue)

        self.assertTrue(rs.has_more_rows)

    def test_inline_results_are_not_prefetched(self):
        rs, backend, _ = self._result_set([], queue=Mock(spec=ArrowQueue))

        self.assertTrue(rs.has_more_rows)
        backend.fetch_result_links.assert_not_called()

    def test_complete_results_are_not_prefetched(self):
        rs, _, queue = self._result_set([_link(0)], has_more_rows=False)

        queue.download_manager.set_link_source.assert_not_called()

    def test_prefetch_starts_after_the_first_fetch_without_direct_results(self):
        backend = Mock(spec=ThriftDatabricksClient)
        queue = Mock(spec=ThriftCloudFetchQueue)
        queue.result_links = [_link(0)]
        queue.download_manager = Mock()
        backend.fetch_results.return_value = (queue, True, 1)

        rs = ThriftResultSet(
            connection=Mock(),
            execute_response=ExecuteResponse(
                command_id=Mock(),
                status=None,
                has_been_closed_server_side=False,
                description=[],
            ),
            thrift_client=backend,
        )

        queue.download_manager.set_link_source.assert_called_once()
        self.assertFalse(rs.has_more_rows)


@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
@patch.object(download_manager, "ResultSetDownloadHandler", _ArrowFileHandler)
class ResultSetPrefetchEndToEndTests(unittest.TestCase):
    """Real ThriftCloudFetchQueue and download manager; only the download is stubbed."""

    ROWS_PER_LINK = 10

    def _result_set(self, initial_links=2, total_links=11, links_per_reply=3):
        links = [
            _link(i * self.ROWS_PER_LINK, self.ROWS_PER_LINK)
            for i in range(total_links)
        ]
        rest = links[initial_links:]
        expected_offsets = []

        def fetch_result_links(**kwargs):
            expected_offsets.append(kwargs["expected_row_start_offset"])
            reply = rest[:links_per_reply]
            del rest[:links_per_reply]
            return reply, bool(rest)

        backend = Mock(spec=ThriftDatabricksClient)
        backend.fetch_result_links.side_effect = fetch_result_links
        rs = ThriftResultSet(
            connection=Mock(),
            execute_response=ExecuteResponse(
                command_id=Mock(),
                status=None,
                has_been_closed_server_side=False,
                description=[("id", "bigint", None, None, None, None, None)],
                lz4_compressed=False,
                arrow_schema_bytes=pa.schema([("id", pa.int64())])
                .serialize()
                .to_pybytes(),
                result_format=ttypes.TSparkRowSetType.URL_BASED_SET,
            ),
            thrift_client=backend,
            arraysize=1000,
            t_row_set=ttypes.TRowSet(
                startRowOffset=0, rows=[], resultLinks=links[:initial_links]
            ),
            max_download_threads=4,
        )
        return rs, backend, expected_offsets

    def test_fetchall_arrow_reads_every_link_in_order(self):
        rs, backend, expected_offsets = self._result_set()

        self.assertFalse(rs.has_more_rows)
        ids = rs.fetchall_arrow().column(0).to_pylist()

        self.assertEqual(ids, list(range(110)))
        self.assertEqual(expected_offsets, [20, 50, 80])
        backend.fetch_results.assert_not_called()

    def test_fetchmany_and_fetchone_cross_link_batches(self):
        rs, _, _ = self._result_set()

        ids = [rs.fetchone()[0] for _ in range(15)]
        while True:
            table = rs.fetchmany_arrow(25)
            if table.num_rows == 0:
                break
            ids += table.column(0).to_pylist()

        self.assertEqual(ids, list(range(110)))

    def test_close_mid_stream_cancels_the_download_manager(self):
        rs, backend, _ = self._result_set()
        manager = rs.results.download_manager
        rs.fetchmany_arrow(35)

        rs.close()

        self.assertTrue(manager._cancelled.is_set())
        self.assertIsNone(manager._link_source)
        backend.close_command.assert_called_once()

    def test_cancelled_downloads_fail_the_next_fetch_instead_of_truncating(self):
        rs, _, _ = self._result_set()
        rs.fetchmany_arrow(15)

        rs._cancel_downloads()

        with self.assertRaises(OperationalError):
            rs.fetchall_arrow()

    def test_a_retry_after_a_failed_link_fetch_fails_instead_of_skipping_rows(self):
        rs, backend, _ = self._result_set()
        backend.fetch_result_links.side_effect = [
            DataError("FetchResults failed"),
            ([_link(20, self.ROWS_PER_LINK)], False),
        ]

        with self.assertRaises(DataError):
            rs.fetchmany_arrow(25)
        # The rows collected by the failed call are gone, so retrying must fail too
        with self.assertRaises(OperationalError) as raised:
            rs.fetchmany_arrow(25)
        self.assertIsInstance(raised.exception.__cause__, DataError)
        self.assertEqual(backend.fetch_result_links.call_count, 1)


class ThriftClientPrefetchTests(unittest.TestCase):
    def _client(self, **kwargs):
        return ThriftDatabricksClient(
            "foobar",
            443,
            "path",
            [],
            auth_provider=AuthProvider(),
            ssl_options=SSLOptions(),
            http_client=MagicMock(),
            **kwargs,
        )

    def test_prefetch_is_on_by_default_and_can_be_disabled(self):
        self.assertTrue(self._client()._cloudfetch_prefetch)
        for off in (False, "false", "False", "0", "no", "off"):
            self.assertFalse(
                self._client(_cloudfetch_prefetch=off)._cloudfetch_prefetch
            )
        for on in (True, "true", "1", "yes", "on", None):
            self.assertTrue(self._client(_cloudfetch_prefetch=on)._cloudfetch_prefetch)

    def test_unrecognized_strings_keep_prefetch_on_with_a_warning(self):
        with self.assertLogs(
            "databricks.sql.backend.thrift_backend", level="WARNING"
        ) as logs:
            client = self._client(_cloudfetch_prefetch="disabled")

        self.assertTrue(client._cloudfetch_prefetch)
        self.assertIn("_cloudfetch_prefetch='disabled'", logs.output[0])

    # get_schemas/get_tables/get_columns import ThriftResultSet locally
    @patch("databricks.sql.result_set.ThriftResultSet")
    @patch("databricks.sql.backend.thrift_backend.ThriftResultSet")
    @patch("databricks.sql.backend.thrift_backend.TCLIService.Client", autospec=True)
    def test_every_entry_point_passes_the_setting_to_the_result_set(
        self, _, backend_result_set, module_result_set
    ):
        client = self._client(_cloudfetch_prefetch=False)
        client._handle_execute_response = Mock(return_value=(Mock(), Mock()))
        client._hive_schema_to_description = Mock(return_value=[])
        entry_points = {
            "execute_command": lambda: client.execute_command(
                "foo", Mock(), 100, 200, Mock(), Mock(), Mock()
            ),
            "get_execution_result": lambda: client.get_execution_result(Mock(), Mock()),
            "get_catalogs": lambda: client.get_catalogs(Mock(), 100, 200, Mock()),
            "get_schemas": lambda: client.get_schemas(Mock(), 100, 200, Mock()),
            "get_tables": lambda: client.get_tables(Mock(), 100, 200, Mock()),
            "get_columns": lambda: client.get_columns(Mock(), 100, 200, Mock()),
        }
        for name, call in entry_points.items():
            with self.subTest(name):
                backend_result_set.reset_mock()
                module_result_set.reset_mock()

                call()

                result_set = (
                    backend_result_set
                    if backend_result_set.called
                    else module_result_set
                )
                self.assertIs(result_set.call_args.kwargs["cloudfetch_prefetch"], False)

    @patch("databricks.sql.backend.thrift_backend.TCLIService.Client", autospec=True)
    def test_fetch_result_links(self, tcli_service_class):
        links = [_link(200, 50)]
        tcli_service_class.return_value.FetchResults.return_value = (
            ttypes.TFetchResultsResp(
                status=ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS),
                hasMoreRows=True,
                results=ttypes.TRowSet(startRowOffset=200, rows=[], resultLinks=links),
            )
        )
        client = self._client()

        got = client.fetch_result_links(
            command_id=Mock(),
            max_rows=1000,
            max_bytes=123,
            expected_row_start_offset=200,
        )

        self.assertEqual(got, (links, True))
        req = tcli_service_class.return_value.FetchResults.call_args.args[0]
        self.assertEqual(
            (req.maxRows, req.maxBytes, req.orientation),
            (1000, 123, ttypes.TFetchOrientation.FETCH_NEXT),
        )
        with self.assertRaises(DataError):
            client.fetch_result_links(
                command_id=Mock(),
                max_rows=1,
                max_bytes=1,
                expected_row_start_offset=100,
            )

    @patch("databricks.sql.backend.thrift_backend.TCLIService.Client", autospec=True)
    def test_fetch_result_links_rejects_rows_that_are_not_links(
        self, tcli_service_class
    ):
        ok = ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS)
        inline_rows = ttypes.TRowSet(
            startRowOffset=0,
            rows=[],
            arrowBatches=[ttypes.TSparkArrowBatch(batch=b"x", rowCount=1)],
        )
        arrow_format = ttypes.TGetResultSetMetadataResp(
            status=ok, resultFormat=ttypes.TSparkRowSetType.ARROW_BASED_SET
        )
        client = self._client()

        for resp in (
            ttypes.TFetchResultsResp(status=ok, hasMoreRows=False, results=inline_rows),
            ttypes.TFetchResultsResp(
                status=ok,
                hasMoreRows=False,
                results=ttypes.TRowSet(startRowOffset=0, rows=[]),
                resultSetMetadata=arrow_format,
            ),
        ):
            tcli_service_class.return_value.FetchResults.return_value = resp
            with self.assertRaises(DataError):
                client.fetch_result_links(
                    command_id=Mock(),
                    max_rows=1,
                    max_bytes=1,
                    expected_row_start_offset=0,
                )


class CursorCancelTests(unittest.TestCase):
    def test_cancel_stops_the_active_result_sets_downloads(self):
        backend = Mock()
        cursor = Cursor(Mock(), backend)
        cursor.active_command_id = Mock()
        cursor.active_result_set = Mock()

        cursor.cancel()

        backend.cancel_command.assert_called_once_with(cursor.active_command_id)
        cursor.active_result_set._cancel_downloads.assert_called_once_with()

    def test_downloads_stop_even_if_the_cancel_request_fails(self):
        backend = Mock()
        backend.cancel_command.side_effect = RequestError("session gone")
        cursor = Cursor(Mock(), backend)
        cursor.active_command_id = Mock()
        cursor.active_result_set = Mock()

        with self.assertRaises(RequestError):
            cursor.cancel()
        cursor.active_result_set._cancel_downloads.assert_called_once_with()

    def test_a_failing_download_cancel_does_not_hide_the_cancel_error(self):
        backend = Mock()
        backend.cancel_command.side_effect = RequestError("session gone")
        cursor = Cursor(Mock(), backend)
        cursor.active_command_id = Mock()
        cursor.active_result_set = Mock()
        cursor.active_result_set._cancel_downloads.side_effect = RuntimeError("boom")

        with self.assertRaises(RequestError):
            cursor.cancel()

    def test_thrift_result_set_cancels_its_cloudfetch_downloads(self):
        queue = Mock(spec=ThriftCloudFetchQueue)
        queue.download_manager = Mock()
        rs = Mock(spec=ThriftResultSet, results=queue)

        ThriftResultSet._cancel_downloads(rs)

        queue.download_manager.cancel.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
