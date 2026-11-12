import unittest
from unittest.mock import patch, MagicMock, Mock

import lz4.frame
import pytest
import requests

try:
    import pyarrow
except ImportError:
    pyarrow = None

import databricks.sql.cloudfetch.downloader as downloader
from databricks.sql.utils import convert_arrow_based_file_to_arrow_table
from databricks.sql.exc import Error
from databricks.sql.types import SSLOptions


def create_mock_response(**kwargs):
    """Create a mock response object for testing"""
    mock_response = MagicMock()
    for k, v in kwargs.items():
        setattr(mock_response, k, v)
    mock_response.close = Mock()
    return mock_response


class DownloaderTests(unittest.TestCase):
    """
    Unit tests for checking downloader logic.
    """

    def _setup_mock_http_response(self, mock_http_client, status=200, data=b""):
        """Helper method to setup mock HTTP client with response context manager."""
        mock_response = MagicMock()
        mock_response.status = status
        mock_response.data = data
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_response
        mock_context_manager.__exit__.return_value = None
        mock_http_client.request_context.return_value = mock_context_manager
        return mock_response

    def _setup_time_mock_for_download(self, mock_time, end_time):
        """Helper to setup time mock that handles logging system calls."""
        call_count = [0]

        def time_side_effect():
            call_count[0] += 1
            if call_count[0] <= 2:  # First two calls (validation, start_time)
                return 1000
            else:  # All subsequent calls (logging, duration calculation)
                return end_time

        mock_time.side_effect = time_side_effect

    @patch("time.time", return_value=1000)
    def test_run_link_expired(self, mock_time):
        mock_http_client = MagicMock()
        settings = Mock()
        result_link = Mock()
        # Already expired
        result_link.expiryTime = 999
        d = downloader.ResultSetDownloadHandler(
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
            http_client=mock_http_client,
        )

        with self.assertRaises(Error) as context:
            d.run()
        self.assertTrue("link has expired" in context.exception.message)

        mock_time.assert_called_once()

    @patch("time.time", return_value=1000)
    def test_run_link_past_expiry_buffer(self, mock_time):
        mock_http_client = MagicMock()
        settings = Mock(link_expiry_buffer_secs=5)
        result_link = Mock()
        # Within the expiry buffer time
        result_link.expiryTime = 1004
        d = downloader.ResultSetDownloadHandler(
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
            http_client=mock_http_client,
        )

        with self.assertRaises(Error) as context:
            d.run()
        self.assertTrue("link has expired" in context.exception.message)

        mock_time.assert_called_once()

    @patch("time.time", return_value=1000)
    def test_run_get_response_not_ok(self, mock_time):
        mock_http_client = MagicMock()
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0)
        settings.download_timeout = 0
        settings.use_proxy = False
        result_link = Mock(expiryTime=1001)

        # Setup mock HTTP response using helper method
        self._setup_mock_http_response(mock_http_client, status=404, data=b"1234")

        d = downloader.ResultSetDownloadHandler(
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
            http_client=mock_http_client,
        )
        with self.assertRaises(Exception) as context:
            d.run()
        self.assertTrue("404" in str(context.exception))

    @patch("time.time")
    def test_run_uncompressed_successful(self, mock_time):
        self._setup_time_mock_for_download(mock_time, 1000.5)

        mock_http_client = MagicMock()
        file_bytes = b"1234567890" * 10
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = False
        settings.min_cloudfetch_download_speed = 1.0
        result_link = Mock(expiryTime=1001, bytesNum=len(file_bytes))
        result_link.fileLink = "https://s3.amazonaws.com/bucket/file.arrow?token=xyz789"

        # Setup mock HTTP response using helper method
        self._setup_mock_http_response(mock_http_client, status=200, data=file_bytes)

        # Patch the log metrics method to avoid division by zero
        with patch.object(downloader.ResultSetDownloadHandler, '_log_download_metrics'):
            d = downloader.ResultSetDownloadHandler(
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                session_id_hex=Mock(),
                statement_id=Mock(),
                http_client=mock_http_client,
            )
            file = d.run()
            self.assertEqual(file.file_bytes, file_bytes)
            self.assertEqual(file.start_row_offset, result_link.startRowOffset)
            self.assertEqual(file.row_count, result_link.rowCount)

    @patch("time.time")
    def test_run_compressed_successful(self, mock_time):
        self._setup_time_mock_for_download(mock_time, 1000.2)

        mock_http_client = MagicMock()
        file_bytes = b"1234567890" * 10
        compressed_bytes = b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = True
        settings.min_cloudfetch_download_speed = 1.0
        result_link = Mock(expiryTime=1001, bytesNum=len(file_bytes))
        result_link.fileLink = "https://s3.amazonaws.com/bucket/file.arrow?token=xyz789"

        # Setup mock HTTP response using helper method
        self._setup_mock_http_response(
            mock_http_client, status=200, data=compressed_bytes
        )

        # Mock the decompression method and log metrics to avoid issues
        with patch.object(
            downloader.ResultSetDownloadHandler,
            "_decompress_data",
            return_value=file_bytes,
        ), patch.object(downloader.ResultSetDownloadHandler, "_log_download_metrics"):
            d = downloader.ResultSetDownloadHandler(
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                session_id_hex=Mock(),
                statement_id=Mock(),
                http_client=mock_http_client,
            )
            file = d.run()
            self.assertEqual(file.file_bytes, file_bytes)
            self.assertEqual(file.start_row_offset, result_link.startRowOffset)
            self.assertEqual(file.row_count, result_link.rowCount)
            downloader.ResultSetDownloadHandler._decompress_data.assert_called_once_with(
                compressed_bytes, len(file_bytes)
            )

    @patch("time.time", return_value=1000)
    def test_download_connection_error(self, mock_time):
        mock_http_client = MagicMock()
        settings = Mock(
            link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True
        )
        result_link = Mock(bytesNum=100, expiryTime=1001)

        mock_http_client.request_context.side_effect = ConnectionError("foo")

        d = downloader.ResultSetDownloadHandler(
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
            http_client=mock_http_client,
        )
        with self.assertRaises(ConnectionError):
            d.run()

    @patch("time.time", return_value=1000)
    def test_download_timeout(self, mock_time):
        mock_http_client = MagicMock()
        settings = Mock(
            link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True
        )
        result_link = Mock(bytesNum=100, expiryTime=1001)

        mock_http_client.request_context.side_effect = TimeoutError("foo")

        d = downloader.ResultSetDownloadHandler(
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
            http_client=mock_http_client,
        )
        with self.assertRaises(TimeoutError):
            d.run()


def _arrow_ipc_stream(num_rows=5000):
    table = pyarrow.table(
        {"id": list(range(num_rows)), "s": [f"row-{i}" for i in range(num_rows)]}
    )
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=1000)
    return table, sink.getvalue().to_pybytes()


@pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
class DecompressDataTests(unittest.TestCase):
    def setUp(self):
        self.table, self.raw = _arrow_ipc_stream()
        self.compressed = lz4.frame.compress(self.raw)
        self.decompress = downloader.ResultSetDownloadHandler._decompress_data

    def test_expected_size_decompresses_into_private_memory_map(self):
        with patch.object(
            downloader.mmap, "mmap", wraps=downloader.mmap.mmap
        ) as mmap_spy:
            result = self.decompress(self.compressed, len(self.raw))

        mmap_spy.assert_called_once_with(-1, len(self.raw), **downloader._MMAP_PRIVATE)
        if hasattr(downloader.mmap, "MAP_PRIVATE"):
            self.assertEqual(
                downloader._MMAP_PRIVATE, {"flags": downloader.mmap.MAP_PRIVATE}
            )
        self.assertIsInstance(result, pyarrow.Buffer)
        self.assertEqual(result.to_pybytes(), self.raw)
        self.assertTrue(
            convert_arrow_based_file_to_arrow_table(result).equals(self.table)
        )

    def test_mmap_failure_falls_back_to_python_lz4(self):
        with patch.object(downloader.mmap, "mmap", side_effect=OSError("ENOMEM")):
            result = self.decompress(self.compressed, len(self.raw))

        self.assertIsInstance(result, bytes)
        self.assertEqual(result, self.raw)

    def test_oversized_expected_size_returns_exact_length(self):
        result = self.decompress(self.compressed, len(self.raw) + 4096)

        self.assertIsInstance(result, pyarrow.Buffer)
        self.assertEqual(result.to_pybytes(), self.raw)

    def test_undersized_expected_size_falls_back_to_python_lz4(self):
        result = self.decompress(self.compressed, len(self.raw) - 1)

        self.assertIsInstance(result, bytes)
        self.assertEqual(result, self.raw)

    def test_unknown_expected_size_uses_python_lz4(self):
        # SEA links carry the compressed size, which is not a usable output size
        for expected_size in (None, 0, len(self.compressed)):
            with patch.object(downloader.mmap, "mmap") as mmap_mock:
                result = self.decompress(self.compressed, expected_size)
            mmap_mock.assert_not_called()
            self.assertIsInstance(result, bytes)
            self.assertEqual(result, self.raw)

    def test_concatenated_frames(self):
        middle = len(self.raw) // 2
        compressed = lz4.frame.compress(self.raw[:middle]) + lz4.frame.compress(
            self.raw[middle:]
        )
        for expected_size in (len(self.raw), None):
            result = self.decompress(compressed, expected_size)
            self.assertEqual(bytes(result), self.raw)

    def test_corrupt_data_raises_python_lz4_error(self):
        # Databricks frames carry block and content checksums
        compressed = lz4.frame.compress(
            self.raw, block_checksum=True, content_checksum=True
        )
        corrupt = bytearray(compressed)
        corrupt[len(corrupt) // 2] ^= 0xFF
        truncated = compressed[:-10]
        for data in (bytes(corrupt), truncated):
            for expected_size in (len(self.raw), None):
                with self.assertRaises(RuntimeError):
                    self.decompress(data, expected_size)

    def test_without_arrow_lz4_codec_uses_python_lz4(self):
        with patch.object(downloader, "_ARROW_LZ4_AVAILABLE", False):
            result = self.decompress(self.compressed, len(self.raw))

        self.assertIsInstance(result, bytes)
        self.assertEqual(result, self.raw)
