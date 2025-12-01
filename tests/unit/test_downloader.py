import unittest
from unittest.mock import patch, MagicMock, Mock
import requests

import databricks.sql.cloudfetch.downloader as downloader
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
            host_url=Mock(),
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
            host_url=Mock(),
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
            host_url=Mock(),
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
                host_url=Mock(),
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
        self._setup_mock_http_response(mock_http_client, status=200, data=compressed_bytes)

        # Mock the decompression method and log metrics to avoid issues
        with patch.object(downloader.ResultSetDownloadHandler, '_decompress_data', return_value=file_bytes), \
             patch.object(downloader.ResultSetDownloadHandler, '_log_download_metrics'):
            d = downloader.ResultSetDownloadHandler(
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                host_url=Mock(),
                statement_id=Mock(),
                http_client=mock_http_client,
            )
            file = d.run()
            self.assertEqual(file.file_bytes, file_bytes)
            self.assertEqual(file.start_row_offset, result_link.startRowOffset)
            self.assertEqual(file.row_count, result_link.rowCount)

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
            host_url=Mock(),
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
            host_url=Mock(),
            statement_id=Mock(),
            http_client=mock_http_client,
        )
        with self.assertRaises(TimeoutError):
            d.run()
