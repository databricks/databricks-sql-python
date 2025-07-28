from contextlib import contextmanager
import unittest
from unittest.mock import Mock, patch, MagicMock

import requests

import databricks.sql.cloudfetch.downloader as downloader
from databricks.sql.common.http import DatabricksHttpClient
from databricks.sql.exc import Error
from databricks.sql.types import SSLOptions


def create_response(**kwargs) -> requests.Response:
    result = requests.Response()
    for k, v in kwargs.items():
        setattr(result, k, v)
    result.close = Mock()
    return result


class DownloaderTests(unittest.TestCase):
    """
    Unit tests for checking downloader logic.
    """

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
        settings = Mock()
        result_link = Mock()
        # Already expired
        result_link.expiryTime = 999
        d = downloader.ResultSetDownloadHandler(
            settings, result_link, ssl_options=SSLOptions()
        )

        with self.assertRaises(Error) as context:
            d.run()
        self.assertTrue("link has expired" in context.exception.message)

        mock_time.assert_called_once()

    @patch("time.time", return_value=1000)
    def test_run_link_past_expiry_buffer(self, mock_time):
        settings = Mock(link_expiry_buffer_secs=5)
        result_link = Mock()
        # Within the expiry buffer time
        result_link.expiryTime = 1004
        d = downloader.ResultSetDownloadHandler(
            settings, result_link, ssl_options=SSLOptions()
        )

        with self.assertRaises(Error) as context:
            d.run()
        self.assertTrue("link has expired" in context.exception.message)

        mock_time.assert_called_once()

    @patch("time.time", return_value=1000)
    def test_run_get_response_not_ok(self, mock_time):
        http_client = DatabricksHttpClient.get_instance()
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0)
        settings.download_timeout = 0
        settings.use_proxy = False
        result_link = Mock(expiryTime=1001)

        with patch.object(
            http_client,
            "execute",
            return_value=create_response(status_code=404, _content=b"1234"),
        ):
            d = downloader.ResultSetDownloadHandler(
                settings, result_link, ssl_options=SSLOptions()
            )
            with self.assertRaises(requests.exceptions.HTTPError) as context:
                d.run()
            self.assertTrue("404" in str(context.exception))

    @patch("time.time")
    def test_run_uncompressed_successful(self, mock_time):
        self._setup_time_mock_for_download(mock_time, 1000.5)
        
        http_client = DatabricksHttpClient.get_instance()
        file_bytes = b"1234567890" * 10
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = False
        settings.min_cloudfetch_download_speed = 1.0
        result_link = Mock(bytesNum=100, expiryTime=1001)
        result_link.fileLink = "https://s3.amazonaws.com/bucket/file.arrow?token=abc123"

        with patch.object(
            http_client,
            "execute",
            return_value=create_response(status_code=200, _content=file_bytes),
        ):
            d = downloader.ResultSetDownloadHandler(
                settings, result_link, ssl_options=SSLOptions()
            )
            file = d.run()

            assert file.file_bytes == b"1234567890" * 10

    @patch("time.time")
    def test_run_compressed_successful(self, mock_time):
        self._setup_time_mock_for_download(mock_time, 1000.2)
        
        http_client = DatabricksHttpClient.get_instance()
        file_bytes = b"1234567890" * 10
        compressed_bytes = b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = True
        settings.min_cloudfetch_download_speed = 1.0
        result_link = Mock(bytesNum=100, expiryTime=1001)
        result_link.fileLink = "https://s3.amazonaws.com/bucket/file.arrow?token=xyz789"
        with patch.object(
            http_client,
            "execute",
            return_value=create_response(status_code=200, _content=compressed_bytes),
        ):
            d = downloader.ResultSetDownloadHandler(
                settings, result_link, ssl_options=SSLOptions()
            )
            file = d.run()

            assert file.file_bytes == b"1234567890" * 10

    @patch("time.time", return_value=1000)
    def test_download_connection_error(self, mock_time):

        http_client = DatabricksHttpClient.get_instance()
        settings = Mock(
            link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True
        )
        result_link = Mock(bytesNum=100, expiryTime=1001)

        with patch.object(http_client, "execute", side_effect=ConnectionError("foo")):
            d = downloader.ResultSetDownloadHandler(
                settings, result_link, ssl_options=SSLOptions()
            )
            with self.assertRaises(ConnectionError):
                d.run()

    @patch("time.time", return_value=1000)
    def test_download_timeout(self, mock_time):
        http_client = DatabricksHttpClient.get_instance()
        settings = Mock(
            link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True
        )
        result_link = Mock(bytesNum=100, expiryTime=1001)

        with patch.object(http_client, "execute", side_effect=TimeoutError("foo")):
            d = downloader.ResultSetDownloadHandler(
                settings, result_link, ssl_options=SSLOptions()
            )
            with self.assertRaises(TimeoutError):
                d.run()
