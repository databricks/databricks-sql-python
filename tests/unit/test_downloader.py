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

    @patch("time.time", return_value=1000)
    def test_run_link_expired(self, mock_time):
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
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
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

        d = downloader.ResultSetDownloadHandler(
            settings,
            result_link,
            ssl_options=SSLOptions(),
            chunk_id=0,
            session_id_hex=Mock(),
            statement_id=Mock(),
        )
        with self.assertRaises(requests.exceptions.HTTPError) as context:
            d.run()
        self.assertTrue("404" in str(context.exception))

    @patch("time.time", return_value=1000)
    def test_run_uncompressed_successful(self, mock_time):
        http_client = DatabricksHttpClient.get_instance()
        file_bytes = b"1234567890" * 10
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = False
        result_link = Mock(bytesNum=100, expiryTime=1001)

        with patch.object(
            http_client,
            "execute",
            return_value=create_response(status_code=200, _content=file_bytes),
        ):
            d = downloader.ResultSetDownloadHandler(
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                session_id_hex=Mock(),
                statement_id=Mock(),
            )
            file = d.run()

            assert file.file_bytes == b"1234567890" * 10

    @patch("time.time", return_value=1000)
    def test_run_compressed_successful(self, mock_time):
        http_client = DatabricksHttpClient.get_instance()
        file_bytes = b"1234567890" * 10
        compressed_bytes = b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = True
        result_link = Mock(bytesNum=100, expiryTime=1001)
        with patch.object(
            http_client,
            "execute",
            return_value=create_response(status_code=200, _content=compressed_bytes),
        ):
            d = downloader.ResultSetDownloadHandler(
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                session_id_hex=Mock(),
                statement_id=Mock(),
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
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                session_id_hex=Mock(),
                statement_id=Mock(),
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
                settings,
                result_link,
                ssl_options=SSLOptions(),
                chunk_id=0,
                session_id_hex=Mock(),
                statement_id=Mock(),
            )
            with self.assertRaises(TimeoutError):
                d.run()
