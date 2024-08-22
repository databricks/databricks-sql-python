import unittest
from unittest.mock import Mock, patch, MagicMock

import requests

import databricks.sql.cloudfetch.downloader as downloader
from databricks.sql.exc import Error
from databricks.sql.types import SSLOptions


def create_response(**kwargs) -> requests.Response:
    result = requests.Response()
    for k, v in kwargs.items():
        setattr(result, k, v)
    return result


class DownloaderTests(unittest.TestCase):
    """
    Unit tests for checking downloader logic.
    """

    @patch('time.time', return_value=1000)
    def test_run_link_expired(self, mock_time):
        settings = Mock()
        result_link = Mock()
        # Already expired
        result_link.expiryTime = 999
        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())

        with self.assertRaises(Error) as context:
            d.run()
        self.assertTrue('link has expired' in context.exception.message)

        mock_time.assert_called_once()

    @patch('time.time', return_value=1000)
    def test_run_link_past_expiry_buffer(self, mock_time):
        settings = Mock(link_expiry_buffer_secs=5)
        result_link = Mock()
        # Within the expiry buffer time
        result_link.expiryTime = 1004
        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())

        with self.assertRaises(Error) as context:
            d.run()
        self.assertTrue('link has expired' in context.exception.message)

        mock_time.assert_called_once()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=None)))
    @patch('time.time', return_value=1000)
    def test_run_get_response_not_ok(self, mock_time, mock_session):
        mock_session.return_value.get.return_value = create_response(status_code=404)

        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0)
        settings.download_timeout = 0
        settings.use_proxy = False
        result_link = Mock(expiryTime=1001)

        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())
        with self.assertRaises(requests.exceptions.HTTPError) as context:
            d.run()
        self.assertTrue('404' in str(context.exception))

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=None)))
    @patch('time.time', return_value=1000)
    def test_run_uncompressed_successful(self, mock_time, mock_session):
        file_bytes = b"1234567890" * 10
        mock_session.return_value.get.return_value = create_response(status_code=200, _content=file_bytes)

        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = False
        result_link = Mock(bytesNum=100, expiryTime=1001)

        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())
        file = d.run()

        assert file.file_bytes == b"1234567890" * 10

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(ok=True))))
    @patch('time.time', return_value=1000)
    def test_run_compressed_successful(self, mock_time, mock_session):
        file_bytes = b"1234567890" * 10
        compressed_bytes = b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'
        mock_session.return_value.get.return_value = create_response(status_code=200, _content=compressed_bytes)

        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = True
        result_link = Mock(bytesNum=100, expiryTime=1001)

        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())
        file = d.run()

        assert file.file_bytes == b"1234567890" * 10

    @patch('requests.Session.get', side_effect=ConnectionError('foo'))
    @patch('time.time', return_value=1000)
    def test_download_connection_error(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True)
        result_link = Mock(bytesNum=100, expiryTime=1001)
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())
        with self.assertRaises(ConnectionError):
            d.run()

    @patch('requests.Session.get', side_effect=TimeoutError('foo'))
    @patch('time.time', return_value=1000)
    def test_download_timeout(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True)
        result_link = Mock(bytesNum=100, expiryTime=1001)
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link, ssl_options=SSLOptions())
        with self.assertRaises(TimeoutError):
            d.run()
