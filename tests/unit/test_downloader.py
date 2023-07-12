import unittest
from unittest.mock import Mock, patch, MagicMock

import databricks.sql.cloudfetch.downloader as downloader


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
        d = downloader.ResultSetDownloadHandler(settings, result_link)
        assert not d.is_link_expired
        d.run()
        assert d.is_link_expired
        mock_time.assert_called_once()

    @patch('time.time', return_value=1000)
    def test_run_link_past_expiry_buffer(self, mock_time):
        settings = Mock(link_expiry_buffer_secs=5)
        result_link = Mock()
        # Within the expiry buffer time
        result_link.expiryTime = 1004
        d = downloader.ResultSetDownloadHandler(settings, result_link)
        assert not d.is_link_expired
        d.run()
        assert d.is_link_expired
        mock_time.assert_called_once()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(ok=False))))
    @patch('time.time', return_value=1000)
    def test_run_get_response_not_ok(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0)
        settings.download_timeout = 0
        settings.use_proxy = False
        result_link = Mock(expiryTime=1001)

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session',
           return_value=MagicMock(get=MagicMock(return_value=MagicMock(ok=True, content=b"1234567890" * 9))))
    @patch('time.time', return_value=1000)
    def test_run_uncompressed_data_length_incorrect(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False, is_lz4_compressed=False)
        result_link = Mock(bytesNum=100, expiryTime=1001)

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(ok=True))))
    @patch('time.time', return_value=1000)
    def test_run_compressed_data_length_incorrect(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = True
        result_link = Mock(bytesNum=100, expiryTime=1001)
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@Z\x00\x00\x00\x00\x00\x00\x00\xec\x14\x00\x00\x00\xaf1234567890\n\x008P67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session',
           return_value=MagicMock(get=MagicMock(return_value=MagicMock(ok=True, content=b"1234567890" * 10))))
    @patch('time.time', return_value=1000)
    def test_run_uncompressed_successful(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = False
        result_link = Mock(bytesNum=100, expiryTime=1001)

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert d.result_file == b"1234567890" * 10
        assert d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(ok=True))))
    @patch('time.time', return_value=1000)
    def test_run_compressed_successful(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, download_timeout=0, use_proxy=False)
        settings.is_lz4_compressed = True
        result_link = Mock(bytesNum=100, expiryTime=1001)
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert d.result_file == b"1234567890" * 10
        assert d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session.get', side_effect=ConnectionError('foo'))
    @patch('time.time', return_value=1000)
    def test_download_connection_error(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True)
        result_link = Mock(bytesNum=100, expiryTime=1001)
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session.get', side_effect=TimeoutError('foo'))
    @patch('time.time', return_value=1000)
    def test_download_timeout(self, mock_time, mock_session):
        settings = Mock(link_expiry_buffer_secs=0, use_proxy=False, is_lz4_compressed=True)
        result_link = Mock(bytesNum=100, expiryTime=1001)
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch("threading.Event.wait", return_value=True)
    def test_is_file_download_successful_has_finished(self, mock_wait):
        for timeout in [0, 1]:
            with self.subTest(timeout=timeout):
                settings = Mock(download_timeout=timeout)
                result_link = Mock()
                handler = downloader.ResultSetDownloadHandler(settings, result_link)

                status = handler.is_file_download_successful()
                assert status == handler.is_file_downloaded_successfully

    def test_is_file_download_successful_times_outs(self):
        settings = Mock(download_timeout=1)
        result_link = Mock()
        handler = downloader.ResultSetDownloadHandler(settings, result_link)

        status = handler.is_file_download_successful()
        assert not status
        assert handler.is_download_timedout
