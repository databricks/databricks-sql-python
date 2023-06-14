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
        result_link.expiryTime = 999999
        d = downloader.ResultSetDownloadHandler(settings, result_link)
        assert not d.is_link_expired
        d.run()
        assert d.is_link_expired
        mock_time.assert_called_once()

    @patch('time.time', return_value=1000)
    def test_run_link_past_expiry_buffer(self, mock_time):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0.005
        result_link = Mock()
        # Within the expiry buffer time
        result_link.expiryTime = 1000004
        d = downloader.ResultSetDownloadHandler(settings, result_link)
        assert not d.is_link_expired
        d.run()
        assert d.is_link_expired
        mock_time.assert_called_once()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(status_code=500))))
    @patch('time.time', return_value=1000)
    def test_run_get_response_not_200(self, mock_time, mock_session):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.download_timeout = 0
        settings.use_proxy = False
        result_link = Mock()
        result_link.expiryTime = 1000001

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session',
           return_value=MagicMock(get=MagicMock(return_value=MagicMock(status_code=200, content=b"1234567890" * 9))))
    @patch('time.time', return_value=1000)
    def test_run_uncompressed_data_length_incorrect(self, mock_time, mock_session):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.download_timeout = 0
        settings.use_proxy = False
        settings.is_lz4_compressed = False
        result_link = Mock()
        result_link.bytesNum = 100
        result_link.expiryTime = 1000001

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(status_code=200))))
    @patch('time.time', return_value=1000)
    def test_run_compressed_data_length_incorrect(self, mock_time, mock_session):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.download_timeout = 0
        settings.use_proxy = False
        settings.is_lz4_compressed = True
        result_link = Mock()
        result_link.bytesNum = 100
        result_link.expiryTime = 1000001
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@Z\x00\x00\x00\x00\x00\x00\x00\xec\x14\x00\x00\x00\xaf1234567890\n\x008P67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session',
           return_value=MagicMock(get=MagicMock(return_value=MagicMock(status_code=200, content=b"1234567890" * 10))))
    @patch('time.time', return_value=1000)
    def test_run_uncompressed_successful(self, mock_time, mock_session):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.download_timeout = 0
        settings.use_proxy = False
        settings.is_lz4_compressed = False
        result_link = Mock()
        result_link.bytesNum = 100
        result_link.expiryTime = 1000001

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert d.result_file == b"1234567890" * 10
        assert d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session', return_value=MagicMock(get=MagicMock(return_value=MagicMock(status_code=200))))
    @patch('time.time', return_value=1000)
    def test_run_compressed_successful(self, mock_time, mock_session):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.download_timeout = 0
        settings.use_proxy = False
        settings.is_lz4_compressed = True
        result_link = Mock()
        result_link.bytesNum = 100
        result_link.expiryTime = 1000001
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
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.use_proxy = False
        settings.is_lz4_compressed = True
        result_link = Mock()
        result_link.bytesNum = 100
        result_link.expiryTime = 1000001
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch('requests.Session.get', side_effect=TimeoutError('foo'))
    @patch('time.time', return_value=1000)
    def test_download_timeout(self, mock_time, mock_session):
        settings = Mock()
        settings.result_file_link_expiry_buffer = 0
        settings.use_proxy = False
        settings.is_lz4_compressed = True
        result_link = Mock()
        result_link.bytesNum = 100
        result_link.expiryTime = 1000001
        mock_session.return_value.get.return_value.content = \
            b'\x04"M\x18h@d\x00\x00\x00\x00\x00\x00\x00#\x14\x00\x00\x00\xaf1234567890\n\x00BP67890\x00\x00\x00\x00'

        d = downloader.ResultSetDownloadHandler(settings, result_link)
        d.run()

        assert not d.is_file_downloaded_successfully
        assert d.is_download_finished.is_set()

    @patch("threading.Event.is_set", return_value=True)
    def test_is_download_successful_with_event_set(self, mock_is_set):
        settings = Mock()
        result_link = Mock()
        handler = downloader.ResultSetDownloadHandler(settings, result_link)

        status = handler.is_file_download_successful()
        assert status == handler.is_file_downloaded_successfully

    @patch("threading.Event.is_set", return_value=False)
    @patch("threading.Semaphore.acquire", return_value=True)
    def test_is_file_download_successful_null_timeout_download_completes(self, mock_acquire, mock_is_set):
        settings = Mock()
        settings.download_timeout = None
        result_link = Mock()
        handler = downloader.ResultSetDownloadHandler(settings, result_link)

        status = handler.is_file_download_successful()
        assert status == handler.is_file_downloaded_successfully
        mock_acquire.assert_called()

    @patch("threading.Event.is_set", return_value=False)
    @patch("threading.Semaphore.acquire", return_value=True)
    def test_is_file_download_successful_zero_timeout_download_completes(self, mock_acquire, mock_is_set):
        settings = Mock()
        settings.download_timeout = 0
        result_link = Mock()
        handler = downloader.ResultSetDownloadHandler(settings, result_link)

        status = handler.is_file_download_successful()
        assert status == handler.is_file_downloaded_successfully
        mock_acquire.assert_called()
        assert not handler.is_download_timedout

    @patch("threading.Event.is_set", return_value=False)
    @patch("threading.Semaphore.acquire", return_value=True)
    def test_is_file_download_successful_with_timeout_download_completes(self, mock_acquire, mock_is_set):
        settings = Mock()
        settings.download_timeout = 10
        result_link = Mock()
        handler = downloader.ResultSetDownloadHandler(settings, result_link)

        status = handler.is_file_download_successful()
        assert status == handler.is_file_downloaded_successfully
        mock_acquire.assert_called_with(timeout=settings.download_timeout)
        assert not handler.is_download_timedout

    @patch("threading.Event.is_set", return_value=False)
    @patch("threading.Semaphore.acquire", return_value=False)
    def test_is_file_download_successful_download_times_out(self, mock_acquire, mock_is_set):
        settings = Mock()
        settings.download_timeout = 10
        result_link = Mock()
        result_link.fileLink = "foo"
        handler = downloader.ResultSetDownloadHandler(settings, result_link)

        status = handler.is_file_download_successful()
        assert not status
        mock_acquire.assert_called_with(timeout=settings.download_timeout)
        assert handler.is_download_timedout
