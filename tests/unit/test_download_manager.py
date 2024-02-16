import unittest
from unittest.mock import patch, MagicMock

import databricks.sql.cloudfetch.download_manager as download_manager
import databricks.sql.cloudfetch.downloader as downloader
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink


class DownloadManagerTests(unittest.TestCase):
    """
    Unit tests for checking download manager logic.
    """

    def create_download_manager(self):
        max_download_threads = 10
        lz4_compressed = True
        return download_manager.ResultFileDownloadManager(max_download_threads, lz4_compressed)

    def create_result_link(
            self,
            file_link: str = "fileLink",
            start_row_offset: int = 0,
            row_count: int = 8000,
            bytes_num: int = 20971520
    ):
        return TSparkArrowResultLink(file_link, None, start_row_offset, row_count, bytes_num)

    def create_result_links(self, num_files: int, start_row_offset: int = 0):
        result_links = []
        for i in range(num_files):
            file_link = "fileLink_" + str(i)
            result_link = self.create_result_link(file_link=file_link, start_row_offset=start_row_offset)
            result_links.append(result_link)
            start_row_offset += result_link.rowCount
        return result_links

    def test_add_file_links_zero_row_count(self):
        links = [self.create_result_link(row_count=0, bytes_num=0)]
        manager = self.create_download_manager()
        manager.add_file_links(links)

        assert not manager.download_handlers

    def test_add_file_links_success(self):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)

        assert len(manager.download_handlers) == 10

    def test_remove_past_handlers_one(self):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)

        manager._remove_past_handlers(8000)
        assert len(manager.download_handlers) == 9

    def test_remove_past_handlers_all(self):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)

        manager._remove_past_handlers(8000*10)
        assert len(manager.download_handlers) == 0

    @patch("concurrent.futures.ThreadPoolExecutor.submit")
    def test_schedule_downloads_partial_already_scheduled(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)

        for i in range(5):
            manager.download_handlers[i].is_download_scheduled = True

        manager._schedule_downloads()
        assert mock_submit.call_count == 5
        assert sum([1 if handler.is_download_scheduled else 0 for handler in manager.download_handlers]) == 10

    @patch("concurrent.futures.ThreadPoolExecutor.submit")
    def test_schedule_downloads_will_not_schedule_twice(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)

        for i in range(5):
            manager.download_handlers[i].is_download_scheduled = True

        manager._schedule_downloads()
        assert mock_submit.call_count == 5
        assert sum([1 if handler.is_download_scheduled else 0 for handler in manager.download_handlers]) == 10

        manager._schedule_downloads()
        assert mock_submit.call_count == 5

    @patch("concurrent.futures.ThreadPoolExecutor.submit", side_effect=[True, KeyError("foo")])
    def test_schedule_downloads_submit_fails(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)

        manager._schedule_downloads()
        assert mock_submit.call_count == 2
        assert sum([1 if handler.is_download_scheduled else 0 for handler in manager.download_handlers]) == 1

    @patch("concurrent.futures.ThreadPoolExecutor.submit")
    def test_find_next_file_index_all_scheduled_next_row_0(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)
        manager._schedule_downloads()

        assert manager._find_next_file_index(0) == 0

    @patch("concurrent.futures.ThreadPoolExecutor.submit")
    def test_find_next_file_index_all_scheduled_next_row_7999(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)
        manager._schedule_downloads()

        assert manager._find_next_file_index(7999) is None

    @patch("concurrent.futures.ThreadPoolExecutor.submit")
    def test_find_next_file_index_all_scheduled_next_row_8000(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)
        manager._schedule_downloads()

        assert manager._find_next_file_index(8000) == 1

    @patch("concurrent.futures.ThreadPoolExecutor.submit", side_effect=[True, KeyError("foo")])
    def test_find_next_file_index_one_scheduled_next_row_8000(self, mock_submit):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager()
        manager.add_file_links(links)
        manager._schedule_downloads()

        assert manager._find_next_file_index(8000) is None