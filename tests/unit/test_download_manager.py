import unittest
from unittest.mock import patch, MagicMock

import databricks.sql.cloudfetch.download_manager as download_manager
from databricks.sql.types import SSLOptions
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink


class DownloadManagerTests(unittest.TestCase):
    """
    Unit tests for checking download manager logic.
    """

    def create_download_manager(self, links, max_download_threads=10, lz4_compressed=True):
        return download_manager.ResultFileDownloadManager(
            links,
            max_download_threads,
            lz4_compressed,
            ssl_options=SSLOptions(),
        )

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
        manager = self.create_download_manager(links)

        assert len(manager._pending_links) == 0  # the only link supplied contains no data, so should be skipped
        assert len(manager._download_tasks) == 0

    def test_add_file_links_success(self):
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager(links)

        assert len(manager._pending_links) == len(links)
        assert len(manager._download_tasks) == 0

    @patch("concurrent.futures.ThreadPoolExecutor.submit")
    def test_schedule_downloads(self, mock_submit):
        max_download_threads = 4
        links = self.create_result_links(num_files=10)
        manager = self.create_download_manager(links, max_download_threads=max_download_threads)

        manager._schedule_downloads()
        assert mock_submit.call_count == max_download_threads
        assert len(manager._pending_links) == len(links) - max_download_threads
        assert len(manager._download_tasks) == max_download_threads
