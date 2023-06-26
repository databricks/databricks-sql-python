import logging

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Union

from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

logger = logging.getLogger(__name__)


@dataclass
class DownloadableResultSettings:
    is_lz4_compressed: bool
    link_expiry_buffer: float = 0.0
    download_timeout: float = 0.0
    max_consecutive_file_download_retries: int = 0


@dataclass
class DownloadedFile:
    file_bytes: bytes
    start_row_offset: int
    row_count: int


class ResultFileDownloadManager:

    def __init__(self, max_download_threads: int, lz4_compressed: bool):
        self.download_handlers = []
        self.thread_pool = ThreadPoolExecutor(max_workers=max_download_threads + 1)
        self.downloadable_result_settings = DownloadableResultSettings(lz4_compressed)
        self.fetch_need_retry = False
        self.num_consecutive_result_file_download_retries = 0
        self.cloud_fetch_index = 0

    def add_file_links(self, t_spark_arrow_result_links: List[TSparkArrowResultLink], next_row_index: int) -> None:
        for link in t_spark_arrow_result_links:
            if link.rowCount <= 0:
                continue
            self.download_handlers.append(ResultSetDownloadHandler(
                self.downloadable_result_settings, link))
        self.cloud_fetch_index = next_row_index

    def get_next_downloaded_file(self, next_row_index: int) -> Union[DownloadedFile, None]:
        if not self.download_handlers:
            self._shutdown_manager()
            return None

        # Remove handlers we don't need anymore
        self._remove_past_handlers(next_row_index)

        # Schedule the downloads
        self._schedule_downloads()

        # Find next file
        idx = self._find_next_file_index(next_row_index)
        if idx is None:
            self._shutdown_manager()
            return None
        handler = self.download_handlers[idx]

        # Check (and wait) for download status
        if self._check_if_download_successful(handler):
            # Buffer should be empty so set buffer to new ArrowQueue with result_file
            result = DownloadedFile(
                handler.result_file,
                handler.result_link.startRowOffset,
                handler.result_link.rowCount,
            )
            self.cloud_fetch_index += handler.result_link.rowCount
            self.download_handlers.pop(idx)
            # Return True upon successful download to continue loop and not force a retry
            return result
        # Download was not successful for next download item, force a retry
        self._shutdown_manager()
        return None

    def _remove_past_handlers(self, next_row_index: int):
        """
        Remove any download handlers whose start to end range doesn't include the next row to be fetched
        i.e. no need to download
        """
        i = 0
        while i < len(self.download_handlers):
            result_link = self.download_handlers[i].result_link
            if result_link.startRowOffset + result_link.rowCount > next_row_index:
                i += 1
                continue
            self.download_handlers.pop(i)

    def _schedule_downloads(self):
        """
        Schedule downloads for all download handlers if not already scheduled
        """
        for handler in self.download_handlers:
            if handler.is_download_scheduled:
                continue
            try:
                self.thread_pool.submit(handler.run)
            except Exception as e:
                logger.error(e)
                break
            handler.is_download_scheduled = True

    def _find_next_file_index(self, next_row_index: int):
        # Get the next downloaded file
        next_indices = [i for i, handler in enumerate(self.download_handlers)
                        if handler.is_download_scheduled and handler.result_link.startRowOffset == next_row_index]
        return next_indices[0] if len(next_indices) > 0 else None

    def _check_if_download_successful(self, handler: ResultSetDownloadHandler):
        if not handler.is_file_download_successful():
            if handler.is_link_expired:
                self._shutdown_manager()
                self.fetch_need_retry = True
                return False
            elif handler.is_download_timedout:
                if self.num_consecutive_result_file_download_retries >= \
                        self.downloadable_result_settings.max_consecutive_file_download_retries:
                    self.fetch_need_retry = True
                    return False
                self.num_consecutive_result_file_download_retries += 1
                self.thread_pool.submit(handler)
                return self._check_if_download_successful(handler)
            else:
                self.fetch_need_retry = True
                return False

        self.num_consecutive_result_file_download_retries = 0
        self.fetch_need_retry = False
        return True

    def _shutdown_manager(self):
        self.download_handlers = []
        self.thread_pool.shutdown(wait=False, cancel_futures=True)
