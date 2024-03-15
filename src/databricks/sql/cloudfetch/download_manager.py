import logging

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Union

from databricks.sql.cloudfetch.downloader import (
    ResultSetDownloadHandler,
    DownloadableResultSettings,
)
from databricks.sql.exc import ResultSetDownloadError
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

logger = logging.getLogger(__name__)


@dataclass
class DownloadedFile:
    """
    Class for the result file and metadata.

    Attributes:
        file_bytes (bytes): Downloaded file in bytes.
        start_row_offset (int): The offset of the starting row in relation to the full result.
        row_count (int): Number of rows the file represents in the result.
    """

    file_bytes: bytes
    start_row_offset: int
    row_count: int


class ResultFileDownloadManager:
    def __init__(self, max_download_threads: int, lz4_compressed: bool):
        self.download_handlers: List[ResultSetDownloadHandler] = []
        self.thread_pool = ThreadPoolExecutor(max_workers=max_download_threads + 1)
        self.downloadable_result_settings = DownloadableResultSettings(lz4_compressed)

    def add_file_links(
        self, t_spark_arrow_result_links: List[TSparkArrowResultLink]
    ) -> None:
        """
        Create download handler for each cloud fetch link.

        Args:
            t_spark_arrow_result_links: List of cloud fetch links consisting of file URL and metadata.
        """
        for link in t_spark_arrow_result_links:
            if link.rowCount <= 0:
                continue
            self.download_handlers.append(
                ResultSetDownloadHandler(self.downloadable_result_settings, link)
            )

    def get_next_downloaded_file(
        self, next_row_offset: int
    ) -> Union[DownloadedFile, None]:
        """
        Get next file that starts at given offset.

        This function gets the next downloaded file in which its rows start at the specified next_row_offset
        in relation to the full result. File downloads are scheduled if not already, and once the correct
        download handler is located, the function waits for the download status and returns the resulting file.
        If there are no more downloads, a download was not successful, or the correct file could not be located,
        this function shuts down the thread pool and returns None.

        Args:
            next_row_offset (int): The offset of the starting row of the next file we want data from.
        """
        # No more files to download from this batch of links
        if not self.download_handlers:
            self._shutdown_manager()
            return None

        # Remove handlers we don't need anymore
        self._remove_past_handlers(next_row_offset)

        # Schedule the downloads
        self._schedule_downloads()

        # Find next file
        idx = self._find_next_file_index(next_row_offset)
        # is this correct?
        if idx is None:
            self._shutdown_manager()
            logger.debug("could not find next file index")
            return None
        handler = self.download_handlers[idx]

        # Check (and wait) for download status
        if handler.is_file_download_successful():
            # Buffer should be empty so set buffer to new ArrowQueue with result_file
            result = DownloadedFile(
                handler.result_file,
                handler.result_link.startRowOffset,
                handler.result_link.rowCount,
            )
            self.download_handlers.pop(idx)
            # Return True upon successful download to continue loop and not force a retry
            return result
        # Download was not successful for next download item. Fail
        self._shutdown_manager()
        raise ResultSetDownloadError(
            f"Download failed for result set starting at {next_row_offset}"
        )

    def _remove_past_handlers(self, next_row_offset: int):
        # Any link in which its start to end range doesn't include the next row to be fetched does not need downloading
        i = 0
        while i < len(self.download_handlers):
            result_link = self.download_handlers[i].result_link
            if result_link.startRowOffset + result_link.rowCount > next_row_offset:
                i += 1
                continue
            self.download_handlers.pop(i)

    def _schedule_downloads(self):
        # Schedule downloads for all download handlers if not already scheduled.
        for handler in self.download_handlers:
            if handler.is_download_scheduled:
                continue
            try:
                self.thread_pool.submit(handler.run)
            except Exception as e:
                logger.error(e)
                break
            handler.is_download_scheduled = True

    def _find_next_file_index(self, next_row_offset: int):
        # Get the handler index of the next file in order
        next_indices = [
            i
            for i, handler in enumerate(self.download_handlers)
            if handler.is_download_scheduled
            and handler.result_link.startRowOffset == next_row_offset
        ]
        return next_indices[0] if len(next_indices) > 0 else None

    def _shutdown_manager(self):
        # Clear download handlers and shutdown the thread pool
        self.download_handlers = []
        self.thread_pool.shutdown(wait=False)
