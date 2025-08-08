import logging

from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, Union, Tuple, Optional

from databricks.sql.cloudfetch.downloader import (
    ResultSetDownloadHandler,
    DownloadableResultSettings,
    DownloadedFile,
)
from databricks.sql.types import SSLOptions
from databricks.sql.telemetry.models.event import StatementType
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

logger = logging.getLogger(__name__)


class ResultFileDownloadManager:
    def __init__(
        self,
        links: List[TSparkArrowResultLink],
        max_download_threads: int,
        lz4_compressed: bool,
        ssl_options: SSLOptions,
        session_id_hex: Optional[str],
        statement_id: str,
        chunk_id: int,
        http_client,
    ):
        self._pending_links: List[Tuple[int, TSparkArrowResultLink]] = []
        self.chunk_id = chunk_id
        for i, link in enumerate(links, start=chunk_id):
            if link.rowCount <= 0:
                continue
            logger.debug(
                "ResultFileDownloadManager: adding file link, chunk id {}, start offset {}, row count: {}".format(
                    i, link.startRowOffset, link.rowCount
                )
            )
            self._pending_links.append((i, link))
        self.chunk_id += len(links)

        self._download_tasks: List[Future[DownloadedFile]] = []
        self._max_download_threads: int = max_download_threads
        self._thread_pool = ThreadPoolExecutor(max_workers=self._max_download_threads)

        self._downloadable_result_settings = DownloadableResultSettings(lz4_compressed)
        self._ssl_options = ssl_options
        self.session_id_hex = session_id_hex
        self.statement_id = statement_id
        self._http_client = http_client

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

        # Make sure the download queue is always full
        self._schedule_downloads()

        # No more files to download from this batch of links
        if len(self._download_tasks) == 0:
            self._shutdown_manager()
            return None

        task = self._download_tasks.pop(0)
        # Future's `result()` method will wait for the call to complete, and return
        # the value returned by the call. If the call throws an exception - `result()`
        # will throw the same exception
        file = task.result()
        if (next_row_offset < file.start_row_offset) or (
            next_row_offset > file.start_row_offset + file.row_count
        ):
            logger.debug(
                "ResultFileDownloadManager: file does not contain row {}, start {}, row count {}".format(
                    next_row_offset, file.start_row_offset, file.row_count
                )
            )

        return file

    def _schedule_downloads(self):
        """
        While download queue has a capacity, peek pending links and submit them to thread pool.
        """
        logger.debug("ResultFileDownloadManager: schedule downloads")
        while (len(self._download_tasks) < self._max_download_threads) and (
            len(self._pending_links) > 0
        ):
            chunk_id, link = self._pending_links.pop(0)
            logger.debug(
                "- chunk: {}, start: {}, row count: {}".format(
                    chunk_id, link.startRowOffset, link.rowCount
                )
            )
            handler = ResultSetDownloadHandler(
                settings=self._downloadable_result_settings,
                link=link,
                ssl_options=self._ssl_options,
                chunk_id=chunk_id,
                session_id_hex=self.session_id_hex,
                statement_id=self.statement_id,
                http_client=self._http_client,
            )
            task = self._thread_pool.submit(handler.run)
            self._download_tasks.append(task)

    def add_link(self, link: TSparkArrowResultLink):
        """
        Add more links to the download manager.

        Args:
            link: Link to add
        """

        if link.rowCount <= 0:
            return

        logger.debug(
            "ResultFileDownloadManager: adding file link, start offset {}, row count: {}".format(
                link.startRowOffset, link.rowCount
            )
        )
        self._pending_links.append((self.chunk_id, link))
        self.chunk_id += 1

    def _shutdown_manager(self):
        # Clear download handlers and shutdown the thread pool
        self._pending_links = []
        self._download_tasks = []
        self._thread_pool.shutdown(wait=False)
