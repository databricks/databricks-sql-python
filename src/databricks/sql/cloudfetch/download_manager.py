import logging

from concurrent.futures import ThreadPoolExecutor, Future
import threading
from typing import Callable, List, Optional, Union, Generic, TypeVar, Tuple, Optional

from databricks.sql.cloudfetch.downloader import (
    ResultSetDownloadHandler,
    DownloadableResultSettings,
    DownloadedFile,
)
from databricks.sql.exc import Error
from databricks.sql.types import SSLOptions
from databricks.sql.telemetry.models.event import StatementType
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TaskWithMetadata(Generic[T]):
    """
    Wrapper around Future that stores additional metadata (the link).
    Provides type-safe access to both the Future result and the associated link.
    """

    def __init__(self, future: Future[T], link: TSparkArrowResultLink):
        self.future = future
        self.link = link

    def result(self, timeout: Optional[float] = None) -> T:
        """Get the result of the Future, blocking if necessary."""
        return self.future.result(timeout)

    def cancel(self) -> bool:
        """Cancel the Future if possible."""
        return self.future.cancel()


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
        expiry_callback: Callable[[TSparkArrowResultLink], None],
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

        self._max_download_threads: int = max_download_threads

        self._download_condition = threading.Condition()
        self._download_tasks: List[TaskWithMetadata[DownloadedFile]] = []
        self._thread_pool = ThreadPoolExecutor(max_workers=self._max_download_threads)

        self._downloadable_result_settings = DownloadableResultSettings(lz4_compressed)
        self._ssl_options = ssl_options
        self._expiry_callback = expiry_callback
        self.session_id_hex = session_id_hex
        self.statement_id = statement_id

    def get_next_downloaded_file(self, next_row_offset: int) -> DownloadedFile:
        """
        Get next file that starts at given offset.

        This function gets the next downloaded file in which its rows start at the specified next_row_offset
        in relation to the full result. File downloads are scheduled if not already, and once the correct
        download handler is located, the function waits for the download status and returns the resulting file.

        Args:
            next_row_offset (int): The offset of the starting row of the next file we want data from.
        """

        # Make sure the download queue is always full
        self._schedule_downloads()

        # No more files to download from this batch of links
        while len(self._download_tasks) == 0:
            if self._thread_pool._shutdown:
                raise Error("download manager shut down before file was ready")
            with self._download_condition:
                self._download_condition.wait()

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

    def cancel_tasks_from_offset(self, start_row_offset: int):
        """
        Cancel all download tasks starting from a specific row offset.
        This is used when links expire and we need to restart from a certain point.

        Args:
            start_row_offset (int): Row offset from which to cancel tasks
        """

        def to_cancel(link: TSparkArrowResultLink) -> bool:
            return link.startRowOffset < start_row_offset

        tasks_to_cancel = [
            task for task in self._download_tasks if to_cancel(task.link)
        ]
        for task in tasks_to_cancel:
            task.cancel()
        logger.info(
            f"ResultFileDownloadManager: cancelled {len(tasks_to_cancel)} tasks from offset {start_row_offset}"
        )

        # Remove cancelled tasks from the download queue
        tasks_to_keep = [
            task for task in self._download_tasks if not to_cancel(task.link)
        ]
        self._download_tasks = tasks_to_keep

        pending_links_to_keep = [
            link for link in self._pending_links if not to_cancel(link[1])
        ]
        self._pending_links = pending_links_to_keep
        logger.info(
            f"ResultFileDownloadManager: removed {len(self._pending_links) - len(pending_links_to_keep)} links from pending links"
        )

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
                expiry_callback=self._expiry_callback,
                chunk_id=chunk_id,
                session_id_hex=self.session_id_hex,
                statement_id=self.statement_id,
            )
            future = self._thread_pool.submit(handler.run)
            task = TaskWithMetadata(future, link)
            self._download_tasks.append(task)

        with self._download_condition:
            self._download_condition.notify_all()

    def add_links(self, links: List[TSparkArrowResultLink]):
        """
        Add more links to the download manager.

        Args:
            link (TSparkArrowResultLink): The link to add to the download manager.
        """
        for link in links:
            if link.rowCount <= 0:
                continue
            logger.debug(
                "ResultFileDownloadManager: adding file link, start offset {}, row count: {}".format(
                    link.startRowOffset, link.rowCount
                )
            )
            self._pending_links.append((self.chunk_id, link))
            self.chunk_id += 1

        self._schedule_downloads()

    def _shutdown_manager(self):
        # Clear download handlers and shutdown the thread pool
        self._pending_links = []
        self._download_tasks = []
        self._thread_pool.shutdown(wait=False)
        with self._download_condition:
            self._download_condition.notify_all()
