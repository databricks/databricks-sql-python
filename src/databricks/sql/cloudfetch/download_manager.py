from __future__ import annotations

import logging
import threading

from concurrent.futures import CancelledError, ThreadPoolExecutor, Future
from typing import Callable, List, Union, Tuple, Optional, TYPE_CHECKING

from databricks.sql.exc import OperationalError
from databricks.sql.cloudfetch.downloader import (
    ResultSetDownloadHandler,
    DownloadableResultSettings,
    DownloadedFile,
)
from databricks.sql.types import SSLOptions
from databricks.sql.telemetry.models.event import StatementType

if TYPE_CHECKING:
    # Type-annotation-only import; see the note in downloader.py. Keeping the
    # ``thrift`` package out of this module lets the SEA/kernel backends use the
    # cloud-fetch download manager without importing Apache Thrift.
    from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

logger = logging.getLogger(__name__)

# Called with the number of free download slots; returns (new links, has more rows).
LinkSource = Callable[[int], Tuple[List["TSparkArrowResultLink"], bool]]


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
        self._link_source: Optional[LinkSource] = None
        # Set on shutdown so downloads that have not started (or finished) stop early.
        self._cancelled = threading.Event()
        # Set by cancel(): reads fail once they need data the cancel stopped.
        self._cancel_requested = False
        # A failed link fetch, raised again by every later read (see _schedule_downloads)
        self._link_error: Optional[BaseException] = None

    def cancel(self) -> None:
        """Stop downloading because the statement was cancelled.

        May be called from another thread while the consumer is reading. Files that
        finished downloading are still returned; a read that needs anything else
        raises instead of reporting the end of the result.
        """
        self._cancel_requested = True
        self._cancelled.set()
        self._thread_pool.shutdown(wait=False, cancel_futures=True)

    def set_link_source(self, link_source: LinkSource) -> bool:
        """Fetch further links on demand.

        Whenever a download slot is free and no links are pending, ``link_source`` is
        asked for more, so the next links arrive while earlier files still download.
        Returns False, without attaching it, if the manager is already shut down.
        """
        if self._cancelled.is_set():
            return False
        self._link_source = link_source
        return True

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

        if self._link_error is not None:
            raise OperationalError(
                "Fetching more CloudFetch results failed earlier; the result can't "
                "be read further, execute the statement again"
            ) from self._link_error
        if not self._cancel_requested:
            # Make sure the download queue is always full
            self._schedule_downloads()

        # No more files to download from this batch of links
        if len(self._download_tasks) == 0:
            if self._cancel_requested and (
                self._pending_links or self._link_source is not None
            ):
                # The cancel stopped part of the result: fail rather than end early
                raise OperationalError("CloudFetch download cancelled")
            self._shutdown_manager()
            return None

        task = self._download_tasks.pop(0)
        # Future's `result()` method will wait for the call to complete, and return
        # the value returned by the call. If the call throws an exception - `result()`
        # will throw the same exception
        try:
            file = task.result()
        except CancelledError:
            raise OperationalError("CloudFetch download cancelled") from None
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
        self._submit_pending_links()
        while (
            self._link_source is not None
            and not self._pending_links
            and len(self._download_tasks) < self._max_download_threads
        ):
            scheduled = len(self._download_tasks)
            try:
                links, has_more = self._link_source(
                    self._max_download_threads - scheduled
                )
            except Exception as e:
                # This read is failing part-way, so the caller loses the rows it had
                # collected. Fail every later read too, so a retry cannot skip them,
                # and stop the downloads nobody will read.
                self._link_error = e
                self._shutdown_manager()
                raise
            if self._cancelled.is_set():
                # Shut down while the request was in flight
                return
            if not has_more:
                self._link_source = None
            for link in links:
                self.add_link(link)
            self._submit_pending_links()
            if self._download_tasks and len(self._download_tasks) == scheduled:
                # Nothing new to download (no links, or only empty ones): ask again
                # on the next call instead of polling the server now
                break

    def _submit_pending_links(self):
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
                cancel_event=self._cancelled,
            )
            try:
                task = self._thread_pool.submit(handler.run)
            except RuntimeError:
                # cancel() shut the pool down from another thread; keep the link so
                # the result still counts as incomplete
                if self._cancel_requested:
                    self._pending_links.insert(0, (chunk_id, link))
                    return
                raise
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
        self._link_source = None
        self._pending_links = []
        self._download_tasks = []
        self._cancelled.set()
        self._thread_pool.shutdown(wait=False, cancel_futures=True)
