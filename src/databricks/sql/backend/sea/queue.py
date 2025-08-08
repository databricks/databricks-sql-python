from __future__ import annotations

from abc import ABC
import threading
from typing import Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager
from databricks.sql.telemetry.models.enums import StatementType

from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler

try:
    import pyarrow
except ImportError:
    pyarrow = None

import dateutil

if TYPE_CHECKING:
    from databricks.sql.backend.sea.backend import SeaDatabricksClient
    from databricks.sql.backend.sea.models.base import (
        ExternalLink,
        ResultData,
        ResultManifest,
    )
from databricks.sql.backend.sea.utils.constants import ResultFormat
from databricks.sql.exc import ProgrammingError, ServerOperationError
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink
from databricks.sql.types import SSLOptions
from databricks.sql.utils import (
    ArrowQueue,
    CloudFetchQueue,
    ResultSetQueue,
    create_arrow_table_from_arrow_file,
)

import logging

logger = logging.getLogger(__name__)


class SeaResultSetQueueFactory(ABC):
    @staticmethod
    def build_queue(
        result_data: ResultData,
        manifest: ResultManifest,
        statement_id: str,
        ssl_options: SSLOptions,
        description: List[Tuple],
        max_download_threads: int,
        sea_client: SeaDatabricksClient,
        lz4_compressed: bool,
        http_client,
    ) -> ResultSetQueue:
        """
        Factory method to build a result set queue for SEA backend.

        Args:
            result_data (ResultData): Result data from SEA response
            manifest (ResultManifest): Manifest from SEA response
            statement_id (str): Statement ID for the query
            description (List[List[Any]]): Column descriptions
            max_download_threads (int): Maximum number of download threads
            sea_client (SeaDatabricksClient): SEA client for fetching additional links
            lz4_compressed (bool): Whether the data is LZ4 compressed

        Returns:
            ResultSetQueue: The appropriate queue for the result data
        """

        if manifest.format == ResultFormat.JSON_ARRAY.value:
            # INLINE disposition with JSON_ARRAY format
            return JsonQueue(result_data.data)
        elif manifest.format == ResultFormat.ARROW_STREAM.value:
            if result_data.attachment is not None:
                # direct results from Hybrid disposition
                arrow_file = (
                    ResultSetDownloadHandler._decompress_data(result_data.attachment)
                    if lz4_compressed
                    else result_data.attachment
                )
                arrow_table = create_arrow_table_from_arrow_file(
                    arrow_file, description
                )
                logger.debug(f"Created arrow table with {arrow_table.num_rows} rows")
                return ArrowQueue(arrow_table, manifest.total_row_count)

            # EXTERNAL_LINKS disposition
            return SeaCloudFetchQueue(
                result_data=result_data,
                max_download_threads=max_download_threads,
                ssl_options=ssl_options,
                sea_client=sea_client,
                statement_id=statement_id,
                total_chunk_count=manifest.total_chunk_count,
                lz4_compressed=lz4_compressed,
                description=description,
                http_client=http_client,
            )
        raise ProgrammingError("Invalid result format")


class JsonQueue(ResultSetQueue):
    """Queue implementation for JSON_ARRAY format data."""

    def __init__(self, data_array: Optional[List[List[str]]]):
        """Initialize with JSON array data."""
        self.data_array = data_array or []
        self.cur_row_index = 0
        self.num_rows = len(self.data_array)

    def next_n_rows(self, num_rows: int) -> List[List[str]]:
        """Get the next n rows from the data array."""
        length = min(num_rows, self.num_rows - self.cur_row_index)
        slice = self.data_array[self.cur_row_index : self.cur_row_index + length]
        self.cur_row_index += length
        return slice

    def remaining_rows(self) -> List[List[str]]:
        """Get all remaining rows from the data array."""
        slice = self.data_array[self.cur_row_index :]
        self.cur_row_index += len(slice)
        return slice

    def close(self):
        return


class LinkFetcher:
    """
    Background helper that incrementally retrieves *external links* for a
    result set produced by the SEA backend and feeds them to a
    :class:`databricks.sql.cloudfetch.download_manager.ResultFileDownloadManager`.

    The SEA backend splits large result sets into *chunks*.  Each chunk is
    stored remotely (e.g., in object storage) and exposed via a signed URL
    encapsulated by an :class:`ExternalLink`.  Only the first batch of links is
    returned with the initial query response.  The remaining links must be
    pulled on demand using the *next-chunk* token embedded in each
    :pyattr:`ExternalLink.next_chunk_index`.

    LinkFetcher takes care of this choreography so callers (primarily
    ``SeaCloudFetchQueue``) can simply ask for the link of a specific
    ``chunk_index`` and block until it becomes available.

    Key responsibilities:

    • Maintain an in-memory mapping from ``chunk_index`` → ``ExternalLink``.
    • Launch a background worker thread that continuously requests the next
      batch of links from the backend until all chunks have been discovered or
      an unrecoverable error occurs.
    • Bridge SEA link objects to the Thrift representation expected by the
      existing download manager.
    • Provide a synchronous API (`get_chunk_link`) that blocks until the desired
      link is present in the cache.
    """

    def __init__(
        self,
        download_manager: ResultFileDownloadManager,
        backend: SeaDatabricksClient,
        statement_id: str,
        initial_links: List[ExternalLink],
        total_chunk_count: int,
    ):
        self.download_manager = download_manager
        self.backend = backend
        self._statement_id = statement_id

        self._shutdown_event = threading.Event()

        self._link_data_update = threading.Condition()
        self._error: Optional[Exception] = None
        self.chunk_index_to_link: Dict[int, ExternalLink] = {}

        self._add_links(initial_links)
        self.total_chunk_count = total_chunk_count

        # DEBUG: capture initial state for observability
        logger.debug(
            "LinkFetcher[%s]: initialized with %d initial link(s); expecting %d total chunk(s)",
            statement_id,
            len(initial_links),
            total_chunk_count,
        )

    def _add_links(self, links: List[ExternalLink]):
        """Cache *links* locally and enqueue them with the download manager."""
        logger.debug(
            "LinkFetcher[%s]: caching %d link(s) – chunks %s",
            self._statement_id,
            len(links),
            ", ".join(str(l.chunk_index) for l in links) if links else "<none>",
        )
        for link in links:
            self.chunk_index_to_link[link.chunk_index] = link
            self.download_manager.add_link(LinkFetcher._convert_to_thrift_link(link))

    def _get_next_chunk_index(self) -> Optional[int]:
        """Return the next *chunk_index* that should be requested from the backend, or ``None`` if we have them all."""
        with self._link_data_update:
            max_chunk_index = max(self.chunk_index_to_link.keys(), default=None)
            if max_chunk_index is None:
                return 0
            max_link = self.chunk_index_to_link[max_chunk_index]
            return max_link.next_chunk_index

    def _trigger_next_batch_download(self) -> bool:
        """Fetch the next batch of links from the backend and return *True* on success."""
        logger.debug(
            "LinkFetcher[%s]: requesting next batch of links", self._statement_id
        )
        next_chunk_index = self._get_next_chunk_index()
        if next_chunk_index is None:
            return False

        try:
            links = self.backend.get_chunk_links(self._statement_id, next_chunk_index)
            with self._link_data_update:
                self._add_links(links)
                self._link_data_update.notify_all()
        except Exception as e:
            logger.error(
                f"LinkFetcher: Error fetching links for chunk {next_chunk_index}: {e}"
            )
            with self._link_data_update:
                self._error = e
                self._link_data_update.notify_all()
            return False

        logger.debug(
            "LinkFetcher[%s]: received %d new link(s)",
            self._statement_id,
            len(links),
        )
        return True

    def get_chunk_link(self, chunk_index: int) -> Optional[ExternalLink]:
        """Return (blocking) the :class:`ExternalLink` associated with *chunk_index*."""
        logger.debug(
            "LinkFetcher[%s]: waiting for link of chunk %d",
            self._statement_id,
            chunk_index,
        )
        if chunk_index >= self.total_chunk_count:
            return None

        with self._link_data_update:
            while chunk_index not in self.chunk_index_to_link:
                if self._error:
                    raise self._error
                if self._shutdown_event.is_set():
                    raise ProgrammingError(
                        "LinkFetcher is shutting down without providing link for chunk index {}".format(
                            chunk_index
                        )
                    )
                self._link_data_update.wait()

            return self.chunk_index_to_link[chunk_index]

    @staticmethod
    def _convert_to_thrift_link(link: ExternalLink) -> TSparkArrowResultLink:
        """Convert SEA external links to Thrift format for compatibility with existing download manager."""
        # Parse the ISO format expiration time
        expiry_time = int(dateutil.parser.parse(link.expiration).timestamp())
        return TSparkArrowResultLink(
            fileLink=link.external_link,
            expiryTime=expiry_time,
            rowCount=link.row_count,
            bytesNum=link.byte_count,
            startRowOffset=link.row_offset,
            httpHeaders=link.http_headers or {},
        )

    def _worker_loop(self):
        """Entry point for the background thread."""
        logger.debug("LinkFetcher[%s]: worker thread started", self._statement_id)
        while not self._shutdown_event.is_set():
            links_downloaded = self._trigger_next_batch_download()
            if not links_downloaded:
                self._shutdown_event.set()
        logger.debug("LinkFetcher[%s]: worker thread exiting", self._statement_id)
        with self._link_data_update:
            self._link_data_update.notify_all()

    def start(self):
        """Spawn the worker thread."""
        logger.debug("LinkFetcher[%s]: starting worker thread", self._statement_id)
        self._worker_thread = threading.Thread(
            target=self._worker_loop, name=f"LinkFetcher-{self._statement_id}"
        )
        self._worker_thread.start()

    def stop(self):
        """Signal the worker thread to stop and wait for its termination."""
        logger.debug("LinkFetcher[%s]: stopping worker thread", self._statement_id)
        self._shutdown_event.set()
        self._worker_thread.join()
        logger.debug("LinkFetcher[%s]: worker thread stopped", self._statement_id)


class SeaCloudFetchQueue(CloudFetchQueue):
    """Queue implementation for EXTERNAL_LINKS disposition with ARROW format for SEA backend."""

    def __init__(
        self,
        result_data: ResultData,
        max_download_threads: int,
        ssl_options: SSLOptions,
        sea_client: SeaDatabricksClient,
        statement_id: str,
        total_chunk_count: int,
        http_client,
        lz4_compressed: bool = False,
        description: List[Tuple] = [],
    ):
        """
        Initialize the SEA CloudFetchQueue.

        Args:
            initial_links: Initial list of external links to download
            schema_bytes: Arrow schema bytes
            max_download_threads: Maximum number of download threads
            ssl_options: SSL options for downloads
            sea_client: SEA client for fetching additional links
            statement_id: Statement ID for the query
            total_chunk_count: Total number of chunks in the result set
            lz4_compressed: Whether the data is LZ4 compressed
            description: Column descriptions
        """

        super().__init__(
            max_download_threads=max_download_threads,
            ssl_options=ssl_options,
            statement_id=statement_id,
            schema_bytes=None,
            lz4_compressed=lz4_compressed,
            description=description,
            # TODO: fix these arguments when telemetry is implemented in SEA
            session_id_hex=None,
            chunk_id=0,
            http_client=http_client,
        )

        logger.debug(
            "SeaCloudFetchQueue: Initialize CloudFetch loader for statement {}, total chunks: {}".format(
                statement_id, total_chunk_count
            )
        )

        initial_links = result_data.external_links or []

        # Track the current chunk we're processing
        self._current_chunk_index = 0

        self.link_fetcher = None  # for empty responses, we do not need a link fetcher
        if total_chunk_count > 0:
            self.link_fetcher = LinkFetcher(
                download_manager=self.download_manager,
                backend=sea_client,
                statement_id=statement_id,
                initial_links=initial_links,
                total_chunk_count=total_chunk_count,
            )
            self.link_fetcher.start()

        # Initialize table and position
        self.table = self._create_next_table()

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        if self.link_fetcher is None:
            return None

        chunk_link = self.link_fetcher.get_chunk_link(self._current_chunk_index)
        if chunk_link is None:
            return None

        row_offset = chunk_link.row_offset
        # NOTE: link has already been submitted to download manager at this point
        arrow_table = self._create_table_at_offset(row_offset)

        self._current_chunk_index += 1

        return arrow_table

    def close(self):
        super().close()
        if self.link_fetcher:
            self.link_fetcher.stop()
