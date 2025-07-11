from __future__ import annotations

from abc import ABC
import threading
from typing import Dict, List, Optional, Tuple, Union

from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager

from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler

try:
    import pyarrow
except ImportError:
    pyarrow = None

import dateutil

from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.sea.models.base import (
    ExternalLink,
    ResultData,
    ResultManifest,
)
from databricks.sql.backend.sea.utils.constants import ResultFormat
from databricks.sql.exc import ProgrammingError, Error
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
                initial_links=result_data.external_links or [],
                max_download_threads=max_download_threads,
                ssl_options=ssl_options,
                sea_client=sea_client,
                statement_id=statement_id,
                total_chunk_count=manifest.total_chunk_count,
                lz4_compressed=lz4_compressed,
                description=description,
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


class LinkFetcher:
    def __init__(
        self,
        download_manager: ResultFileDownloadManager,
        backend: "SeaDatabricksClient",
        statement_id: str,
        initial_links: List["ExternalLink"],
        total_chunk_count: int,
    ):
        self.download_manager = download_manager
        self.backend = backend
        self._statement_id = statement_id

        self._shutdown_event = threading.Event()

        self._link_data_update = threading.Condition()
        self._error: Optional[Exception] = None
        self.chunk_index_to_link: Dict[int, "ExternalLink"] = {}

        # Add initial links (no notification needed during init)
        self._add_links_to_manager(initial_links, notify=False)
        self.total_chunk_count = total_chunk_count
        self._worker_thread: Optional[threading.Thread] = None

    def _add_links_to_manager(self, links: List["ExternalLink"], notify: bool = True):
        """
        Add external links to both chunk mapping and download manager.
        
        Args:
            links: List of external links to add
            notify: Whether to notify waiting threads (default True)
        """
        for link in links:
            self.chunk_index_to_link[link.chunk_index] = link
            self.download_manager.add_link(self._convert_to_thrift_link(link))
        
        if notify:
            self._link_data_update.notify_all()

    def _clear_chunks_from_index(self, start_chunk_index: int):
        """
        Clear all chunks >= start_chunk_index from the chunk mapping.
        
        Args:
            start_chunk_index: The chunk index to start clearing from (inclusive)
        """
        chunks_to_remove = [
            chunk_idx for chunk_idx in self.chunk_index_to_link.keys() 
            if chunk_idx >= start_chunk_index
        ]
        
        logger.debug(f"LinkFetcher: Clearing chunks {chunks_to_remove} from index {start_chunk_index}")
        for chunk_idx in chunks_to_remove:
            del self.chunk_index_to_link[chunk_idx]

    def _fetch_and_add_links(self, chunk_index: int) -> List["ExternalLink"]:
        """
        Fetch links from backend and add them to manager.
        
        Args:
            chunk_index: The chunk index to fetch
            
        Returns:
            List of fetched external links
            
        Raises:
            Exception: If fetching fails
        """
        logger.debug(f"LinkFetcher: Fetching links for chunk {chunk_index}")
        
        try:
            links = self.backend.get_chunk_links(self._statement_id, chunk_index)
            self._add_links_to_manager(links, notify=True)
            logger.debug(f"LinkFetcher: Added {len(links)} links starting from chunk {chunk_index}")
            return links
            
        except Exception as e:
            logger.error(f"LinkFetcher: Failed to fetch chunk {chunk_index}: {e}")
            self._error = e
            self._link_data_update.notify_all()
            raise e

    def _get_next_chunk_index(self) -> Optional[int]:
        with self._link_data_update:
            max_chunk_index = max(self.chunk_index_to_link.keys(), default=None)
            if max_chunk_index is None:
                return 0
            max_link = self.chunk_index_to_link[max_chunk_index]
            return max_link.next_chunk_index

    def _trigger_next_batch_download(self) -> bool:
        next_chunk_index = self._get_next_chunk_index()
        if next_chunk_index is None:
            return False

        with self._link_data_update:
            try:
                self._fetch_and_add_links(next_chunk_index)
                return True
            except Exception:
                # Error already logged and set by _fetch_and_add_links
                return False

    def get_chunk_link(self, chunk_index: int) -> Optional["ExternalLink"]:
        if chunk_index >= self.total_chunk_count:
            return None

        with self._link_data_update:
            while chunk_index not in self.chunk_index_to_link:
                if self._error:
                    raise self._error
                self._link_data_update.wait()

            return self.chunk_index_to_link.get(chunk_index, None)

    def restart_from_chunk(self, chunk_index: int):
        """
        Restart the LinkFetcher from a specific chunk index.
        
        This method handles both cases:
        1. LinkFetcher is done/closed but we need to restart it
        2. LinkFetcher is active but we need it to start from the expired chunk
        
        The key insight: we need to clear all chunks >= restart_chunk_index
        so that _get_next_chunk_index() returns the correct next chunk.
        
        Args:
            chunk_index: The chunk index to restart from
        """
        logger.debug(f"LinkFetcher: Restarting from chunk {chunk_index}")
        
        # Stop the current worker if running
        self.stop()
        
        with self._link_data_update:
            # Clear error state
            self._error = None
            
            # 🔥 CRITICAL: Clear all chunks >= restart_chunk_index
            # This ensures _get_next_chunk_index() works correctly
            self._clear_chunks_from_index(chunk_index)
            
            # Now fetch the restart chunk (and potentially its batch)
            # This becomes our new "max chunk" and starting point
            try:
                self._fetch_and_add_links(chunk_index)
            except Exception as e:
                # Error already logged and set by _fetch_and_add_links
                raise e
        
        # Start the worker again - now _get_next_chunk_index() will work correctly
        self.start()
        logger.debug(f"LinkFetcher: Successfully restarted from chunk {chunk_index}")

    def _convert_to_thrift_link(self, link: "ExternalLink") -> TSparkArrowResultLink:
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
        while not self._shutdown_event.is_set():
            links_downloaded = self._trigger_next_batch_download()
            if not links_downloaded:
                break

    def start(self):
        if self._worker_thread and self._worker_thread.is_alive():
            return  # Already running
        
        self._shutdown_event.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop)
        self._worker_thread.start()

    def stop(self):
        if self._worker_thread and self._worker_thread.is_alive():
            self._shutdown_event.set()
            self._worker_thread.join()


class SeaCloudFetchQueue(CloudFetchQueue):
    """Queue implementation for EXTERNAL_LINKS disposition with ARROW format for SEA backend."""

    def __init__(
        self,
        initial_links: List["ExternalLink"],
        max_download_threads: int,
        ssl_options: SSLOptions,
        sea_client: "SeaDatabricksClient",
        statement_id: str,
        total_chunk_count: int,
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
            schema_bytes=None,
            lz4_compressed=lz4_compressed,
            description=description,
        )

        self._sea_client = sea_client
        self._statement_id = statement_id

        logger.debug(
            "SeaCloudFetchQueue: Initialize CloudFetch loader for statement {}, total chunks: {}".format(
                statement_id, total_chunk_count
            )
        )

        if total_chunk_count < 1:
            return

        self.current_chunk_index = 0

        self.download_manager = ResultFileDownloadManager(
            links=[],
            max_download_threads=max_download_threads,
            lz4_compressed=lz4_compressed,
            ssl_options=ssl_options,
            expired_link_callback=self._handle_expired_link,
        )

        self.link_fetcher = LinkFetcher(
            download_manager=self.download_manager,
            backend=self._sea_client,
            statement_id=self._statement_id,
            initial_links=initial_links,
            total_chunk_count=total_chunk_count,
        )
        self.link_fetcher.start()

        # Initialize table and position
        self.table = self._create_next_table()

    def _handle_expired_link(self, expired_link: TSparkArrowResultLink) -> TSparkArrowResultLink:
        """
        Handle expired link for SEA backend.
        
        For SEA backend, we can handle expired links robustly by:
        1. Cancelling all pending downloads
        2. Finding the chunk index for the expired link
        3. Restarting the LinkFetcher from that chunk
        4. Returning the requested link
        
        Args:
            expired_link: The expired link
            
        Returns:
            A new link with the same row offset
            
        Raises:
            Error: If unable to fetch new link
        """
        logger.warning(
            "SeaCloudFetchQueue: Link expired for offset {}, row count {}. Attempting to fetch new links.".format(
                expired_link.startRowOffset, expired_link.rowCount
            )
        )
        
        try:
            # Step 1: Cancel all pending downloads
            self.download_manager.cancel_all_downloads()
            logger.debug("SeaCloudFetchQueue: Cancelled all pending downloads")
            
            # Step 2: Find which chunk contains the expired link
            target_chunk_index = self._find_chunk_index_for_row_offset(expired_link.startRowOffset)
            if target_chunk_index is None:
                # If we can't find the chunk, we may need to search more broadly
                # For now, let's assume it's a reasonable chunk based on the row offset
                # This is a fallback - in practice this should be rare
                logger.warning(
                    "SeaCloudFetchQueue: Could not find chunk index for row offset {}, using fallback approach".format(
                        expired_link.startRowOffset
                    )
                )
                # Try to estimate chunk index - this is a heuristic
                target_chunk_index = 0  # Start from beginning as fallback
            
            # Step 3: Restart LinkFetcher from the target chunk
            # This handles both stopped and active LinkFetcher cases
            self.link_fetcher.restart_from_chunk(target_chunk_index)
            
            # Step 4: Find and return the link that matches the expired link's row offset
            # After restart, the chunk should be available
            for chunk_index, external_link in self.link_fetcher.chunk_index_to_link.items():
                if external_link.row_offset == expired_link.startRowOffset:
                    new_thrift_link = self.link_fetcher._convert_to_thrift_link(external_link)
                    logger.debug(
                        "SeaCloudFetchQueue: Found replacement link for offset {}, row count {}".format(
                            new_thrift_link.startRowOffset, new_thrift_link.rowCount
                        )
                    )
                    return new_thrift_link
            
            # If we still can't find it, raise an error
            logger.error(
                "SeaCloudFetchQueue: Could not find replacement link for row offset {} after restart".format(
                    expired_link.startRowOffset
                )
            )
            raise Error(f"CloudFetch link has expired and could not be renewed for offset {expired_link.startRowOffset}")
            
        except Exception as e:
            logger.error(
                "SeaCloudFetchQueue: Error handling expired link: {}".format(str(e))
            )
            if isinstance(e, Error):
                raise e
            else:
                raise Error(f"CloudFetch link has expired and renewal failed: {str(e)}")

    def _find_chunk_index_for_row_offset(self, row_offset: int) -> Optional[int]:
        """
        Find the chunk index that contains the given row offset.
        
        Args:
            row_offset: The row offset to find
            
        Returns:
            The chunk index, or None if not found
        """
        # Search through our known chunks to find the one containing this row offset
        for chunk_index, external_link in self.link_fetcher.chunk_index_to_link.items():
            if external_link.row_offset == row_offset:
                return chunk_index
        
        # If not found in known chunks, return None and let the caller handle it
        return None

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        if not self.download_manager:
            logger.debug("SeaCloudFetchQueue: No download manager, returning")
            return None

        chunk_link = self.link_fetcher.get_chunk_link(self.current_chunk_index)
        if not chunk_link:
            return None

        row_offset = chunk_link.row_offset
        arrow_table = self._create_table_at_offset(row_offset)

        self.current_chunk_index += 1

        return arrow_table
