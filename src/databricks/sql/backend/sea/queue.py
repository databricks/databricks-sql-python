from __future__ import annotations

from abc import ABC
import threading
from typing import Dict, List, Optional, Tuple, Union

from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager

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
from databricks.sql.exc import ProgrammingError
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink
from databricks.sql.types import SSLOptions
from databricks.sql.utils import CloudFetchQueue, ResultSetQueue

import logging

logger = logging.getLogger(__name__)


class SeaResultSetQueueFactory(ABC):
    @staticmethod
    def build_queue(
        result_data: ResultData,
        manifest: ResultManifest,
        statement_id: str,
        ssl_options: Optional[SSLOptions] = None,
        description: List[Tuple] = [],
        max_download_threads: Optional[int] = None,
        sea_client: Optional[SeaDatabricksClient] = None,
        lz4_compressed: bool = False,
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
            # EXTERNAL_LINKS disposition
            if not max_download_threads:
                raise ValueError(
                    "Max download threads is required for EXTERNAL_LINKS disposition"
                )
            if not ssl_options:
                raise ValueError(
                    "SSL options are required for EXTERNAL_LINKS disposition"
                )
            if not sea_client:
                raise ValueError(
                    "SEA client is required for EXTERNAL_LINKS disposition"
                )

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
        current_chunk_link: Optional["ExternalLink"] = None,
    ):
        self.download_manager = download_manager
        self.backend = backend
        self._statement_id = statement_id
        self._current_chunk_link = current_chunk_link

        self._shutdown_event = threading.Event()

        self._map_lock = threading.Lock()
        self.chunk_index_to_link: Dict[int, "ExternalLink"] = {}

    def _set_current_chunk_link(self, link: "ExternalLink"):
        with self._map_lock:
            self.chunk_index_to_link[link.chunk_index] = link

    def get_chunk_link(self, chunk_index: int) -> Optional["ExternalLink"]:
        with self._map_lock:
            return self.chunk_index_to_link.get(chunk_index, None)

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

    def _progress_chunk_link(self):
        """Progress to the next chunk link."""
        if not self._current_chunk_link:
            return None

        next_chunk_index = self._current_chunk_link.next_chunk_index

        if next_chunk_index is None:
            self._current_chunk_link = None
            return None

        try:
            self._current_chunk_link = self.backend.get_chunk_link(
                self._statement_id, next_chunk_index
            )
        except Exception as e:
            logger.error(
                "LinkFetcher: Error fetching link for chunk {}: {}".format(
                    next_chunk_index, e
                )
            )
            self._current_chunk_link = None

    def _worker_loop(self):
        while not (self._shutdown_event.is_set() or self._current_chunk_link is None):
            self._set_current_chunk_link(self._current_chunk_link)
            self.download_manager.add_link(
                self._convert_to_thrift_link(self._current_chunk_link)
            )

            self._progress_chunk_link()

    def start(self):
        self._worker_thread = threading.Thread(target=self._worker_loop)
        self._worker_thread.start()

    def stop(self):
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

        initial_link = next((l for l in initial_links if l.chunk_index == 0), None)
        if not initial_link:
            return
        self.current_chunk_index = initial_link.chunk_index

        self.download_manager = ResultFileDownloadManager(
            links=[],
            max_download_threads=max_download_threads,
            lz4_compressed=lz4_compressed,
            ssl_options=ssl_options,
        )

        self.link_fetcher = LinkFetcher(
            self.download_manager, self._sea_client, statement_id, initial_link
        )
        self.link_fetcher.start()

        # Initialize table and position
        self.table = self._create_next_table()

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        current_chunk_link = self.link_fetcher.get_chunk_link(self.current_chunk_index)
        if not current_chunk_link:
            return None

        row_offset = current_chunk_link.row_offset
        arrow_table = self._create_table_at_offset(row_offset)

        self.current_chunk_index = current_chunk_link.next_chunk_index

        return arrow_table
