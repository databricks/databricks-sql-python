from __future__ import annotations

from abc import ABC
from typing import List, Optional, Tuple, Union, TYPE_CHECKING

from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager
from databricks.sql.telemetry.models.enums import StatementType

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
from databricks.sql.utils import CloudFetchQueue, ResultSetQueue

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
            chunk_id=0,
            schema_bytes=None,
            lz4_compressed=lz4_compressed,
            description=description,
            session_id_hex=None,  # TODO: fix this argument when telemetry is implemented in SEA
        )

        self._sea_client = sea_client
        self._statement_id = statement_id
        self._total_chunk_count = total_chunk_count

        logger.debug(
            "SeaCloudFetchQueue: Initialize CloudFetch loader for statement {}, total chunks: {}".format(
                statement_id, total_chunk_count
            )
        )

        initial_links = result_data.external_links or []
        first_link = next((l for l in initial_links if l.chunk_index == 0), None)
        if not first_link:
            # possibly an empty response
            return None

        # Track the current chunk we're processing
        self._current_chunk_index = 0
        # Initialize table and position
        self.table = self._create_table_from_link(first_link)

    def _convert_to_thrift_link(self, link: ExternalLink) -> TSparkArrowResultLink:
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

    def _get_chunk_link(self, chunk_index: int) -> Optional[ExternalLink]:
        """Progress to the next chunk link."""
        if chunk_index >= self._total_chunk_count:
            return None

        try:
            return self._sea_client.get_chunk_link(self._statement_id, chunk_index)
        except Exception as e:
            raise ServerOperationError(
                f"Error fetching link for chunk {chunk_index}: {e}",
                {
                    "operation-id": self._statement_id,
                    "diagnostic-info": None,
                },
            )

    def _create_table_from_link(
        self, link: ExternalLink
    ) -> Union["pyarrow.Table", None]:
        """Create a table from a link."""

        thrift_link = self._convert_to_thrift_link(link)
        self.download_manager.add_link(thrift_link)

        row_offset = link.row_offset
        arrow_table = self._create_table_at_offset(row_offset)

        return arrow_table

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        self._current_chunk_index += 1
        next_chunk_link = self._get_chunk_link(self._current_chunk_index)
        if not next_chunk_link:
            return None
        return self._create_table_from_link(next_chunk_link)
