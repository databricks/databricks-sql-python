from __future__ import annotations

from abc import ABC
from typing import List, Optional, Tuple

from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
from databricks.sql.utils import ResultSetQueue


class SeaResultSetQueueFactory(ABC):
    @staticmethod
    def build_queue(
        sea_result_data: ResultData,
        manifest: Optional[ResultManifest],
        statement_id: str,
        description: List[Tuple] = [],
        max_download_threads: Optional[int] = None,
        sea_client: Optional[SeaDatabricksClient] = None,
        lz4_compressed: bool = False,
    ) -> ResultSetQueue:
        """
        Factory method to build a result set queue for SEA backend.

        Args:
            sea_result_data (ResultData): Result data from SEA response
            manifest (ResultManifest): Manifest from SEA response
            statement_id (str): Statement ID for the query
            description (List[List[Any]]): Column descriptions
            max_download_threads (int): Maximum number of download threads
            sea_client (SeaDatabricksClient): SEA client for fetching additional links
            lz4_compressed (bool): Whether the data is LZ4 compressed

        Returns:
            ResultSetQueue: The appropriate queue for the result data
        """

        if sea_result_data.data is not None:
            # INLINE disposition with JSON_ARRAY format
            return JsonQueue(sea_result_data.data)
        elif sea_result_data.external_links is not None:
            # EXTERNAL_LINKS disposition
            raise NotImplementedError(
                "EXTERNAL_LINKS disposition is not implemented for SEA backend"
            )
        return JsonQueue([])


class JsonQueue(ResultSetQueue):
    """Queue implementation for JSON_ARRAY format data."""

    def __init__(self, data_array):
        """Initialize with JSON array data."""
        self.data_array = data_array
        self.cur_row_index = 0
        self.num_rows = len(data_array)

    def next_n_rows(self, num_rows):
        """Get the next n rows from the data array."""
        length = min(num_rows, self.num_rows - self.cur_row_index)
        slice = self.data_array[self.cur_row_index : self.cur_row_index + length]
        self.cur_row_index += length
        return slice

    def remaining_rows(self):
        """Get all remaining rows from the data array."""
        slice = self.data_array[self.cur_row_index :]
        self.cur_row_index += len(slice)
        return slice
