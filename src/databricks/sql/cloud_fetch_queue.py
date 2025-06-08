"""
CloudFetchQueue implementations for different backends.

This module contains the base class and implementations for cloud fetch queues
that handle EXTERNAL_LINKS disposition with ARROW format.
"""

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.backend.sea_backend import SeaDatabricksClient
    from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager

from abc import ABC, abstractmethod
import logging
import dateutil.parser
import lz4.frame

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink
from databricks.sql.types import SSLOptions
from databricks.sql.backend.models.base import ExternalLink
from databricks.sql.utils import ResultSetQueue

logger = logging.getLogger(__name__)


def create_arrow_table_from_arrow_file(
    file_bytes: bytes, description
) -> "pyarrow.Table":
    """
    Create an Arrow table from an Arrow file.

    Args:
        file_bytes: The bytes of the Arrow file
        description: The column descriptions

    Returns:
        pyarrow.Table: The Arrow table
    """
    arrow_table = convert_arrow_based_file_to_arrow_table(file_bytes)
    return convert_decimals_in_arrow_table(arrow_table, description)


def convert_arrow_based_file_to_arrow_table(file_bytes: bytes):
    """
    Convert an Arrow file to an Arrow table.

    Args:
        file_bytes: The bytes of the Arrow file

    Returns:
        pyarrow.Table: The Arrow table
    """
    try:
        return pyarrow.ipc.open_stream(file_bytes).read_all()
    except Exception as e:
        raise RuntimeError("Failure to convert arrow based file to arrow table", e)


def convert_decimals_in_arrow_table(table, description) -> "pyarrow.Table":
    """
    Convert decimal columns in an Arrow table to the correct precision and scale.

    Args:
        table: The Arrow table
        description: The column descriptions

    Returns:
        pyarrow.Table: The Arrow table with correct decimal types
    """
    new_columns = []
    new_fields = []

    for i, col in enumerate(table.itercolumns()):
        field = table.field(i)

        if description[i][1] == "decimal":
            precision, scale = description[i][4], description[i][5]
            assert scale is not None
            assert precision is not None
            # create the target decimal type
            dtype = pyarrow.decimal128(precision, scale)

            new_col = col.cast(dtype)
            new_field = field.with_type(dtype)

            new_columns.append(new_col)
            new_fields.append(new_field)
        else:
            new_columns.append(col)
            new_fields.append(field)

    new_schema = pyarrow.schema(new_fields)

    return pyarrow.Table.from_arrays(new_columns, schema=new_schema)


def convert_arrow_based_set_to_arrow_table(arrow_batches, lz4_compressed, schema_bytes):
    """
    Convert a set of Arrow batches to an Arrow table.

    Args:
        arrow_batches: The Arrow batches
        lz4_compressed: Whether the batches are LZ4 compressed
        schema_bytes: The schema bytes

    Returns:
        Tuple[pyarrow.Table, int]: The Arrow table and the number of rows
    """
    ba = bytearray()
    ba += schema_bytes
    n_rows = 0
    for arrow_batch in arrow_batches:
        n_rows += arrow_batch.rowCount
        ba += (
            lz4.frame.decompress(arrow_batch.batch)
            if lz4_compressed
            else arrow_batch.batch
        )
    arrow_table = pyarrow.ipc.open_stream(ba).read_all()
    return arrow_table, n_rows


class CloudFetchQueue(ResultSetQueue):
    """Base class for cloud fetch queues that handle EXTERNAL_LINKS disposition with ARROW format."""

    def __init__(
        self,
        schema_bytes: bytes,
        max_download_threads: int,
        ssl_options: SSLOptions,
        lz4_compressed: bool = True,
        description: Optional[List[List[Any]]] = None,
    ):
        """
        Initialize the base CloudFetchQueue.

        Args:
            schema_bytes: Arrow schema bytes
            max_download_threads: Maximum number of download threads
            ssl_options: SSL options for downloads
            lz4_compressed: Whether the data is LZ4 compressed
            description: Column descriptions
        """
        self.schema_bytes = schema_bytes
        self.lz4_compressed = lz4_compressed
        self.description = description
        self._ssl_options = ssl_options
        self.max_download_threads = max_download_threads

        # Table state
        self.table = None
        self.table_row_index = 0

        # Initialize download manager - subclasses must set this
        self.download_manager: Optional[ResultFileDownloadManager] = None

    def next_n_rows(self, num_rows: int) -> "pyarrow.Table":
        """Get up to the next n rows of the cloud fetch Arrow dataframes."""
        if not self.table:
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()

        results = pyarrow.Table.from_pydict({})  # Empty table
        while num_rows > 0 and self.table:
            # Get remaining of num_rows or the rest of the current table, whichever is smaller
            length = min(num_rows, self.table.num_rows - self.table_row_index)
            table_slice = self.table.slice(self.table_row_index, length)

            # Concatenate results if we have any
            if results.num_rows > 0:
                results = pyarrow.concat_tables([results, table_slice])
            else:
                results = table_slice

            self.table_row_index += table_slice.num_rows

            # Replace current table with the next table if we are at the end of the current table
            if self.table_row_index == self.table.num_rows:
                self.table = self._create_next_table()
                self.table_row_index = 0

            num_rows -= table_slice.num_rows

        return results

    @abstractmethod
    def remaining_rows(self) -> "pyarrow.Table":
        """Get all remaining rows of the cloud fetch Arrow dataframes."""
        pass

    @abstractmethod
    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        pass

    def _create_empty_table(self) -> "pyarrow.Table":
        """Create a 0-row table with just the schema bytes."""
        return create_arrow_table_from_arrow_file(self.schema_bytes, self.description)


class SeaCloudFetchQueue(CloudFetchQueue):
    """Queue implementation for EXTERNAL_LINKS disposition with ARROW format for SEA backend."""

    def __init__(
        self,
        initial_links: List["ExternalLink"],
        schema_bytes: bytes,
        max_download_threads: int,
        ssl_options: SSLOptions,
        sea_client: "SeaDatabricksClient",
        statement_id: str,
        total_chunk_count: int,
        lz4_compressed: bool = False,
        description: Optional[List[List[Any]]] = None,
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
            schema_bytes=schema_bytes,
            max_download_threads=max_download_threads,
            ssl_options=ssl_options,
            lz4_compressed=lz4_compressed,
            description=description,
        )

        self._sea_client = sea_client
        self._statement_id = statement_id
        self._total_chunk_count = total_chunk_count

        # Track which links we've already fetched
        self._fetched_chunk_indices = set()
        for link in initial_links:
            self._fetched_chunk_indices.add(link.chunk_index)

        # Create a mapping from chunk index to link
        self._chunk_index_to_link = {link.chunk_index: link for link in initial_links}

        # Initialize download manager
        self.download_manager = ResultFileDownloadManager(
            links=self._convert_to_thrift_links(initial_links),
            max_download_threads=max_download_threads,
            lz4_compressed=self.lz4_compressed,
            ssl_options=self._ssl_options,
        )

        # Initialize table and position
        self.table = self._create_next_table()

    def _convert_to_thrift_links(
        self, links: List["ExternalLink"]
    ) -> List[TSparkArrowResultLink]:
        """Convert SEA external links to Thrift format for compatibility with existing download manager."""
        thrift_links = []
        for link in links:
            # Parse the ISO format expiration time
            expiry_time = int(dateutil.parser.parse(link.expiration).timestamp())

            thrift_link = TSparkArrowResultLink(
                fileLink=link.external_link,
                expiryTime=expiry_time,
                rowCount=link.row_count,
                bytesNum=link.byte_count,
                startRowOffset=link.row_offset,
                httpHeaders=link.http_headers or {},
            )
            thrift_links.append(thrift_link)
        return thrift_links

    def _fetch_links_for_chunk(self, chunk_index: int) -> List["ExternalLink"]:
        """Fetch links for the specified chunk index."""
        if chunk_index in self._fetched_chunk_indices:
            return [self._chunk_index_to_link[chunk_index]]

        # Find the link that has this chunk_index as its next_chunk_index
        next_chunk_link = None
        next_chunk_internal_link = None
        for link in self._chunk_index_to_link.values():
            if link.next_chunk_index == chunk_index:
                next_chunk_link = link
                next_chunk_internal_link = link.next_chunk_internal_link
                break

        if not next_chunk_internal_link:
            # If we can't find a link with next_chunk_index, we can't fetch the chunk
            logger.warning(
                f"Cannot find next_chunk_internal_link for chunk {chunk_index}"
            )
            return []

        logger.info(f"Fetching chunk {chunk_index} using SEA client")

        # Use the SEA client to fetch the chunk links
        links = self._sea_client.fetch_chunk_links(self._statement_id, chunk_index)

        # Update our tracking
        for link in links:
            self._fetched_chunk_indices.add(link.chunk_index)
            self._chunk_index_to_link[link.chunk_index] = link

            # Log link details
            logger.info(
                f"Link details: chunk_index={link.chunk_index}, row_offset={link.row_offset}, row_count={link.row_count}, next_chunk_index={link.next_chunk_index}"
            )

        # Add to download manager
        if self.download_manager:
            self.download_manager.add_links(self._convert_to_thrift_links(links))

        return links

    def remaining_rows(self) -> "pyarrow.Table":
        """Get all remaining rows of the cloud fetch Arrow dataframes."""
        if not self.table:
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()

        results = pyarrow.Table.from_pydict({})  # Empty table

        # First, fetch the current table's remaining rows
        if self.table_row_index < self.table.num_rows:
            table_slice = self.table.slice(
                self.table_row_index, self.table.num_rows - self.table_row_index
            )
            results = table_slice
            self.table_row_index += table_slice.num_rows

        # Now, try to fetch all remaining chunks
        for chunk_index in range(self._total_chunk_count):
            if chunk_index not in self._fetched_chunk_indices:
                try:
                    # Try to fetch this chunk
                    self._fetch_links_for_chunk(chunk_index)
                except Exception as e:
                    logger.error(f"Error fetching chunk {chunk_index}: {e}")
                    continue

                # If we successfully fetched the chunk, get its data
                if chunk_index in self._fetched_chunk_indices:
                    link = self._chunk_index_to_link[chunk_index]
                    downloaded_file = self.download_manager.get_next_downloaded_file(
                        link.row_offset
                    )
                    if downloaded_file:
                        arrow_table = create_arrow_table_from_arrow_file(
                            downloaded_file.file_bytes, self.description
                        )

                        # Ensure the table has the correct number of rows
                        if arrow_table.num_rows > downloaded_file.row_count:
                            arrow_table = arrow_table.slice(
                                0, downloaded_file.row_count
                            )

                        # Concatenate with results
                        if results.num_rows > 0:
                            results = pyarrow.concat_tables([results, arrow_table])
                        else:
                            results = arrow_table

        self.table = None  # We've fetched everything, so clear the current table
        self.table_row_index = 0

        return results

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        # Get the next chunk index based on current state
        next_chunk_index = 0
        if self.table is not None:
            # Find the current chunk we're processing
            current_chunk = None
            for chunk_index, link in self._chunk_index_to_link.items():
                # We're looking for the chunk that contains our current position
                if (
                    link.row_offset
                    <= self.table_row_index
                    < link.row_offset + link.row_count
                ):
                    current_chunk = link
                    break

            if current_chunk and current_chunk.next_chunk_index is not None:
                next_chunk_index = current_chunk.next_chunk_index
                logger.info(
                    f"Found next_chunk_index {next_chunk_index} from current chunk {current_chunk.chunk_index}"
                )
            else:
                # If we can't find the next chunk, try to fetch the next sequential one
                next_chunk_index = (
                    max(self._fetched_chunk_indices) + 1
                    if self._fetched_chunk_indices
                    else 0
                )
                logger.info(f"Using sequential next_chunk_index {next_chunk_index}")

        # Check if we've reached the end of all chunks
        if next_chunk_index >= self._total_chunk_count:
            logger.info(
                f"Reached end of chunks: next_chunk_index {next_chunk_index} >= total_chunk_count {self._total_chunk_count}"
            )
            return None

        # Check if we need to fetch links for this chunk
        if next_chunk_index not in self._fetched_chunk_indices:
            try:
                logger.info(f"Fetching links for chunk {next_chunk_index}")
                self._fetch_links_for_chunk(next_chunk_index)
            except Exception as e:
                logger.error(f"Error fetching links for chunk {next_chunk_index}: {e}")
                # If we can't fetch the next chunk, try to return what we have
                return None
        else:
            logger.info(f"Already have links for chunk {next_chunk_index}")

        # Find the next downloaded file
        link = self._chunk_index_to_link.get(next_chunk_index)
        if not link:
            logger.error(f"No link found for chunk {next_chunk_index}")
            return None

        row_offset = link.row_offset
        logger.info(
            f"Getting downloaded file for chunk {next_chunk_index} with row_offset {row_offset}"
        )
        if not self.download_manager:
            logger.error(f"No download manager available")
            return None

        downloaded_file = self.download_manager.get_next_downloaded_file(row_offset)
        if not downloaded_file:
            logger.error(
                f"No downloaded file found for chunk {next_chunk_index} with row_offset {row_offset}"
            )
            return None

        arrow_table = create_arrow_table_from_arrow_file(
            downloaded_file.file_bytes, self.description
        )

        # Ensure the table has the correct number of rows
        if arrow_table.num_rows > downloaded_file.row_count:
            arrow_table = arrow_table.slice(0, downloaded_file.row_count)

        logger.info(
            f"Created arrow table for chunk {next_chunk_index} with {arrow_table.num_rows} rows"
        )
        return arrow_table


class ThriftCloudFetchQueue(CloudFetchQueue):
    """Queue implementation for EXTERNAL_LINKS disposition with ARROW format for Thrift backend."""

    def __init__(
        self,
        schema_bytes,
        max_download_threads: int,
        ssl_options: SSLOptions,
        start_row_offset: int = 0,
        result_links: Optional[List[TSparkArrowResultLink]] = None,
        lz4_compressed: bool = True,
        description: Optional[List[List[Any]]] = None,
    ):
        """
        Initialize the Thrift CloudFetchQueue.

        Args:
            schema_bytes: Table schema in bytes
            max_download_threads: Maximum number of downloader thread pool threads
            ssl_options: SSL options for downloads
            start_row_offset: The offset of the first row of the cloud fetch links
            result_links: Links containing the downloadable URL and metadata
            lz4_compressed: Whether the files are lz4 compressed
            description: Hive table schema description
        """
        super().__init__(
            schema_bytes=schema_bytes,
            max_download_threads=max_download_threads,
            ssl_options=ssl_options,
            lz4_compressed=lz4_compressed,
            description=description,
        )

        self.start_row_index = start_row_offset
        self.result_links = result_links or []

        logger.debug(
            "Initialize CloudFetch loader, row set start offset: {}, file list:".format(
                start_row_offset
            )
        )
        if self.result_links:
            for result_link in self.result_links:
                logger.debug(
                    "- start row offset: {}, row count: {}".format(
                        result_link.startRowOffset, result_link.rowCount
                    )
                )

        # Initialize download manager
        self.download_manager = ResultFileDownloadManager(
            links=self.result_links,
            max_download_threads=self.max_download_threads,
            lz4_compressed=self.lz4_compressed,
            ssl_options=self._ssl_options,
        )

        # Initialize table and position
        self.table = self._create_next_table()

    def remaining_rows(self) -> "pyarrow.Table":
        """
        Get all remaining rows of the cloud fetch Arrow dataframes.

        Returns:
            pyarrow.Table
        """
        if not self.table:
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()

        results = pyarrow.Table.from_pydict({})  # Empty table
        while self.table:
            table_slice = self.table.slice(
                self.table_row_index, self.table.num_rows - self.table_row_index
            )
            if results.num_rows > 0:
                results = pyarrow.concat_tables([results, table_slice])
            else:
                results = table_slice

            self.table_row_index += table_slice.num_rows
            self.table = self._create_next_table()
            self.table_row_index = 0

        return results

    def _create_next_table(self) -> Union["pyarrow.Table", None]:
        """Create next table by retrieving the logical next downloaded file."""
        logger.debug(
            "ThriftCloudFetchQueue: Trying to get downloaded file for row {}".format(
                self.start_row_index
            )
        )
        # Create next table by retrieving the logical next downloaded file, or return None to signal end of queue
        if not self.download_manager:
            logger.debug("ThriftCloudFetchQueue: No download manager available")
            return None

        downloaded_file = self.download_manager.get_next_downloaded_file(
            self.start_row_index
        )
        if not downloaded_file:
            logger.debug(
                "ThriftCloudFetchQueue: Cannot find downloaded file for row {}".format(
                    self.start_row_index
                )
            )
            # None signals no more Arrow tables can be built from the remaining handlers if any remain
            return None

        arrow_table = create_arrow_table_from_arrow_file(
            downloaded_file.file_bytes, self.description
        )

        # The server rarely prepares the exact number of rows requested by the client in cloud fetch.
        # Subsequently, we drop the extraneous rows in the last file if more rows are retrieved than requested
        if arrow_table.num_rows > downloaded_file.row_count:
            arrow_table = arrow_table.slice(0, downloaded_file.row_count)

        # At this point, whether the file has extraneous rows or not, the arrow table should have the correct num rows
        assert downloaded_file.row_count == arrow_table.num_rows
        self.start_row_index += arrow_table.num_rows

        logger.debug(
            "ThriftCloudFetchQueue: Found downloaded file, row count: {}, new start offset: {}".format(
                arrow_table.num_rows, self.start_row_index
            )
        )

        return arrow_table
