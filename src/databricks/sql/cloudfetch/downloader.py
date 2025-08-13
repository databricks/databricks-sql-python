import logging
from dataclasses import dataclass
from typing import Optional

import lz4.frame
import time
from databricks.sql.common.http import HttpMethod
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink
from databricks.sql.exc import Error
from databricks.sql.types import SSLOptions
from databricks.sql.telemetry.latency_logger import log_latency
from databricks.sql.telemetry.models.event import StatementType
from databricks.sql.common.unified_http_client import UnifiedHttpClient

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


@dataclass
class DownloadableResultSettings:
    """
    Class for settings common to each download handler.

    Attributes:
        is_lz4_compressed (bool): Whether file is expected to be lz4 compressed.
        link_expiry_buffer_secs (int): Time in seconds to prevent download of a link before it expires. Default 0 secs.
        download_timeout (int): Timeout for download requests. Default 60 secs.
        max_consecutive_file_download_retries (int): Number of consecutive download retries before shutting down.
        min_cloudfetch_download_speed (float): Threshold in MB/s below which to log warning. Default 0.1 MB/s.
    """

    is_lz4_compressed: bool
    link_expiry_buffer_secs: int = 0
    download_timeout: int = 60
    max_consecutive_file_download_retries: int = 0
    min_cloudfetch_download_speed: float = 0.1


class ResultSetDownloadHandler:
    def __init__(
        self,
        settings: DownloadableResultSettings,
        link: TSparkArrowResultLink,
        ssl_options: SSLOptions,
        chunk_id: int,
        session_id_hex: Optional[str],
        statement_id: str,
        http_client,
    ):
        self.settings = settings
        self.link = link
        self._ssl_options = ssl_options
        self._http_client = http_client
        self.chunk_id = chunk_id
        self.session_id_hex = session_id_hex
        self.statement_id = statement_id

    @log_latency(StatementType.QUERY)
    def run(self) -> DownloadedFile:
        """
        Download the file described in the cloud fetch link.

        This function checks if the link has or is expiring, gets the file via a requests session, decompresses the
        file, and signals to waiting threads that the download is finished and whether it was successful.
        """

        logger.debug(
            "ResultSetDownloadHandler: starting file download, chunk id %s, offset %s, row count %s",
            self.chunk_id,
            self.link.startRowOffset,
            self.link.rowCount,
        )

        # Check if link is already expired or is expiring
        ResultSetDownloadHandler._validate_link(
            self.link, self.settings.link_expiry_buffer_secs
        )

        start_time = time.time()

        with self._http_client.request_context(
            method=HttpMethod.GET,
            url=self.link.fileLink,
            timeout=self.settings.download_timeout,
            headers=self.link.httpHeaders,
        ) as response:
            if response.status >= 400:
                raise Exception(f"HTTP {response.status}: {response.data.decode()}")
            compressed_data = response.data

        # Log download metrics
        download_duration = time.time() - start_time
        self._log_download_metrics(
            self.link.fileLink, len(compressed_data), download_duration
        )

        decompressed_data = (
            ResultSetDownloadHandler._decompress_data(compressed_data)
            if self.settings.is_lz4_compressed
            else compressed_data
        )

        # The size of the downloaded file should match the size specified from TSparkArrowResultLink
        if len(decompressed_data) != self.link.bytesNum:
            logger.debug(
                "ResultSetDownloadHandler: downloaded file size %s does not match the expected value %s",
                len(decompressed_data),
                self.link.bytesNum,
            )

        logger.debug(
            "ResultSetDownloadHandler: successfully downloaded file, offset %s, row count %s",
            self.link.startRowOffset,
            self.link.rowCount,
        )

        return DownloadedFile(
            decompressed_data,
            self.link.startRowOffset,
            self.link.rowCount,
        )

    def _log_download_metrics(
        self, url: str, bytes_downloaded: int, duration_seconds: float
    ):
        """Log download speed metrics at INFO/WARN levels."""
        # Calculate speed in MB/s (ensure float division for precision)
        speed_mbps = (float(bytes_downloaded) / (1024 * 1024)) / duration_seconds

        urlEndpoint = url.split("?")[0]
        # INFO level logging
        logger.info(
            "CloudFetch download completed: %.4f MB/s, %d bytes in %.3fs from %s",
            speed_mbps,
            bytes_downloaded,
            duration_seconds,
            urlEndpoint,
        )

        # WARN level logging if below threshold
        if speed_mbps < self.settings.min_cloudfetch_download_speed:
            logger.warning(
                "CloudFetch download slower than threshold: %.4f MB/s (threshold: %.1f MB/s) from %s",
                speed_mbps,
                self.settings.min_cloudfetch_download_speed,
                url,
            )

    @staticmethod
    def _validate_link(link: TSparkArrowResultLink, expiry_buffer_secs: int):
        """
        Check if a link has expired or will expire.

        Expiry buffer can be set to avoid downloading files that has not expired yet when the function is called,
        but may expire before the file has fully downloaded.
        """
        current_time = int(time.time())
        if (
            link.expiryTime <= current_time
            or link.expiryTime - current_time <= expiry_buffer_secs
        ):
            raise Error("CloudFetch link has expired")

    @staticmethod
    def _decompress_data(compressed_data: bytes) -> bytes:
        """
        Decompress lz4 frame compressed data.

        Decompresses data that has been lz4 compressed, either via the whole frame or by series of chunks.
        """
        uncompressed_data, bytes_read = lz4.frame.decompress(
            compressed_data, return_bytes_read=True
        )
        # The last cloud fetch file of the entire result is commonly punctuated by frequent end-of-frame markers.
        # Full frame decompression above will short-circuit, so chunking is necessary
        if bytes_read < len(compressed_data):
            d_context = lz4.frame.create_decompression_context()
            start = 0
            uncompressed_data = bytearray()
            while start < len(compressed_data):
                data, num_bytes, is_end = lz4.frame.decompress_chunk(
                    d_context, compressed_data[start:]
                )
                uncompressed_data += data
                start += num_bytes
        return uncompressed_data
