import logging
from dataclasses import dataclass

import requests
import lz4.frame
import time

from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

from databricks.sql.exc import Error

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
    """

    is_lz4_compressed: bool
    link_expiry_buffer_secs: int = 0
    download_timeout: int = 60
    max_consecutive_file_download_retries: int = 0


class ResultSetDownloadHandler:
    def __init__(
        self,
        settings: DownloadableResultSettings,
        link: TSparkArrowResultLink,
    ):
        self.settings = settings
        self.link = link

    def run(self) -> DownloadedFile:
        """
        Download the file described in the cloud fetch link.

        This function checks if the link has or is expiring, gets the file via a requests session, decompresses the
        file, and signals to waiting threads that the download is finished and whether it was successful.
        """

        # Check if link is already expired or is expiring
        ResultSetDownloadHandler._validate_link(
            self.link, self.settings.link_expiry_buffer_secs
        )

        session = requests.Session()
        session.timeout = self.settings.download_timeout
        # TODO: Retry:
        # from requests.adapters import HTTPAdapter, Retry
        # retries = Retry(total=5,
        #         backoff_factor=0.1,
        #         status_forcelist=[ 500, 502, 503, 504 ])
        # session.mount('http://', HTTPAdapter(max_retries=retries))

        try:
            # Get the file via HTTP request
            response = session.get(self.link.fileLink)
            response.raise_for_status()

            # Save (and decompress if needed) the downloaded file
            compressed_data = response.content
            decompressed_data = (
                ResultSetDownloadHandler._decompress_data(compressed_data)
                if self.settings.is_lz4_compressed
                else compressed_data
            )

            # The size of the downloaded file should match the size specified from TSparkArrowResultLink
            if len(decompressed_data) != self.link.bytesNum:
                logger.debug(
                    "ResultSetDownloadHandler: downloaded file size {} does not match the expected value {}".format(
                        len(decompressed_data), self.link.bytesNum
                    )
                )

            logger.debug(
                "ResultSetDownloadHandler: successfully downloaded file, offset {}, row count {}".format(
                    self.link.startRowOffset, self.link.rowCount
                )
            )

            return DownloadedFile(
                decompressed_data,
                self.link.startRowOffset,
                self.link.rowCount,
            )
        finally:
            session and session.close()

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
