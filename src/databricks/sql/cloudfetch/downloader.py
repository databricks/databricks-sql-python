import logging
from dataclasses import dataclass
import requests
import lz4.frame
import threading
import time
import os
import re
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink

logger = logging.getLogger(__name__)

DEFAULT_CLOUD_FILE_TIMEOUT = int(os.getenv("DATABRICKS_CLOUD_FILE_TIMEOUT", 60))


@dataclass
class DownloadableResultSettings:
    """
    Class for settings common to each download handler.

    Attributes:
        is_lz4_compressed (bool): Whether file is expected to be lz4 compressed.
        link_expiry_buffer_secs (int): Time in seconds to prevent download of a link before it expires. Default 0 secs.
        download_timeout (int): Timeout for download requests. Default 60 secs.
        download_max_retries (int): Number of consecutive download retries before shutting down.
        max_retries (int): Number of consecutive download retries before shutting down.
        backoff_factor (int): Factor to increase wait time between retries.

    """

    is_lz4_compressed: bool
    link_expiry_buffer_secs: int = 0
    download_timeout: int = DEFAULT_CLOUD_FILE_TIMEOUT
    max_retries: int = 5
    backoff_factor: int = 2


class ResultSetDownloadHandler(threading.Thread):
    def __init__(
        self,
        downloadable_result_settings: DownloadableResultSettings,
        t_spark_arrow_result_link: TSparkArrowResultLink,
    ):
        super().__init__()
        self.settings = downloadable_result_settings
        self.result_link = t_spark_arrow_result_link
        self.is_download_scheduled = False
        self.is_download_finished = threading.Event()
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False
        self.result_file = None

    def is_file_download_successful(self) -> bool:
        """
        Check and report if cloud fetch file downloaded successfully.

        This function will block until a file download finishes or until a timeout.
        """
        timeout = (
            self.settings.download_timeout
            if self.settings.download_timeout > 0
            else None
        )
        try:
            logger.debug(
                f"waiting for at most {timeout} seconds for download file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )

            if not self.is_download_finished.wait(timeout=timeout):
                self.is_download_timedout = True
                logger.error(
                    f"cloud fetch download timed out after {self.settings.download_timeout} seconds for link representing rows {self.result_link.startRowOffset} to {self.result_link.startRowOffset + self.result_link.rowCount}"
                )
                # there are some weird cases when the is_download_finished is not set, but the file is downloaded successfully
                return self.is_file_downloaded_successfully

            logger.debug(
                f"finish waiting for download file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )
        except Exception as e:
            logger.error(e)
            return False
        return self.is_file_downloaded_successfully

    def run(self):
        """
        Download the file described in the cloud fetch link.

        This function checks if the link has or is expiring, gets the file via a requests session, decompresses the
        file, and signals to waiting threads that the download is finished and whether it was successful.
        """
        self._reset()

        try:
            # Check if link is already expired or is expiring
            if ResultSetDownloadHandler.check_link_expired(
                self.result_link, self.settings.link_expiry_buffer_secs
            ):
                self.is_link_expired = True
                return

            logger.debug(
                f"started to download file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )

            # Get the file via HTTP request
            response = http_get_with_retry(
                url=self.result_link.fileLink,
                max_retries=self.settings.max_retries,
                backoff_factor=self.settings.backoff_factor,
                download_timeout=self.settings.download_timeout,
            )

            if not response:
                logger.error(
                    f"failed downloading file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
                )
                return

            logger.debug(
                f"success downloading file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )

            # Save (and decompress if needed) the downloaded file
            compressed_data = response.content
            decompressed_data = (
                ResultSetDownloadHandler.decompress_data(compressed_data)
                if self.settings.is_lz4_compressed
                else compressed_data
            )
            self.result_file = decompressed_data

            # The size of the downloaded file should match the size specified from TSparkArrowResultLink
            success = len(self.result_file) == self.result_link.bytesNum
            logger.debug(
                f"download successful file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )
            self.is_file_downloaded_successfully = success
        except Exception as e:
            logger.error(
                f"exception downloading file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )
            logger.error(e)
            self.is_file_downloaded_successfully = False

        finally:
            logger.debug(
                f"signal finished file: startRow {self.result_link.startRowOffset}, rowCount {self.result_link.rowCount}, endRow {self.result_link.startRowOffset + self.result_link.rowCount}"
            )
            # Awaken threads waiting for this to be true which signals the run is complete
            self.is_download_finished.set()

    def _reset(self):
        """
        Reset download-related flags for every retry of run()
        """
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False
        self.is_download_finished = threading.Event()

    @staticmethod
    def check_link_expired(
        link: TSparkArrowResultLink, expiry_buffer_secs: int
    ) -> bool:
        """
        Check if a link has expired or will expire.

        Expiry buffer can be set to avoid downloading files that has not expired yet when the function is called,
        but may expire before the file has fully downloaded.
        """
        current_time = int(time.time())
        if (
            link.expiryTime < current_time
            or link.expiryTime - current_time < expiry_buffer_secs
        ):
            logger.debug("link expired")
            return True
        return False

    @staticmethod
    def decompress_data(compressed_data: bytes) -> bytes:
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


def http_get_with_retry(url, max_retries=5, backoff_factor=2, download_timeout=60):
    attempts = 0
    pattern = re.compile(r"(\?|&)([\w-]+)=([^&\s]+)")
    mask = r"\1\2=<REDACTED>"

    # TODO: introduce connection pooling. I am seeing weird errors without it.
    while attempts < max_retries:
        try:
            session = requests.Session()
            session.timeout = download_timeout
            response = session.get(url)

            # Check if the response status code is in the 2xx range for success
            if response.status_code == 200:
                return response
            else:
                logger.error(response)
        except requests.RequestException as e:
            # if this is not redacted, it will print the pre-signed URL
            logger.error(f"request failed with exception: {re.sub(pattern, mask, str(e))}")
        finally:
            session.close()
        # Exponential backoff before the next attempt
        wait_time = backoff_factor**attempts
        logger.info(f"retrying in {wait_time} seconds...")
        time.sleep(wait_time)

        attempts += 1

    logger.error(
        f"exceeded maximum number of retries ({max_retries}) while downloading result."
    )
    return None
