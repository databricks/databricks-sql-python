from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler
from databricks.sql.utils import ArrowQueue


class ResultFileDownloadManager:

    def __init__(self, connection):
        self.download_handlers = []
        self.thread_pool = ThreadPoolExecutor(max_workers=connection.max_download_threads + 1)
        self.downloadable_result_settings = _get_downloadable_result_settings(connection)
        self.download_need_retry = False
        self.num_consecutive_result_file_download_retries = 0
        self.cloud_fetch_index = 0

    def add_file_links(self, t_spark_arrow_result_links, next_row_index):
        for t_spark_arrow_result_link in t_spark_arrow_result_links:
            if t_spark_arrow_result_link.row_count <= 0:
                continue
            self.download_handlers.append(ResultSetDownloadHandler(
                self.downloadable_result_settings, t_spark_arrow_result_link))
        self.cloud_fetch_index = next_row_index

    def get_next_downloaded_file(self, result_set):
        if not self.download_handlers:
            return False
        i = 0
        # Remove any download handlers whose start to end range doesn't include the next row to be fetched
        # i.e. no need to download
        while i < len(self.download_handlers):
            result_link = self.download_handlers[i].result_link
            if result_link.start_row_offset + result_link.row_count >= result_set._next_row_index:
                i += 1
                continue
            self.download_handlers.pop(i)

        # Schedule the downloads
        for handler in self.download_handlers:
            if handler.is_download_scheduled:
                continue
            # TODO: break if download result files size + bytes num > arrowMaxBytesPerFetch
            try:
                self.thread_pool.submit(handler)
            except:
                # TODO: better exception handling
                break
            handler.is_download_scheduled = True
            # TODO: downloadedResultFilesSize update

        # Get the next downloaded file
        i = 0
        while i < len(self.download_handlers):
            handler = self.download_handlers[i]
            # Skip handlers without download scheduled, typically tasks rejected by thread pool
            if not handler.is_download_scheduled:
                i += 1
                continue

            # Only evaluate the next file to download
            if handler.result_link.start_row_offset != self.cloud_fetch_index:
                i += 1
                continue

            # Download was successful for next download item
            if self._check_if_download_successful(handler, result_set):
                # Buffer should be empty so set buffer to new ArrowQueue with result_file
                result_set.results = ArrowQueue(handler.result_file, handler.result_link.rowCount)
                self.cloud_fetch_index += handler.result_link.rowCount
                self.download_handlers.pop(i)
                # Return True upon successful download to continue loop and not force a retry
                return True
            # TODO: Need to signal that server has more rows
            # Download was not successful for next download item, force a retry
            return False
        return False

    def _check_if_download_successful(self, handler, result_set):
        if not handler.is_file_download_successfully:
            self._stop_all_downloads_and_clear_handlers()
            # TODO: downloadedResultFilesSize update to 0
            if handler.is_link_expired or handler.is_download_timedout:  # TODO: add all the other conditions!
                if self.num_consecutive_result_file_download_retries >= \
                        self.downloadable_result_settings.max_consecutive_file_download_retries:
                    raise Exception("File download exceeded max retry limit")
                self.num_consecutive_result_file_download_retries += 1
            else:
                raise Exception("File download error")
            self.download_need_retry = True
            return False
        self.num_consecutive_result_file_download_retries = 0
        self.download_need_retry = False
        return True

    def _stop_all_downloads_and_clear_handlers(self):
        # TODO: issue stop http request
        self.download_handlers = []


DownloadableResultSettings = namedtuple(
    "DownloadableResultSettings",
    "is_lz4_compressed result_file_link_expiry_buffer download_timeout use_proxy disable_proxy_for_cloud_fetch "
    "proxy_host proxy_port proxy_uid proxy_pwd max_consecutive_file_download_retries download_retry_wait_time"
)


def _get_downloadable_result_settings(connection):
    # TODO: pipe and get the default values from JDBC
    return DownloadableResultSettings(
        is_lz4_compressed=connection.lz4_compression,
        result_file_link_expiry_buffer=0,
        download_timeout=0,
        use_proxy=False,
        disable_proxy_for_cloud_fetch=False,
        proxy_host="",
        proxy_port=0,
        proxy_uid="",
        proxy_pwd="",
        max_consecutive_file_download_retries=0,
        download_retry_wait_time=0.1
    )
