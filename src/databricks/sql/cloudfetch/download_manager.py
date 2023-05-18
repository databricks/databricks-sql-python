from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler


class ResultFileDownloadManager:
    _instance = None

    def __init__(self, connection):
        self.download_handlers = []
        self.thread_pool = ThreadPoolExecutor(max_workers=connection.max_download_threads + 1)
        self.downloadable_result_settings = _get_downloadable_result_settings(connection)
        self.download_need_retry = False
        self.num_consecutive_result_file_download_retries = 0

    def __new__(cls, connection):
        if not cls._instance:
            cls._instance = super(ResultFileDownloadManager, cls).__new__(cls, connection)

    def add_file_links(self, t_spark_arrow_result_links):
        for t_spark_arrow_result_link in t_spark_arrow_result_links:
            if t_spark_arrow_result_link.row_count <= 0:
                continue
            self.download_handlers.append(ResultSetDownloadHandler(
                self.downloadable_result_settings, t_spark_arrow_result_link))

    def get_next_downloaded_file(self, result_set):
        if not self.download_handlers:
            return True
        i = 0
        # Remove any download handlers whose range of rows doesn't include the next row to be fetched
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

        # Check on downloads
        i = 0
        while i < len(self.download_handlers):
            handler = self.download_handlers[i]
            if handler.is_download_scheduled:
                i += 1
                continue
            if not self._check_and_handle_download_error(handler, result_set):
                pass

    def _check_and_handle_download_error(self, handler, result_set):
        if not handler.is_file_download_successfully:
            self._stop_all_downloads_and_clear_handlers()
            # TODO: downloadedResultFilesSize update to 0
            if handler.is_link_expired or handler.is_download_timedout:  # TODO: add all the other conditions!
                if self.num_consecutive_result_file_download_retries >= self.downloadable_result_settings.max_consecutive_file_download_retries:
                    raise Exception("File download exceeded max retry limit")
            else:
                raise Exception("File download error")
            self.download_need_retry = True
            return True
        self.num_consecutive_result_file_download_retries = 0
        self.download_need_retry = False
        return False

    def _stop_all_downloads_and_clear_handlers(self):
        pass


DownloadableResultSettings = namedtuple(
    "DownloadableResultSettings",
    "is_lz4_compressed result_file_link_expiry_buffer download_timeout use_proxy disable_proxy_for_cloud_fetch "
    "proxy_host proxy_port proxy_uid proxy_pwd max_consecutive_file_download_retries"
)


def _get_downloadable_result_settings(connection):
    # TODO: pipe and get the default values from JDBC
    return DownloadableResultSettings(
        is_lz4_compressed=connection.lz4_compression,
        result_file_link_expiry_buffer=None,
        download_timeout=0,
        use_proxy=False,
        disable_proxy_for_cloud_fetch=False,
        proxy_host="",
        proxy_port=0,
        proxy_uid="",
        proxy_pwd="",
        max_consecutive_file_download_retries=0
    )
