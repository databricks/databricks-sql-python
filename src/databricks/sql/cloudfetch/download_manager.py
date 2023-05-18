from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler


class ResultFileDownloadManager:
    _instance = None

    def __init__(self, connection):
        self.download_handlers = []
        self.downloadable_result_settings = _get_downloadable_result_settings(connection)

    def __new__(cls, connection):
        if not cls._instance:
            cls._instance = super(ResultFileDownloadManager, cls).__new__(cls, connection)

    def add_file_links(self, t_spark_arrow_result_links):
        for t_spark_arrow_result_link in t_spark_arrow_result_links:
            if t_spark_arrow_result_link.row_count <= 0:
                continue
            self.download_handlers.append(ResultSetDownloadHandler(None, t_spark_arrow_result_link))


DownloadableResultSettings = namedtuple(
    "DownloadableResultSettings",
    "is_lz4_compressed"
)


def _get_downloadable_result_settings(connection):
    return DownloadableResultSettings(
        is_lz4_compressed=connection.lz4_compression
    )
