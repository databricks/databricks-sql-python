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
            self.download_handlers.append(ResultSetDownloadHandler(
                self.downloadable_result_settings, t_spark_arrow_result_link))


DownloadableResultSettings = namedtuple(
    "DownloadableResultSettings",
    "is_lz4_compressed result_file_link_expiry_buffer download_timeout use_proxy disable_proxy_for_cloud_fetch "
    "proxy_host proxy_port proxy_uid proxy_pwd"
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
        proxy_pwd=""
    )
