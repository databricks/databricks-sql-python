from concurrent.futures import ThreadPoolExecutor
from databricks.sql.cloudfetch.downloader import ResultSetDownloadHandler


class ResultFileDownloadManager:
    _instance = None

    def __init__(self):
        self.download_handlers = []

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(ResultFileDownloadManager, cls).__new__(cls)

    def add_file_links(self, t_spark_arrow_result_links):
        for t_spark_arrow_result_link in t_spark_arrow_result_links:
            if t_spark_arrow_result_link.row_count <= 0:
                continue
            self.download_handlers.append(ResultSetDownloadHandler(None, t_spark_arrow_result_link))
