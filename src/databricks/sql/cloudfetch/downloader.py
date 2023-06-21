import logging

import requests
import lz4.frame
import threading
import time

logger = logging.getLogger(__name__)


class ResultSetDownloadHandler(threading.Thread):

    def __init__(self, downloadable_result_settings, t_spark_arrow_result_link):
        super().__init__()
        self.settings = downloadable_result_settings
        self.result_link = t_spark_arrow_result_link
        self.is_download_scheduled = False
        self.is_download_finished = threading.Event()
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False
        self.http_code = None
        self.result_file = None
        self.check_result_file_link_expiry = True
        self.download_completion_semaphore = threading.Semaphore(0)

    def is_file_download_successful(self):
        try:
            if not self.is_download_finished.is_set():
                if self.settings.download_timeout and self.settings.download_timeout > 0:
                    if not self.download_completion_semaphore.acquire(timeout=self.settings.download_timeout):
                        self.is_download_timedout = True
                        logger.debug("Cloud fetch download timed out after {} seconds for url: {}"
                                     .format(self.settings.download_timeout, self.result_link.file_link)
                                     )
                        return False
                else:
                    self.download_completion_semaphore.acquire()
        except:
            return False
        return self.is_file_downloaded_successfully

    def run(self):
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False
        self.is_download_finished = threading.Event()

        if self.check_result_file_link_expiry:
            current_time = int(time.time() * 1000)
            if (self.result_link.expiryTime < current_time) or (
                    self.result_link.expiryTime - current_time < (
                    self.settings.result_file_link_expiry_buffer * 1000)
            ):
                self.is_link_expired = True
                return

        session = requests.Session()
        session.timeout = self.settings.download_timeout

        if (
                self.settings.use_proxy
                and not self.settings.disable_proxy_for_cloud_fetch
        ):
            proxy = {
                "http": f"http://{self.settings.proxy_host}:{self.settings.proxy_port}",
                "https": f"https://{self.settings.proxy_host}:{self.settings.proxy_port}",
            }
            session.proxies.update(proxy)

            # ProxyAuthentication -> static enum BASIC and NONE
            if self.settings.proxy_auth == "BASIC":
                session.auth = requests.auth.HTTPBasicAuth(self.settings.proxy_uid, self.settings.proxy_pwd)

        try:
            response = session.get(self.result_link.fileLink)
            self.http_code = response.status_code

            if self.http_code != 200:
                self.is_file_downloaded_successfully = False
            else:
                if self.settings.is_lz4_compressed:
                    compressed_data = response.content
                    uncompressed_data, bytes_read = lz4.frame.decompress(compressed_data, return_bytes_read=True)
                    if bytes_read < len(compressed_data):
                        d_context = lz4.frame.create_decompression_context()
                        start = 0
                        uncompressed_data = bytearray()
                        while start < len(compressed_data):
                            data, b, e = lz4.frame.decompress_chunk(d_context, compressed_data[start:])
                            uncompressed_data += data
                            start += b

                    self.result_file = uncompressed_data
                    self.is_file_downloaded_successfully = True

                else:
                    self.result_file = response.content
                    self.is_file_downloaded_successfully = True
        except:
            self.is_file_downloaded_successfully = False

        finally:
            session.close()
            self.is_download_finished.set()
            self.download_completion_semaphore.release()
