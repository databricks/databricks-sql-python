import requests
import lz4.frame
import threading
import time


class ResultSetDownloadHandler(threading.Thread):

    def __init__(self, downloadable_result_settings, t_spark_arrow_result_link):
        super().__init__()
        self.settings = downloadable_result_settings
        self.result_link = t_spark_arrow_result_link
        self.is_download_finished = threading.Event()
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False
        self.http_code = None
        self.result_file = None
        self.check_result_file_link_expiry = True
        self.download_completion_semaphore = threading.Semaphore(0)

    def is_file_download_successfully(self):
        try:
            if not self.is_download_finished.is_set():
                if self.settings.download_timeout > 0:
                    if not self.download_completion_semaphore.acquire(timeout=self.settings.download_timeout):
                        self.is_download_timedout = True
                        raise RuntimeError("Result file download timeout")
                else:
                    self.download_completion_semaphore.acquire()
        except:
            return False
        return self.is_file_downloaded_successfully

    def run(self):
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False

        if self.check_result_file_link_expiry:
            current_time = int(time.time() * 1000)
            if (self.result_link.expiry_time < current_time) or (
                    self.result_link.expiry_time - current_time < (
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
            response = session.get(self.result_link.file_link)
            self.http_code = response.status_code

            if self.http_code != 200:
                self.is_file_downloaded_successfully = False
            else:
                if self.settings.is_lz4_compressed:
                    compressed_data = response.content
                    uncompressed_data = lz4.frame.decompress(compressed_data)
                    self.result_file = uncompressed_data

                    if len(uncompressed_data) != self.result_link.bytes_num:
                        self.is_file_downloaded_successfully = False
                    else:
                        self.is_file_downloaded_successfully = True

                else:
                    self.result_file = response.content
                    if len(self.result_file) != self.result_link.bytes_num:
                        self.is_file_downloaded_successfully = False
                    else:
                        self.is_file_downloaded_successfully = True
        except:
            self.is_file_downloaded_successfully = False

        finally:
            session.close()
            self.is_download_finished.set()
            self.download_completion_semaphore.release()
