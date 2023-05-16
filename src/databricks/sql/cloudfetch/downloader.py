import requests
import lz4.frame
import threading
import time

class ResultSetDownloadHandler(threading.Thread):

    def __init__(self, downloadable_execution_context, t_spark_arrow_result_link):
        super().__init__()
        # TODO: we need to define a typed DownloadableExecutionContext instead of a map
        self.execution_context = downloadable_execution_context
        self.result_link = t_spark_arrow_result_link
        self.is_download_finished = threading.Event()
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False
        self.http_code = None
        self.result_file = None
        self.check_result_file_link_expiry = True
        self.download_completion_semaphore = threading.Semaphore()

    def run(self):
        # TODO: log function entrance
        self.is_file_downloaded_successfully = False
        self.is_link_expired = False
        self.is_download_timedout = False

        if self.check_result_file_link_expiry:
            current_time = int(time.time() * 1000)
            if (self.result_link.expiry_time < current_time) or (
                    self.result_link.expiry_time - current_time < (
                    # DownloadableExecutionContext > HiveExecutionContext > HiveJDBCSettings > DownloadableResultSettings > int
                    self.execution_context.settings.downloadable_result_settings.result_file_link_expiry_buffer / 1000)
            ):
                self.is_link_expired = True
                return

        timeout = self.execution_context.settings.downloadable_result_settings.download_timeout
        session = requests.Session()
        # TODO: investigate if I need to set connectTimeout or socketTimeout
        session.timeout = timeout

        if (
                # DownloadableExecutionContext > HiveExecutionContext > HiveJDBCSettings > ProxySettings > boolean
                self.execution_context.settings.proxy_settings.use_proxy
                # DownloadableExecutionContext > HiveExecutionContext > HiveJDBCSettings > ProxySettings > boolean
                and not self.execution_context.settings.proxy_settings.disable_proxy_for_cloud_fetch
        ):
            proxy_settings = self.execution_context.settings.proxy_settings
            proxy = {
                "http": f"http://{proxy_settings.proxy_host}:{proxy_settings.proxy_port}",
                "https": f"http://{proxy_settings.proxy_host}:{proxy_settings.proxy_port}",
            }
            session.proxies.update(proxy)

            # ProxyAuthentication -> static enum BASIC and NONE
            # TODO: create enum or just use string for proxy_settings.proxy_auth
            if proxy_settings.proxy_auth == "BASIC":
                # TODO: see if the below code is the equivalent of BasicCredentialsProvider, AuthScope, and UsernamePasswordCredentials
                session.auth = requests.auth.HTTPBasicAuth(proxy_settings.proxy_uid, proxy_settings.proxy_pwd)

        # TODO: look into ResultFileDownloadMonitor.getResultFileDownloadMonitor();
        try:
            # TODO: verify this is the same as CloseableHttpClient, httpClientBuilder
            response = session.get(self.result_link.file_link)
            self.http_code = response.status_code
            # TODO: look into ResultFileDownloadMonitor.getResultFileDownloadMonitor().addDownloadTask(self.http_request)

            # TODO: there's some logic that roughly translates to this:
            # DownloadableExecutionContext > HiveExecutionContext > HiveJDBCSettings > TEHTTPSettings > Map<String, HttpErrorEmulationSettings> > HttpErrorEmulationSettings
            # download_settings = self.execution_context.settings.thrift_http_settings.http_error_emulation_settings_map.get("Download")
            # if download_settings:
                # download_settings.api_index += 1
                # if download_settings.shouldEmulateError():
                    # self.http_code = download_settings.populate_emulated_http_response().status_line.status_code
            if self.http_code != 200:
                # TODO: log warning
                self.is_file_downloaded_successfully = False
            else:
                if self.execution_context.is_lz4_compressed:
                    # TODO: make sure the next 3 lines correspond to the Lz4 logic in JDBC
                    compressed_data = response.content
                    uncompressed_data = lz4.frame.decompress(compressed_data)
                    self.result_file = uncompressed_data

                    # TODO: verify that lines 174-179 in JDBC doesn't need to be ported
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
                # TODO: do I need to do something about EntityUtils.consume(response.content)?
        # TODO: add IOException and Exception if the below isn't already
        except requests.exceptions.RequestException as e:
            self.is_file_downloaded_successfully = False

        finally:
            self.is_download_finished = True
            self.download_completion_semaphore.release()
            session.close()
