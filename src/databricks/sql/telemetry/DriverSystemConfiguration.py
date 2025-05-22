import json
from dataclasses import dataclass, asdict
import platform
import sys
import locale
from databricks.sql import __version__


@dataclass
class DriverSystemConfiguration:
    driver_version: str
    os_name: str
    os_version: str
    os_arch: str
    runtime_name: str
    runtime_version: str
    runtime_vendor: str
    client_app_name: str
    locale_name: str
    driver_name: str
    char_set_encoding: str

    def __init__(self):
        self.driver_version = __version__
        self.os_name = platform.system()
        self.os_version = platform.version()
        self.os_arch = platform.machine()
        self.runtime_name = platform.python_implementation()
        self.runtime_version = platform.python_version()
        self.runtime_vendor = sys.implementation.name
        self.client_app_name = "databricks-sql-python"
        self.locale_name = locale.getdefaultlocale()[0]
        self.driver_name = "databricks-sql-python"
        self.char_set_encoding = "UTF-8"

    def to_json(self):
        return json.dumps(asdict(self))
