import json
from dataclasses import dataclass, asdict
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

    def to_json(self):
        return json.dumps(asdict(self))
    
# Part of TelemetryEvent
# DriverSystemConfiguration systemConfig = new DriverSystemConfiguration(
#     driver_version = "2.9.3",  
#     os_name = "Darwin",        
#     os_version = "24.4.0",     
#     os_arch = "arm64",         
#     runtime_name = "CPython",   
#     runtime_version = "3.13.3", 
#     runtime_vendor = "cpython",
#     client_app_name = "databricks-sql-python", 
#     locale_name = "en_US", 
#     driver_name = "databricks-sql-python",     
#     char_set_encoding = "UTF-8" 
# )