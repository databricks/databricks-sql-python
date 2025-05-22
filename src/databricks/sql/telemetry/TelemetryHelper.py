import platform
import sys
import uuid
import time
from typing import Optional

from databricks.sql import __version__
from databricks.sql.telemetry.DriverSystemConfiguration import DriverSystemConfiguration


class TelemetryHelper:

    # Singleton instance of DriverSystemConfiguration
    _DRIVER_SYSTEM_CONFIGURATION = None

    @classmethod
    def getDriverSystemConfiguration(cls) -> DriverSystemConfiguration:
        if cls._DRIVER_SYSTEM_CONFIGURATION is None:
            cls._DRIVER_SYSTEM_CONFIGURATION = DriverSystemConfiguration(
                driverName="Databricks SQL Python Connector",
                driverVersion=__version__,
                runtimeName=f"Python {sys.version.split()[0]}",
                runtimeVendor=platform.python_implementation(),
                runtimeVersion=platform.python_version(),
                osName=platform.system(),
                osVersion=platform.release(),
                osArch=platform.machine(),
                clientAppName=None,
                localeName=f"{platform.system()}_{platform.release()}",
                charSetEncoding=sys.getdefaultencoding(),
            )
        return cls._DRIVER_SYSTEM_CONFIGURATION
