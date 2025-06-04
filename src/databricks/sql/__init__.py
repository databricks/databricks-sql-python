import datetime

from databricks.sql.exc import *

# PEP 249 module globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections.

paramstyle = "named"

import re


class RedactUrlQueryParamsFilter(logging.Filter):
    pattern = re.compile(r"(\?|&)([\w-]+)=([^&]+)")
    mask = r"\1\2=<REDACTED>"

    def __init__(self):
        super().__init__()

    def redact(self, string):
        return re.sub(self.pattern, self.mask, str(string))

    def filter(self, record):
        record.msg = self.redact(str(record.msg))
        if isinstance(record.args, dict):
            for k in record.args.keys():
                record.args[k] = (
                    self.redact(record.args[k])
                    if isinstance(record.arg[k], str)
                    else record.args[k]
                )
        else:
            record.args = tuple(
                (self.redact(arg) if isinstance(arg, str) else arg)
                for arg in record.args
            )

        return True


logging.getLogger("urllib3.connectionpool").addFilter(RedactUrlQueryParamsFilter())


class DBAPITypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values

    def __repr__(self):
        return "DBAPITypeObject({})".format(self.values)


STRING = DBAPITypeObject("string")
BINARY = DBAPITypeObject("binary")
NUMBER = DBAPITypeObject(
    "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "decimal"
)
DATETIME = DBAPITypeObject("timestamp")
DATE = DBAPITypeObject("date")
ROWID = DBAPITypeObject()

__version__ = "3.1.0"
USER_AGENT_NAME = "PyDatabricksSqlConnector"

# These two functions are pyhive legacy
Date = datetime.date
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


def connect(server_hostname, http_path, access_token=None, **kwargs):
    from .client import Connection

    return Connection(server_hostname, http_path, access_token, **kwargs)
