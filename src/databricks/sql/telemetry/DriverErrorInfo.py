import json
from dataclasses import dataclass, asdict


@dataclass
class DriverErrorInfo:
    error_name: str
    stack_trace: str

    def to_json(self):
        return json.dumps(asdict(self))


"""Required for ErrorLogs
DriverErrorInfo errorInfo = new DriverErrorInfo(
    errorName="CONNECTION_ERROR",
    stackTrace="Connection failure while using the Databricks SQL Python connector. Failed to connect to server: https://my-workspace.cloud.databricks.com\n" +
              "databricks.sql.exc.OperationalError: Connection refused: connect\n" +
              "at databricks.sql.thrift_backend.ThriftBackend.make_request(ThriftBackend.py:329)\n" +
              "at databricks.sql.thrift_backend.ThriftBackend.attempt_request(ThriftBackend.py:366)\n" +
              "at databricks.sql.thrift_backend.ThriftBackend.open_session(ThriftBackend.py:575)\n" +
              "at databricks.sql.client.Connection.__init__(client.py:69)\n" +
              "at databricks.sql.client.connect(connection.py:123)")
"""
