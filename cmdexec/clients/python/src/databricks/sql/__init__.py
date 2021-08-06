class _DBAPITypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


STRING = _DBAPITypeObject('string')
BINARY = _DBAPITypeObject('binary')
NUMBER = _DBAPITypeObject('boolean', 'byte', 'short', 'integer', 'long', 'double', 'decimal')
DATETIME = _DBAPITypeObject('timestamp')
DATE = _DBAPITypeObject('date')
ROWID = _DBAPITypeObject()

__version__ = "1.0.0"
USER_AGENT_NAME = "PyDatabricksSqlConnector"


def connect(**kwargs):
    from .client import Connection
    return Connection(**kwargs)
