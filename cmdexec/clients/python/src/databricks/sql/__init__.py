from .client import Connection


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


def connect(**kwargs):
    return Connection(**kwargs)
