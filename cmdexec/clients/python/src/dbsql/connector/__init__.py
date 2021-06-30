from .client import Connection


def connect(**kwargs):
    return Connection(**kwargs)
