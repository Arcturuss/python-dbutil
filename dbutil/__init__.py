from urllib.parse import urlparse
from .dbutil import DbConnection

version = (0, 5, 0)


def _connect_mysql(url):
    import MySQLdb as db
    params = {}
    if url.username:
        params["user"] = url.username
    else:
        params["user"] = "root"
    if url.password:
        params["passwd"] = url.password
    if url.hostname:
        params["host"] = url.hostname
    if url.port:
        params["port"] = url.port
    if url.path:
        params["db"] = url.path[1:]
    return connection(db.connect(**params))


def _connect_sqlite3(url):
    import sqlite3 as db
    params = {}
    if url.path:
        if url.path in ["memory", ""]:
            params["database"] = ":memory:"
        else:
            params["database"] = url.path
    return DbConnection(db.connect(**params))


DEFAULT_LOOKUP = {
    "sqlite3": _connect_sqlite3,
    "mysql": _connect_mysql
}


def connect(url, lookup=None):
    url = urlparse(url)
    if lookup is None:
        lookup = DEFAULT_LOOKUP
    return lookup[url.scheme](url)
