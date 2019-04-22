#!/usr/bin/env python3
"""
A simple sql

Unapologetically Python3 only.
"""
import re
import inspect
from collections import OrderedDict


def dbrow_factory(cur, row):
    """DB API helper to turn rows from a cursor into DbRow objects"""
    result = DbRow()
    for idx, col in enumerate(cur.description):
        result[col[0]] = row[idx]
    return result


def make_sqlparam(self, name, index=0, paramstyle='?'):
    """Returns a sql parameter for the specified paramstyle"""
    # we need to be extra careful with params to prevent injection.
    if not isinstance(index, int):
        raise ValueError(f"non-integer index value: {index}")
    if ';' in name or "'" in name:
        raise ValueError("Unsupported name value: {name}")

    # https://www.python.org/dev/peps/pep-0249/#paramstyle
    if paramstyle == 'qmark':
        return '?'
    elif paramstyle == 'numeric':
        return f':{index}'
    elif paramstyle == 'named':
        return f':{name}'
    elif paramstyle == 'format':
        return f'%s'
    elif paramstyle == 'pyformat':
        return f'%({name})s'
    else:
        raise ValueError(f"Unsupported paramstyle: {paramstyle}")


class DotDict(OrderedDict):
    """
    A dot-able dict, which allows access via properties in addition to
    traditional dict key/value access.
    """

    def __init__(self, *args, **kwargs):
        super(DotDict, self).__init__(*args, **kwargs)

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as err:
            raise AttributeError(f"Attribute not found {name}")

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as err:
            raise AttributeError(f"Attribute not found {k}")

    __setattr__ = OrderedDict.__setitem__


class DbRow(DotDict):
    """
    Represents a row in the database. Implemented as a dot-able dict.
    """

    def __init__(self, *args, **kwargs):
        super(DbRow, self).__init__(*args, **kwargs)


class DbCursor:
    """
    A wrapper for any DB-API v2.0+ database cursor implementation.

    Args:
        impl: Any DB-API v2.0 database cursor implementation
    """

    def __init__(self, impl):
        self.impl = impl

    def __getattr__(self, key):
        return getattr(self.impl, key)

    def iter(self, sql, params=()):
        """Yields each row from a SQL query"""
        for row in self.impl.execute(sql, params):
            yield dbrow_factory(self, row)

    def one(self, sql, params=()):
        """Returns a single value"""
        self.impl.execute(sql, params)
        if self.impl.rowcount == 0:
            return None
        return self.impl.fetchone()[0]

    def row(self, sql, params=()):
        """Returns a single row"""
        self.impl.execute(sql, params)
        if self.impl.rowcount == 0:
            return None
        return dbrow_factory(self, self.impl.fetchone())

    def all(self, sql, params=()):
        """Returns all the results of a query"""
        return list(self.iter(sql, params))

    def execute(self, sql, params=()):
        """Executes an SQL statement, returns number of rows affected"""
        self.impl.execute(sql, params)
        return self.impl.rowcount

    def executemany(self, sql, params):
        """Executes an SQL statement, returns number of rows affected"""
        self.impl.executemany(sql, params)
        return self.impl.rowcount


class DbConnection:
    """
    A wrapper for any DB-API v2.0+ database connection implementation.

    Args:
        impl: Any DB-API v2.0 database connection
    """

    def __init__(self, impl):
        self.impl = impl
        self.cur = self.cursor()

    def __getattr__(self, key):
        return getattr(self.impl, key)

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        self.impl.__exit__(*args)

    def cursor(self):
        return DbCursor(self.impl.cursor())

    def close(self):
        self.__exit__()

    # pass calls to default cursor
    def iter(self, sql, params=()):
        return self.cur.iter(sql, params)

    def one(self, sql, params=()):
        return self.cur.one(sql, params)

    def row(self, sql, params=()):
        return self.cur.row(sql, params)

    def all(self, sql, params=()):
        return self.cur.all(sql, params)

    def execute(self, sql, params=()):
        return self.cur.execute(sql, params)

    def executemany(self, sql, params):
        return self.cur.executemany(sql, params)
