from collections import OrderedDict
from contextlib import contextmanager
import os
import sqlite3
import tempfile
import psycopg2


class DotDict(dict):
    '''
    Quick and dirty implementation of a dot-able dict.
    '''

    def __init__(self, *args, **kwargs):
        super(DotDict, self).__init__(*args, **kwargs)

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as err:
            raise AttributeError()

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as err:
            raise AttributeError()

    __setattr__ = dict.__setitem__


class TestDb():
    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sqlfile_path = os.path.join(dir_path, "starwars.sql")
        with open(sqlfile_path, 'r') as f:
            self.starwars_sql = f.read()

    @contextmanager
    def connect(self, dbpath=":memory:"):
        if dbpath == ":memory:":
            con = sqlite3.connect(':memory:')
            con.executescript(self.starwars_sql)
            yield con
        elif dbpath in [":temp:", ":tempfile:", ":tempdb:"]:
            with tempfile.NamedTemporaryFile(prefix="starwars_",
                                             suffix='.sqlite') as tmpdb:
                with sqlite3.connect(tmpdb.name) as con:
                    con.executescript(self.starwars_sql)
                    yield con
        else:
            with sqlite3.connect(dbpath) as con:
                con.executescript(self.starwars_sql)
                yield con


class TestDbPostgres():
    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sqlfile_path = os.path.join(dir_path, "starwars_postgres.sql")
        with open(sqlfile_path, 'r') as f:
            self.starwars_sql = f.read()

    @contextmanager
    def connect(self,
                host="localhost",
                port=5432,
                username="postgres",
                password="3edc#EDC",
                database="starwars"):
        with psycopg2.connect(host=host,
                              port=port,
                              user=username,
                              password=password,
                              database=database) as con:

            # for postgres autoincement
            # self.starwars_sql = self.starwars_sql.replace('INTEGER', 'SERIAL')
            # self.starwars_sql = self.starwars_sql.replace('IN (0, 1)', 'IN(true, false)')

            queries = self.starwars_sql.split(';')
            with con.cursor() as cur:
                cur.execute('DROP SCHEMA public CASCADE;')
                cur.execute('CREATE SCHEMA public;')
                cur.execute('GRANT ALL ON SCHEMA public TO postgres;')
                for q in queries:
                    try:
                        cur.execute(q)
                    except Exception as e:
                        print(e)
            yield con
