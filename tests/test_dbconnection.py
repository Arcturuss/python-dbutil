import unittest
from collections import namedtuple
from dbutil import DbConnection
from .utils import TestDb


class DbConnectionDefaultTest(unittest.TestCase):
    def test_iter(self):
        TestData = namedtuple('TestData', 'query map expected')
        tests = (
            TestData(
                "select release_year from movies order by release_year",
                lambda x: x.release_year,
                [1977, 1980, 1983, 1999, 2002, 2005, 2015, 2016, 2017]
            ),
        )

        with TestDb().connect() as tcon, DbConnection(tcon) as con:
            for td in tests:
                actual = [td.map(x) for x in con.iter(td.query)]
                self.assertEqual(td.expected, actual)

    def test_all(self):
        TestData = namedtuple('TestData', 'query map expected')
        tests = (
            TestData(
                "select * from movies order by release_year desc",
                lambda x: x.release_year,
                [2017, 2016, 2015, 2005, 2002, 1999, 1983, 1980, 1977]
            ),
        )

        with TestDb().connect() as tcon, DbConnection(tcon) as con:
            for td in tests:
                actual = [td.map(x) for x in con.all(td.query)]
                self.assertEqual(td.expected, actual)

    def test_one(self):
        TestData = namedtuple('TestData', 'query map expected')
        tests = (
            TestData(
                "select min(release_year) as release_year from movies",
                lambda x: x.release_year,
                1977
            ),
        )

        with TestDb().connect() as tcon, DbConnection(tcon) as con:
            for td in tests:
                actual = td.map(con.row(td.query))
                self.assertEqual(td.expected, actual)

    def test_scalar(self):
        TestData = namedtuple('TestData', 'query expected')
        tests = (
            TestData("select 1", 1),
            TestData("select ''", ""),
            TestData("select NULL", None),
            TestData("select max(release_year) from movies", 2017),
        )

        with TestDb().connect() as tcon, DbConnection(tcon) as con:
            for td in tests:
                actual = con.one(td.query)
                self.assertEqual(td.expected, actual)

    def test_execute(self):
        TestData = namedtuple('TestData', 'query rows select expected')
        tests = (
            TestData(
                """insert into movies
                (name, episode, director, release_year, chronology)
                values ('test1', 'test', 'test', 1999, 42)""",
                1,
                "select release_year from movies where name='test1'",
                1999
            ),
        )
        with TestDb().connect() as tcon, DbConnection(tcon) as con:
            for td in tests:
                actual = con.execute(td.query)
                self.assertEqual(td.rows, actual)
                actual = con.one(td.select)
                self.assertEqual(td.expected, actual)

    def test_executemany(self):
        query = """insert into movies
                (name, episode, director, release_year, chronology)
                values (?, ?, ?, ?, ?)"""
        params = [
            ['test2', 'test2', 'test2', 1111, 20],
            ['test3', 'test3', 'test3', 2222, 21],
        ]
        select = "select release_year from movies where name='test3'"
        with TestDb().connect() as tcon, DbConnection(tcon) as con:
            actual = con.executemany(query, params)
            self.assertEqual(2, actual)
            actual = con.one(select)
            self.assertEqual(2222, actual)


if __name__ == '__main__' and __package__ is None:
    unittest.main()
