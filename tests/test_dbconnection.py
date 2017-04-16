#!/usr/bin/env python3

import unittest
from unittest import mock
from collections import namedtuple
from dbutil import DbConnection
from .utils import TestDb


class DbConnectionTest(unittest.TestCase):
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
                cur = con.cursor()
                actual = [td.map(x) for x in cur.iter(td.query)]
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
                cur = con.cursor()
                actual = [td.map(x) for x in cur.all(td.query)]
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
                cur = con.cursor()
                actual = td.map(cur.one(td.query))
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
                cur = con.cursor()
                actual = cur.scalar(td.query)
                self.assertEqual(td.expected, actual)


if __name__ == '__main__' and __package__ is None:
    unittest.main()
