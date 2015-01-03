#!/usr/bin/env python

import pynuodb
import unittest

from nuodb_base import NuoBase
from pynuodb.exception import DataError, ProgrammingError, BatchError

class NuoDBCursorTest(NuoBase):

    def test_cursor_description(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("SELECT 'abc' AS XYZ , 123 AS `123` FROM DUAL")
        descriptions = cursor.description

        self.assertEquals(len(descriptions), 2)
        self.assertEquals(descriptions[0][0], 'XYZ')
        self.assertEquals(descriptions[0][1], self.driver.STRING)
        self.assertEquals(descriptions[0][2], 3)


        self.assertEquals(descriptions[1][0], '123')
        self.assertEquals(descriptions[1][1], self.driver.NUMBER)
        self.assertEquals(descriptions[1][2], 11)

    def test_cursor_rowcount_select(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL")
        self.assertEquals(cursor.rowcount, -1)


    def test_insufficient_parameters(self):
        con = self._connect()
        cursor = con.cursor()
#       cursor.execute("SELECT ? FROM DUAL")
#       cursor.execute("SELECT ? FROM DUAL", [0])

        try:
            cursor.execute("SELECT ?, ? FROM DUAL", [1])
            self.fail()
        except ProgrammingError as e:
            self.assertIsNotNone(e)

    def test_toomany_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        try:
            cursor.execute("SELECT 1 FROM DUAL", [1])
            self.fail()
        except ProgrammingError as e:
            self.assertIsNotNone(e)

        try:
            cursor.execute("SELECT ? FROM DUAL", [1,2])
            self.fail()
        except ProgrammingError as e:
            self.assertIsNotNone(e)

    def test_incorrect_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        try:
            cursor.execute("SELECT ? + 1 FROM DUAL", ['abc'])
            self.fail()
        except DataError as e:
            self.assertIsNotNone(e)

    def test_executemany(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [ [ 1 , 2 ], [ 3, 4 ] ])

        cursor.execute("SELECT * FROM executemany_table")

        ret = cursor.fetchall()

        self.assertEquals(ret[0][0], 1)
        self.assertEquals(ret[0][1], 2)
        self.assertEquals(ret[1][0], 3)
        self.assertEquals(ret[1][1], 4)

        cursor.execute("DROP TABLE executemany_table")

    def test_executemany_bad_parameters(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        # 3rd tuple has too many params
        try:
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [ [ 1, 2 ], [ 3, 4 ], [ 1, 2, 3] ])
            self.fail()
        except ProgrammingError as e:
            self.assertIsNotNone(e)

        cursor.execute("DROP TABLE executemany_table")

    def test_executemany_somefail(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        cursor.execute('CREATE UNIQUE INDEX "f1idx" ON "executemany_table" ("f1");')
        # 3rd tuple has uniqueness conflict
        try:
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [ [ 1, 2 ], [ 3, 4 ], [ 1, 2], [5,6], [5,6] ])
            self.fail()
        except BatchError as e:
            self.assertEquals(e.results[0], 1)
            self.assertEquals(e.results[1], 1)
            self.assertEquals(e.results[2], -3)
            self.assertEquals(e.results[3], 1)
            self.assertEquals(e.results[4], -3)
        except:
            self.fail()

        # test that they all made it save the bogus one
        cursor.execute("select * from executemany_table;")
        self.assertEquals(len(cursor.fetchall()), 3)

        cursor.execute("DROP TABLE executemany_table")

if __name__ == '__main__':
    unittest.main()
