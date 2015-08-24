#!/usr/bin/env python

import unittest

from .nuodb_base import NuoBase
from pynuodb.exception import DataError, ProgrammingError, BatchError, OperationalError
from distutils.version import LooseVersion
from os import getenv


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

    def test_cursor_rowcount_and_last_query(self):
        con = self._connect()
        cursor = con.cursor()
        statement = "SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL"
        cursor.execute(statement)
        self.assertEquals(cursor.rowcount, -1)
        self.assertEquals(cursor.query, statement)

    def test_insufficient_parameters(self):
        con = self._connect()
        cursor = con.cursor()

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
            cursor.execute("SELECT ? FROM DUAL", [1, 2])
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
        cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4]])

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
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4], [1, 2, 3]])
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
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4], [1, 2], [5, 6], [5, 6]])
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

    def test_result_set_gets_closed(self):
        current_version = getenv('NUODB_VERSION', None) # skipif <2.1
        if current_version is not None and LooseVersion(current_version) < LooseVersion("2.1"):
        # Server will throw error after 1000 open result sets
            con = self._connect()
            for j in [False, True]:
                for i in range(2015):
                    if not j:
                        cursor = con.cursor()
                        cursor.execute('select 1 from dual;')
                        con.commit()
                        cursor.close()
                    else:
                        if i >= 1000:
                            with self.assertRaises(OperationalError):
                                cursor = con.cursor()
                                cursor.execute('select 1 from dual;')
                                con.commit()
                        else:
                            cursor = con.cursor()
                            cursor.execute('select 1 from dual;')
                            con.commit()


if __name__ == '__main__':
    unittest.main()
