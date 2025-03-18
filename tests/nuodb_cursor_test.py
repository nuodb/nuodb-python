"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import pytest

from pynuodb.exception import DataError, ProgrammingError, BatchError, OperationalError

from . import nuodb_base


class TestNuoDBCursor(nuodb_base.NuoBase):

    def test_cursor_description(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("SELECT 'abc' AS XYZ, 123 AS `123` FROM DUAL")
        descriptions = cursor.description
        dstr = "Descriptions: %s" % (str(descriptions))
        assert len(descriptions) == 2, dstr

        assert descriptions[0][0] == 'XYZ', dstr
        assert descriptions[0][1] == self.driver.STRING, dstr
        # We don't get back a length for this type (it's 0)
        # assert descriptions[0][2] == 3, dstr

        assert descriptions[1][0] == '123', dstr
        assert descriptions[1][1] == self.driver.NUMBER, dstr
        # I think this should be 6 but there is disagreement?
        # assert descriptions[1][2] == 5, dstr

    def test_cursor_rowcount_and_last_query(self):
        con = self._connect()
        cursor = con.cursor()
        statement = "SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL"
        cursor.execute(statement)
        assert cursor.rowcount == -1
        assert cursor.query == statement

    def test_insufficient_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ?, ? FROM DUAL", [1])

    def test_toomany_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT 1 FROM DUAL", [1])

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ? FROM DUAL", [1, 2])

    def test_incorrect_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(DataError):
            cursor.execute("SELECT ? + 1 FROM DUAL", ['abc'])

    def test_executemany(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4]])

        cursor.execute("SELECT * FROM executemany_table")

        ret = cursor.fetchall()

        assert ret[0][0] == 1
        assert ret[0][1] == 2
        assert ret[1][0] == 3
        assert ret[1][1] == 4

        cursor.execute("DROP TABLE executemany_table")

    def test_executemany_bad_parameters(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        # 3rd tuple has too many params
        with pytest.raises(ProgrammingError):
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4], [1, 2, 3]])

        cursor.execute("DROP TABLE executemany_table")

    def test_executemany_somefail(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        cursor.execute('CREATE UNIQUE INDEX "f1idx" ON "executemany_table" ("f1");')
        # 3rd tuple has uniqueness conflict
        with pytest.raises(BatchError) as ex:
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)",
                               [[1, 2], [3, 4], [1, 2], [5, 6], [5, 6]])

        assert ex.value.results[0] == 1
        assert ex.value.results[1] == 1
        assert ex.value.results[2] == -3
        assert ex.value.results[3] == 1
        assert ex.value.results[4] == -3

        # test that they all made it save the bogus one
        cursor.execute("select * from executemany_table;")
        assert len(cursor.fetchall()) == 3

        cursor.execute("DROP TABLE executemany_table")

    def test_result_set_gets_closed(self):
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
                        with pytest.raises(OperationalError):
                            cursor = con.cursor()
                            cursor.execute('select 1 from dual;')
                            con.commit()
                    else:
                        cursor = con.cursor()
                        cursor.execute('select 1 from dual;')
                        con.commit()
