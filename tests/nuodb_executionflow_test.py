"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

This tests checks for various out-of-order execution situations.
E.g., attempting to run a query after being disconnected from the database.
"""

import pytest

from pynuodb.exception import Error

from . import nuodb_base


class TestNuoDBExecutionFlow(nuodb_base.NuoBase):
    def test_commit_after_disconnect(self):
        con = self._connect()

        con.close()

        with pytest.raises(Error) as ex:
            con.commit()
        assert str(ex.value) == 'connection is closed'

    def test_cursor_after_disconnect(self):
        con = self._connect()

        con.close()

        with pytest.raises(Error) as ex:
            con.cursor()
        assert str(ex.value) == 'connection is closed'

    def test_execute_after_disconnect(self):
        con = self._connect()

        cursor = con.cursor()
        con.close()

        with pytest.raises(Error) as ex:
            cursor.execute("SELECT 1 FROM DUAL")
        assert str(ex.value) == 'connection is closed'

    def test_fetchone_after_disconnect(self):
        con = self._connect()

        cursor = con.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        con.close()

        with pytest.raises(Error) as ex:
            cursor.fetchone()
        assert str(ex.value) == 'connection is closed'

    def test_execute_after_close(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.close()

        with pytest.raises(Error) as ex:
            cursor.execute("SELECT 1 FROM DUAL")
        assert str(ex.value) == 'cursor is closed'

    def test_fetchone_without_execute(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(Error) as ex:
            cursor.fetchone()
        assert str(ex.value) == 'Previous execute did not produce any results or no call was issued yet'

    def test_fetchone_after_close(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        cursor.close()

        with pytest.raises(Error) as ex:
            cursor.fetchone()
        assert str(ex.value) == 'cursor is closed'

    def test_fetchone_on_ddl(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE fetchone_on_ddl IF EXISTS")

        with pytest.raises(Error) as ex:
            cursor.fetchone()
        assert str(ex.value) == 'Previous execute did not produce any results or no call was issued yet'

    def test_fetchone_on_empty(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("SELECT 1 FROM DUAL WHERE FALSE")
        assert cursor.fetchone() is None

    def test_fetchone_beyond_eof(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("SELECT 1 FROM DUAL")
        cursor.fetchone()
        assert cursor.fetchone() is None

    def test_fetchmany_beyond_eof(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL")
        many = cursor.fetchmany(100)
        assert len(many) == 2

    def test_fetch_after_error(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(Error) as e1:
            cursor.execute("SYNTAX ERROR")
        assert str(e1.value) == 'SYNTAX_ERROR: syntax error on line 1\nSYNTAX ERROR\n^ expected statement got SYNTAX\n'

        with pytest.raises(Error) as e2:
            cursor.fetchone()
        assert str(e2.value) == 'Previous execute did not produce any results or no call was issued yet'

    def test_execute_after_error(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(Error) as ex:
            cursor.execute("syntax error")
        assert str(ex.value) == 'SYNTAX_ERROR: syntax error on line 1\nsyntax error\n^ expected statement got syntax\n'

        cursor.execute("SELECT 1 FROM DUAL")
        cursor.fetchone()

    def test_error_after_error(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(Error) as e1:
            cursor.execute("syntax1 error")
        assert str(e1.value) == 'SYNTAX_ERROR: syntax error on line 1\nsyntax1 error\n^ expected statement got syntax1\n'

        with pytest.raises(Error) as e2:
            cursor.execute("syntax2 error")
        assert str(e2.value) == 'SYNTAX_ERROR: syntax error on line 1\nsyntax2 error\n^ expected statement got syntax2\n'

    def test_execute_ten_million_with_result_sets(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS execute_ten_million_with_result_sets")
        cursor.execute("CREATE TABLE execute_ten_million_with_result_sets (value INTEGER)")
        for i in range(10000):
            cursor.execute("insert into execute_ten_million_with_result_sets (value) Values (%d)" % (i))
            con.commit()
            cursor.execute("select count(*) from execute_ten_million_with_result_sets;")
            res = cursor.fetchone()[0]
            assert res == i + 1
