"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

from . import nuodb_base


class TestNuoDBHuge(nuodb_base.NuoBase):
    def test_wide_select(self):

        con = self._connect()
        cursor = con.cursor()
        total_columns = 5120

        alphabet = "ABCDEFGHIJKLMNOPQRSTUWXYZ"
        select_string = "SELECT "

        for col in range(1, total_columns + 1):
            select_string += "'" + alphabet + str(col) + "'"
            if col < total_columns:
                select_string += " , "
            else:
                select_string += " FROM DUAL"

        cursor.execute(select_string)
        row = cursor.fetchone()

        assert len(row) == total_columns

        for col in range(total_columns):
            assert row[col] == alphabet + str(col + 1)

    def test_wide_string(self):

        con = self._connect()
        cursor = con.cursor()

        total_width = 5120
        alphabet = "ABCDEFGHIJKLMNOPQRSTUWXYZ"
        alphabet_multi = ""

        for col in range(1, total_width + 1):
            alphabet_multi += alphabet

        select_string = "SELECT '" + alphabet_multi + "' , ? , '" + alphabet_multi + "' = ? FROM DUAL"

        cursor.execute(select_string, [alphabet_multi, alphabet_multi])
        row = cursor.fetchone()
        assert len(row[0]) == total_width * len(alphabet)
        assert len(row[1]) == total_width * len(alphabet)
        assert row[2]

    def test_long_select(self):

        con = self._connect()
        cursor = con.cursor()

        cursor.execute("DROP TABLE IF EXISTS ten")
        cursor.execute("DROP SEQUENCE IF EXISTS s1")
        cursor.execute("DROP TABLE IF EXISTS huge_select")

        cursor.execute("CREATE TABLE ten (f1 INTEGER)")
        cursor.execute("INSERT INTO ten VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")

        cursor.execute("CREATE SEQUENCE s1")
        cursor.execute("CREATE TABLE huge_select (f1 INTEGER GENERATED BY DEFAULT AS IDENTITY(s1))")
        cursor.execute(
            "INSERT INTO huge_select SELECT NEXT VALUE FOR s1 FROM ten AS a1,ten AS a2,ten AS a3,ten AS a4,ten AS a5, ten AS a6")

        cursor.execute("SELECT * FROM huge_select")

        total_rows = 0
        while (1):
            rows = cursor.fetchmany(10000)
            if rows is None or len(rows) == 0:
                break
            total_rows = total_rows + len(rows)

        assert total_rows == 1000000

        cursor.execute("DROP TABLE ten")
        cursor.execute("DROP TABLE huge_select")
