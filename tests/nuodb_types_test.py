"""
(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import decimal
import datetime
from . import nuodb_base
from .mock_tzs import localize


class TestNuoDBTypes(nuodb_base.NuoBase):
    def test_boolean_types(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("CREATE TEMPORARY TABLE tmp (v1 BOOLEAN)")
        cursor.execute("INSERT INTO tmp VALUES (true)")

        cursor.execute("SELECT * FROM tmp")
        row = cursor.fetchone()

        assert len(row) == 1
        assert row[0]

    def test_string_types(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("CREATE TEMPORARY TABLE tmp ("
                       " v1 STRING,"
                       " v2 CHARACTER,"
                       " v3 CHARACTER LARGE OBJECT)")
        cursor.execute("INSERT INTO tmp VALUES ('simple', 'a', 'clob')")

        cursor.execute("SELECT * FROM tmp")
        row = cursor.fetchone()

        assert len(row) == 3
        assert row[0] == 'simple'
        assert row[1] == 'a'
        assert row[2] == 'clob'

    def test_numeric_types(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("CREATE TEMPORARY TABLE tmp ("
                       " v1 SMALLINT,"
                       " v2 INTEGER,"
                       " v3 BIGINT,"
                       " v4 NUMERIC(30, 1),"
                       " v5 FLOAT,"
                       " v6 DOUBLE)")

        cursor.execute("INSERT INTO tmp VALUES (1, 2, 9223372036854775807,"
                       " 9223372036854775807111.5, 5.6, 7.8)")

        cursor.execute("SELECT * FROM tmp")
        row = cursor.fetchone()

        assert len(row) == 6
        assert row[0] == 1
        assert row[1] == 2
        assert row[2] == 9223372036854775807
        assert row[3] == decimal.Decimal('9223372036854775807111.5')
        assert row[4] == 5.6
        assert row[5] == 7.8

    def test_binary_types(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("CREATE TEMPORARY TABLE tmp ("
                       " v1 BLOB,"
                       " v2 BINARY,"
                       " v3 BINARY VARYING(10))")

        cursor.execute("INSERT INTO tmp VALUES ('foo', 'a', 'barbaz')")

        cursor.execute("SELECT * FROM tmp")
        row = cursor.fetchone()

        assert len(row) == 3
        assert row[0] == b'foo'
        assert row[1] == b'a'
        assert row[2] == b'barbaz'

    def test_datetime_types(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("CREATE TEMPORARY TABLE tmp ("
                       " v1 DATE,"
                       " v2 TIME,"
                       " v3 TIMESTAMP,"
                       " v4 TIMESTAMP WITHOUT TIME ZONE)")

        cursor.execute("INSERT INTO tmp VALUES ("
                       " '1/1/2000',"
                       " '05:44:33.2211',"
                       " '1/1/2000 05:44:33.2211',"
                       " '1/1/2000 05:44:33.2211')")

        cursor.execute("SELECT * FROM tmp")
        row = cursor.fetchone()

        assert len(row) == 4
        assert row[0] == datetime.date(2000, 1, 1)
        assert row[1] == datetime.time(5, 44, 33, 221100)
        assert row[2] == localize(datetime.datetime(2000, 1, 1, 5, 44, 33, 221100))
        assert row[3] == localize(datetime.datetime(2000, 1, 1, 5, 44, 33, 221100))


    def test_null_type(self):
        con = self._connect()
        cursor = con.cursor()

        null_type = self.driver.TypeObjectFromNuodb('<null>')

        cursor.execute("SELECT NULL from dual")
        row = cursor.fetchone()

        assert len(row) == 1
        assert cursor.description[0][1] == null_type
        assert row[0] is None
