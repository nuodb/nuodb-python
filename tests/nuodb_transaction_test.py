"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

from . import nuodb_base


class TestNuoDBTransaction(nuodb_base.NuoBase):
    def test_connection_isolation(self):
        con1 = self._connect()
        con2 = self._connect()

        cursor1 = con1.cursor()
        cursor2 = con2.cursor()

        cursor1.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL")
        cursor2.execute("SELECT 3 FROM DUAL UNION ALL SELECT 4 FROM DUAL")

        assert cursor1.fetchone()[0] == 1
        assert cursor2.fetchone()[0] == 3

        assert cursor1.fetchone()[0] == 2
        assert cursor2.fetchone()[0] == 4

    def test_cursor_isolation(self):
        con = self._connect()

        cursor1 = con.cursor()
        cursor2 = con.cursor()

        cursor1.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL")
        cursor2.execute("SELECT 3 FROM DUAL UNION ALL SELECT 4 FROM DUAL")

        assert cursor1.fetchone()[0] == 1
        assert cursor2.fetchone()[0] == 3

        assert cursor1.fetchone()[0] == 2
        assert cursor2.fetchone()[0] == 4

    def test_rollback(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("DROP TABLE IF EXISTS rollback_table")
        cursor.execute("CREATE TABLE rollback_table (f1 integer)")

        con.commit()

        cursor.execute("INSERT INTO rollback_table VALUES (1)")
        con.rollback()

        cursor.execute("SELECT COUNT(*) FROM rollback_table")
        assert cursor.fetchone()[0] == 0

        cursor.execute("DROP TABLE rollback_table")

    def test_commit(self):
        con1 = self._connect()
        con2 = self._connect()

        cursor1 = con1.cursor()
        cursor2 = con2.cursor()

        cursor1.execute("DROP TABLE IF EXISTS commit_table")
        cursor1.execute("CREATE TABLE commit_table (f1 integer)")

        con1.commit()

        cursor1.execute("INSERT INTO commit_table VALUES (1)")

        cursor2.execute("SELECT COUNT(*) FROM commit_table")
        assert cursor2.fetchone()[0] == 0

        con1.commit()
        con2.commit()

        cursor2.execute("SELECT COUNT(*) FROM commit_table")
        assert cursor2.fetchone()[0] == 1

        cursor1.execute("DROP TABLE commit_table")

    def test_rollback_disconnect(self):
        con1 = self._connect()
        cursor1 = con1.cursor()

        cursor1.execute("DROP TABLE IF EXISTS rollback_disconnect")
        cursor1.execute("CREATE TABLE rollback_disconnect (f1 integer)")

        con1.commit()

        cursor1.execute("INSERT INTO rollback_disconnect VALUES (1)")
        con1.close()

        con2 = self._connect()
        cursor2 = con2.cursor()
        cursor2.execute("SELECT COUNT(*) FROM rollback_disconnect")
        assert cursor2.fetchone()[0] == 0

        cursor2.execute("DROP TABLE rollback_disconnect")

    def test_autocommit_set(self):
        con1 = self._connect()
        con2 = self._connect()

        assert not con1.auto_commit

        con1.auto_commit = True
        assert con1.auto_commit

        con2.auto_commit = True
        assert con2.auto_commit

        cursor1 = con1.cursor()
        cursor1.execute("DROP TABLE IF EXISTS autocommit_set")
        cursor1.execute("CREATE TABLE autocommit_set (f1 integer)")

        cursor1.execute("INSERT INTO autocommit_set VALUES (1)")

        cursor2 = con2.cursor()
        cursor2.execute("SELECT COUNT(*) FROM autocommit_set")
        assert cursor2.fetchone()[0] == 1
        cursor2.execute("TRUNCATE TABLE autocommit_set")

        con1.auto_commit = False
        assert not con1.auto_commit

        cursor1.execute("INSERT INTO autocommit_set VALUES (1)")
        cursor2.execute("SELECT COUNT(*) FROM autocommit_set")
        assert cursor2.fetchone()[0] == 0

        con1.commit()
        cursor2.execute("SELECT COUNT(*) FROM autocommit_set")
        assert cursor2.fetchone()[0] ==  1

        cursor1.execute("DROP TABLE autocommit_set")
