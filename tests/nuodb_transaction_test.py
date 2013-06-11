#!/usr/bin/env python

import pynuodb
import unittest

from nuodb_base import NuoBase

class NuoDBTransactionTest(NuoBase):
        
    def test_connection_isolation(self):
        
        con1 = self._connect();
        con2 = self._connect();
        
        cursor1 = con1.cursor();
        cursor2 = con2.cursor();

        cursor1.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL");
        cursor2.execute("SELECT 3 FROM DUAL UNION ALL SELECT 4 FROM DUAL");

        self.assertEquals(cursor1.fetchone()[0], 1);
        self.assertEquals(cursor2.fetchone()[0], 3);

        self.assertEquals(cursor1.fetchone()[0], 2);
        self.assertEquals(cursor2.fetchone()[0], 4);

    def test_cursor_isolation(self):

        con = self._connect();

        cursor1 = con.cursor();
        cursor2 = con.cursor();

        cursor1.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL");
        cursor2.execute("SELECT 3 FROM DUAL UNION ALL SELECT 4 FROM DUAL");

        self.assertEquals(cursor1.fetchone()[0], 1);
        self.assertEquals(cursor2.fetchone()[0], 3);

        self.assertEquals(cursor1.fetchone()[0], 2);
        self.assertEquals(cursor2.fetchone()[0], 4);
   
    def test_rollback(self):

        con = self._connect();
        cursor = con.cursor();

        cursor.execute("DROP TABLE IF EXISTS rollback_table");
        cursor.execute("CREATE TABLE rollback_table (f1 integer)");

        con.commit();

        cursor.execute("INSERT INTO rollback_table VALUES (1)");
        con.rollback();

        cursor.execute("SELECT COUNT(*) FROM rollback_table");
        self.assertEquals(cursor.fetchone()[0], 0);

        cursor.execute("DROP TABLE rollback_table");

    def test_commit(self):

        con1 = self._connect();
        con2 = self._connect();

        cursor1 = con1.cursor();
        cursor2 = con2.cursor();

        cursor1.execute("DROP TABLE IF EXISTS commit_table");
        cursor1.execute("CREATE TABLE commit_table (f1 integer)");

        con1.commit();

        cursor1.execute("INSERT INTO commit_table VALUES (1)");

        cursor2.execute("SELECT COUNT(*) FROM commit_table");
        self.assertEquals(cursor2.fetchone()[0], 0);

        con1.commit();
        con2.commit();

        cursor2.execute("SELECT COUNT(*) FROM commit_table");
        self.assertEquals(cursor2.fetchone()[0], 1);

        cursor1.execute("DROP TABLE commit_table");
         
    def test_rollback_disconnect(self):

        con1 = self._connect();
        cursor1 = con1.cursor();

        cursor1.execute("DROP TABLE IF EXISTS rollback_disconnect");
        cursor1.execute("CREATE TABLE rollback_disconnect (f1 integer)");

        con1.commit();

        cursor1.execute("INSERT INTO rollback_disconnect VALUES (1)");
        con1.close();

        con2 = self._connect();
        cursor2 = con2.cursor();
        cursor2.execute("SELECT COUNT(*) FROM rollback_disconnect");
        self.assertEquals(cursor2.fetchone()[0], 0);

        cursor2.execute("DROP TABLE rollback_disconnect");

    def DISABLEDtest_autocommit_set(self):
        con = self._connect();

        self.assertEquals(con.auto_commit, False);
 
        con.auto_commit = True;
        self.assertEquals(con.auto_commit, True);

        cursor1 = con.cursor();
        cursor1.execute("DROP TABLE IF EXISTS autocommit_set");
        cursor1.execute("CREATE TABLE autocommit_set (f1 integer)");
        con.commit();

        cursor1.execute("INSERT INTO autocommit_set VALUES (1)");

        cursor2 = con.cursor();
        cursor2.execute("SELECT COUNT(*) FROM autocommit_set");
        self.assertEquals(cursor2.fetchone()[0], 1);
        cursor2.execute("TRUNCATE TABLE autocommit_set");

        con.auto_commit = False;
        self.assertEquals(con.auto_commit, False);

        cursor1.execute("INSERT INTO autocommit_set VALUES (1)");
        cursor2.execute("SELECT COUNT(*) FROM autocommit_set");
        self.assertEquals(cursor2.fetchone()[0], 0);

        con.commit();
        cursor2.execute("SELECT COUNT(*) FROM autocommit_set");
        self.assertEquals(cursor2.fetchone()[0], 1);

        cursor1.execute("DROP TABLE autocommit_set");
        
if __name__ == '__main__':
    unittest.main()
