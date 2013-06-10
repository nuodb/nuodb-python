#!/usr/bin/env python

import pynuodb
import unittest

from nuodb_base import NuoBase

class NuoDBTransactionTest(NuoBase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def _connect(self):
        return pynuodb.connect("test", "localhost", "dba", "goalie",  options = {"schema":"hockey"})
    
    def test_connection_isolation(self):
        
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, integer_col integer)")
        con.commit()
        con.close()
            
      
        con1 = self._connect()
        con2 = self._connect()
        cursor1 = con1.cursor()
        cursor2 = con2.cursor()
        
        cursor1.execute("insert into typetest values (1)")
        cursor2.execute("insert into typetest values (2)")
        
        cursor1.execute("select count(*) from typetest")
        c1 = cursor1.fetchone()[0]
        self.assertEqual(c1, 1)
        
        cursor2.execute("select count(*) from typetest")
        c2 = cursor1.fetchone()[0]
        self.assertEqual(c2, 1)
        
    def test_rollback(self):
        pass
    
    def test_rollback_savepoint(self):
        pass
        
if __name__ == '__main__':
    unittest.main()
