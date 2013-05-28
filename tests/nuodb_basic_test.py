#!/usr/bin/env python

"""
These tests assume that the quickstart database exists.

To create it run /opt/nuodb/run-quickstart or use the web console.
"""

import pynuodb
import unittest

from nuodb_base import NuoBase

class NuoDBBasicTest(NuoBase):
    
    def setUp(self):
        NuoBase.setUp(self)

    def tearDown(self):
        NuoBase.tearDown(self)
        
    def _connect(self):
        return pynuodb.connect("test", "localhost", "dba", "goalie", schema="hockey")
    
    def test_noop(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("select 1 from dual")
        row = cursor.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 1)
        
    def test_numeric_types(self):
        con = self._connect()
        cursor = con.cursor()
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
            
            # Basic test
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (0, 0, 0, 0, 0, 0, 0)")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], 0);
                
                
            # param test
            test_vals = (-3424, 23453464, 45453453454545, 234355.33, 976.2, 34524584057.3434234, 10000.999)
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (?, ?, ?, ?, ?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i - 1]);
            
        finally:
            cursor.execute("drop table typetest if exists")
        

if __name__ == '__main__':
    unittest.main()