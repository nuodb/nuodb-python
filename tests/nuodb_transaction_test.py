#!/usr/bin/env python

import pynuodb
import unittest

from nuodb_base import NuoBase

class NuoDBTransactionTest(NuoBase):
    
    def setUp(self):
        NuoBase.setUp(self)

    def tearDown(self):
        NuoBase.tearDown(self)
        
    def _connect(self):
        return pynuodb.connect("test", "localhost", "dba", "goalie", schema="hockey")
    
    def test_connection_isolation(self):
        
        con1 = self._connect()
        con2 = self._connect()
        
        cursor1 = con1.cursor()
        cursor2 = con2.cursor()
        
    def test_rollback(self):
        pass
    
    def test_rollback_savepoint(self):
        pass
        
if __name__ == '__main__':
    unittest.main()
