
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
        
if __name__ == '__main__':
    unittest.main()