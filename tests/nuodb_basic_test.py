
import unittest
import pynuodb

class NuoDBBasicTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
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