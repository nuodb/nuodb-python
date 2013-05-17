import unittest
import pynuodb

class NuoDBTransactionTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def _connect(self):
        return pynuodb.connect("test", "localhost" "dba", "goalie", schema="hockey")
    
    def test_connection_isolation(self):
        
        
        con1 = self._connect()
        con2 = self._connect()
        
        cursor1 = con1.cursor()
        cursor2 = con2.cursor()
        
        
if __name__ == '__main__':
    unittest.main()
