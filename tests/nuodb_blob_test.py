#!/usr/bin/env python

import pynuodb;
import unittest;

from struct import *;
from nuodb_base import NuoBase;
from pynuodb.exception import Error, DataError;

class NuoDBBlobTest(NuoBase):

    def test_blob_prepared(self):
        con = self._connect();
        cursor = con.cursor();

        binary_data = pack('hhl', 1, 2, 3);

        cursor.execute("SELECT ? FROM DUAL", [ binary_data ] );
        row = cursor.fetchone();

        array1 = unpack('hhl', row[0]);
        self.assertEquals(len(array1), 3);
        self.assertEquals(array1[2], 3);

        cursor.execute("SELECT ? FROM DUAL", [ pynuodb.Binary(binary_data) ] );
        row = cursor.fetchone();

        array2 = unpack('hhl', str(row[0]));  
        self.assertEquals(len(array2), 3);
        self.assertEquals(array2[2], 3);

if __name__ == '__main__':
    unittest.main()
