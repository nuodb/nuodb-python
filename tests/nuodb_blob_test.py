#!/usr/bin/env python

import unittest
import struct

import pynuodb
import sys
from .nuodb_base import NuoBase

systemVersion = sys.version[0]


class NuoDBBlobTest(NuoBase):
    def test_blob_prepared(self):
        con = self._connect()
        cursor = con.cursor()

        binary_data = struct.pack('hhl', 1, 2, 3)

        cursor.execute("SELECT ? FROM DUAL", [pynuodb.Binary(binary_data)])
        row = cursor.fetchone()

        currentRow = str(row[0])
        if systemVersion == '3':
            currentRow = bytes(currentRow, 'latin-1')
        array2 = struct.unpack('hhl', currentRow)
        self.assertEqual(len(array2), 3)
        self.assertEqual(array2[2], 3)


if __name__ == '__main__':
    unittest.main()
