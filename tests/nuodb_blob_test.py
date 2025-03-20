"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import struct

import pynuodb
import sys
from . import nuodb_base

systemVersion = sys.version[0]


class TestNuoDBBlob(nuodb_base.NuoBase):
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
        assert len(array2) == 3
        assert array2[2] == 3
