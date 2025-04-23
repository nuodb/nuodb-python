"""
(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import decimal
import datetime

from . import nuodb_base


class TestNuoDBDescription(nuodb_base.NuoBase):

    def test_description(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("CREATE TEMPORARY TABLE tmp (v1 INTEGER, v2 STRING)" )
        cursor.execute("INSERT INTO tmp VALUES (1,'a'), (2,'b')")
        cursor.execute("SELECT v1 AS c1, concat(v2,'') as c2 FROM tmp")
        row = cursor.fetchone()
        d = cursor.description

        assert d[0][0].lower() == 'c1'
        assert d[1][0].lower() == 'c2'
