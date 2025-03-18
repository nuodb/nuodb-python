"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

from . import dbapi20
from . import nuodb_base


class TestNuoDBAPI20(nuodb_base.NuoBase, dbapi20.DatabaseAPI20Test):
    # Unsupported tests
    def test_nextset(self):
        pass

    def test_setoutputsize(self):
        pass

    def test_callproc(self):
        pass
