"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import pynuodb

from . import nuodb_base


class TestNuoDBGlobals(nuodb_base.NuoBase):
    def test_module_globals(self):
        assert pynuodb.apilevel == '2.0'
        assert pynuodb.threadsafety == 1
        assert pynuodb.paramstyle == 'qmark'
