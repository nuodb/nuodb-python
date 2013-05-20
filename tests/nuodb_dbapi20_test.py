#!/usr/bin/env python

import dbapi20
import unittest

from nuodb_base import NuoBase

class test_NuoDB(NuoBase, dbapi20.DatabaseAPI20Test):

    def setUp(self):
        NuoBase.setUp(self)
        dbapi20.DatabaseAPI20Test.setUp(self)

    def tearDown(self):
        NuoBase.tearDown(self)
        dbapi20.DatabaseAPI20Test.tearDown(self)

    # Unsupported tests
    def test_nextset(self): pass
    def test_setoutputsize(self): pass
    def test_callproc(self): pass

if __name__ == '__main__':
    unittest.main()
