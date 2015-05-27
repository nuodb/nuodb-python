#!/usr/bin/env python

import pynuodb
import unittest

from nuodb_base import NuoBase
from pynuodb.exception import Error, DataError

class NuoDBGlobalsTest(NuoBase):
    def test_module_globals(self):
        self.assertEquals(pynuodb.apilevel, '2.0')
        self.assertEquals(pynuodb.threadsafety, 1)
        self.assertEquals(pynuodb.paramstyle, 'qmark')


if __name__ == '__main__':
    unittest.main()
