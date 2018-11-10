#!/usr/bin/env python

import unittest

import pynuodb
from .nuodb_base import NuoBase


class NuoDBGlobalsTest(NuoBase):
    def test_module_globals(self):
        self.assertEqual(pynuodb.apilevel, '2.0')
        self.assertEqual(pynuodb.threadsafety, 1)
        self.assertEqual(pynuodb.paramstyle, 'qmark')


if __name__ == '__main__':
    unittest.main()
