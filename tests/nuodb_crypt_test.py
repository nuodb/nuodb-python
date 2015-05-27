#!/usr/bin/env python

import unittest

import pynuodb


class NuoDBBasicTest(unittest.TestCase):
    def test_toByteString(self):
        self.assertEqual(pynuodb.crypt.toSignedByteString(1), '01'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(127), '7F'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(254), '00FE'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(255), '00FF'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(-1), 'FF'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(-2), 'FE'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(-256), 'FF00'.decode('hex'))
        self.assertEqual(pynuodb.crypt.toSignedByteString(-258), 'FEFE'.decode('hex'))

    def test_fromByteString(self):
        self.assertEqual(pynuodb.crypt.fromSignedByteString('01'.decode('hex')), 1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('00FF'.decode('hex')), 255)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('FF'.decode('hex')), -1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('FF01'.decode('hex')), -255)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('FF00'.decode('hex')), -256)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('FEFE'.decode('hex')), -258)

    def test_bothByteString(self):
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(1)), 1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(0)), 0)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(-1)), -1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(256)), 256)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(-256)), -256)


if __name__ == '__main__':
    unittest.main()
