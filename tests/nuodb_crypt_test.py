#!/usr/bin/env python

import unittest
import pynuodb


class NuoDBBasicTest(unittest.TestCase):
    def test_toByteString(self):
        self.assertEqual(pynuodb.crypt.toSignedByteString(1), '\x01')
        self.assertEqual(pynuodb.crypt.toSignedByteString(127), '\x7f')
        self.assertEqual(pynuodb.crypt.toSignedByteString(254), '\x00\xfe')
        self.assertEqual(pynuodb.crypt.toSignedByteString(255), '\x00\xff')
        self.assertEqual(pynuodb.crypt.toSignedByteString(-1), '\xff')
        self.assertEqual(pynuodb.crypt.toSignedByteString(-2), '\xfe')
        self.assertEqual(pynuodb.crypt.toSignedByteString(-256), '\xff\x00')
        self.assertEqual(pynuodb.crypt.toSignedByteString(-258), '\xfe\xfe')

    def test_fromByteString(self):
        self.assertEqual(pynuodb.crypt.fromSignedByteString('\x01'), 1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('\x00\xff'), 255)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('\xff'), -1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('\xff\x01'), -255)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('\xff\x00'), -256)
        self.assertEqual(pynuodb.crypt.fromSignedByteString('\xfe\xfe'), -258)

    def test_bothByteString(self):
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(1)), 1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(0)), 0)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(-1)), -1)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(256)), 256)
        self.assertEqual(pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(-256)), -256)


if __name__ == '__main__':
    unittest.main()
