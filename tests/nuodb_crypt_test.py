#!/usr/bin/env python

import unittest
import pynuodb


class NuoDBBasicTest(unittest.TestCase):
    """Run basic tests of the crypt module."""

    CVT = {1: bytearray([1]),
           127: bytearray([127]),
           254: bytearray([0, 254]),
           255: bytearray([0, 255]),
           -1: bytearray([255]),
           -2: bytearray([254]),
           -256: bytearray([255, 0]),
           -258: bytearray([254, 254])}

    def test_toByteString(self):
        """Test toSignedByteString."""
        for val, data in self.CVT.items():
            self.assertEqual(pynuodb.crypt.toSignedByteString(val), data)

    def test_fromByteString(self):
        """Test fromSignedByteString."""
        for val, data in self.CVT.items():
            self.assertEqual(pynuodb.crypt.fromSignedByteString(data), val)

    def test_bothByteString(self):
        """Test to and from signed bytes tring."""
        for val in self.CVT.keys():
            self.assertEqual(
                pynuodb.crypt.fromSignedByteString(pynuodb.crypt.toSignedByteString(val)),
                val)


if __name__ == '__main__':
    unittest.main()
