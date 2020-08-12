#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import socket

import pynuodb
from .nuodb_base import NuoBase, DATABASE_NAME, DBA_PASSWORD, DBA_USER
from pynuodb.exception import ProgrammingError
from pynuodb.session import SessionException


class NuoDBConnectTest(NuoBase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_nosuchdatabase(self):
        with self.assertRaises(SessionException):
            pynuodb.connect("nosuchdatabase", self.host, "dba", "dba_password")

    def test_nosuchport(self):
        # Different versions of Python give different exceptions here
        try:
            pynuodb.connect(DATABASE_NAME, "localhost:23456", DBA_USER, DBA_PASSWORD)
            self.fail("Connection to bogus port succeeded")
        except Exception:
            pass

    def test_nosuchhost(self):
        with self.assertRaises(socket.gaierror):
            pynuodb.connect(DATABASE_NAME, "nosuchhost", DBA_USER, DBA_PASSWORD)

    def test_nosuchuser(self):
        with self.assertRaises(ProgrammingError):
            pynuodb.connect(DATABASE_NAME, self.host, "nosuchuser", DBA_PASSWORD)

    def test_nosuchpassword(self):
        with self.assertRaises(ProgrammingError):
            pynuodb.connect(DATABASE_NAME, self.host, DBA_USER, "nosuchpassword")


if __name__ == '__main__':
    unittest.main()
