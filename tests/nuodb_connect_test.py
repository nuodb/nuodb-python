#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

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
        try:
            con = pynuodb.connect("nosuchdatabase", self.host, "dba", "dba_password")
            self.fail()
        except SessionException:
            pass
        except:
            self.fail()

    def test_nosuchport(self):
        try:
            con = pynuodb.connect(DATABASE_NAME, "localhost:23456", DBA_USER, DBA_PASSWORD)
            self.fail()
        except:
            pass

    def test_nosuchhost(self):
        try:
            con = pynuodb.connect(DATABASE_NAME, "nosuchhost", DBA_USER, DBA_PASSWORD)
            self.fail()
        except:
            pass

    def test_nosuchuser(self):
        try:
            con = pynuodb.connect(DATABASE_NAME, self.host, "nosuchuser", DBA_PASSWORD)
            self.fail()
        except ProgrammingError:
            pass
        except:
            self.fail()

    def test_nosuchpassword(self):
        try:
            con = pynuodb.connect(DATABASE_NAME, self.host, DBA_USER, "nosuchpassword")
            self.fail()
        except ProgrammingError:
            pass
        except:
            self.fail()


if __name__ == '__main__':
    unittest.main()
