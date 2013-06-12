#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
import pynuodb;
import unittest;
import os;

from nuodb_base import NuoBase;
from pynuodb.exception import ProgrammingError;
from pynuodb.session import SessionException;

class NuoDBConnectTest(NuoBase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def test_nosuchdatabase(self):
        try:
            con = pynuodb.connect("nosuchdatabase", "localhost", "dba", "goalie");
            self.fail();
        except SessionException:
            pass;
        except:
            self.fail();

    def test_nosuchport(self):
        try:
            con = pynuodb.connect("test", "localhost:23456", "dba", "goalie")
            self.fail();
        except:
            pass;

    def test_nosuchhost(self):
        try:
            con = pynuodb.connect("test", "nosuchhost", "dba", "goalie")
            self.fail();
        except:
            pass;

    def test_nosuchuser(self):
        try:
            con = pynuodb.connect("test", "localhost", "nosuchuser", "goalie")
            self.fail();
        except ProgrammingError:
            pass;
        except:
            self.fail()

    def test_nosuchpassword(self):
        try:
            con = pynuodb.connect("test", "localhost", "dba", "nosuchpassword")
            self.fail();
        except ProgrammingError:
            pass;
        except:
            self.fail();

if __name__ == '__main__':
    unittest.main()
