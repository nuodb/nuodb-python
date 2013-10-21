#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
"""
This tests checks for various out-of-order execution situations. E.g. attempting to run a query
after being disconnected from the database

"""

import pynuodb
import unittest
import decimal
import time
import os
from distutils.version import StrictVersion

from nuodb_base import NuoBase
from pynuodb.session import Session, SessionException
from pynuodb.exception import Error

class NuoDBExecutionFlowTest(NuoBase):

    def test_commit_after_disconnect(self):
        con = self._connect();

        con.close();

        try:
            con.commit();
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'connection is closed'");
        
    def test_cursor_after_disconnect(self):
        con = self._connect();

        con.close();

        try:
            con.cursor();
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'connection is closed'");

    def test_execute_after_disconnect(self):
        con = self._connect();

        cursor = con.cursor();
        con.close();

        try:
            cursor.execute("SELECT 1 FROM DUAL");
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'connection is closed'");

    def test_fetchone_after_disconnect(self):
        con = self._connect();

        cursor = con.cursor();
        cursor.execute("SELECT 1 FROM DUAL");
        con.close();

        try:
            cursor.fetchone();
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'connection is closed'");

    def test_execute_after_close(self):
        con = self._connect();
        cursor = con.cursor();

        cursor.close();

        try:
            cursor.execute("SELECT 1 FROM DUAL");
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'cursor is closed'");

    def test_fetchone_without_execute(self):
        con = self._connect();
        cursor = con.cursor();

        try:
            cursor.fetchone();
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'Previous execute did not produce any results or no call was issued yet'");

    def test_fetchone_after_close(self):
        con = self._connect();
        cursor = con.cursor();
        cursor.execute("SELECT 1 FROM DUAL");
        cursor.close();

        try:
            cursor.fetchone();
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'cursor is closed'");

    def test_fetchone_on_ddl(self):
        con = self._connect();
        cursor = con.cursor();
        cursor.execute("DROP TABLE fetchone_on_ddl IF EXISTS");

        try:
            cursor.fetchone();
            self.fail();
        except Error as e:
            self.assertEqual(str(e), "'Previous execute did not produce any results or no call was issued yet'");

    def test_fetchone_on_empty(self):
        con = self._connect();
        cursor = con.cursor();
        cursor.execute("SELECT 1 FROM DUAL WHERE FALSE");
        self.assertIsNone(cursor.fetchone());

    def test_fetchone_beyond_eof(self):
        con = self._connect();
        cursor = con.cursor();

        cursor.execute("SELECT 1 FROM DUAL");
        cursor.fetchone();
        self.assertIsNone(cursor.fetchone());

    def test_fetchmany_beyond_eof(self):
        con = self._connect();
        cursor = con.cursor();

        cursor.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL");
        many = cursor.fetchmany(100);
        self.assertEquals(len(many), 2);

    def test_fetch_after_error(self):
        con = self._connect();
        cursor = con.cursor();

        try:
            cursor.execute("SYNTAX ERROR");
            self.fail();
        except Error as e1:
            self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nSYNTAX ERROR\\n^ expected statement got SYNTAX\\n'");
        
        try:
            cursor.fetchone();
            self.fail();
        except Error as e2:
            self.assertEqual(str(e2), "'Previous execute did not produce any results or no call was issued yet'");

    def test_execute_after_error(self):
        con = self._connect();
        cursor = con.cursor();

        try:
            cursor.execute("syntax error");
            self.fail();
        except Error as e1:
            if version_lt("2.0"): 
                self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nsyntax error\\n^ expected statement got SYNTAX\\n'")
            else:   
                self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nsyntax error\\n^ expected statement got syntax\\n'")
        
        cursor.execute("SELECT 1 FROM DUAL");
        cursor.fetchone();

    def test_error_after_error(self):
        con = self._connect();
        cursor = con.cursor();

        try:
            cursor.execute("syntax1 error");
            self.fail();
        except Error as e1:
            if version_lt("2.0"):
                self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nsyntax1 error\\n^ expected statement got SYNTAX1\\n'");
            else:
                self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nsyntax1 error\\n^ expected statement got syntax1\\n'");

        try:
            cursor.execute("syntax2 error");
            self.fail();
        except Error as e1:
            if version_lt("2.0"): 
                self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nsyntax2 error\\n^ expected statement got SYNTAX2\\n'");
            else:
                self.assertEqual(str(e1), "'SYNTAX_ERROR: syntax error on line 1\\nsyntax2 error\\n^ expected statement got syntax2\\n'");

def version_lt(version):
    current_version = os.getenv('NUODB_VERSION', None)
    if current_version is not None and StrictVersion(current_version) < StrictVersion(version):
        return True
        
    return False

if __name__ == '__main__':
    unittest.main()
