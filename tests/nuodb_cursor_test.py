#!/usr/bin/env python

import pynuodb;
import unittest;

from nuodb_base import NuoBase;
from pynuodb.exception import DataError, ProgrammingError;

class NuoDBCursorTest(NuoBase):
    
    def setUp(self):
        NuoBase.setUp(self);

    def tearDown(self):
        NuoBase.tearDown(self);
        
    def _connect(self):
        return pynuodb.connect("test", "localhost", "dba", "goalie", options = { "schema" : "hockey" } );

    def test_cursor_description(self):
        con = self._connect();
        cursor = con.cursor();

        cursor.execute("SELECT 'abc' AS XYZ , 123 AS `123` FROM DUAL");
        descriptions = cursor.description;

        self.assertEquals(len(descriptions), 2);
        self.assertEquals(descriptions[0][0], 'XYZ');
        self.assertEquals(descriptions[0][1], self.driver.STRING);
        self.assertEquals(descriptions[0][2], 3);


        self.assertEquals(descriptions[1][0], '123');
        self.assertEquals(descriptions[1][1], self.driver.NUMBER);
        self.assertEquals(descriptions[1][2], 11);

    def test_cursor_rowcount_select(self):
        con = self._connect();
        cursor = con.cursor();

        cursor.execute("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL");
        self.assertEquals(cursor.rowcount, -1);


    def test_insufficient_parameters(self):
        
        con = self._connect();
        cursor = con.cursor();
#       cursor.execute("SELECT ? FROM DUAL");
#       cursor.execute("SELECT ? FROM DUAL", [0]);

        try:
            cursor.execute("SELECT ?, ? FROM DUAL", [1]);
            self.fail();
        except ProgrammingError as e:
            self.assertIsNotNone(e)

    def test_toomany_parameters(self):
        
        con = self._connect();
        cursor = con.cursor();
        
        try:
            cursor.execute("SELECT 1 FROM DUAL", [1]);
            self.fail();
        except ProgrammingError as e:
            self.assertIsNotNone(e);

        try:
            cursor.execute("SELECT ? FROM DUAL", [1,2]);
            self.fail();
        except ProgrammingError as e:
            self.assertIsNotNone(e);

    def test_incorrect_parameters(self):

        con = self._connect();
        cursor = con.cursor();
        try:
            cursor.execute("SELECT ? + 1 FROM DUAL", ['abc']);
            self.fail();
        except DataError as e:
            self.assertIsNotNone(e);
        
if __name__ == '__main__':
    unittest.main()
