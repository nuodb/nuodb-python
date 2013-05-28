#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
"""
These tests assume that the quickstart database exists.

To create it run /opt/nuodb/run-quickstart or use the web console.
"""

import pynuodb
import unittest
import decimal

from nuodb_base import NuoBase

class NuoDBBasicTest(NuoBase):
    
    def setUp(self):
        NuoBase.setUp(self)

    def tearDown(self):
        NuoBase.tearDown(self)
        
    def _connect(self):
        return pynuodb.connect("test", "localhost", "dba", "goalie", schema="hockey")
    
    def test_noop(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("select 1 from dual")
        row = cursor.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 1)
        
    def test_numeric_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
            
            # Basic test
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (0, 0, 0, 0, 0, 0, 0)")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], 0);
            
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_param_numeric_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
            
            test_vals = (-3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'), decimal.Decimal('34524584057.3434234'), 10000.999)
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (?, ?, ?, ?, ?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i - 1]);
            
        finally:
            cursor.execute("drop table typetest if exists")
            
            
    def test_string_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, string_col string, " +
                           "varchar_col varchar(10), char_col char(10))")
            
            # basic
            cursor.execute("insert into typetest (string_col, varchar_col, char_col) " +
                           "values ('', '', '')")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], '');
            
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_param_string_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, string_col string, " +
                           "varchar_col varchar(10), char_col char(10))")
            
            
            # param    
            test_vals = ("The quick brown fox jumpped over the lazy dog.", "The", "Quick")   
            cursor.execute("insert into typetest (string_col, varchar_col, char_col) " +
                           "values (?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
            
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_utf8_string_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, string_col string, " +
                           "varchar_col varchar(10), char_col char(10))")
            
            # utf-8    
            test_vals = (" 私はガラスを食べられます。それは私を傷つけません。我能吞下玻璃而不伤身体。Я могу есть стекло, оно мне не вредит.", "나는 유리를 먹을", "ฉันกินก")   
            cursor.execute("insert into typetest (string_col, varchar_col, char_col) " +
                           "values (?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
            
            
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_date_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, date_col date, " +
                           "time_col time, timestamp_col timestamp)")
            
            test_vals = (pynuodb.Date(2013, 5, 28), pynuodb.Time(8, 13, 34), pynuodb.Timestamp(2013, 3, 24, 12, 3, 26))
            cursor.execute("insert into typetest (date_col, time_col, timestamp_col) " +
                           "values ('" + str(test_vals[0]) + "','" + str(test_vals[1]) + "','" + str(test_vals[2]) + "')")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            self.assertIsInstance(row[1], pynuodb.Date)
            self.assertIsInstance(row[2], pynuodb.Time)
            self.assertIsInstance(row[3], pynuodb.Timestamp)
            
            self.assertEqual(row[1].year, test_vals[0].year)
            self.assertEqual(row[1].month, test_vals[0].month)
            self.assertEqual(row[1].day, test_vals[0].day)
            
            self.assertEqual(row[2].hour, test_vals[1].hour)
            self.assertEqual(row[2].minute, test_vals[1].minute)
            self.assertEqual(row[2].second, test_vals[1].second)
            
            self.assertEqual(row[3].year, test_vals[2].year)
            self.assertEqual(row[3].month, test_vals[2].month)
            self.assertEqual(row[3].day, test_vals[2].day)
            self.assertEqual(row[3].hour, test_vals[2].hour)
            self.assertEqual(row[3].minute, test_vals[2].minute)
            self.assertEqual(row[3].second, test_vals[2].second)      
            
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_param_date_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, date_col date, " +
                           "time_col time, timestamp_col timestamp)")

            test_vals = (pynuodb.Date(1, 1, 1), pynuodb.Time(0, 0, 0), pynuodb.Timestamp(1970, 1, 1, 0, 0, 0))
            cursor.execute("insert into typetest (date_col, time_col, timestamp_col) " +
                           "values (?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            self.assertIsInstance(row[1], pynuodb.Date)
            self.assertIsInstance(row[2], pynuodb.Time)
            self.assertIsInstance(row[3], pynuodb.Timestamp)
            
            self.assertEqual(row[1].year, test_vals[0].year)
            self.assertEqual(row[1].month, test_vals[0].month)
            self.assertEqual(row[1].day, test_vals[0].day)
            
            self.assertEqual(row[2].hour, test_vals[1].hour)
            self.assertEqual(row[2].minute, test_vals[1].minute)
            self.assertEqual(row[2].second, test_vals[1].second)
            
            self.assertEqual(row[3].year, test_vals[2].year)
            self.assertEqual(row[3].month, test_vals[2].month)
            self.assertEqual(row[3].day, test_vals[2].day)
            self.assertEqual(row[3].hour, test_vals[2].hour)
            self.assertEqual(row[3].minute, test_vals[2].minute)
            self.assertEqual(row[3].second, test_vals[2].second)            
            
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_other_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, bool_col boolean, " +
                           "binary_col binary)")

            test_vals = (False, pynuodb.Binary("other binary"))
            cursor.execute("insert into typetest (bool_col, binary_col) " +
                           "values (" + str(test_vals[0]) + ", " + str(test_vals[1]) + ")")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
                   
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
        finally:
            cursor.execute("drop table typetest if exists")
            
    def test_param_other_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, bool_col boolean, " +
                           "binary_col binary)")

            test_vals = (True, pynuodb.Binary("binary"))
            cursor.execute("insert into typetest (bool_col, binary_col) " +
                           "values (?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
                   
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
        finally:
            cursor.execute("drop table typetest if exists")
        

if __name__ == '__main__':
    unittest.main()