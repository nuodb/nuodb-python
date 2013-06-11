#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
"""
These tests assume that the quickstart database exists.

To create it run /opt/nuodb/run-quickstart or use the web console.
"""

import pynuodb
import unittest
import decimal
import time
import os

from nuodb_base import NuoBase

class NuoDBBasicTest(NuoBase):
               
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
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double precision)")
            
            # Basic test
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (0, 0, 0, 0, 0, 0, 0)")
            
            con.commit()
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], 0);
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_double_precision(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col float, int_col double precision, bigint_col double, " + 
                           "smallnegint_col double, negint_col double, bignegint_col double, double_col double)")       
            
            test_vals = (1, 100000, 10000000000000000, -1, -100000, -10000000000000000, 0.000000000001)
            cursor.execute("insert into typetest (smallint_col, int_col, bigint_col, smallnegint_col, negint_col, bignegint_col, double_col) " +
                           "values (" + str(test_vals[0]) + ", " + str(test_vals[1]) + ", " + str(test_vals[2]) + ", " + str(test_vals[3]) + ", " +
                           str(test_vals[4]) + ", " + str(test_vals[5]) + ", " + str(test_vals[6]) + ")")
            
            con.commit()
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i - 1]);
                        
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()             
            
    def test_param_numeric_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
            
            test_vals = (0, 0, 0, decimal.Decimal(0), decimal.Decimal(0), decimal.Decimal(0), 0.0)
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (?, ?, ?, ?, ?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i - 1]);
                        
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
 
    def test_param_numeric_types_pos(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")      
                 
            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'), decimal.Decimal('34524584057.3434234'), 10000.999)
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (?, ?, ?, ?, ?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i - 1]);
                
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
 
    def test_param_numeric_types_neg(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")     
                
            test_vals = (-13546, -156465465, -3135135132132104354, decimal.Decimal('-354564.12'), decimal.Decimal('-77788864.6'), decimal.Decimal('-46543213.01324654'), -999.999999)
            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, number_col, double_col) " +
                           "values (?, ?, ?, ?, ?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i - 1]);
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
                
    def test_overflow_numeric_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
            
            with self.assertRaises(pynuodb.DatabaseError):
                test_vals = (10**99,)
                cursor.execute("insert into typetest (smallint_col) " +
                               "values (?)", test_vals)
                
            with self.assertRaises(pynuodb.DatabaseError):
                test_vals = (10**99,)
                cursor.execute("insert into typetest (integer_col) " +
                               "values (?)", test_vals)
                
            with self.assertRaises(pynuodb.DatabaseError):
                test_vals = (10**99,)
                cursor.execute("insert into typetest (bigint_col,) " +
                               "values (?)", test_vals)
                
                
            with self.assertRaises(pynuodb.DatabaseError):
                test_vals = (-(10**99),)
                cursor.execute("insert into typetest (smallint_col) " +
                               "values (?)", test_vals)
                
            with self.assertRaises(pynuodb.DatabaseError):
                test_vals = (-(10**99),)
                cursor.execute("insert into typetest (integer_col) " +
                               "values (?)", test_vals)
                
            with self.assertRaises(pynuodb.DatabaseError):
                test_vals = (-(10**99),)
                cursor.execute("insert into typetest (bigint_col,) " +
                               "values (?)", test_vals)
                
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
                
    def test_int_into_decimal(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
                
            test_vals = (1,)
            cursor.execute("insert into typetest (decimal_col) " +
                           "values (?)", test_vals)
            cursor.execute("select decimal_col from typetest order by id desc limit 1")
            row = cursor.fetchone()
            self.assertEqual(row[0], decimal.Decimal(test_vals[0]))
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
                
    def test_float_into_number(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
            
            test_vals = (1.215,)
            cursor.execute("insert into typetest (number_col) " +
                           "values (?)", test_vals)
            cursor.execute("select number_col from typetest order by id desc limit 1")
            row = cursor.fetchone()
            self.assertEqual(row[0], decimal.Decimal(test_vals[0]))

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
                
    def test_string_into_decimal(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")

            test_vals = ('91.56',)
            cursor.execute("insert into typetest (decimal_col) " +
                           "values (?)", test_vals)
            cursor.execute("select decimal_col from typetest order by id desc limit 1")
            row = cursor.fetchone()
            self.assertEqual(row[0], decimal.Decimal(test_vals[0]))
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
# TODO: uncomment when DB-3298 is fixed
#     def test_string_into_number(self):
#         con = self._connect()
#         cursor = con.cursor()
#         cursor.execute("drop table typetest if exists")
#         try:
#             cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, integer_col integer, bigint_col bigint, " + 
#                            "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double)")
#             
#             test_vals = ('54.4978',)
#             cursor.execute("insert into typetest (number_col) " +
#                            "values (?)", test_vals)
#             cursor.execute("select number_col from typetest order by id desc limit 1")
#             row = cursor.fetchone()
#             self.assertEqual(row[0], decimal.Decimal(test_vals[0]))
#             
#         finally:
#             try:
#                 cursor.execute("drop table typetest if exists")
#             finally:
#                 con.close()
            
            
    def test_string_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, string_col string, " +
                           "varchar_col varchar(10), char_col char(10), clob_col clob)")
            
            cursor.execute("insert into typetest (string_col, varchar_col, char_col, clob_col) " +
                           "values ('', '', '', '')")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], '');
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
    def test_param_string_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, string_col string, " +
                           "varchar_col varchar(10), char_col char(10), clob_col clob)")
            
            
            # param    
            test_vals = ("The quick brown fox jumpped over the lazy dog.", "The", "Quick", "The quick brown fox jumpped over the lazy dog2.")   
            cursor.execute("insert into typetest (string_col, varchar_col, char_col, clob_col) " +
                           "values (?, ?, ?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
    def test_long_string_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, string_col string, " +
                           "clob_col clob)")
            
            # param
            f = open("holmes.txt", "r")
            text = f.read()
            f.close()
            test_vals = (text, text)   
            cursor.execute("insert into typetest (string_col, clob_col) " +
                           "values (?, ?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
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
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
    def test_date_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, date_col date, " +
                           "time_col time, timestamp_col timestamp)")
            
            test_vals = (pynuodb.Date(2008, 1, 1), pynuodb.Time(8, 13, 34), pynuodb.Timestamp(2013, 3, 24, 12, 3, 26))
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
            self.assertEqual(row[2].microsecond, test_vals[1].microsecond)
            
            self.assertEqual(row[3].year, test_vals[2].year)
            self.assertEqual(row[3].month, test_vals[2].month)
            self.assertEqual(row[3].day, test_vals[2].day)
            self.assertEqual(row[3].hour, test_vals[2].hour)
            self.assertEqual(row[3].minute, test_vals[2].minute)
            self.assertEqual(row[3].second, test_vals[2].second)    
            self.assertEqual(row[3].microsecond, test_vals[2].microsecond)  
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
    def test_param_date_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, date_col date, " +
                           "time_col time, timestamp_col timestamp)")

            test_vals = (pynuodb.Date(1970, 1, 1), pynuodb.Time(0, 0, 0), pynuodb.Timestamp(2010, 12, 31, 19, 0, 0))
            cursor.execute("insert into typetest (date_col, time_col, timestamp_col) " +
                           "values (?, ?, ?)", test_vals)
            con.commit()
            
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
            self.assertEqual(row[2].microsecond, test_vals[1].microsecond)
            
            self.assertEqual(row[3].year, test_vals[2].year)
            self.assertEqual(row[3].month, test_vals[2].month)
            self.assertEqual(row[3].day, test_vals[2].day)
            self.assertEqual(row[3].hour, test_vals[2].hour)
            self.assertEqual(row[3].minute, test_vals[2].minute)
            self.assertEqual(row[3].second, test_vals[2].second)
            self.assertEqual(row[3].microsecond, test_vals[2].microsecond)            
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
    def test_other_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, bool_col boolean, " +
                           "binary_col binary)")

            test_vals = (False, pynuodb.Binary("other binary"))
            cursor.execute("insert into typetest (bool_col, binary_col) " +
                           "values ('" + str(test_vals[0]) + "', '" + str(test_vals[1]) + "')")
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
                   
            for i in xrange(1, len(row)):
                self.assertEqual(row[i], test_vals[i-1])
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
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
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
                
    
                
    def test_param_binary_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, binary_col binary)")

            f = open("640px-Starling.JPG", "rb")
            data = f.read()
            f.close()
            
            test_vals = (pynuodb.Binary(data),)
            cursor.execute("insert into typetest (binary_col) " +
                           "values (?)", test_vals)
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            self.assertIsInstance(row[1], pynuodb.Binary)
            self.assertEqual(row[1], pynuodb.Binary(data))
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
            
    def test_timezones(self):
        try:
            os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
            time.tzset()
            
            con = self._connect()
            cursor = con.cursor()
            cursor.execute("drop table typetest if exists")    
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, timestamp_col timestamp)")
            vals = (pynuodb.Timestamp(2013, 05, 24, 0, 0, 1),)
            cursor.execute("insert into typetest (timestamp_col) values (?)", vals)
            con.commit()
            con.close()
            
            
            os.environ['TZ'] = 'PST+08PDT,M4.1.0,M10.5.0'
            time.tzset()
            con = self._connect()
            cursor = con.cursor()
            cursor.execute("select * from typetest")
            row = cursor.fetchone()
            
            self.assertEqual(vals[0].year, row[1].year)
            self.assertEqual(vals[0].month, row[1].month)
            self.assertEqual(vals[0].day, row[1].day + 1)
            self.assertEqual(vals[0].hour, (row[1].hour + 3) % 24)
            self.assertEqual(vals[0].minute, row[1].minute)
            self.assertEqual(vals[0].second, row[1].second)
            self.assertEqual(vals[0].microsecond, row[1].microsecond)
            con.close()
            
            
            os.environ['TZ'] = 'CET-01CEST,M4.1.0,M10.5.0'
            time.tzset()
            con = self._connect()
            cursor = con.cursor()
            cursor.execute("select * from typetest")
            row = cursor.fetchone()
            
            self.assertEqual(vals[0].year, row[1].year)
            self.assertEqual(vals[0].month, row[1].month)
            self.assertEqual(vals[0].day, row[1].day)
            self.assertEqual(vals[0].hour, (row[1].hour - 6) % 24)
            self.assertEqual(vals[0].minute, row[1].minute)
            self.assertEqual(vals[0].second, row[1].second)
            self.assertEqual(vals[0].microsecond, row[1].microsecond)
            
            cursor.execute("drop table typetest if exists")
            con.close()
            
        finally:
            try:
                os.environ.pop('TZ')
            except:
                pass
            
    def test_param_time_micro_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, time_col time, timestamp_col timestamp)")

            test_vals = (pynuodb.Time(5, 8, 20, 12), pynuodb.Timestamp(1999, 12, 31, 19, 0, 0, 1400))
            cursor.execute("insert into typetest (time_col, timestamp_col) " +
                           "values (?, ?)", test_vals)
            con.commit()
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            self.assertIsInstance(row[1], pynuodb.Time)
            self.assertIsInstance(row[2], pynuodb.Timestamp)
            
            self.assertEqual(row[1].hour, test_vals[0].hour)
            self.assertEqual(row[1].minute, test_vals[0].minute)
            self.assertEqual(row[1].second, test_vals[0].second)
            self.assertEqual(row[1].microsecond, test_vals[0].microsecond)
            
            self.assertEqual(row[2].year, test_vals[1].year)
            self.assertEqual(row[2].month, test_vals[1].month)
            self.assertEqual(row[2].day, test_vals[1].day)
            self.assertEqual(row[2].hour, test_vals[1].hour)
            self.assertEqual(row[2].minute, test_vals[1].minute)
            self.assertEqual(row[2].second, test_vals[1].second)
            self.assertEqual(row[2].microsecond, test_vals[1].microsecond)            
            
        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()
        
    def test_all_types(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, binary_col binary, " +
                           "bool_col boolean, timestamp_col timestamp, time_col time, date_col date, string_col string, " +
                           "varchar_col varchar(10), char_col char(10), smallint_col smallint, integer_col integer, bigint_col bigint, " +
                           "numeric_col numeric(10, 2), decimal_col decimal(10, 2), number_col number, double_col double, clob_col clob, blob_col blob)")
            
            vals = (
                        pynuodb.Binary("binary"),
                        True,
                        pynuodb.Timestamp(1990, 12, 31, 19, 0, 0),
                        pynuodb.Time(10, 30, 44),
                        pynuodb.Date(1998, 1, 1),
                        "this",
                        "is a",
                        "test",
                        -13546, 
                        156465465, 
                        -3135135132132104354, 
                        decimal.Decimal('-354564.12'), 
                        decimal.Decimal('77788864.6'), 
                        decimal.Decimal('-46543213.01324654'), 
                        -999.999999,
                        "The test",
                        pynuodb.Binary("test"),
                   )
            cursor.execute("insert into typetest (binary_col, bool_col, timestamp_col, time_col, date_col, string_col, " +
                           "varchar_col, char_col, smallint_col, integer_col, bigint_col, numeric_col, decimal_col, " +
                           "number_col, double_col, clob_col, blob_col) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", vals)
            con.commit()
            
            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()
            
            for i in xrange(1, 3):
                self.assertEqual(row[i], vals[i - 1]);
            
            self.assertIsInstance(row[3], pynuodb.Timestamp)
            self.assertIsInstance(row[4], pynuodb.Time)
            self.assertIsInstance(row[5], pynuodb.Date)
            
            self.assertEqual(row[3].year, vals[2].year)
            self.assertEqual(row[3].month, vals[2].month)
            self.assertEqual(row[3].day, vals[2].day)
            self.assertEqual(row[3].hour, vals[2].hour)
            self.assertEqual(row[3].minute, vals[2].minute)
            self.assertEqual(row[3].second, vals[2].second)
            self.assertEqual(row[3].microsecond, vals[2].microsecond)  
            
            self.assertEqual(row[4].hour, vals[3].hour)
            self.assertEqual(row[4].minute, vals[3].minute)
            self.assertEqual(row[4].second, vals[3].second)
            self.assertEqual(row[4].microsecond, vals[3].microsecond)
            
            self.assertEqual(row[5].year, vals[4].year)
            self.assertEqual(row[5].month, vals[4].month)
            self.assertEqual(row[5].day, vals[4].day)
            
            for i in xrange(6, len(row)):
                self.assertEqual(row[i], vals[i - 1]);

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()  
                
    def test_param_date_error(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, date_col date)")

        test_vals = (pynuodb.Date(1900, 1, 1),)
            
        with self.assertRaises(pynuodb.DataError): 
            cursor.execute("insert into typetest (date_col) values (?)", test_vals)            

if __name__ == '__main__':
    unittest.main()