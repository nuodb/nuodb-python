"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import decimal

from . import nuodb_base


class TestNuoDBStatementManagement(nuodb_base.NuoBase):
    def test_stable_statement(self):
        con = self._connect()
        cursor = con.cursor()
        init_handle = extract_statement_handle(cursor)
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double precision)")

            cursor.execute("insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, "
                           "double_col) values (0, 0, 0, 0, 0, 0)")

            con.commit()

            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()

            for i in range(1, len(row)):
                assert row[i] == 0

            current_handle = extract_statement_handle(cursor)
            assert init_handle == current_handle

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_statement_per_cursor(self):
        con = self._connect()
        try:
            cursor1 = con.cursor()
            cursor2 = con.cursor()
            cursor3 = con.cursor()

            assert extract_statement_handle(cursor1) != extract_statement_handle(cursor2)
            assert extract_statement_handle(cursor2) != extract_statement_handle(cursor3)
            assert extract_statement_handle(cursor1) != extract_statement_handle(cursor3)

        finally:
            con.close()

    def test_prepared_statement_cache(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double)")

            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'),
                         10000.999)
            query = "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, " \
                    "double_col) values (?, ?, ?, ?, ?, ?)"

            cursor.execute(query, test_vals)

            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()

            for i in range(1, len(row)):
                assert row[i] == test_vals[i - 1]

            ps_cache = extract_prepared_statement_dict(cursor)
            assert len(ps_cache) == 1
            assert query in ps_cache

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_prepared_statement_cache_should_not_grow(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double)")

            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'),
                         10000.999)
            query = "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, " \
                    "double_col) values (?, ?, ?, ?, ?, ?)"

            for _ in range(0, 20):
                cursor.execute(query, test_vals)

            cursor.execute("select * from typetest order by id desc limit 1")
            row = cursor.fetchone()

            for i in range(1, len(row)):
                assert row[i] == test_vals[i - 1]

            ps_cache = extract_prepared_statement_dict(cursor)
            assert len(ps_cache) == 1
            assert query in ps_cache

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_prepared_statement_cache_stable(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double)")

            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'),
                         10000.999)
            query = "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, " \
                    "double_col) values (?, ?, ?, ?, ?, ?)"

            handle = None
            for _ in range(0, 20):
                cursor.execute(query, test_vals)

                cursor.execute("select * from typetest order by id desc limit 1")
                row = cursor.fetchone()

                for i in range(1, len(row)):
                    assert row[i] == test_vals[i - 1]

                ps_cache = extract_prepared_statement_dict(cursor)
                assert len(ps_cache) == 1
                assert query in ps_cache
                if handle is None:
                    handle = ps_cache[query].handle
                else:
                    assert handle == ps_cache[query].handle

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_prepared_statement_cache_should_grow(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double)")

            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'),
                         10000.999)

            queries = ["insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, "
                       "double_col) values (?, ?, ?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col) values "
                       "(?, ?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col) values (?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col) values (?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col) values (?, ?)",
                       "insert into typetest (smallint_col) values (?)"]

            for _ in range(0, 10):
                for i in range(0, len(queries)):
                    cursor.execute(queries[i], test_vals[0:len(queries) - i])

            ps_cache = extract_prepared_statement_dict(cursor)
            assert len(queries) == len(ps_cache)
            for query in queries:
                assert query in ps_cache

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_prepared_statement_cache_eviction(self):
        con = self._connect()
        cache_size = 5
        cursor = con.cursor(prepared_statement_cache_size=cache_size)
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double)")

            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'),
                         10000.999)

            queries = ["insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, "
                       "double_col) values (?, ?, ?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col) values "
                       "(?, ?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col) values (?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col) values (?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col) values (?, ?)",
                       "insert into typetest (smallint_col) values (?)"]

            for i in range(0, len(queries)):
                cursor.execute(queries[i], test_vals[0:len(queries) - i])

            ps_cache = extract_prepared_statement_dict(cursor)
            assert cache_size == len(ps_cache)
            for query in queries[len(queries) - cache_size:]:
                assert query in ps_cache

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()

    def test_prepared_statement_cache_eviction_lru(self):
        con = self._connect()
        cache_size = 4
        cursor = con.cursor(prepared_statement_cache_size=cache_size)
        cursor.execute("drop table typetest if exists")
        try:
            cursor.execute("create table typetest (id integer GENERATED ALWAYS AS IDENTITY, smallint_col smallint, "
                           "integer_col integer, bigint_col bigint, numeric_col numeric(10, 2), "
                           "decimal_col decimal(10, 2), double_col double)")

            test_vals = (3424, 23453464, 45453453454545, decimal.Decimal('234355.33'), decimal.Decimal('976.2'),
                         10000.999)

            queries = ["insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col, "
                       "double_col) values (?, ?, ?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col, decimal_col) values "
                       "(?, ?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col, numeric_col) values (?, ?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col, bigint_col) values (?, ?, ?)",
                       "insert into typetest (smallint_col, integer_col) values (?, ?)",
                       "insert into typetest (smallint_col) values (?)"]

            query_order = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
                           3, 1, 4, 5, 1, 4, 2, 3, 1, 5, 4, 3, 5, 1,
                           5, 1, 5, 3, 1, 2, 1, 1]
            for i in query_order:
                cursor.execute(queries[i], test_vals[0:len(queries) - i])

            ps_cache = extract_prepared_statement_dict(cursor)
            assert cache_size == len(ps_cache)
            for query in [queries[1], queries[2], queries[3], queries[5]]:
                assert query in ps_cache

            for query in [queries[0], queries[4]]:
                assert query not in ps_cache

        finally:
            try:
                cursor.execute("drop table typetest if exists")
            finally:
                con.close()


def extract_statement_handle(cursor):
    return cursor._statement_cache._statement.handle


def extract_prepared_statement_dict(cursor):
    return cursor._statement_cache._ps_cache
