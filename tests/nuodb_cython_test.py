# -*- coding: utf-8 -*-
"""Verify that the Cython acceleration extension is built and wired in.

(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import pytest

import pynuodb
import pynuodb.result_set as _rs

from . import nuodb_base


def test_fetch_extension_importable():
    """The compiled extension module must be importable."""
    import pynuodb._fetch  # noqa: F401  pylint: disable=unused-import


def test_result_set_is_cython():
    """pynuodb.result_set.ResultSet must be the Cython class, not the
    pure-Python fallback."""
    assert _rs.ResultSet.__module__ == 'pynuodb._fetch', (
        "ResultSet came from %s; the Cython extension is not active"
        % (_rs.ResultSet.__module__,))


def test_decode_next_batch_exported():
    """The batch decoder used by EncodedSession must be exported."""
    import pynuodb._fetch as _fetch
    assert callable(getattr(_fetch, 'decode_next_batch', None))


_MIXED_TYPES_QUERY = """
    select cast(42 as int),
           cast('hello' as varchar(16)),
           cast(3.5 as double),
           cast(99.95 as decimal(10,2)),
           cast('2024-01-15' as date),
           cast('12:34:56' as time),
           cast('2024-01-15 12:34:56' as timestamp),
           true,
           null
      from system.dual
    union all
    select cast(-1 as int),
           cast('naive cafe' as varchar(16)),
           cast(0.0 as double),
           cast(0.00 as decimal(10,2)),
           cast('1970-01-01' as date),
           cast('00:00:00' as time),
           cast('1970-01-01 00:00:00' as timestamp),
           false,
           null
      from system.dual
"""


class TestNuoDBCython(nuodb_base.NuoBase):
    def test_cython_matches_pure_python(self):
        """fetchall() results must be byte-identical between the Cython
        decode path and the pure-Python fallback, across one value of
        every wire type the fast path covers."""
        from pynuodb import encodedsession

        if not getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            pytest.skip("Cython extension not loaded; nothing to compare")

        def run_query():
            con = self._connect()
            try:
                cursor = con.cursor()
                cursor.execute(_MIXED_TYPES_QUERY)
                return cursor.fetchall()
            finally:
                con.close()

        cython_rows = run_query()

        encodedsession._HAVE_FETCH_ACCEL = False
        try:
            python_rows = run_query()
        finally:
            encodedsession._HAVE_FETCH_ACCEL = True

        assert cython_rows == python_rows

    def test_cython_matches_pure_python_multi_batch(self):
        """A result set big enough to span several server batches must
        decode identically under Cython and pure Python.  This is the
        actual hot path the PR optimises (fetch_result_set_next called
        repeatedly)."""
        from pynuodb import encodedsession

        if not getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            pytest.skip("Cython extension not loaded; nothing to compare")

        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute("DROP TABLE IF EXISTS cython_ten")
            cursor.execute("CREATE TABLE cython_ten (f1 INTEGER)")
            cursor.execute(
                "INSERT INTO cython_ten"
                " VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
            con.commit()
        finally:
            con.close()

        # 10^4 rows -- well above any plausible single-batch size.
        query = ("SELECT a.f1, b.f1, c.f1, d.f1"
                 "  FROM cython_ten AS a, cython_ten AS b,"
                 "       cython_ten AS c, cython_ten AS d"
                 " ORDER BY a.f1, b.f1, c.f1, d.f1")

        def run_query():
            con2 = self._connect()
            try:
                cursor = con2.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            finally:
                con2.close()

        try:
            cython_rows = run_query()

            encodedsession._HAVE_FETCH_ACCEL = False
            try:
                python_rows = run_query()
            finally:
                encodedsession._HAVE_FETCH_ACCEL = True

            assert len(cython_rows) == 10000
            assert cython_rows == python_rows
        finally:
            con = self._connect()
            try:
                con.cursor().execute("DROP TABLE IF EXISTS cython_ten")
                con.commit()
            finally:
                con.close()

    def test_empty_result_set(self):
        """fetchall() on a query returning zero rows must work through
        the Cython decode path (first batch arrives with complete=True
        and no rows)."""
        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute(
                "select 1 from system.dual where 1 = 0")
            assert cursor.fetchall() == []
        finally:
            con.close()

    def test_bool_and_null_singletons(self):
        """Regression: an earlier revision of _fetch.pyx evaluated
        <PyObject*>True at compile time, casting the Python bool literal
        to int and yielding a junk pointer (segfault on any SELECT with
        a boolean column).  The fix takes the address of an object-typed
        local instead.  Guard against the bug returning if the .pyx is
        ever refactored."""
        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute("select true, false, null from system.dual")
            assert cursor.fetchall() == [(True, False, None)]
        finally:
            con.close()

    def test_exotic_type_bridge(self):
        """Types the Cython fast path doesn't inline (e.g. VECTOR) must
        round-trip via the _cython_exotic_decode bridge back into Python's
        getValue().  VECTOR is the user-facing exotic type per the
        _fetch.pyx module docstring."""
        from pynuodb.datatype import Vector
        payload = Vector(Vector.DOUBLE, [0.0, 4.0, 5.0])
        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute(
                "select cast(? as vector(3, double)) from system.dual",
                [payload])
            row = cursor.fetchone()
            assert list(row[0]) == [0.0, 4.0, 5.0]
        finally:
            con.close()

    def test_fetchone_through_cython(self):
        """fetchall() goes through cursor's batch-drain path; fetchone()
        is what actually invokes the Cython ResultSet.fetchone cpdef.
        Make sure that path works too."""
        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute(
                "select 1 from system.dual"
                " union all select 2 from system.dual"
                " union all select 3 from system.dual")
            seen = []
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                seen.append(row)
            assert seen == [(1,), (2,), (3,)]
        finally:
            con.close()

    def test_integer_wire_encodings(self):
        """Each NuoDB integer wire encoding (INT0..INTLEN8) gets exercised
        by a different magnitude.  Make sure the Cython int decoder
        returns the same value as the Python one for boundary values."""
        from pynuodb import encodedsession

        values = [0, 1, -1, 127, -128, 128, -129,
                  32767, -32768, 65535,
                  2**31 - 1, -(2**31), 2**31,
                  2**62, -(2**62)]
        select_parts = ["select cast(%d as bigint) from system.dual" % v
                        for v in values]
        query = " union all ".join(select_parts)

        def run_query():
            con = self._connect()
            try:
                cursor = con.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            finally:
                con.close()

        cython_rows = run_query()
        assert [r[0] for r in cython_rows] == values

        if getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            encodedsession._HAVE_FETCH_ACCEL = False
            try:
                python_rows = run_query()
            finally:
                encodedsession._HAVE_FETCH_ACCEL = True
            assert cython_rows == python_rows
