# -*- coding: utf-8 -*-
"""Verify that the Cython acceleration extension is built and wired in.

(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import pytest

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
    def test_fetchall_uses_cython_decoder(self):
        """End-to-end: a SELECT returning a mix of types must round-trip
        correctly through the Cython decode path."""
        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute(
                "select 1, 'hello', cast(3.5 as double), true, null"
                " from system.dual")
            rows = cursor.fetchall()
            assert rows == [(1, 'hello', 3.5, True, None)]
        finally:
            con.close()

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
