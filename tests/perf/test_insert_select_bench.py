# -*- coding: utf-8 -*-
"""Insert / select micro-benchmarks.

(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

Ported from test-performance/timesInsert.py so the numbers live alongside
the correctness suite and can be run via `pytest --benchmark-only`.

Each test measures one operation the driver's hot paths care about:

    * bulk INSERT via executemany (encode path, session send)
    * fetchall over a small result set (decode + per-row dispatch)
    * fetchall over a large result set (batched decode + refill loop)

pytest-benchmark auto-repeats each function and reports min / mean /
median / stddev.  Numbers move meaningfully with the crypt, session,
cursor and Cython PRs; that's the point.
"""

import pytest

from tests import nuodb_base


# Skip this whole module unless `--run-perf` is passed on the pytest
# command line.  We don't want `make fulltest` to sit through a 100k-row
# insert on every commit.
pytestmark = pytest.mark.perf


_DDL_DROP   = "DROP TABLE IF EXISTS perf_bench"
_DDL_CREATE = "CREATE TABLE perf_bench (a INT, b VARCHAR(64))"

_SMALL = 100
_LARGE = 100_000


def _rows(n):
    return [(i, 'A dark and stormy night %d' % i) for i in range(n)]


class TestInsertSelectPerf(nuodb_base.NuoBase):

    def _reset(self, con):
        cur = con.cursor()
        cur.execute(_DDL_DROP)
        cur.execute(_DDL_CREATE)
        con.commit()

    def _seed(self, con, n):
        self._reset(con)
        con.cursor().executemany(
            "INSERT INTO perf_bench (a, b) VALUES (?, ?)", _rows(n))
        con.commit()

    # -- INSERT ---------------------------------------------------------

    def test_insert_small(self, benchmark):
        """100 rows via executemany.  Sensitive to per-row putValue cost."""
        con = self._connect()
        try:
            self._reset(con)
            cur = con.cursor()
            rows = _rows(_SMALL)

            def target():
                cur.executemany(
                    "INSERT INTO perf_bench (a, b) VALUES (?, ?)", rows)
                con.commit()
                # Truncate between iterations so we measure a clean insert.
                cur.execute(_DDL_DROP)
                cur.execute(_DDL_CREATE)
                con.commit()

            benchmark(target)
        finally:
            con.close()

    def test_insert_large(self, benchmark):
        """100k rows via executemany """
        con = self._connect()
        try:
            self._reset(con)
            cur = con.cursor()
            rows = _rows(_LARGE)

            def target():
                cur.executemany(
                    "INSERT INTO perf_bench (a, b) VALUES (?, ?)", rows)
                con.commit()
                cur.execute(_DDL_DROP)
                cur.execute(_DDL_CREATE)
                con.commit()

            # Large insert is slow; cap repetitions so a benchmark run
            # finishes in seconds rather than minutes.
            benchmark.pedantic(target, rounds=3, iterations=1)
        finally:
            con.close()

    # -- SELECT ---------------------------------------------------------

    def test_fetchall_small(self, benchmark):
        """fetchall over 100 rows.  Sensitive to fixed per-query overhead."""
        con = self._connect()
        try:
            self._seed(con, _SMALL)
            cur = con.cursor()

            def target():
                cur.execute("SELECT a, b FROM perf_bench")
                return cur.fetchall()

            rows = benchmark(target)
            assert len(rows) == _SMALL
        finally:
            con.close()

    def test_fetchall_large(self, benchmark):
        """fetchall over 100k rows. """
        con = self._connect()
        try:
            self._seed(con, _LARGE)
            cur = con.cursor()

            def target():
                cur.execute("SELECT a, b FROM perf_bench")
                return cur.fetchall()

            rows = benchmark.pedantic(target, rounds=5, iterations=1)
            assert len(rows) == _LARGE
        finally:
            con.close()

    def test_fetchmany_large(self, benchmark):
        """fetchmany(1000) over 100k rows"""
        con = self._connect()
        try:
            self._seed(con, _LARGE)
            cur = con.cursor()

            def target():
                cur.execute("SELECT a, b FROM perf_bench")
                total = 0
                while True:
                    batch = cur.fetchmany(1000)
                    if not batch:
                        break
                    total += len(batch)
                return total

            total = benchmark.pedantic(target, rounds=5, iterations=1)
            assert total == _LARGE
        finally:
            con.close()
