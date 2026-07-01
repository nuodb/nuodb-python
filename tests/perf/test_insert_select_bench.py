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

import math
import time

import pytest

from tests import nuodb_base


# Small tests have tiny absolute times (sub-ms to a few ms), so per-round
# jitter dominates unless we run *both* many rounds *and* for a long enough
# total wall-time to average out kernel/network noise.  This helper probes
# a single call to size `rounds` so that rounds * per_call_time >= min_seconds,
# with a floor of `min_rounds`.
def _rounds_for(target, min_rounds, min_seconds, setup=None, probes=5,
                iterations=1):
    total = 0.0
    for _ in range(probes):
        if setup is not None:
            setup()
        t0 = time.perf_counter()
        target()
        total += time.perf_counter() - t0
    per_call = total / probes
    if per_call <= 0:
        return min_rounds
    per_round = per_call * iterations
    return max(min_rounds, int(math.ceil(min_seconds / per_round)))


# Skip this whole module unless `--run-perf` is passed on the pytest
# command line.  We don't want `make fulltest` to sit through a 100k-row
# insert on every commit.
pytestmark = pytest.mark.perf


_DDL_DROP     = "DROP TABLE IF EXISTS perf_bench"
_DDL_CREATE   = "CREATE TABLE perf_bench (a INT, b VARCHAR(64))"
_DDL_TRUNCATE = "TRUNCATE TABLE perf_bench"

_SMALL = 100
_LARGE = 20_000


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

            def setup():
                # Runs before each round but is NOT included in the timing.
                cur.execute(_DDL_TRUNCATE)
                con.commit()

            rounds = _rounds_for(target, min_rounds=500, min_seconds=10.0,
                                 setup=setup, iterations=10)
            benchmark.pedantic(target, setup=setup, warmup_rounds=5,
                               rounds=rounds, iterations=10)
        finally:
            con.close()

    def test_insert_large(self, benchmark):
        """20k rows via executemany """
        con = self._connect()
        try:
            self._reset(con)
            cur = con.cursor()
            rows = _rows(_LARGE)

            def target():
                cur.executemany(
                    "INSERT INTO perf_bench (a, b) VALUES (?, ?)", rows)
                con.commit()

            def setup():
                cur.execute(_DDL_TRUNCATE)
                con.commit()

            benchmark.pedantic(target, setup=setup, warmup_rounds=2,
                               rounds=100,
                               iterations=1)
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

            rounds = _rounds_for(target, min_rounds=500, min_seconds=10.0,
                                 iterations=10)
            rows = benchmark.pedantic(target, warmup_rounds=5, rounds=rounds,
                                      iterations=10)
            assert len(rows) == _SMALL
        finally:
            con.close()

    def test_fetchall_large(self, benchmark):
        """fetchall over 20k rows. """
        con = self._connect()
        try:
            self._seed(con, _LARGE)
            cur = con.cursor()

            def target():
                cur.execute("SELECT a, b FROM perf_bench")
                return cur.fetchall()

            rows = benchmark.pedantic(target, warmup_rounds=2, rounds=100,
                                      iterations=1)
            assert len(rows) == _LARGE
        finally:
            con.close()

    def test_fetchmany_large(self, benchmark):
        """fetchmany(1000) over 20k rows"""
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

            total = benchmark.pedantic(target, warmup_rounds=2, rounds=100,
                                       iterations=1)
            assert total == _LARGE
        finally:
            con.close()

    def test_fetchone_loop_large(self, benchmark):
        """fetchone() in a loop over 20k rows.  Isolates per-row
        overhead """
        con = self._connect()
        try:
            self._seed(con, _LARGE)
            cur = con.cursor()

            def target():
                cur.execute("SELECT a, b FROM perf_bench")
                n = 0
                while True:
                    row = cur.fetchone()
                    if row is None:
                        break
                    n += 1
                return n

            n = benchmark.pedantic(target, warmup_rounds=2, rounds=100,
                                   iterations=1)
            assert n == _LARGE
        finally:
            con.close()

    # -- Wide rows / mixed types ---------------------------------------

    _WIDE_COLS = 50
    _WIDE_ROWS = 1000

    def test_fetchall_wide(self, benchmark):
        """50 columns x 1000 rows """
        cols = ["c%d INT" % i for i in range(self._WIDE_COLS)]
        col_names = ", ".join("c%d" % i for i in range(self._WIDE_COLS))
        placeholders = ", ".join(["?"] * self._WIDE_COLS)

        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS perf_wide")
            cur.execute("CREATE TABLE perf_wide (%s)" % (", ".join(cols),))
            con.commit()
            rows = [tuple(range(self._WIDE_COLS)) for _ in range(self._WIDE_ROWS)]
            cur.executemany(
                "INSERT INTO perf_wide (%s) VALUES (%s)"
                % (col_names, placeholders),
                rows)
            con.commit()

            def target():
                cur.execute("SELECT %s FROM perf_wide" % col_names)
                return cur.fetchall()

            result = benchmark.pedantic(target, warmup_rounds=2, rounds=100,
                                        iterations=1)
            assert len(result) == self._WIDE_ROWS
            assert len(result[0]) == self._WIDE_COLS
        finally:
            con.close()

    def test_fetchall_mixed_types(self, benchmark):
        """SELECT with variety of types: int / decimal / double / timestamp / bool /
        varchar / null. """
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS perf_mixed")
            cur.execute(
                "CREATE TABLE perf_mixed ("
                "  i INT,"
                "  d DECIMAL(12, 4),"
                "  f DOUBLE,"
                "  ts TIMESTAMP,"
                "  bl BOOLEAN,"
                "  s VARCHAR(64),"
                "  n INT"
                ")")
            con.commit()
            rows = [
                (i, i * 1.25, i / 3.0,
                 '2024-01-01 12:34:56', bool(i & 1),
                 'row #%d' % i, None)
                for i in range(_LARGE)
            ]
            cur.executemany(
                "INSERT INTO perf_mixed (i, d, f, ts, bl, s, n)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows)
            con.commit()

            def target():
                cur.execute("SELECT i, d, f, ts, bl, s, n FROM perf_mixed")
                return cur.fetchall()

            result = benchmark.pedantic(target, warmup_rounds=2, rounds=100,
                                        iterations=1)
            assert len(result) == _LARGE
        finally:
            con.close()
