# -*- coding: utf-8 -*-
"""Verify that the Cython acceleration extension is built and wired in.

(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import decimal
import random
import struct

import pytest

import pynuodb
import pynuodb.result_set as _rs
from pynuodb import protocol as _protocol

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


def _no_exotic(pos):
    raise AssertionError("exotic_fn should not be called by these buffers")


def test_decode_next_batch_rejects_truncated_length_prefix():
    """A length-prefix byte count that runs past the end of the buffer
    (OPAQUECOUNT/UTF8COUNT/etc.) must raise EndOfStream, not read past the
    buffer. OPAQUECOUNT0+2 = 74 claims a 2-byte length prefix but the buffer
    ends right after the type code."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import EndOfStream

    buf = bytearray([51, 74])
    with pytest.raises(EndOfStream):
        _fetch.decode_next_batch(buf, 0, 1, [], _no_exotic, None)


def test_decode_next_batch_rejects_truncated_payload():
    """A length prefix that's valid on its own but whose claimed payload
    runs past the end of the buffer must raise EndOfStream. UTF8LEN0+5 = 114
    claims 5 payload bytes; only 2 are present."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import EndOfStream

    buf = bytearray([51, 114, ord('h'), ord('i')])
    with pytest.raises(EndOfStream):
        _fetch.decode_next_batch(buf, 0, 1, [], _no_exotic, None)


def test_decode_next_batch_propagates_invalid_utf8():
    """Malformed UTF-8 in a string cell must raise UnicodeDecodeError
    instead of silently storing a NULL pointer into the row tuple.
    UTF8LEN0+2 = 111 claims 2 payload bytes; 0xFF is not a valid UTF-8
    start byte."""
    import pynuodb._fetch as _fetch

    buf = bytearray([51, 111, 0xFF, 0xFE])
    with pytest.raises(UnicodeDecodeError):
        _fetch.decode_next_batch(buf, 0, 1, [], _no_exotic, None)


def test_decode_next_batch_rejects_code_zero():
    """Wire code 0 has no defined meaning in protocol.py, and pure-Python
    getValue() raises DataError for it. The NULL/TRUE/FALSE fast path
    (`code <= FALSE_V`) must not also match 0 and silently decode it as
    False -- it must fall through to exotic_fn like every other
    unrecognized code."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import DataError

    def raise_data_error(pos):
        raise DataError("getValue: Invalid type code: 0")

    buf = bytearray([51, 0])   # marker (nonzero inline), then type code 0
    with pytest.raises(DataError):
        _fetch.decode_next_batch(buf, 0, 1, [], raise_data_error, None)


def test_decode_next_batch_row_marker_exotic_fn_must_advance():
    """A non-inline row marker (outside INTMINUS10..INT31) is decoded via
    exotic_fn. If exotic_fn ever returned without advancing pos, the outer
    `while pos < n` loop would never terminate. Must raise instead of
    hanging."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import EndOfStream

    def stuck_exotic_fn(pos):
        return 1, pos   # does not advance

    buf = bytearray([5])   # 5 is outside INTMINUS10(10)..INT31(51): non-inline marker
    with pytest.raises(EndOfStream):
        _fetch.decode_next_batch(buf, 0, 1, [], stuck_exotic_fn, None)


def test_decode_next_batch_zero_marker_via_exotic_fn_ends_batch():
    """The reference decoder treats *any* integer-valued row marker of 0 as
    end-of-batch (`if self.getInt() == 0: complete = True`), including one
    encoded as INTLEN1 rather than the inline zero code. The Cython path
    routes non-inline markers through exotic_fn but never checked the
    decoded value against 0, so this case fell through to decoding a row
    out of whatever bytes happened to follow."""
    import pynuodb._fetch as _fetch

    def zero_via_intlen1(pos):
        return 0, pos + 2   # simulates getInt() consuming INTLEN1(52) + value byte 0x00

    buf = bytearray([52, 0])   # INTLEN1(52), value byte 0x00
    results = []
    pos, complete = _fetch.decode_next_batch(buf, 0, 1, results, zero_via_intlen1, None)
    assert complete is True
    assert results == []
    assert pos == 2


def test_decode_next_batch_non_integer_marker_raises():
    """A row marker that isn't an integer-shaped code (10-59) must raise,
    matching the reference decoder's getInt(), which raises DataError for
    any other code rather than accepting whatever getValue() would have
    decoded it as."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import DataError

    def non_integer_marker(pos):
        return "not an int", pos + 1

    buf = bytearray([200])   # any non-inline code; the stub ignores it
    with pytest.raises(DataError):
        _fetch.decode_next_batch(buf, 0, 1, [], non_integer_marker, None)


def test_decode_next_batch_scaled_date_rejects_one_byte_short_buffer():
    """_read_scaled_operand is called with `pos` still pointing at the
    (unconsumed) type-code byte: the scale byte is at pos+1 and the data
    bytes run through pos+nbytes+1. A buffer that is exactly one byte too
    short to hold the last data byte must raise EndOfStream. Before the
    fix, _check_avail(pos, 1+nbytes, n) was satisfied by a buffer one byte
    short of what the helper actually reads, an out-of-bounds read on a
    truncated buffer."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import EndOfStream
    from pynuodb import protocol as _protocol

    nbytes = 4
    code = _protocol.SCALEDDATELEN0 + nbytes
    full = bytearray([51, code, 2, 0x00, 0x01, 0x02, 0x03])  # marker, code, scale, 4 data bytes
    truncated = full[:-1]   # missing the last data byte
    with pytest.raises(EndOfStream):
        _fetch.decode_next_batch(truncated, 0, 1, [], _no_exotic, None)


# --- Synthetic wire-buffer encoder, used only by the truncation fuzz test
# below. Independent of EncodedSession's own encoder (putValue() etc.) --
# it builds raw bytes directly from protocol.py's constants, matching the
# formats decode_next_batch documents for each branch.

def _min_signed_bytes(value):
    nbytes = 1
    while True:
        try:
            value.to_bytes(nbytes, 'big', signed=True)
            return nbytes
        except OverflowError:
            nbytes += 1


def _encode_null():
    return bytes([_protocol.NULL])


def _encode_bool(value):
    return bytes([_protocol.TRUE if value else _protocol.FALSE])


def _encode_int(value):
    if -10 <= value <= 31:
        return bytes([_protocol.INT0 + value])
    nbytes = _min_signed_bytes(value)
    return bytes([_protocol.INTLEN0 + nbytes]) + value.to_bytes(nbytes, 'big', signed=True)


def _encode_str(value):
    payload = value.encode('utf-8')
    if len(payload) <= 39:
        return bytes([_protocol.UTF8LEN0 + len(payload)]) + payload
    nbytes = max(1, -(-len(payload).bit_length() // 8))
    header = bytes([_protocol.UTF8COUNT0 + nbytes])
    length_bytes = len(payload).to_bytes(nbytes, 'big')
    return header + length_bytes + payload


def _encode_bytes(value):
    if len(value) <= 39:
        return bytes([_protocol.OPAQUELEN0 + len(value)]) + value
    nbytes = max(1, -(-len(value).bit_length() // 8))
    header = bytes([_protocol.OPAQUECOUNT0 + nbytes])
    length_bytes = len(value).to_bytes(nbytes, 'big')
    return header + length_bytes + value


def _encode_double(value):
    return bytes([_protocol.DOUBLELEN0 + 8]) + struct.pack('>d', value)


def _encode_uuid(raw16):
    return bytes([_protocol.UUID]) + raw16


def _encode_scaled(value, scale):
    nbytes = _min_signed_bytes(value)
    header = bytes([_protocol.SCALEDLEN0 + nbytes, scale & 0xFF])
    return header + value.to_bytes(nbytes, 'big', signed=True)


def _encode_row(col_bytes_list):
    marker = bytes([_protocol.INT0 + 1])   # any non-zero inline marker: a row follows
    return marker + b''.join(col_bytes_list)


def _encode_batch(rows):
    body = b''.join(_encode_row(r) for r in rows)
    end_marker = bytes([_protocol.INT0])
    return bytearray(body + end_marker)


def test_decode_next_batch_truncation_never_misbehaves():
    """Truncating a valid multi-row, multi-type buffer at every possible
    length must never do anything but raise a clean exception or return
    normally -- never read past the buffer, never crash. This sweeps the
    bounds check ahead of every variable-length read across every column
    type, truncation point, and column ordering, not just the handful of
    hand-picked cases in the tests above."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import EndOfStream

    columns = [
        _encode_null(),
        _encode_bool(True),
        _encode_bool(False),
        _encode_int(5),
        _encode_int(-12345),
        _encode_int(9876543210),
        _encode_str(''),
        _encode_str('short'),
        _encode_str('x' * 80),
        _encode_bytes(b''),
        _encode_bytes(b'\x00\x01\x02short'),
        _encode_bytes(b'\xff' * 80),
        _encode_double(3.5),
        _encode_uuid(bytes(range(16))),
        _encode_scaled(123456789, 4),
    ]

    rng = random.Random(20260910)
    for _ in range(15):
        cols = columns[:]
        rng.shuffle(cols)
        full = _encode_batch([cols, cols])
        for trunc_len in range(len(full)):
            buf = bytearray(full[:trunc_len])
            try:
                pos, _complete = _fetch.decode_next_batch(
                    buf, 0, len(cols), [], _no_exotic, None)
                assert 0 <= pos <= len(buf)
            except (EndOfStream, UnicodeDecodeError):
                pass


def _encode_int_width(value, nbytes):
    """Like _encode_int, but forces a specific INTLEN width (1-8) instead
    of picking the minimal one, so a small value can still exercise every
    width _pynuodb_be_i64 supports."""
    return bytes([_protocol.INTLEN0 + nbytes]) + value.to_bytes(nbytes, 'big', signed=True)


def test_decode_next_batch_signed_int_matches_reference():
    """_pynuodb_be_i64 now derives every width from _pynuodb_be_u64 plus
    one branchless sign-extension formula ((v ^ m) - m) instead of a
    hand-unrolled switch per width, with cases 3/5/6/7 no longer even
    special-cased in _pynuodb_be_u64. Sweep INTLEN1..INTLEN8 (every
    nbytes 1-8), including each width's min/max boundary values where the
    sign bit sits right at the edge of the mask, and compare against
    Python's own signed big-endian decode."""
    import pynuodb._fetch as _fetch

    rng = random.Random(20260910)
    for nbytes in range(1, 9):
        lo, hi = -(1 << (nbytes * 8 - 1)), (1 << (nbytes * 8 - 1)) - 1
        candidates = {lo, hi, lo + 1, hi - 1, 0}
        candidates |= {rng.randint(lo, hi) for _ in range(50)}
        for value in candidates:
            col = _encode_int_width(value, nbytes)
            buf = _encode_batch([[col]])
            results = []
            _fetch.decode_next_batch(buf, 0, 1, results, _no_exotic, None)
            assert results[0][0] == value, (nbytes, value, results[0][0])


def test_decode_next_batch_scaled_decimal_matches_reference():
    """_make_decimal now builds the Decimal from a formatted string
    ("%dE%d" % (value, -scale)) instead of a manually-built digit tuple.
    Sweep values across every real nbytes width (1-8 signed bytes) and a
    range of scale-byte values (the wire scale byte is unsigned, 0-255),
    and compare the decoded Decimal -- both by == and by as_tuple(), which
    would catch a same-value-different-representation divergence that ==
    alone would miss -- against decimal.Decimal built the same way the
    reference decoder's getScaledInt() does it (encodedsession.py),
    independently of _fetch.pyx."""
    import pynuodb._fetch as _fetch

    def reference_decimal(value, scale):
        sign = 1 if value < 0 else 0
        digits = tuple(int(c) for c in str(abs(value)))
        return decimal.Decimal((sign, digits, -scale))

    rng = random.Random(20260910)
    values = []
    for nbytes in range(1, 9):
        lo, hi = -(1 << (nbytes * 8 - 1)), (1 << (nbytes * 8 - 1)) - 1
        values += [lo, hi, 0]
        values += [rng.randint(lo, hi) for _ in range(20)]
    scales = [0, 1, 6, 127, 255] + [rng.randint(0, 255) for _ in range(10)]

    for value in values:
        for scale in scales:
            col = _encode_scaled(value, scale)
            buf = _encode_batch([[col]])
            results = []
            _fetch.decode_next_batch(buf, 0, 1, results, _no_exotic, None)
            got = results[0][0]
            expected = reference_decimal(value, scale)
            assert got.as_tuple() == expected.as_tuple(), (value, scale, got, expected)
            assert got == expected


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

    def test_cython_matches_pure_python_blob_clob(self):
        """BLOB/CLOB columns must decode identically under Cython and pure
        Python, across an empty value, a short value (inline OPAQUELEN/
        UTF8LEN encoding), and a long value (counted OPAQUECOUNT/UTF8COUNT
        encoding, >39 bytes/chars)."""
        from pynuodb import encodedsession

        if not getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            pytest.skip("Cython extension not loaded; nothing to compare")

        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute("DROP TABLE IF EXISTS cython_blob_clob")
            cursor.execute("CREATE TABLE cython_blob_clob (b BLOB, c CLOB)")
            rows = [
                (pynuodb.Binary(b''), ''),
                (pynuodb.Binary(b'short blob'), 'short clob'),
                (pynuodb.Binary(b'x' * 500), 'y' * 500),
            ]
            cursor.executemany(
                "INSERT INTO cython_blob_clob (b, c) VALUES (?, ?)", rows)
            con.commit()
        finally:
            con.close()

        def run_query():
            con2 = self._connect()
            try:
                cursor = con2.cursor()
                cursor.execute(
                    "SELECT b, c FROM cython_blob_clob ORDER BY LENGTH(c)")
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

            assert len(cython_rows) == 3
            assert cython_rows == python_rows
            assert [type(r[0]) for r in cython_rows] == [type(r[0]) for r in python_rows]
        finally:
            con = self._connect()
            try:
                con.cursor().execute("DROP TABLE IF EXISTS cython_blob_clob")
                con.commit()
            finally:
                con.close()

    def test_cython_matches_pure_python_multi_batch(self):
        """A result set big enough to span several server batches must
        decode identically under Cython and pure Python -- this exercises
        fetch_result_set_next() called repeatedly, not just the first
        batch."""
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
        a boolean column). The current code gets None/True/False from
        _pynuodb_none()/_pynuodb_true()/_pynuodb_false() in _cutil.h,
        which sidesteps the issue entirely by never doing that cast in
        Cython at all."""
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
        getValue(). VECTOR is used here because, unlike most of the other
        exotic codes, it's directly reachable through the public DB-API
        (cast(... as vector(...))."""
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

    def test_decode_next_batch_random_value_fuzz(self):
        """Random values across every column type in one batch, decoded
        through both the Cython and pure-Python paths, must match exactly.
        Many random rows per run, covering both the inline/counted
        boundary (39/40 bytes or chars) and the NULL-substitution path
        for every column, rather than one fixed row of hand-picked
        values."""
        from pynuodb import encodedsession

        if not getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            pytest.skip("Cython extension not loaded; nothing to compare")

        rng = random.Random(20260910)
        kinds = ['int', 'str', 'double', 'decimal', 'bool', 'blob', 'clob']
        num_rows = 40

        def random_value(kind):
            if kind == 'int':
                magnitude = rng.choice([10, 100, 1000, 10 ** 6, 10 ** 9,
                                        10 ** 15, 2 ** 62])
                return rng.randint(-magnitude, magnitude)
            if kind == 'str':
                length = rng.choice([0, 1, 5, 39, 40, 41, 100, 300])
                alphabet = ('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
                            '0123456789 ')
                return ''.join(rng.choice(alphabet) for _ in range(length))
            if kind == 'double':
                return rng.choice([0.0, -0.0, 1.5, -2.25, 3.14159265358979,
                                   rng.uniform(-1e10, 1e10)])
            if kind == 'decimal':
                sign = '-' if rng.random() < 0.5 else ''
                whole = rng.randint(0, 10 ** 12)
                frac = rng.randint(0, 9999)
                return decimal.Decimal('%s%d.%04d' % (sign, whole, frac))
            if kind == 'bool':
                return rng.choice([True, False])
            if kind == 'blob':
                length = rng.choice([0, 1, 39, 40, 41, 100, 300])
                return pynuodb.Binary(bytes(rng.randrange(256)
                                            for _ in range(length)))
            if kind == 'clob':
                length = rng.choice([0, 1, 39, 40, 41, 100, 300])
                alphabet = 'abcdefghijklmnopqrstuvwxyz '
                return ''.join(rng.choice(alphabet) for _ in range(length))
            raise AssertionError(kind)

        rows = []
        for i in range(num_rows):
            row = [None if rng.random() < 0.1 else random_value(kind)
                   for kind in kinds]
            rows.append(tuple(row) + (i,))

        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute("DROP TABLE IF EXISTS cython_fuzz")
            cursor.execute(
                "CREATE TABLE cython_fuzz ("
                "int_col BIGINT, str_col VARCHAR(300), dbl_col DOUBLE, "
                "dec_col DECIMAL(18,4), bool_col BOOLEAN, "
                "blob_col BLOB, clob_col CLOB, seq_col INTEGER)")
            cursor.executemany(
                "INSERT INTO cython_fuzz (int_col, str_col, dbl_col, dec_col,"
                " bool_col, blob_col, clob_col, seq_col)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
            con.commit()
        finally:
            con.close()

        def run_query():
            con2 = self._connect()
            try:
                cursor = con2.cursor()
                cursor.execute(
                    "SELECT int_col, str_col, dbl_col, dec_col, bool_col,"
                    " blob_col, clob_col FROM cython_fuzz ORDER BY seq_col")
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

            assert len(cython_rows) == num_rows
            assert cython_rows == python_rows
        finally:
            con = self._connect()
            try:
                con.cursor().execute("DROP TABLE IF EXISTS cython_fuzz")
                con.commit()
            finally:
                con.close()
