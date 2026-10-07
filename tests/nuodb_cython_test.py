# -*- coding: utf-8 -*-
"""Verify that the Cython acceleration extension is built and wired in.

(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import datetime
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


def test_encode_batch_rows_exported():
    """The batch encoder used by EncodedSession must be exported."""
    import pynuodb._fetch as _fetch
    assert callable(getattr(_fetch, 'encode_batch_rows', None))


def test_decode_batch_results_exported():
    """The batch result-code decoder used by EncodedSession must be
    exported."""
    import pynuodb._fetch as _fetch
    assert callable(getattr(_fetch, 'decode_batch_results', None))


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


# --- encode_batch_rows() / decode_batch_results(): the write-side
# counterpart to decode_next_batch(), used by
# EncodedSession.execute_batch_prepared_statement(). Expected wire bytes are
# built directly from the same putInt()/putString()/putDouble()/putNull()
# rules _encode_int/_encode_str/_encode_double/_encode_null already model
# above, independent of encode_batch_rows() itself. Note bool is NOT
# _encode_bool() here: putValue()'s historic behaviour (and encode_batch_rows'
# fast path, matching it) encodes bool parameters as plain integers (0/1),
# not the TRUE/FALSE wire codes _encode_bool() produces for decoded values.

def _no_exotic_encode(value):
    raise AssertionError("exotic_fn should not be called for this value")


def _reference_encode_row(row):
    out = _encode_int(len(row))
    for value in row:
        if value is None:
            out += _encode_null()
        elif isinstance(value, bool):
            out += _encode_int(1 if value else 0)
        elif isinstance(value, str):
            out += _encode_str(value)
        elif isinstance(value, float):
            out += _encode_double(value)
        else:
            out += _encode_int(value)
    return out


def test_encode_batch_rows_matches_reference():
    """A batch of plain int/str/float/bool/None rows must encode to exactly
    the same bytes the pure-Python putInt()/putString()/putDouble()/
    putNull() rules would produce, with no trailing garbage from the
    growable buffer's over-allocation."""
    import pynuodb._fetch as _fetch

    rows = [
        (1, 'hello', 3.5, True, None),
        (-12345, 'x' * 80, -2.25, False, None),
        (0, '', 0.0, True, -1),
    ]
    expected = bytearray()
    for row in rows:
        expected += _reference_encode_row(row)

    output = bytearray()
    _fetch.encode_batch_rows(output, rows, 5, _no_exotic_encode)
    assert bytes(output) == bytes(expected)


def test_encode_batch_rows_preserves_output_prefix():
    """`output` is EncodedSession.__output, which already holds the message
    header (message id, statement handle, etc.) by the time
    execute_batch_prepared_statement() calls in -- encode_batch_rows must
    append, never overwrite or truncate what's already there."""
    import pynuodb._fetch as _fetch

    prefix = bytearray(b'\x01\x02\x03')
    output = bytearray(prefix)
    _fetch.encode_batch_rows(output, [(7,)], 1, _no_exotic_encode)
    assert bytes(output[:3]) == bytes(prefix)
    assert bytes(output[3:]) == bytes(_encode_int(1) + _encode_int(7))


def test_encode_batch_rows_rejects_wrong_param_count():
    """A row whose length doesn't match expected_param_count must raise
    ProgrammingError, matching execute_batch_prepared_statement()'s own
    check."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import ProgrammingError

    with pytest.raises(ProgrammingError):
        _fetch.encode_batch_rows(bytearray(), [(1, 2)], 3, _no_exotic_encode)


def test_encode_batch_rows_truncates_on_exception():
    """If a row fails the parameter-count check partway through a batch,
    the rows encoded before it must still be present and the buffer must
    still be truncated to exactly that many bytes (no leftover capacity
    from the growable buffer's over-allocation) -- _Buf.finalize() runs in
    a `finally`, not only on a clean return."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import ProgrammingError

    output = bytearray()
    with pytest.raises(ProgrammingError):
        _fetch.encode_batch_rows(output, [(1,), (2, 3)], 1, _no_exotic_encode)
    assert bytes(output) == bytes(_encode_int(1) + _encode_int(1))


def test_encode_batch_rows_routes_exotic_types_through_bridge():
    """Anything that isn't None/bool/int/str/float/Binary/Decimal/Date/
    Time/Timestamp (Vector, an oversized int, ...) must be routed through
    exotic_fn(value), and the bytes it returns spliced in verbatim at that
    column's position. Vector here, not Decimal: Decimal now has its own
    fast path (_put_scaled_decimal), so it's no longer an example of the
    exotic case -- Vector still is."""
    import pynuodb._fetch as _fetch
    from pynuodb.datatype import Vector

    seen = []

    def exotic_fn(value):
        seen.append(value)
        return b'\xEE\xEE'

    vec = Vector(Vector.DOUBLE, [1.0, 2.0])
    rows = [(1, vec, 'x')]
    output = bytearray()
    _fetch.encode_batch_rows(output, rows, 3, exotic_fn)

    expected = _encode_int(3) + _encode_int(1) + b'\xEE\xEE' + _encode_str('x')
    assert bytes(output) == bytes(expected)
    assert seen == [vec]


def test_encode_batch_rows_oversized_int_routes_through_exotic():
    """An int outside the signed-64-bit range that putInt()/
    toSignedByteString would still encode fine in pure Python (Python ints
    are arbitrary precision) can't go through the C `long long` fast path
    (PyLong_AsLongLongAndOverflow), so it must fall back to exotic_fn
    instead of silently truncating."""
    import pynuodb._fetch as _fetch

    huge = 2 ** 100
    seen = []

    def exotic_fn(value):
        seen.append(value)
        return b'\xAB'

    output = bytearray()
    _fetch.encode_batch_rows(output, [(huge,)], 1, exotic_fn)
    assert bytes(output) == bytes(_encode_int(1) + b'\xAB')
    assert seen == [huge]


def test_encode_batch_rows_bool_uses_int_path_not_exotic():
    """bool encodes as an integer (True/False -> 1/0) on the fast path, not
    through exotic_fn -- matching putValue()'s documented historic
    behaviour that bools encode as integers, not the TRUE/FALSE wire
    codes."""
    import pynuodb._fetch as _fetch

    output = bytearray()
    _fetch.encode_batch_rows(output, [(True, False)], 2, _no_exotic_encode)
    assert bytes(output) == bytes(_encode_int(2) + _encode_int(1) + _encode_int(0))


def test_encode_batch_rows_large_batch_matches_reference():
    """A batch large/varied enough to force several _Buf growth doublings
    (long strings crossing the inline/counted 39/40-byte boundary, many
    rows) must still match the reference encoding exactly -- this is the
    write-side counterpart to the decode truncation/fuzz tests above, aimed
    at the growable-buffer bookkeeping (ensure/put_byte/put_bytes/finalize)
    rather than the wire-format rules themselves."""
    import pynuodb._fetch as _fetch

    rng = random.Random(20260915)
    rows = []
    for i in range(200):
        length = rng.choice([0, 5, 39, 40, 41, 300])
        rows.append((
            rng.randint(-(2 ** 40), 2 ** 40),
            ''.join(rng.choice('abcdef ') for _ in range(length)),
            rng.uniform(-1e6, 1e6),
            rng.choice([True, False]),
            None if rng.random() < 0.2 else i,
        ))

    expected = bytearray()
    for row in rows:
        expected += _reference_encode_row(row)

    output = bytearray()
    _fetch.encode_batch_rows(output, rows, 5, _no_exotic_encode)
    assert bytes(output) == bytes(expected)


# --- encode_batch_rows(): Binary/Decimal/Date/Time/Timestamp fast paths.
#
# These were added after the original None/bool/int/str/float/Binary pass:
# decode_next_batch() already fast-pathed SCALEDLEN/SCALEDDATE/SCALEDTIME/
# SCALEDTIMESTAMP; encode_batch_rows() did not, so Decimal/Date/Time/
# Timestamp parameters went through the exotic_fn bridge (a full
# EncodedSession.putValue() round-trip per value) even though the
# underlying wire encoding is simple once the Python-level ticks/scale
# value has been computed. Reference bytes below are built from the same
# ground-truth primitives (crypt.toSignedByteString, datatype.*ToTicks)
# the pure-Python EncodedSession methods use -- independent of the new
# Cython code, not a copy of it.

def test_encode_batch_rows_binary_uses_opaque_fast_path_not_exotic():
    """A datatype.Binary parameter must encode via OPAQUELEN/OPAQUECOUNT
    (putOpaque()'s wire rule) without ever calling exotic_fn -- both the
    short/inline (<40 bytes) and long/counted (>=40 bytes) cases."""
    import pynuodb._fetch as _fetch
    import pynuodb

    short = pynuodb.Binary(b'\x00\x01\x02short')
    long_ = pynuodb.Binary(b'\xff' * 80)
    rows = [(short,), (long_,)]
    expected = (_encode_int(1) + _encode_bytes(short)
               + _encode_int(1) + _encode_bytes(long_))

    output = bytearray()
    _fetch.encode_batch_rows(output, rows, 1, _no_exotic_encode)
    assert bytes(output) == bytes(expected)


def _reference_scaled_decimal(value):
    """Ground truth for putScaledInt()'s wire bytes, built independently
    of _put_scaled_decimal: same formula, but via crypt.toSignedByteString
    and decimal.Decimal directly rather than the Cython encoder."""
    import decimal
    from pynuodb import crypt

    normalized = value + 0
    scale = abs(normalized.as_tuple()[2])
    data = crypt.toSignedByteString(int(normalized * decimal.Decimal(10 ** scale)))
    return bytes([_protocol.SCALEDLEN0 + len(data), scale & 0xFF]) + bytes(data)


def test_encode_batch_rows_decimal_matches_reference():
    """Decimal parameters across a range of magnitudes/scales/signs must
    match putScaledInt()'s wire bytes exactly, and never call exotic_fn."""
    import decimal
    import pynuodb._fetch as _fetch

    values = [
        decimal.Decimal('0'), decimal.Decimal('-0'),
        decimal.Decimal('1.5'), decimal.Decimal('-1.5'),
        decimal.Decimal('99.95'), decimal.Decimal('0.0001'),
        decimal.Decimal('123456789.4321'), decimal.Decimal('-123456789.4321'),
        decimal.Decimal('1E+10'), decimal.Decimal('1E-10'),
    ]
    rows = [(v,) for v in values]
    expected = bytearray()
    for v in values:
        expected += _encode_int(1) + _reference_scaled_decimal(v)

    output = bytearray()
    _fetch.encode_batch_rows(output, rows, 1, _no_exotic_encode)
    assert bytes(output) == bytes(expected)


def test_encode_batch_rows_decimal_special_value_falls_back_to_exotic():
    """NaN/Infinity have a non-integer exponent ('n'/'N'/'F'); putScaledInt()
    raises ValueError for these. The fast path must recognize it can't
    handle them and defer to exotic_fn (which reaches that same error)
    instead of misinterpreting the exponent."""
    import decimal
    import pynuodb._fetch as _fetch

    seen = []

    def exotic_fn(value):
        seen.append(value)
        return b'\xAA'

    nan = decimal.Decimal('NaN')
    output = bytearray()
    _fetch.encode_batch_rows(output, [(nan,)], 1, exotic_fn)
    assert bytes(output) == bytes(_encode_int(1) + b'\xAA')
    assert seen == [nan]


def test_encode_batch_rows_decimal_overflow_falls_back_to_exotic():
    """A Decimal whose scaled integer needs more than 8 bytes must fall
    back to exotic_fn (which reaches putScaledCount2()'s unbounded wire
    format) rather than truncating or misencoding."""
    import decimal
    import pynuodb._fetch as _fetch

    huge = decimal.Decimal('1' + '0' * 30)  # far beyond signed-64-bit range
    seen = []

    def exotic_fn(value):
        seen.append(value)
        return b'\xCC'

    output = bytearray()
    _fetch.encode_batch_rows(output, [(huge,)], 1, exotic_fn)
    assert bytes(output) == bytes(_encode_int(1) + b'\xCC')
    assert seen == [huge]


def test_encode_batch_rows_date_matches_reference():
    """Date parameters must match putScaledDate()'s wire bytes exactly
    (DateToTicks() + crypt.toSignedByteString, scale always 0), and never
    call exotic_fn."""
    import datetime
    from pynuodb import crypt
    from pynuodb.datatype import DateToTicks
    import pynuodb._fetch as _fetch

    dates = [
        datetime.date(1970, 1, 1), datetime.date(1969, 12, 31),
        datetime.date(2024, 3, 15), datetime.date(1, 1, 1),
        datetime.date(9999, 12, 31),
    ]
    rows = [(d,) for d in dates]
    expected = bytearray()
    for d in dates:
        data = crypt.toSignedByteString(DateToTicks(d))
        expected += _encode_int(1) + bytes([_protocol.SCALEDDATELEN0 + len(data), 0]) + bytes(data)

    output = bytearray()
    _fetch.encode_batch_rows(output, rows, 1, _no_exotic_encode)
    assert bytes(output) == bytes(expected)


def test_encode_batch_rows_time_and_timestamp_match_reference():
    """Time and Timestamp parameters must match putScaledTime()/
    putScaledTimestamp()'s wire bytes exactly (TimeToTicks()/
    TimestampToTicks() + crypt.toSignedByteString), given the same tz_info
    passed to both the reference computation and encode_batch_rows(), and
    never call exotic_fn."""
    import datetime
    from zoneinfo import ZoneInfo
    from pynuodb import crypt
    from pynuodb.datatype import TimeToTicks, TimestampToTicks
    import pynuodb._fetch as _fetch

    tz = ZoneInfo('America/New_York')
    times = [datetime.time(0, 0, 0), datetime.time(23, 59, 59, 123456),
            datetime.time(12, 30, 45)]
    stamps = [datetime.datetime(1970, 1, 1, 0, 0, 0),
             datetime.datetime(2024, 3, 15, 12, 34, 56, 789000),
             datetime.datetime(2024, 1, 1, tzinfo=ZoneInfo('UTC'))]

    time_rows = [(t,) for t in times]
    expected = bytearray()
    for t in times:
        ticks, scale = TimeToTicks(t, tz)
        data = crypt.toSignedByteString(ticks)
        expected += _encode_int(1) + bytes([_protocol.SCALEDTIMELEN0 + len(data), scale & 0xFF]) + bytes(data)

    output = bytearray()
    _fetch.encode_batch_rows(output, time_rows, 1, _no_exotic_encode, tz)
    assert bytes(output) == bytes(expected)

    stamp_rows = [(s,) for s in stamps]
    expected = bytearray()
    for s in stamps:
        ticks, scale = TimestampToTicks(s, tz)
        data = crypt.toSignedByteString(ticks)
        expected += _encode_int(1) + bytes([_protocol.SCALEDTIMESTAMPLEN0 + len(data), scale & 0xFF]) + bytes(data)

    output = bytearray()
    _fetch.encode_batch_rows(output, stamp_rows, 1, _no_exotic_encode, tz)
    assert bytes(output) == bytes(expected)


def test_encode_batch_rows_timestamp_checked_before_date():
    """datetime.datetime subclasses datetime.date, so a Timestamp value
    must be checked (and encoded as SCALEDTIMESTAMP) before the Date
    isinstance check, or it would be misencoded as a Date and silently
    drop its time-of-day. Regression test for that ordering."""
    import datetime
    from zoneinfo import ZoneInfo
    import pynuodb._fetch as _fetch

    tz = ZoneInfo('UTC')
    stamp = datetime.datetime(2024, 3, 15, 12, 34, 56)
    output = bytearray()
    _fetch.encode_batch_rows(output, [(stamp,)], 1, _no_exotic_encode, tz)

    code = output[1]  # output[0] is the plen prefix (INT0+1)
    assert _protocol.SCALEDTIMESTAMPLEN0 < code <= _protocol.SCALEDTIMESTAMPLEN8, (
        "Timestamp encoded with code %d, expected a SCALEDTIMESTAMP code"
        " (%d, %d] -- looks like it fell through to the Date branch instead"
        % (code, _protocol.SCALEDTIMESTAMPLEN0, _protocol.SCALEDTIMESTAMPLEN8))


# --- decode_batch_results(): result-code readback for a batch execute.

def _encode_batch_result_ok(count):
    return _encode_int(count)


def _encode_batch_result_error(ec, message):
    return _encode_int(-3) + _encode_int(ec) + _encode_str(message)


def test_decode_batch_results_matches_reference():
    """A run of successful per-statement result codes must decode to the
    same list of ints the pure-Python getInt() loop would produce, with no
    error string and pos left exactly after the last code."""
    import pynuodb._fetch as _fetch

    counts = [1, 1, 0, 1, -2]
    buf = bytearray()
    for c in counts:
        buf += _encode_batch_result_ok(c)

    results, pos, error_string = _fetch.decode_batch_results(buf, 0, len(counts), {})
    assert results == counts
    assert pos == len(buf)
    assert error_string is None


def test_decode_batch_results_reports_first_error_only():
    """Matches the pure-Python "only report first" behaviour: every error
    payload (-3 result code, error code, error message) must be fully
    consumed off the wire so the stream stays in sync, but only the first
    error's formatted message ends up in error_string."""
    import pynuodb._fetch as _fetch

    stringify_error = {7: 'first-kind', 9: 'second-kind'}
    buf = bytearray()
    buf += _encode_batch_result_ok(1)
    buf += _encode_batch_result_error(7, 'boom')
    buf += _encode_batch_result_error(9, 'kaboom')
    buf += _encode_batch_result_ok(1)

    results, pos, error_string = _fetch.decode_batch_results(buf, 0, 4, stringify_error)
    assert results == [1, -3, -3, 1]
    assert error_string == 'first-kind:boom'
    assert pos == len(buf)


def test_decode_batch_results_rejects_truncated_buffer():
    """A buffer truncated mid-error-message must raise EndOfStream, not
    read past the end -- same bounds-checking contract as
    decode_next_batch()."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import EndOfStream

    full = bytearray()
    full += _encode_batch_result_error(3, 'a message')
    for trunc_len in range(len(full)):
        buf = bytearray(full[:trunc_len])
        with pytest.raises(EndOfStream):
            _fetch.decode_batch_results(buf, 0, 1, {3: 'kind'})


def test_decode_batch_results_non_integer_code_raises():
    """A result code that isn't an integer-shaped wire code (10-59) must
    raise DataError, matching getInt()'s own behaviour."""
    import pynuodb._fetch as _fetch
    from pynuodb.exception import DataError

    buf = bytearray([200])   # UUID code: not integer-shaped
    with pytest.raises(DataError):
        _fetch.decode_batch_results(buf, 0, 1, {})


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

    def test_cython_matches_pure_python_executemany(self):
        """executemany()'s encode path (encode_batch_rows/_cython_exotic_
        encode) must produce the same server-visible result -- both
        cursor.executemany()'s own return value and the row set actually
        inserted -- whether or not the Cython accelerator is active.
        Covers every type encode_batch_rows fast-paths (int/str/float/
        bool/None/Binary/Decimal/Date/Time/Timestamp) plus the exotic
        bridge (still reached for None values embedded in typed columns
        via cursor.setinputsizes-free NULLs, exercised implicitly here)."""
        from pynuodb import encodedsession

        if not getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            pytest.skip("Cython extension not loaded; nothing to compare")

        rows = [
            (1, 'hello', 3.5, True, decimal.Decimal('1.25'),
             datetime.date(2024, 3, 15), datetime.time(12, 34, 56, 789000),
             datetime.datetime(2024, 3, 15, 12, 34, 56, 789000),
             pynuodb.Binary(b'\x00\x01\x02short')),
            (2, 'x' * 80, -2.25, False, decimal.Decimal('-9.99'),
             datetime.date(1970, 1, 1), datetime.time(0, 0, 0),
             datetime.datetime(1970, 1, 1, 0, 0, 0),
             pynuodb.Binary(b'\xff' * 80)),
            (3, '', 0.0, True, decimal.Decimal('0.00'),
             datetime.date(9999, 12, 31), datetime.time(23, 59, 59, 999999),
             datetime.datetime(9999, 12, 30, 23, 59, 59, 999999),
             pynuodb.Binary(b'')),
            (4, None, None, None, None, None, None, None, None),
        ]

        def run(use_accel):
            encodedsession._HAVE_FETCH_ACCEL = use_accel
            con = self._connect()
            try:
                cursor = con.cursor()
                cursor.execute("DROP TABLE IF EXISTS cython_batch_write")
                cursor.execute(
                    "CREATE TABLE cython_batch_write ("
                    "id INTEGER, s VARCHAR(100), d DOUBLE,"
                    " b BOOLEAN, dec DECIMAL(10,2),"
                    " dt DATE, tm TIME, ts TIMESTAMP, bin BINARY VARYING(200))")
                results = cursor.executemany(
                    "INSERT INTO cython_batch_write"
                    " (id, s, d, b, dec, dt, tm, ts, bin)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
                con.commit()
                cursor.execute(
                    "SELECT id, s, d, b, dec, dt, tm, ts, bin"
                    " FROM cython_batch_write ORDER BY id")
                return results, cursor.fetchall()
            finally:
                con.close()

        try:
            cython_results, cython_rows = run(True)
            python_results, python_rows = run(False)
        finally:
            encodedsession._HAVE_FETCH_ACCEL = True
            con = self._connect()
            try:
                con.cursor().execute("DROP TABLE IF EXISTS cython_batch_write")
                con.commit()
            finally:
                con.close()

        assert cython_results == python_results
        assert cython_rows == python_rows

    def test_executemany_batch_error_matches_pure_python(self):
        """A batch that fails partway (duplicate primary key) must raise
        BatchError with the same message and per-statement results list
        whether decode_batch_results (Cython) or the pure-Python getInt()
        loop decoded the result codes."""
        from pynuodb import encodedsession
        from pynuodb.exception import BatchError

        if not getattr(encodedsession, '_HAVE_FETCH_ACCEL', False):
            pytest.skip("Cython extension not loaded; nothing to compare")

        def run(use_accel):
            encodedsession._HAVE_FETCH_ACCEL = use_accel
            con = self._connect()
            try:
                cursor = con.cursor()
                cursor.execute("DROP TABLE IF EXISTS cython_batch_err")
                cursor.execute(
                    "CREATE TABLE cython_batch_err (id INTEGER PRIMARY KEY)")
                try:
                    cursor.executemany(
                        "INSERT INTO cython_batch_err (id) VALUES (?)",
                        [(1,), (1,), (2,)])
                    raise AssertionError("expected BatchError")
                except BatchError as exc:
                    return str(exc), list(exc.results)
            finally:
                con.close()

        try:
            cython_msg, cython_results = run(True)
            python_msg, python_results = run(False)
        finally:
            encodedsession._HAVE_FETCH_ACCEL = True
            con = self._connect()
            try:
                con.cursor().execute("DROP TABLE IF EXISTS cython_batch_err")
                con.commit()
            finally:
                con.close()

        assert cython_results == python_results
        assert cython_msg == python_msg

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
