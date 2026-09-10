# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
"""Cython-accelerated hot paths for the NuoDB Python driver.

Replaces result_set.ResultSet and the decode loop in
EncodedSession.fetch_result_set_next(). decode_next_batch() handles common
wire types inline; anything else goes through exotic_fn back into
EncodedSession.getValue().
"""

from cpython.bytes     cimport PyBytes_FromStringAndSize
from cpython.bytearray cimport PyByteArray_FromStringAndSize
from cpython.tuple     cimport PyTuple_New
from cpython.ref       cimport PyObject

import decimal as _decimal
import uuid as _uuid
from . import datatype as _datatype
from .exception import DataError, EndOfStream

_Decimal            = _decimal.Decimal
_Binary             = _datatype.Binary
_DateFromTicks      = _datatype.DateFromTicks
_TimeFromTicks      = _datatype.TimeFromTicks
_TimestampFromTicks = _datatype.TimestampFromTicks
_UUID               = _uuid.UUID

cdef tuple _POW10 = tuple(10 ** i for i in range(256))

cdef extern from "_cutil.h":
    void _pynuodb_tuple_steal(PyObject *t, Py_ssize_t i, PyObject *o)
    PyObject *_pynuodb_long_from_long(long v) except NULL
    PyObject *_pynuodb_long_from_longlong(long long v) except NULL
    PyObject *_pynuodb_decode_utf8(const char *s, Py_ssize_t n) except NULL
    PyObject *_pynuodb_incref(PyObject *o)
    PyObject *_pynuodb_none()
    PyObject *_pynuodb_true()
    PyObject *_pynuodb_false()
    double _pynuodb_be_double(const unsigned char *p, int n) nogil
    long long _pynuodb_be_i64(const unsigned char *p, int n) nogil
    unsigned long long _pynuodb_be_u64(const unsigned char *p, int n) nogil
    object _pynuodb_pylong_be_signed(const unsigned char *p, Py_ssize_t n)
    int _pynuodb_avail_ok(Py_ssize_t pos, Py_ssize_t need, Py_ssize_t n) nogil


include "_codes.pxi"
include "_resultset.pxi"


cdef int _raise_bounds_error(Py_ssize_t pos, Py_ssize_t need, Py_ssize_t n,
                             const char* what) except -1:
    if need < 0:
        raise EndOfStream(
            '%s: length prefix exceeds representable size at offset %d'
            % (what.decode('ascii'), pos))
    raise EndOfStream(
        '%s: end of stream reached (need %d bytes at offset %d, have %d)'
        % (what.decode('ascii'), need, pos, n))


cdef inline int _check_avail(Py_ssize_t pos, Py_ssize_t need, Py_ssize_t n,
                             const char* what) except -1:
    """Raise EndOfStream if data[pos:pos+need] would run past the buffer."""
    if not _pynuodb_avail_ok(pos, need, n):
        _raise_bounds_error(pos, need, n, what)
    return 0


cdef inline object _read_scaled_operand(const unsigned char* base, Py_ssize_t* pos,
                                        int len0, int code, Py_ssize_t n,
                                        int* scale_out, const char* what):
    """Read a scale byte then an n-byte big-endian signed pylong operand."""
    cdef int nbytes = code - len0
    _check_avail(pos[0] + 1, 1 + nbytes, n, what)
    scale_out[0] = base[pos[0] + 1]
    cdef object value_obj = _pynuodb_pylong_be_signed(base + pos[0] + 2, nbytes)
    pos[0] += 2 + nbytes
    return value_obj


# helper methods for this file

cdef inline object _make_decimal(value, int scale):
    if scale == 0:
        return _Decimal(value)
    return _Decimal(f"{value}E{-scale}")


# SCALEDTIME/SCALEDTIMESTAMP wire values carry ticks at the column's own
# scale; _unpack_time_scale splits that into (seconds, microseconds) for
# TimeFromTicks/TimestampFromTicks, which both take microsecond precision.
cdef int _MICROS_SCALE   = 6        # microseconds = 10 ** -_MICROS_SCALE seconds
cdef int _MICROS_PER_SEC = 1000000  # 10 ** _MICROS_SCALE


cdef inline object _unpack_time_scale(int scale, time_val):
    cdef object shiftr = _POW10[scale]
    cdef object ticks = time_val // shiftr
    cdef object fraction = time_val % shiftr
    cdef object micros
    if scale > _MICROS_SCALE:
        micros = fraction // _POW10[scale - _MICROS_SCALE]
    else:
        micros = fraction * _POW10[_MICROS_SCALE - scale]
    if micros < 0:
        micros = micros % _MICROS_PER_SEC
        ticks = ticks + 1
    return ticks, micros


cdef inline object _make_scaled_date(date_val, int scale):
    return _DateFromTicks(date_val // ((<object>10) ** scale))


cdef inline object _make_scaled_time(int scale, time_val, tz):
    seconds, micros = _unpack_time_scale(scale, time_val)
    return _TimeFromTicks(seconds, micros, tz)


cdef inline object _make_scaled_ts(int scale, stamp_val, tz):
    seconds, micros = _unpack_time_scale(scale, stamp_val)
    return _TimestampFromTicks(seconds, micros, tz)


cdef inline object _make_scaled_ts_notz(int scale, stamp_val):
    seconds, micros = _unpack_time_scale(scale, stamp_val)
    return _TimestampFromTicks(seconds, micros, None)


def decode_next_batch(bytearray data, Py_ssize_t pos, int col_count,
                      list results, object exotic_fn, object tz_info=None):
    """Decode one server batch from the wire buffer.

    :param data: bytearray holding the raw server message (self.__input).
    :param pos: read cursor (self.__inpos) at entry.
    :param col_count: columns per row.
    :param results: list to which decoded row-tuples are appended in place.
    :param exotic_fn: callable(pos) -> (value, new_pos) for non-fast-path
        types, should be EncodedSession._cython_exotic_decode. `data` must
        not be resized (a bytearray with a live memoryview export refuses
        this at runtime -- self.__input.extend(...), say, would raise
        BufferError) or reassigned: `self.__input = ...` elsewhere is not
        blocked by anything and would leave `base` pointing at the old
        buffer with no error at all.
    :param tz_info: tzinfo for SCALEDTIME / SCALEDTIMESTAMP construction.
        tz_info=None with a tz-bearing code produces a naive value, not the
        local zone.
    :returns: (new_pos, complete)
    """
    cdef:
        Py_ssize_t        n   = len(data)
        unsigned char[:] mv   = data
        const unsigned char* base
        int               code, nbytes, col, scale
        Py_ssize_t        length, marker_pos
        long long         ival
        bint              complete = False
        object            marker_obj, val, value_obj
        object            row_tup
        object            empty_str = u''
        PyObject*         row_ptr
        PyObject*         empty_str_ptr = <PyObject*>empty_str

    if n == 0:
        return pos, False

    base = &mv[0]

    while pos < n:
        code = base[pos]
        if INTMINUS10 <= code <= INT31:
            pos += 1
            if code == INT0:               # marker 0 -> end of batch
                complete = True
                break
        else:
            marker_pos = pos
            marker_obj, pos = exotic_fn(pos)
            if pos <= marker_pos:
                # exotic_fn must consume at least the marker byte, or a
                # col_count == 0 batch would loop here forever.
                raise EndOfStream(
                    'exotic_fn did not advance past offset %d' % marker_pos)
            # The reference decoder reads a row marker with getInt(), which
            # only accepts integer-shaped codes (10-59) and raises for
            # anything else; exotic_fn here goes through the general
            # getValue() dispatch instead, so it must be re-checked the
            # same way -- otherwise a marker byte for, say, a UUID gets
            # silently decoded and treated as "a row follows".
            if type(marker_obj) is not int:
                raise DataError(
                    'Not an integer: row marker at offset %d' % marker_pos)
            if marker_obj == 0:
                complete = True
                break

        row_tup = PyTuple_New(col_count)
        row_ptr = <PyObject*>row_tup
        # Wire codes handled inline below (see protocol.py for the named
        # constants): 1-3 NULL/TRUE/FALSE, 10-59 integers, 61-68 SCALED
        # decimal, 69-72 UTF-8 counted, 73-76 OPAQUE counted, 77-85 DOUBLE,
        # 86-103 MILLISEC/NANOSEC, 104-108 TIME, 109-148 UTF-8 inline,
        # 149-188 OPAQUE inline, 189-193 BLOB, 194-198 CLOB, 200 UUID,
        # 201-208 SCALEDDATE, 209-216 SCALEDTIME, 217-224 SCALEDTIMESTAMP,
        # 241 SCALEDTIMESTAMPNOTZ. Everything else (VECTOR, SCALEDCOUNT2/3,
        # LOBSTREAM, ARRAY, DEBUGBARRIER, a future protocol code) falls
        # through to exotic_fn.
        for col in range(col_count):
            _check_avail(pos, 1, n, b'type code')
            code = base[pos]

            # Code 0 has no defined meaning and must fall through to
            # exotic_fn, not match here.
            if NULL_V <= code <= FALSE_V:
                if code == NULL_V:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(_pynuodb_none()))
                elif code == TRUE_V:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(_pynuodb_true()))
                else:                                   # FALSE_V
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(_pynuodb_false()))
                pos += 1

            elif INTMINUS10 <= code <= INTLEN8:
                if code <= INT31:                      # inline -10 .. 31
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_long(code - INT0))
                    pos += 1
                else:                                   # INTLEN1..INTLEN8: 52..59
                    nbytes = code - INTLEN0
                    pos += 1
                    _check_avail(pos, nbytes, n, b'INTLEN integer')
                    ival = _pynuodb_be_i64(base + pos, nbytes)
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_longlong(ival))
                    pos += nbytes

            elif UTF8LEN0 <= code <= UTF8LEN39:
                length = code - UTF8LEN0
                pos += 1
                if length:
                    _check_avail(pos, length, n, b'UTF8 (inline length)')
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_decode_utf8(<const char*>(base + pos), length))
                else:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(empty_str_ptr))
                pos += length

            elif UTF8COUNT1 <= code <= UTF8COUNT4:
                nbytes = code - UTF8COUNT0
                pos += 1
                _check_avail(pos, nbytes, n, b'UTF8COUNT length prefix')
                length = <Py_ssize_t>_pynuodb_be_u64(base + pos, nbytes)
                pos += nbytes
                if length:
                    _check_avail(pos, length, n, b'UTF8 (counted)')
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_decode_utf8(<const char*>(base + pos), length))
                else:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(empty_str_ptr))
                pos += length

            elif OPAQUELEN0 <= code <= OPAQUELEN39:
                length = code - OPAQUELEN0
                pos += 1
                _check_avail(pos, length, n, b'OPAQUE (inline length)')
                val = _Binary(PyByteArray_FromStringAndSize(
                    <char*>(base + pos), length))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            elif OPAQUECOUNT1 <= code <= OPAQUECOUNT4:
                nbytes = code - OPAQUECOUNT0
                pos += 1
                _check_avail(pos, nbytes, n, b'OPAQUECOUNT length prefix')
                length = <Py_ssize_t>_pynuodb_be_u64(base + pos, nbytes)
                pos += nbytes
                _check_avail(pos, length, n, b'OPAQUE (counted)')
                val = _Binary(PyByteArray_FromStringAndSize(
                    <char*>(base + pos), length))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            elif DOUBLELEN0 <= code <= DOUBLELEN8:
                nbytes = code - DOUBLELEN0
                pos += 1
                _check_avail(pos, nbytes, n, b'DOUBLE')
                val = float(_pynuodb_be_double(base + pos, nbytes))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += nbytes

            elif MILLISECLEN0 <= code <= NANOSECLEN8:
                if code <= MILLISECLEN8:
                    nbytes = code - MILLISECLEN0
                else:
                    nbytes = code - NANOSECLEN0
                pos += 1
                if nbytes == 0:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_long(0))
                else:
                    _check_avail(pos, nbytes, n, b'MILLISEC/NANOSEC timestamp')
                    ival = _pynuodb_be_i64(base + pos, nbytes)
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_longlong(ival))
                    pos += nbytes

            elif TIMELEN0 <= code <= TIMELEN4:
                nbytes = code - TIMELEN0
                pos += 1
                if nbytes == 0:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_long(0))
                else:
                    _check_avail(pos, nbytes, n, b'TIME')
                    ival = <long long>_pynuodb_be_u64(base + pos, nbytes)
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_longlong(ival))
                    pos += nbytes

            # SCALEDLEN8 == UTF8COUNT0 == 68; this range is inside (60,68],
            # distinct from UTF8COUNT (69-72) above.
            elif SCALEDLEN0 < code <= SCALEDLEN8:
                value_obj = _read_scaled_operand(base, &pos, SCALEDLEN0, code, n, &scale, b'SCALED decimal')
                val = _make_decimal(value_obj, scale)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            elif BLOBLEN0 <= code <= BLOBLEN4:
                nbytes = code - BLOBLEN0
                pos += 1
                if nbytes == 0:
                    length = 0
                else:
                    _check_avail(pos, nbytes, n, b'BLOBLEN length prefix')
                    length = <Py_ssize_t>_pynuodb_be_u64(base + pos, nbytes)
                    pos += nbytes
                _check_avail(pos, length, n, b'BLOB')
                val = _Binary(PyByteArray_FromStringAndSize(
                    <char*>(base + pos), length))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            elif CLOBLEN0 <= code <= CLOBLEN4:
                nbytes = code - CLOBLEN0
                pos += 1
                if nbytes == 0:
                    length = 0
                else:
                    _check_avail(pos, nbytes, n, b'CLOBLEN length prefix')
                    length = <Py_ssize_t>_pynuodb_be_u64(base + pos, nbytes)
                    pos += nbytes
                _check_avail(pos, length, n, b'CLOB')
                val = PyByteArray_FromStringAndSize(<char*>(base + pos), length)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            # UUID (200) and SCALEDDATELEN0 (200) are the same wire code;
            # the strict `<` in the SCALEDDATE check below is what keeps
            # them apart, not the order these branches appear in.
            elif code == UUID_C:
                pos += 1
                _check_avail(pos, 16, n, b'UUID')
                val = _UUID(bytes=PyBytes_FromStringAndSize(<char*>(base + pos), 16))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += 16

            elif SCALEDDATELEN0 < code <= SCALEDDATELEN8:
                value_obj = _read_scaled_operand(base, &pos, SCALEDDATELEN0, code, n, &scale, b'SCALEDDATE')
                val = _make_scaled_date(value_obj, scale)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            elif SCALEDTIMELEN0 < code <= SCALEDTIMELEN8:
                value_obj = _read_scaled_operand(base, &pos, SCALEDTIMELEN0, code, n, &scale, b'SCALEDTIME')
                val = _make_scaled_time(scale, value_obj, tz_info)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            elif SCALEDTIMESTAMPLEN0 < code <= SCALEDTIMESTAMPLEN8:
                value_obj = _read_scaled_operand(base, &pos, SCALEDTIMESTAMPLEN0, code, n, &scale, b'SCALEDTIMESTAMP')
                val = _make_scaled_ts(scale, value_obj, tz_info)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            # Code 241, always 8 bytes (LEN0=233).
            elif code == SCALEDTIMESTAMPNOTZ:
                value_obj = _read_scaled_operand(base, &pos, SCALEDTIMESTAMPNOTZLEN0, code, n, &scale, b'SCALEDTIMESTAMPNOTZ')
                val = _make_scaled_ts_notz(scale, value_obj)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            else:
                val, pos = exotic_fn(pos)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

        results.append(row_tup)

    return pos, complete
