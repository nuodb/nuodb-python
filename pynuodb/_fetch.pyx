# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
"""Cython-accelerated hot paths for the NuoDB Python driver.

Replaces result_set.ResultSet and the decode loop in
EncodedSession.fetch_result_set_next(). decode_next_batch() handles common
wire types inline; anything else goes through exotic_fn back into
EncodedSession.getValue().

encode_batch_rows() and decode_batch_results() are the write-side
counterpart, used by EncodedSession.execute_batch_prepared_statement():
encoding every row of an executemany() batch (None/bool/int/str/float/
datatype.Binary/decimal.Decimal/datatype.Date/Time/Timestamp inline,
anything else through exotic_fn back into EncodedSession.putValue() via
_cython_exotic_encode) and decoding the per-statement result codes read
back afterwards.
"""

from cpython.bytes     cimport (
    PyBytes_FromStringAndSize,
    PyBytes_AS_STRING,
    PyBytes_GET_SIZE,
)
from cpython.bytearray cimport (
    PyByteArray_FromStringAndSize,
    PyByteArray_AS_STRING,
    PyByteArray_GET_SIZE,
    PyByteArray_Resize,
)
from cpython.unicode   cimport PyUnicode_AsUTF8String, PyUnicode_DecodeUTF8
from cpython.long      cimport PyLong_AsLongLongAndOverflow
from cpython.tuple     cimport PyTuple_New
from cpython.ref       cimport PyObject
from libc.string       cimport memcpy

import decimal as _decimal
import uuid as _uuid
from . import datatype as _datatype
from .exception import DataError, EndOfStream, ProgrammingError

_Decimal            = _decimal.Decimal
_Binary             = _datatype.Binary
_Timestamp          = _datatype.Timestamp
_Date               = _datatype.Date
_Time               = _datatype.Time
_DateFromTicks      = _datatype.DateFromTicks
_TimeFromTicks      = _datatype.TimeFromTicks
_TimestampFromTicks = _datatype.TimestampFromTicks
_DateToTicks        = _datatype.DateToTicks
_TimeToTicks        = _datatype.TimeToTicks
_TimestampToTicks   = _datatype.TimestampToTicks
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
    int _pynuodb_encode_signed(long long v, unsigned char *out) nogil
    int _pynuodb_encode_unsigned(unsigned long long n, unsigned char *out) nogil
    void _pynuodb_double_to_be(double d, unsigned char *out) nogil


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
    return _DateFromTicks(date_val // _POW10[scale])


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


# ----- Batch-execute result readback -----------------------------------------
#
# execute_batch_prepared_statement() sends one row-count-or-error-code int
# per statement in the batch, then reads them all back in a loop (getInt()
# per row, occasionally followed by an error int + string on failure). This
# mirrors that loop in C, reusing the same bounds-checked primitives
# decode_next_batch() uses (_check_avail, _pynuodb_be_i64/_be_u64).

cdef inline long long _decode_wire_int(const unsigned char* base, Py_ssize_t* pos,
                                       Py_ssize_t n) except? -1:
    """Read one getInt()-shaped value (codes 10-59) and advance *pos."""
    _check_avail(pos[0], 1, n, b'batch result code')
    cdef int code = base[pos[0]]
    cdef int nbytes
    cdef long long ival
    if INTMINUS10 <= code <= INT31:
        pos[0] += 1
        return code - INT0
    if code > INT31 and code <= INTLEN8:      # INTLEN1..INTLEN8: 52..59
        nbytes = code - INTLEN0
        _check_avail(pos[0] + 1, nbytes, n, b'batch result code')
        pos[0] += 1
        ival = _pynuodb_be_i64(base + pos[0], nbytes)
        pos[0] += nbytes
        return ival
    raise DataError('Not an integer: batch result code at offset %d' % pos[0])


cdef inline object _decode_wire_string(const unsigned char* base, Py_ssize_t* pos,
                                       Py_ssize_t n):
    """Read one getString()-shaped value (codes 69-72, 109-148) and advance
    *pos. Only reachable for a batch error message, so only those two ranges
    need handling here (not the full getValue() dispatch)."""
    _check_avail(pos[0], 1, n, b'batch error message')
    cdef int code = base[pos[0]]
    cdef int nbytes
    cdef Py_ssize_t length
    if UTF8LEN0 <= code <= UTF8LEN39:
        length = code - UTF8LEN0
        pos[0] += 1
        if length == 0:
            return u''
        _check_avail(pos[0], length, n, b'batch error message')
        val = PyUnicode_DecodeUTF8(<char*>(base + pos[0]), length, NULL)
        pos[0] += length
        return val
    if UTF8COUNT1 <= code <= UTF8COUNT4:
        nbytes = code - UTF8COUNT0
        _check_avail(pos[0] + 1, nbytes, n, b'batch error message')
        pos[0] += 1
        length = <Py_ssize_t>_pynuodb_be_u64(base + pos[0], nbytes)
        pos[0] += nbytes
        if length == 0:
            return u''
        _check_avail(pos[0], length, n, b'batch error message')
        val = PyUnicode_DecodeUTF8(<char*>(base + pos[0]), length, NULL)
        pos[0] += length
        return val
    raise DataError('getString: Invalid type code: %d' % code)


def decode_batch_results(bytearray data, Py_ssize_t pos, Py_ssize_t count,
                         dict stringify_error):
    """Decode the `count` per-statement result codes of a batch execute.

    Mirrors the pure-Python loop in
    EncodedSession.execute_batch_prepared_statement:

        result = getInt()
        if result == -3:
            ec = getInt(); es = getString()   # error code + message

    Every error payload is fully consumed off the wire (so the stream stays
    in sync) even though only the *first* error's formatted message is kept,
    matching the existing "only report first" behaviour exactly.

    :param data: bytearray holding the raw server message (self.__input).
    :param pos: read cursor (self.__inpos) at entry.
    :param count: number of statements in the batch (len(param_lists)).
    :param stringify_error: protocol.stringifyError, used to format the
        first error the same way the pure-Python path does.
    :returns: (results: list[int], new_pos: int, error_string: str or None).
    """
    cdef:
        Py_ssize_t   n = len(data)
        unsigned char[:] mv = data
        const unsigned char* base
        Py_ssize_t   i
        long long    ival
        int          ec
        object       es
        list         results = []
        object       error_string = None

    if count == 0:
        return results, pos, None

    # An empty buffer with count > 0 is a truncated response, not "nothing
    # to decode": fall through so the first _check_avail (inside
    # _decode_wire_int) raises EndOfStream instead of silently returning no
    # results. &mv[0] itself is unsafe on an empty memoryview, so it's only
    # taken when there's at least one byte.
    base = &mv[0] if n > 0 else NULL

    for i in range(count):
        ival = _decode_wire_int(base, &pos, n)
        results.append(ival)
        if ival == -3:
            ec = <int>_decode_wire_int(base, &pos, n)
            es = _decode_wire_string(base, &pos, n)
            if error_string is None:
                error_string = '%s:%s' % (stringify_error[ec], es)

    return results, pos, error_string


# ----- Batch parameter encoding -----------------------------------------------
#
# The write-side counterpart to decode_next_batch(): the inner loop of
# EncodedSession.execute_batch_prepared_statement (encode every row of an
# executemany() batch into the wire message). Fast-paths None/bool/int/str/
# float inline in C; everything else goes through exotic_fn, which should be
# EncodedSession._cython_exotic_encode -- it runs the value through the
# existing putValue() dispatch into a scratch buffer and hands back the
# resulting wire bytes, so Decimal/datetime/Binary/Vector/oversized-int
# encoding logic lives in exactly one place.

cdef class _Buf:
    """Growable wrapper around a Python bytearray with amortized-doubling
    resize, so the batch encoder doesn't pay a realloc on every value."""

    cdef bytearray obj
    cdef Py_ssize_t used
    cdef Py_ssize_t cap

    def __cinit__(self, bytearray initial):
        self.obj = initial
        self.used = PyByteArray_GET_SIZE(initial)
        self.cap = self.used

    cdef inline void ensure(self, Py_ssize_t extra):
        cdef Py_ssize_t need = self.used + extra
        cdef Py_ssize_t newcap
        if need > self.cap:
            newcap = self.cap * 2 if self.cap > 0 else 64
            if newcap < need:
                newcap = need
            PyByteArray_Resize(self.obj, newcap)
            self.cap = newcap

    cdef inline void put_byte(self, unsigned char b):
        self.ensure(1)
        (<unsigned char*> PyByteArray_AS_STRING(self.obj))[self.used] = b
        self.used += 1

    cdef inline void put_bytes(self, const unsigned char* p, Py_ssize_t n):
        if n == 0:
            return
        self.ensure(n)
        memcpy(<unsigned char*> PyByteArray_AS_STRING(self.obj) + self.used, p, n)
        self.used += n

    cdef finalize(self):
        # ensure() over-allocates capacity ahead of what's actually used;
        # this truncates back to the exact byte count written, matching the
        # pure-Python loop's repeated bytearray.append()/+= (no trailing
        # garbage), whether called after a clean finish or via `finally`
        # after an exception mid-batch.
        PyByteArray_Resize(self.obj, self.used)


cdef inline void _put_int(_Buf buf, long long v):
    """Encode a C long long using the putInt() wire rule."""
    cdef unsigned char data[8]
    cdef int nbytes
    if v > -11 and v < 32:
        buf.put_byte(<unsigned char>(INT0 + v))
    else:
        nbytes = _pynuodb_encode_signed(v, data)
        buf.put_byte(<unsigned char>(INTLEN0 + nbytes))
        buf.put_bytes(data, nbytes)


cdef inline void _put_string(_Buf buf, str value):
    """Encode a str using the putString() wire rule."""
    cdef bytes data = PyUnicode_AsUTF8String(value)
    cdef Py_ssize_t length = PyBytes_GET_SIZE(data)
    cdef unsigned char lenbuf[8]
    cdef int nbytes
    if length < 40:
        buf.put_byte(<unsigned char>(UTF8LEN0 + length))
    else:
        nbytes = _pynuodb_encode_unsigned(<unsigned long long> length, lenbuf)
        buf.put_byte(<unsigned char>(UTF8COUNT0 + nbytes))
        buf.put_bytes(lenbuf, nbytes)
    buf.put_bytes(<const unsigned char*> PyBytes_AS_STRING(data), length)


cdef inline void _put_double(_Buf buf, double value):
    """Encode a float using the putDouble() wire rule (always 8 bytes)."""
    cdef unsigned char data[8]
    _pynuodb_double_to_be(value, data)
    buf.put_byte(<unsigned char>(DOUBLELEN0 + 8))
    buf.put_bytes(data, 8)


cdef inline void _put_opaque(_Buf buf, bytes value):
    """Encode a datatype.Binary value using the putOpaque() wire rule.

    Binary is always written as OPAQUE (OPAQUELEN0-39 / OPAQUECOUNT1-4),
    never BLOBLEN/CLOBLEN -- those are deprecated for encoding (see
    protocol.py), and putValue()/putOpaque() never emit them for a Binary
    parameter regardless of the target column's BLOB/CLOB/BINARY VARYING
    type. Binary subclasses bytes without adding fields (datatype.py's
    Binary.__new__ is just bytes.__new__(cls, data)), so the PyBytes_*
    macros below read its buffer directly, same as for a plain bytes/str
    value.
    """
    cdef Py_ssize_t length = PyBytes_GET_SIZE(value)
    cdef unsigned char lenbuf[8]
    cdef int nbytes
    if length < 40:
        buf.put_byte(<unsigned char>(OPAQUELEN0 + length))
    else:
        nbytes = _pynuodb_encode_unsigned(<unsigned long long> length, lenbuf)
        buf.put_byte(<unsigned char>(OPAQUECOUNT0 + nbytes))
        buf.put_bytes(lenbuf, nbytes)
    buf.put_bytes(<const unsigned char*> PyBytes_AS_STRING(value), length)


cdef inline void _put_exotic(_Buf buf, object exotic_fn, object param):
    """Route `param` through exotic_fn(value) -> bytes and splice the
    result in verbatim. Shared by every fast-path branch's overflow/
    fallback case, and by the final catch-all else."""
    cdef bytes exotic_bytes = exotic_fn(param)
    buf.put_bytes(<const unsigned char*> PyBytes_AS_STRING(exotic_bytes),
                 PyBytes_GET_SIZE(exotic_bytes))


cdef inline bint _put_scaled_date(_Buf buf, object value):
    """Encode a datatype.Date using the putScaledDate() wire rule.

    DateToTicks() is unavoidably Python-level (calendar/Julian-Gregorian
    math via jdcal, or the datetime-subtraction fast path in datatype.py);
    this only skips the exotic_fn round-trip and uses the fast C signed-
    int encoder for the final ticks bytes instead of
    crypt.toSignedByteString. Returns False (writes nothing) if the ticks
    value needs more than 8 bytes -- the caller falls back to exotic_fn,
    same pattern as the oversized-int fast path above.
    """
    cdef object ticks_obj = _DateToTicks(value)
    cdef long long ticks
    cdef int overflow, nbytes
    cdef unsigned char data[8]
    ticks = PyLong_AsLongLongAndOverflow(ticks_obj, &overflow)
    if overflow:
        return False
    nbytes = _pynuodb_encode_signed(ticks, data)
    buf.put_byte(<unsigned char>(SCALEDDATELEN0 + nbytes))
    buf.put_byte(0)  # Date's scale is always 0 (whole days)
    buf.put_bytes(data, nbytes)
    return True


cdef inline bint _put_scaled_time(_Buf buf, object value, object tz_info):
    """Encode a datatype.Time using the putScaledTime() wire rule.

    TimeToTicks()'s scale is always 0-6 (microsecond precision), so it
    always fits the wire format's single scale byte; only the ticks value
    itself can overflow 8 bytes, same fallback as _put_scaled_date.
    """
    cdef object ticks_obj
    cdef int scale
    ticks_obj, scale = _TimeToTicks(value, tz_info)
    cdef long long ticks
    cdef int overflow, nbytes
    cdef unsigned char data[8]
    ticks = PyLong_AsLongLongAndOverflow(ticks_obj, &overflow)
    if overflow:
        return False
    nbytes = _pynuodb_encode_signed(ticks, data)
    buf.put_byte(<unsigned char>(SCALEDTIMELEN0 + nbytes))
    buf.put_byte(<unsigned char> scale)
    buf.put_bytes(data, nbytes)
    return True


cdef inline bint _put_scaled_timestamp(_Buf buf, object value, object tz_info):
    """Encode a datatype.Timestamp using the putScaledTimestamp() wire
    rule. Same shape/fallback as _put_scaled_time."""
    cdef object ticks_obj
    cdef int scale
    ticks_obj, scale = _TimestampToTicks(value, tz_info)
    cdef long long ticks
    cdef int overflow, nbytes
    cdef unsigned char data[8]
    ticks = PyLong_AsLongLongAndOverflow(ticks_obj, &overflow)
    if overflow:
        return False
    nbytes = _pynuodb_encode_signed(ticks, data)
    buf.put_byte(<unsigned char>(SCALEDTIMESTAMPLEN0 + nbytes))
    buf.put_byte(<unsigned char> scale)
    buf.put_bytes(data, nbytes)
    return True


cdef inline bint _put_scaled_decimal(_Buf buf, object value):
    """Encode a decimal.Decimal using the putScaledInt() wire rule.

    Mirrors EncodedSession.putScaledInt() exactly, including the `+ 0`
    normalization (its own comment: folds e-notation/context artifacts
    into a plain Decimal) and computing the scale factor as
    decimal.Decimal(10 ** scale) -- a native Python int power wrapped in
    Decimal, deliberately *not* Decimal(10) ** scale, which would round
    to the current context's precision instead of being exact. All of
    that is unavoidably Python-level (arbitrary-precision decimal
    arithmetic); this only skips the exotic_fn round-trip and uses the
    fast C signed-int encoder for the final bytes.

    Returns False (writes nothing), falling back to exotic_fn, for:
      * a non-integer exponent (NaN/sNaN/Infinity) -- putScaledInt()
        raises ValueError for these; let that exact error come from the
        proven Python path instead of duplicating it here.
      * scale > 255 -- doesn't fit the wire format's single scale byte;
        putScaledInt()'s plain bytearray.append(scale) would raise
        ValueError, but `<unsigned char> scale` would silently truncate.
      * a scaled value needing more than 8 bytes -- putScaledInt() itself
        falls back to putScaledCount2() (a different, unbounded wire
        format) for this; exotic_fn reaches the same code unmodified.
    """
    cdef object normalized = value + 0
    cdef object exponent = normalized.as_tuple()[2]
    if type(exponent) is not int:
        return False
    cdef int scale = abs(exponent)
    if scale > 255:
        return False
    cdef object scaled = int(normalized * _Decimal(10 ** scale))
    cdef long long ival
    cdef int overflow, nbytes
    cdef unsigned char data[8]
    ival = PyLong_AsLongLongAndOverflow(scaled, &overflow)
    if overflow:
        return False
    nbytes = _pynuodb_encode_signed(ival, data)
    buf.put_byte(<unsigned char>(SCALEDLEN0 + nbytes))
    buf.put_byte(<unsigned char> scale)
    buf.put_bytes(data, nbytes)
    return True


def encode_batch_rows(bytearray output, list param_lists,
                      Py_ssize_t expected_param_count, object exotic_fn,
                      object tz_info=None):
    """Encode every row of a batch (plen + each param's value) into `output`.

    :param output: bytearray to append to (self.__output); mutated in
        place, matching the pure-Python loop's behaviour of repeatedly
        appending to the same buffer.
    :param param_lists: the executemany() sequence of parameter tuples.
    :param expected_param_count: prepared_statement.parameter_count; every
        row must match or ProgrammingError is raised (same message as the
        pure-Python path).
    :param exotic_fn: callable(value) -> bytes for anything not fast-pathed
        here (Vector, oversized ints/Decimals, unknown objects). Should be
        EncodedSession._cython_exotic_encode.
    :param tz_info: tzinfo for SCALEDTIME/SCALEDTIMESTAMP construction --
        should be EncodedSession.timezone_info, evaluated once by the
        caller rather than per-value (that property builds a fresh
        ZoneInfo each access). Required whenever a Time or Timestamp
        parameter is present; None is only safe if none are.
    """
    cdef _Buf buf = _Buf(output)
    cdef Py_ssize_t plen
    cdef object row, param, tv
    cdef long long ival
    cdef int overflow
    cdef bint ok

    try:
        for row in param_lists:
            plen = len(row)
            if plen != expected_param_count:
                raise ProgrammingError(
                    "Incorrect number of parameters specified,"
                    " expected %d, got %d" % (expected_param_count, plen))
            _put_int(buf, plen)

            for param in row:
                if param is None:
                    buf.put_byte(NULL_V)
                    continue

                tv = type(param)
                if tv is int or tv is bool:
                    ival = PyLong_AsLongLongAndOverflow(param, &overflow)
                    if overflow:
                        _put_exotic(buf, exotic_fn, param)
                    else:
                        _put_int(buf, ival)
                elif tv is str:
                    _put_string(buf, <str> param)
                elif tv is float:
                    _put_double(buf, <double> param)
                elif isinstance(param, _Binary):
                    # isinstance, not `tv is _Binary`: putValue() itself
                    # dispatches on isinstance(value, datatype.Binary), so
                    # a Binary subclass must hit this fast path too, not
                    # silently fall through to the slower exotic_fn bridge.
                    _put_opaque(buf, <bytes> param)
                elif isinstance(param, _Timestamp):
                    # Checked before _Date: datetime.datetime subclasses
                    # datetime.date, so a Timestamp would also match the
                    # _Date isinstance check below -- same ordering
                    # putValue() itself documents and relies on.
                    ok = _put_scaled_timestamp(buf, param, tz_info)
                    if not ok:
                        _put_exotic(buf, exotic_fn, param)
                elif isinstance(param, _Date):
                    ok = _put_scaled_date(buf, param)
                    if not ok:
                        _put_exotic(buf, exotic_fn, param)
                elif isinstance(param, _Time):
                    ok = _put_scaled_time(buf, param, tz_info)
                    if not ok:
                        _put_exotic(buf, exotic_fn, param)
                elif isinstance(param, _Decimal):
                    ok = _put_scaled_decimal(buf, param)
                    if not ok:
                        _put_exotic(buf, exotic_fn, param)
                else:
                    _put_exotic(buf, exotic_fn, param)
    finally:
        buf.finalize()
