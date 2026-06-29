# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
"""Cython-accelerated hot paths for the NuoDB Python driver.

Two things live here:

  ResultSet
      A cdef class that replaces the pure-Python result_set.ResultSet.  The
      typed cdef attributes and cpdef methods eliminate Python attribute-lookup
      and function-call overhead on the millions of per-row fetchone() and
      is_complete() calls during a full-table scan.

  decode_next_batch()
      The inner decode loop from EncodedSession.fetch_result_set_next(),
      rewritten in C.  It handles every common NuoDB wire type inline:

        * Inline integers   (INTMINUS10 .. INT31,   codes 10-51)
        * Multi-byte ints   (INTLEN1    .. INTLEN8, codes 52-59)
        * Short UTF-8       (UTF8LEN0   .. UTF8LEN39, codes 109-148)
        * Counted UTF-8     (UTF8COUNT1 .. UTF8COUNT4, codes 69-72)
        * Short OPAQUE      (OPAQUELEN0 .. OPAQUELEN39, codes 149-188)
        * Counted OPAQUE    (OPAQUECOUNT1 .. OPAQUECOUNT4, codes 73-76)
        * IEEE-754 DOUBLE   (DOUBLELEN0 .. DOUBLELEN8, codes 77-85)
        * MILLISEC/NANOSEC/TIME ints (codes 86-108)
        * SCALED Decimal    (SCALEDLEN0 .. SCALEDLEN8, codes 60-68)
        * SCALEDDATE        (codes 201-208)
        * SCALEDTIME        (codes 209-216, uses tz_info)
        * SCALEDTIMESTAMP   (codes 217-224, uses tz_info)
        * SCALEDTIMESTAMPNOTZ (code 241)
        * BLOB / CLOB       (codes 189-198)
        * UUID              (code 200)
        * NULL / TRUE / FALSE

      Truly exotic codes (VECTOR, SCALEDCOUNT2/3, LOBSTREAM, ARRAY) fall back
      to a Python callable so correctness is never sacrificed.
"""

from cpython.unicode cimport PyUnicode_DecodeUTF8, PyUnicode_AsUTF8String
from cpython.bytes   cimport PyBytes_FromStringAndSize, PyBytes_AsString, PyBytes_GET_SIZE
from cpython.bytearray cimport (
    PyByteArray_FromStringAndSize,
    PyByteArray_AS_STRING,
    PyByteArray_GET_SIZE,
    PyByteArray_Resize,
)
from cpython.long    cimport PyLong_AsLongLongAndOverflow
from cpython.tuple   cimport PyTuple_New
from cpython.ref     cimport PyObject
from libc.string     cimport memcpy

import struct as _struct
import decimal as _decimal
import uuid as _uuid
from . import datatype as _datatype
from .exception import ProgrammingError

# Cached Python constructors / helpers used by the complex-type branches.
# Looked up once at module import; the decode loop references these via
# the C name-lookup optimization the Cython compiler emits for module
# globals (one indirection, no per-cell `LOAD_GLOBAL`).
_Decimal            = _decimal.Decimal
_Binary             = _datatype.Binary
_DateFromTicks      = _datatype.DateFromTicks
_TimeFromTicks      = _datatype.TimeFromTicks
_TimestampFromTicks = _datatype.TimestampFromTicks
_UUID               = _uuid.UUID

# C-level helpers that bypass Cython's automatic INCREF/DECREF on `object`-typed
# arguments.  PyTuple_SET_ITEM steals a reference, and PyLong_FromLong /
# PyUnicode_DecodeUTF8 return a new reference, so the combination is leak-free
# but Cython's automatic refcount handling on `object` parameters double-INCREFs.
# By passing PyObject* through this thin C wrapper, we eliminate 2 refcount ops
# per cell.
cdef extern from *:
    """
    #include <Python.h>
    #include <string.h>
    #include <stdint.h>
    static CYTHON_INLINE void _pynuodb_tuple_steal(
            PyObject *t, Py_ssize_t i, PyObject *o) {
        PyTuple_SET_ITEM(t, i, o);
    }
    static CYTHON_INLINE PyObject *_pynuodb_long_from_long(long v) {
        return PyLong_FromLong(v);
    }
    static CYTHON_INLINE PyObject *_pynuodb_long_from_longlong(long long v) {
        return PyLong_FromLongLong(v);
    }
    static CYTHON_INLINE PyObject *_pynuodb_decode_utf8(
            const char *s, Py_ssize_t n) {
        return PyUnicode_DecodeUTF8(s, n, NULL);
    }
    static CYTHON_INLINE PyObject *_pynuodb_incref(PyObject *o) {
        Py_INCREF(o);
        return o;
    }
    /* Read a 1-to-8-byte big-endian IEEE-754 double, padding missing low
       bytes with zero (matching getDouble()'s "append zeros until 8 bytes"
       behaviour).  Portable across endianness: builds the IEEE bit pattern
       as a uint64_t in native order, then memcpy's into a double. */
    static CYTHON_INLINE double _pynuodb_be_double(
            const unsigned char *p, int n) {
        unsigned char buf[8] = {0};
        int i;
        for (i = 0; i < n; i++) buf[i] = p[i];
        uint64_t v =
            ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
            ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
            ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
            ((uint64_t)buf[6] <<  8) |  (uint64_t)buf[7];
        double d;
        memcpy(&d, &v, 8);
        return d;
    }
    /* Little-endian double, used for the VECTOR(DOUBLE) payload format. */
    static CYTHON_INLINE double _pynuodb_le_double(const unsigned char *p) {
        uint64_t v =
            ((uint64_t)p[7] << 56) | ((uint64_t)p[6] << 48) |
            ((uint64_t)p[5] << 40) | ((uint64_t)p[4] << 32) |
            ((uint64_t)p[3] << 24) | ((uint64_t)p[2] << 16) |
            ((uint64_t)p[1] <<  8) |  (uint64_t)p[0];
        double d;
        memcpy(&d, &v, 8);
        return d;
    }
    /* Specialised big-endian signed integer reader.  N=1,2,4,8 use a single
       memcpy + bswap (1-2 native instructions); other lengths fall back to a
       small unrolled accumulator with explicit sign-extension on the high bit
       of the first byte.  Compare to the byte-by-byte loop in EncodedInputStream
       C++ -- both compile to the same shape for the hot N=4/8 cases. */
    static CYTHON_INLINE long long _pynuodb_be_i64(
            const unsigned char *p, int n) {
        unsigned long long v;
        switch (n) {
        case 0: return 0;
        case 1: return (long long)(signed char)p[0];
        case 2: { uint16_t x; memcpy(&x, p, 2);
                  return (long long)(int16_t)__builtin_bswap16(x); }
        case 4: { uint32_t x; memcpy(&x, p, 4);
                  return (long long)(int32_t)__builtin_bswap32(x); }
        case 8: { uint64_t x; memcpy(&x, p, 8);
                  return (long long)(int64_t)__builtin_bswap64(x); }
        case 3:
            v = ((unsigned long long)p[0] << 16) |
                ((unsigned long long)p[1] <<  8) |  p[2];
            if (p[0] & 0x80) v |= 0xFFFFFFFFFF000000ULL;
            return (long long)v;
        case 5:
            v = ((unsigned long long)p[0] << 32) |
                ((unsigned long long)p[1] << 24) |
                ((unsigned long long)p[2] << 16) |
                ((unsigned long long)p[3] <<  8) |  p[4];
            if (p[0] & 0x80) v |= 0xFFFFFF0000000000ULL;
            return (long long)v;
        case 6:
            v = ((unsigned long long)p[0] << 40) |
                ((unsigned long long)p[1] << 32) |
                ((unsigned long long)p[2] << 24) |
                ((unsigned long long)p[3] << 16) |
                ((unsigned long long)p[4] <<  8) |  p[5];
            if (p[0] & 0x80) v |= 0xFFFF000000000000ULL;
            return (long long)v;
        case 7:
            v = ((unsigned long long)p[0] << 48) |
                ((unsigned long long)p[1] << 40) |
                ((unsigned long long)p[2] << 32) |
                ((unsigned long long)p[3] << 24) |
                ((unsigned long long)p[4] << 16) |
                ((unsigned long long)p[5] <<  8) |  p[6];
            if (p[0] & 0x80) v |= 0xFF00000000000000ULL;
            return (long long)v;
        default:
            v = 0;
            for (int i = 0; i < n; i++) v = (v << 8) | p[i];
            if (n > 0 && (p[0] & 0x80))
                v -= ((unsigned long long)1) << (n << 3);
            return (long long)v;
        }
    }
    /* Specialised big-endian unsigned reader for length prefixes (n<=8). */
    static CYTHON_INLINE unsigned long long _pynuodb_be_u64(
            const unsigned char *p, int n) {
        switch (n) {
        case 0: return 0;
        case 1: return p[0];
        case 2: { uint16_t x; memcpy(&x, p, 2);
                  return __builtin_bswap16(x); }
        case 4: { uint32_t x; memcpy(&x, p, 4);
                  return __builtin_bswap32(x); }
        case 8: { uint64_t x; memcpy(&x, p, 8);
                  return __builtin_bswap64(x); }
        case 3: return ((unsigned long long)p[0] << 16) |
                       ((unsigned long long)p[1] <<  8) |  p[2];
        case 5: return ((unsigned long long)p[0] << 32) |
                       ((unsigned long long)p[1] << 24) |
                       ((unsigned long long)p[2] << 16) |
                       ((unsigned long long)p[3] <<  8) |  p[4];
        case 6: return ((unsigned long long)p[0] << 40) |
                       ((unsigned long long)p[1] << 32) |
                       ((unsigned long long)p[2] << 24) |
                       ((unsigned long long)p[3] << 16) |
                       ((unsigned long long)p[4] <<  8) |  p[5];
        case 7: return ((unsigned long long)p[0] << 48) |
                       ((unsigned long long)p[1] << 40) |
                       ((unsigned long long)p[2] << 32) |
                       ((unsigned long long)p[3] << 24) |
                       ((unsigned long long)p[4] << 16) |
                       ((unsigned long long)p[5] <<  8) |  p[6];
        default: {
            unsigned long long v = 0;
            for (int i = 0; i < n; i++) v = (v << 8) | p[i];
            return v;
        }
        }
    }
    /* Build a Python int from n big-endian signed bytes WITHOUT first
       allocating a temporary `bytes` object.  _PyLong_FromByteArray is the
       same C function that int.from_bytes() ultimately dispatches to; calling
       it directly here saves one PyBytes allocation + one Python-level method
       call per scaled-Decimal cell. */
    static CYTHON_INLINE PyObject *_pynuodb_pylong_be_signed(
            const unsigned char *p, Py_ssize_t n) {
        return _PyLong_FromByteArray(p, (size_t)n, 0, 1);
    }
    /* Encode a long-long signed integer in the NuoDB wire format directly
       into `buf` (caller-owned, must have >= 9 bytes of headroom).  Writes
       1 byte (INT0 + v) for v in [-10, 31] and otherwise INTLEN0+nbytes
       followed by the minimal two's-complement big-endian bytes.  Returns
       total bytes written. */
    static CYTHON_INLINE int _pynuodb_emit_int(char *buf, long long v) {
        if (v >= -10 && v <= 31) {
            buf[0] = (char)(20 + v);   /* INT0 + v */
            return 1;
        }
        int nbytes;
        if (v < 0) {
            if      (v >= -128LL)               nbytes = 1;
            else if (v >= -32768LL)             nbytes = 2;
            else if (v >= -8388608LL)           nbytes = 3;
            else if (v >= -2147483648LL)        nbytes = 4;
            else if (v >= -549755813888LL)      nbytes = 5;
            else if (v >= -140737488355328LL)   nbytes = 6;
            else if (v >= -36028797018963968LL) nbytes = 7;
            else                                nbytes = 8;
        } else {
            if      (v <= 127LL)                nbytes = 1;
            else if (v <= 32767LL)              nbytes = 2;
            else if (v <= 8388607LL)            nbytes = 3;
            else if (v <= 2147483647LL)         nbytes = 4;
            else if (v <= 549755813887LL)       nbytes = 5;
            else if (v <= 140737488355327LL)    nbytes = 6;
            else if (v <= 36028797018963967LL)  nbytes = 7;
            else                                nbytes = 8;
        }
        buf[0] = (char)(51 + nbytes);   /* INTLEN0 + nbytes */
        unsigned long long uv = (unsigned long long)v;
        int i;
        for (i = 0; i < nbytes; i++) {
            buf[nbytes - i] = (char)(uv & 0xff);
            uv >>= 8;
        }
        return 1 + nbytes;
    }
    """
    void _pynuodb_tuple_steal(PyObject *t, Py_ssize_t i, PyObject *o) nogil
    PyObject *_pynuodb_long_from_long(long v) nogil
    PyObject *_pynuodb_long_from_longlong(long long v) nogil
    PyObject *_pynuodb_decode_utf8(const char *s, Py_ssize_t n) nogil
    PyObject *_pynuodb_incref(PyObject *o) nogil
    double _pynuodb_be_double(const unsigned char *p, int n) nogil
    double _pynuodb_le_double(const unsigned char *p) nogil
    long long _pynuodb_be_i64(const unsigned char *p, int n) nogil
    unsigned long long _pynuodb_be_u64(const unsigned char *p, int n) nogil
    object _pynuodb_pylong_be_signed(const unsigned char *p, Py_ssize_t n)
    int _pynuodb_emit_int(char *buf, long long v) nogil


# Wire-protocol constants as DEF (compile-time, no Python attribute lookup).
# Keep in sync with protocol.py.

DEF NULL_V                  = 1
DEF TRUE_V                  = 2
DEF FALSE_V                 = 3
DEF INTMINUS10              = 10
DEF INT0                    = 20    # inline-integer value 0 (end-of-batch marker)
DEF INT31                   = 51
DEF INTLEN0                 = 51    # == INT31; codes 52-59 carry 1-8 byte integers
DEF INTLEN8                 = 59
DEF SCALEDLEN0              = 60    # base for 1-8 byte scaled decimals (61-68)
DEF SCALEDLEN8              = 68
DEF UTF8COUNT0              = 68    # base for length-prefixed strings (69-72)
DEF UTF8COUNT1              = 69
DEF UTF8COUNT4              = 72
DEF OPAQUECOUNT0            = 72    # base for length-prefixed binary (73-76)
DEF OPAQUECOUNT1            = 73
DEF OPAQUECOUNT4            = 76
DEF DOUBLELEN0              = 77    # 77 == double 0.0; 78-85 carry 1-8 byte doubles
DEF DOUBLELEN8              = 85
DEF MILLISECLEN0            = 86    # base for 1-8 byte millisecond timestamps
DEF MILLISECLEN8            = 94
DEF NANOSECLEN0             = 95    # base for 1-8 byte nanosecond timestamps
DEF NANOSECLEN8             = 103
DEF TIMELEN0                = 104   # base for 1-4 byte ms-since-midnight
DEF TIMELEN4                = 108
DEF UTF8LEN0                = 109   # base for 0-39 byte inline-length strings
DEF UTF8LEN39               = 148
DEF OPAQUELEN0              = 149   # base for 0-39 byte inline-length binary
DEF OPAQUELEN39             = 188
DEF BLOBLEN0                = 189   # base for 0-4 byte length-prefixed BLOB
DEF BLOBLEN4                = 193
DEF CLOBLEN0                = 194   # base for 0-4 byte length-prefixed CLOB
DEF CLOBLEN4                = 198
DEF VECTOR_C                = 199
DEF UUID_C                  = 200
DEF SCALEDDATELEN0          = 200   # 201-208 carry 1-8 byte scaled dates
DEF SCALEDDATELEN8          = 208
DEF SCALEDTIMELEN0          = 208   # 209-216 carry 1-8 byte scaled times
DEF SCALEDTIMELEN8          = 216
DEF SCALEDTIMESTAMPLEN0     = 216   # 217-224 carry 1-8 byte scaled timestamps
DEF SCALEDTIMESTAMPLEN8     = 224
DEF SCALEDTIMESTAMPNOTZLEN0 = 233   # 234-240 carry 1-7 byte scaled timestamps no-tz
DEF SCALEDTIMESTAMPNOTZ     = 241


# ----- ResultSet cdef class --------------------------------------------------

cdef class ResultSet:
    """Drop-in replacement for result_set.ResultSet with C-typed attributes.

    fetchone() and is_complete() become direct C calls when invoked from other
    Cython code (cpdef dispatch).  Python callers see the same interface.
    """

    cdef public int     handle
    cdef public int     col_count
    cdef public list    results
    cdef public int     results_idx
    cdef public bint    complete

    def __init__(self, int handle, int col_count, list initial_results,
                 bint complete):
        self.handle       = handle
        self.col_count    = col_count
        self.results      = initial_results
        self.results_idx  = 0
        self.complete     = complete

    def clear_results(self):
        del self.results[:]
        self.results_idx = 0

    def add_row(self, row):
        self.results.append(row)

    cpdef bint is_complete(self):
        return self.complete or self.results_idx != len(self.results)

    cpdef object fetchone(self):
        cdef int idx = self.results_idx
        if idx == len(self.results):
            return None
        self.results_idx = idx + 1
        return self.results[idx]


# ----- C helpers for big-endian integer reads --------------------------------

cdef inline long long _read_be_signed(const unsigned char* p, int n) nogil:
    """n-byte big-endian signed integer (n in 0..8).

    Forwards to the C helper which uses memcpy + __builtin_bswap for the
    fast N=2/4/8 cases (one native instruction after inlining).
    """
    return _pynuodb_be_i64(p, n)


cdef inline Py_ssize_t _read_be_uint(const unsigned char* p, int n) nogil:
    """n-byte big-endian unsigned integer (n in 0..8)."""
    return <Py_ssize_t>_pynuodb_be_u64(p, n)


# ----- Helpers for complex Python-constructed types --------------------------
#
# These are pure-Python `def` functions so they accept arbitrary-precision
# Python ints (which we get from `int.from_bytes` for multi-byte data).
# Called from the decode loop with already-decoded primitives so each cell
# only crosses the C/Python boundary once, instead of going through the
# generic `exotic_fn` -> `getValue()` dispatch.

def _make_decimal(value, int scale):
    """Build decimal.Decimal((sign, digit_tuple, -scale)) from an arbitrary
    precision Python int and an unsigned scale byte.

    Mirrors EncodedSession.getScaledInt() exactly so callers see identical
    Decimal representations (sign/digits/exponent).
    """
    cdef int sign = 1 if value < 0 else 0
    digits = tuple(int(c) for c in str(abs(value)))
    return _Decimal((sign, digits, -scale))


def _unpack_time_scale(int scale, time_val):
    """Return (seconds, micros) from (scale, raw_ticks).

    Mirrors EncodedSession.__unpack().  Uses Python arithmetic so it works
    for arbitrary-precision raw values without overflow.
    """
    # NOTE: with cython: cdivision=True at the top of this file, the expression
    # `10 ** scale` (10 is a C int literal, scale is a C int) compiles to a
    # C `pow()` call returning double.  Force Python int semantics by casting
    # one operand to `object`.
    cdef object shiftr = (<object>10) ** scale
    cdef object ticks = time_val // shiftr
    cdef object fraction = time_val % shiftr
    cdef object micros
    if scale > 6:
        micros = fraction // ((<object>10) ** (scale - 6))
    else:
        micros = fraction * ((<object>10) ** (6 - scale))
    if micros < 0:
        micros = micros % 1000000
        ticks = ticks + 1
    return ticks, micros


def _make_scaled_date(date_val, int scale):
    """SCALEDDATE: DateFromTicks(date // 10**scale)."""
    return _DateFromTicks(date_val // ((<object>10) ** scale))


def _make_scaled_time(int scale, time_val, tz):
    """SCALEDTIME: TimeFromTicks(seconds, micros, tz)."""
    seconds, micros = _unpack_time_scale(scale, time_val)
    return _TimeFromTicks(seconds, micros, tz)


def _make_scaled_ts(int scale, stamp_val, tz):
    """SCALEDTIMESTAMP: TimestampFromTicks(seconds, micros, tz)."""
    seconds, micros = _unpack_time_scale(scale, stamp_val)
    return _TimestampFromTicks(seconds, micros, tz)


def _make_scaled_ts_notz(int scale, stamp_val):
    """SCALEDTIMESTAMPNOTZ: TimestampFromTicks(seconds, micros, None)."""
    seconds, micros = _unpack_time_scale(scale, stamp_val)
    return _TimestampFromTicks(seconds, micros, None)


# ----- Wire-decode inner loop ------------------------------------------------

def decode_next_batch(bytearray data, Py_ssize_t pos, int col_count,
                      list results, object exotic_fn, object tz_info=None):
    """Decode one server batch from the wire buffer.

    Parameters
    ----------
    data       : bytearray holding the raw server message (self.__input).
    pos        : read cursor (self.__inpos) at entry.
    col_count  : columns per row.
    results    : list to which decoded row-tuples are appended in place.
    exotic_fn  : callable(pos) -> (value, new_pos) for non-fast-path types.
                 Called for unusual row-marker encodings and any wire codes
                 not handled inline (VECTOR, SCALEDCOUNT2/3, LOBSTREAM, ARRAY,
                 DEBUGBARRIER).  Should be EncodedSession._cython_exotic_decode.
    tz_info    : tzinfo for SCALEDTIME / SCALEDTIMESTAMP construction.  May be
                 None if the caller doesn't expect tz-bearing time columns;
                 if a tz-bearing code is encountered with tz_info=None the
                 wire-decode still completes but TimeFromTicks/TimestampFromTicks
                 will use the local zone (matching the Python default).

    Returns
    -------
    (new_pos: int, complete: bool)
    """
    cdef:
        Py_ssize_t        n   = len(data)
        unsigned char[:] mv   = data       # typed memoryview - direct C access
        const unsigned char* base           # raw pointer for multi-byte reads
        int               code, nbytes, col, scale
        Py_ssize_t        length
        long long         ival
        bint              complete = False
        object            marker_obj, val, value_obj
        object            row_tup
        PyObject*         row_ptr
        PyObject*         empty_str_ptr = <PyObject*>u''
        PyObject*         none_ptr      = <PyObject*>None
        PyObject*         true_ptr      = <PyObject*>True
        PyObject*         false_ptr     = <PyObject*>False

    if n == 0:
        return pos, False

    base = &mv[0]

    while pos < n:
        # --- row-presence marker -------------------------------------------
        code = base[pos]
        if INTMINUS10 <= code <= INT31:
            pos += 1
            if code == INT0:               # marker 0 -> end of batch
                complete = True
                break
            # non-zero marker -> row follows; marker value is otherwise ignored
        else:
            # Rare: non-inline marker (e.g. value > 31 encoded as INTLEN...).
            # Can't be zero (zero is always inline), so just advance past it.
            marker_obj, pos = exotic_fn(pos)
            # marker_obj != 0 guaranteed; continue to row decode

        # --- decode one row -------------------------------------------------
        # Build a tuple directly via the C API - avoids allocating a temporary
        # list and the list->tuple copy that tuple() performs.  All slot
        # assignments use _pynuodb_tuple_steal (raw PyObject* -> no auto-INCREF),
        # so each cell is exactly one new-reference creation + one slot store,
        # with no extra refcount bumps.
        row_tup = PyTuple_New(col_count)
        row_ptr = <PyObject*>row_tup
        for col in range(col_count):
            code = base[pos]

            # ·· NULL / TRUE / FALSE: codes 1-3 (must come before int range
            #    test so nullable columns don't pay for the long elif chain) ··
            if code <= FALSE_V:
                if code == NULL_V:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(none_ptr))
                elif code == TRUE_V:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(true_ptr))
                else:                                   # FALSE_V
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(false_ptr))
                pos += 1

            # ·· integers: codes 10-59 ······································
            elif INTMINUS10 <= code <= INTLEN8:
                if code <= INT31:                      # inline -10 .. 31
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_long(code - INT0))
                    pos += 1
                else:                                   # INTLEN1..INTLEN8: 52..59
                    nbytes = code - INTLEN0             # 1..8 bytes follow
                    pos += 1
                    ival = _read_be_signed(base + pos, nbytes)
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_longlong(ival))
                    pos += nbytes

            # ·· short UTF-8: codes 109-148 ·································
            elif UTF8LEN0 <= code <= UTF8LEN39:
                length = code - UTF8LEN0
                pos += 1
                if length:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_decode_utf8(<const char*>(base + pos), length))
                else:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(empty_str_ptr))
                pos += length

            # ·· length-prefixed UTF-8: codes 69-72 ·························
            elif UTF8COUNT1 <= code <= UTF8COUNT4:
                nbytes = code - UTF8COUNT0              # 1..4 length bytes
                pos += 1
                length = _read_be_uint(base + pos, nbytes)
                pos += nbytes
                if length:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_decode_utf8(<const char*>(base + pos), length))
                else:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_incref(empty_str_ptr))
                pos += length

            # ·· short OPAQUE/binary: codes 149-188 ··························
            elif OPAQUELEN0 <= code <= OPAQUELEN39:
                length = code - OPAQUELEN0
                pos += 1
                # Slice yields a bytearray; Binary(bytearray) returns
                # bytes-subclass.  We have to go through Binary's __new__ to
                # preserve the public API contract from getOpaque().
                val = _Binary(data[pos:pos + length])
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            # ·· length-prefixed OPAQUE: codes 73-76 ·························
            elif OPAQUECOUNT1 <= code <= OPAQUECOUNT4:
                nbytes = code - OPAQUECOUNT0
                pos += 1
                length = _read_be_uint(base + pos, nbytes)
                pos += nbytes
                val = _Binary(data[pos:pos + length])
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            # ·· DOUBLE: codes 77-85 ········································
            elif DOUBLELEN0 <= code <= DOUBLELEN8:
                nbytes = code - DOUBLELEN0              # 0..8 bytes
                pos += 1
                val = float(_pynuodb_be_double(base + pos, nbytes))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += nbytes

            # ·· MILLISEC/NANOSEC timestamps as raw ints: codes 86-103 ········
            elif MILLISECLEN0 <= code <= NANOSECLEN8:
                # getTime() in the Python decoder returns the raw integer for
                # both MILLISECLEN and NANOSECLEN families - the caller is
                # expected to interpret the units.  Match that behaviour.
                if code <= MILLISECLEN8:
                    nbytes = code - MILLISECLEN0
                else:
                    nbytes = code - NANOSECLEN0
                pos += 1
                if nbytes == 0:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_long(0))
                else:
                    ival = _read_be_signed(base + pos, nbytes)
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_longlong(ival))
                    pos += nbytes

            # ·· TIME (ms since midnight): codes 104-108 ·····················
            elif TIMELEN0 <= code <= TIMELEN4:
                nbytes = code - TIMELEN0
                pos += 1
                if nbytes == 0:
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_long(0))
                else:
                    # getTime() uses fromByteString here (unsigned).
                    length = _read_be_uint(base + pos, nbytes)
                    _pynuodb_tuple_steal(row_ptr, col,
                        _pynuodb_long_from_longlong(<long long>length))
                    pos += nbytes

            # ·· SCALED Decimal: codes 60-68 ·································
            # Note: SCALEDLEN0 == UTF8COUNT0 == 68; SCALEDLEN values 61-68
            # carry 1-8 byte signed data preceded by a 1-byte scale.  The
            # range here is *inside* the (60..68) bucket and is distinct
            # from the UTF8COUNT range (69-72) checked earlier.
            elif SCALEDLEN0 < code <= SCALEDLEN8:
                nbytes = code - SCALEDLEN0              # 1..8 data bytes
                pos += 1
                scale = base[pos]                       # unsigned scale byte
                pos += 1
                # Use Python int for arbitrary precision (matches getScaledInt)
                value_obj = _pynuodb_pylong_be_signed(base + pos, nbytes)
                pos += nbytes
                val = _make_decimal(value_obj, scale)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            # ·· BLOB: codes 189-193 ·········································
            elif BLOBLEN0 <= code <= BLOBLEN4:
                nbytes = code - BLOBLEN0                # 0..4 length bytes
                pos += 1
                if nbytes == 0:
                    length = 0
                else:
                    length = _read_be_uint(base + pos, nbytes)
                    pos += nbytes
                val = _Binary(data[pos:pos + length])
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            # ·· CLOB: codes 194-198 ·········································
            elif CLOBLEN0 <= code <= CLOBLEN4:
                nbytes = code - CLOBLEN0
                pos += 1
                if nbytes == 0:
                    length = 0
                else:
                    length = _read_be_uint(base + pos, nbytes)
                    pos += nbytes
                # getClob() returns the raw bytearray slice.
                val = data[pos:pos + length]
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += length

            # ·· UUID (code 200) -- must come before SCALEDDATE because they ··
            #    share offsets, but UUID is a single fixed code.
            elif code == UUID_C:
                pos += 1
                # Need bytes (not bytearray) for uuid.UUID(bytes=...)
                val = _UUID(bytes=bytes(data[pos:pos + 16]))
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))
                pos += 16

            # ·· SCALEDDATE: codes 201-208 ···································
            elif SCALEDDATELEN0 < code <= SCALEDDATELEN8:
                nbytes = code - SCALEDDATELEN0
                pos += 1
                scale = base[pos]
                pos += 1
                value_obj = _pynuodb_pylong_be_signed(base + pos, nbytes)
                pos += nbytes
                val = _make_scaled_date(value_obj, scale)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            # ·· SCALEDTIME: codes 209-216 ···································
            elif SCALEDTIMELEN0 < code <= SCALEDTIMELEN8:
                nbytes = code - SCALEDTIMELEN0
                pos += 1
                scale = base[pos]
                pos += 1
                value_obj = _pynuodb_pylong_be_signed(base + pos, nbytes)
                pos += nbytes
                val = _make_scaled_time(scale, value_obj, tz_info)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            # ·· SCALEDTIMESTAMP: codes 217-224 ······························
            elif SCALEDTIMESTAMPLEN0 < code <= SCALEDTIMESTAMPLEN8:
                nbytes = code - SCALEDTIMESTAMPLEN0
                pos += 1
                scale = base[pos]
                pos += 1
                value_obj = _pynuodb_pylong_be_signed(base + pos, nbytes)
                pos += nbytes
                val = _make_scaled_ts(scale, value_obj, tz_info)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            # ·· SCALEDTIMESTAMPNOTZ: code 241 ·······························
            elif code == SCALEDTIMESTAMPNOTZ:
                # Wire format: scale byte + signed bytes (length = code - LEN0).
                # In practice code == 241 means 8 bytes (LEN0=233).
                nbytes = code - SCALEDTIMESTAMPNOTZLEN0
                pos += 1
                scale = base[pos]
                pos += 1
                value_obj = _pynuodb_pylong_be_signed(base + pos, nbytes)
                pos += nbytes
                val = _make_scaled_ts_notz(scale, value_obj)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

            else:
                # Anything we don't handle inline (VECTOR, SCALEDCOUNT2/3,
                # LOBSTREAM, ARRAY, DEBUGBARRIER, or new wire codes from a
                # future protocol bump) goes through the Python fallback.
                val, pos = exotic_fn(pos)
                _pynuodb_tuple_steal(row_ptr, col,
                    _pynuodb_incref(<PyObject*>val))

        results.append(row_tup)

    return pos, complete


# ----- Wire-encode hot path (executemany) ------------------------------------
#
# encode_batch() mirrors the Python loop in
# EncodedSession.execute_batch_prepared_statement(), but inlines the int /
# str / bool / None paths in C.  Anything else (Decimal, Date, Time, Vector,
# Binary, etc.) falls back to session.putValue(), which writes to the same
# bytearray.
#
# Layout per row:
#   <row marker = INT0+parameter_count>  one byte for typical small arity
#   <param_0 wire bytes>
#   ...
#   <param_{n-1} wire bytes>
#
# We grow the destination bytearray in chunks (PyByteArray_Resize is amortised
# constant), write through a raw C pointer for the inline cases, and call
# session.putValue() only for non-inline parameters - keeping pos in sync by
# resizing back to `pos` first and then rereading the size after the call.

DEF NULL_WIRE   = 1
DEF UTF8LEN0_W  = 109
DEF UTF8COUNT0_W = 68
DEF FALSE_WIRE  = 3
DEF TRUE_WIRE   = 2


def encode_batch(bytearray out, list param_lists, int parameter_count,
                 object session):
    """Encode parameter rows for EXECUTEBATCHPREPAREDSTATEMENT into `out`.

    `out`      : the session's outgoing message bytearray (self.__output).
    `param_lists` : iterable of equally-sized parameter tuples/lists.
    `parameter_count` : expected len(row); checked per row.
    `session`  : EncodedSession instance whose putValue() is called for any
                 parameter not handled inline (Decimal, datetime types,
                 Binary, Vector, etc.).  session.__output MUST be the same
                 object as `out`, since the fallback appends to it directly.

    Does NOT emit the batch trailer (the -1 + len(param_lists) tag bytes); the
    caller writes those after this returns so existing protocol semantics are
    unchanged.

    Returns the number of rows encoded.
    """
    cdef Py_ssize_t nrows = len(param_lists)
    if nrows == 0:
        return 0

    cdef Py_ssize_t orig_size = PyByteArray_GET_SIZE(out)

    # Pre-compute the row marker bytes once; parameter_count is constant for
    # the batch.  In the common (small-arity) case this is a single byte.
    cdef char row_marker[16]
    cdef int row_marker_len = _pynuodb_emit_int(row_marker,
                                                <long long>parameter_count)

    # Optimistic up-front growth.  Worst-case per inline int/bool is 9 bytes
    # (INTLEN0+8 + 8 data); short strings tend to be < 16 bytes including the
    # tag.  Use 12 bytes per param as a reasonable initial estimate; if a
    # particular row exceeds it we re-grow in the loop.
    cdef Py_ssize_t per_row_estimate = row_marker_len + parameter_count * 12
    cdef Py_ssize_t want = orig_size + nrows * per_row_estimate
    if PyByteArray_Resize(out, want) != 0:
        raise MemoryError()
    cdef char *buf = PyByteArray_AS_STRING(out)
    cdef Py_ssize_t pos = orig_size
    cdef Py_ssize_t cap = PyByteArray_GET_SIZE(out)

    cdef long long ival
    cdef int overflow
    cdef Py_ssize_t plen, slen, lenbytes_n, i, headroom
    cdef object v, utf8b
    cdef const char *sptr
    cdef object row
    cdef type tv

    for row in param_lists:
        plen = len(row)
        if plen != parameter_count:
            # Resize the bytearray back to the actual end before raising so
            # the caller's view is consistent.
            if PyByteArray_Resize(out, pos) != 0:
                pass
            raise ProgrammingError(
                "Incorrect number of parameters specified, expected %d, got %d"
                % (parameter_count, plen))

        # Ensure capacity for this row's worst case BEFORE writing anything.
        # Strings are length-bounded; the upper bound we can guarantee inline
        # without inspecting values is row_marker + parameter_count * 9 (the
        # int worst case).  Strings are handled with explicit re-grows below.
        headroom = row_marker_len + parameter_count * 9
        if pos + headroom > cap:
            cap = pos + headroom + max(4096, (nrows - 1) * per_row_estimate)
            if PyByteArray_Resize(out, cap) != 0:
                raise MemoryError()
            buf = PyByteArray_AS_STRING(out)
            cap = PyByteArray_GET_SIZE(out)

        # Write row marker.
        for i in range(row_marker_len):
            buf[pos + i] = row_marker[i]
        pos += row_marker_len

        for v in row:
            # ---- None ----------------------------------------------------
            if v is None:
                buf[pos] = <char>NULL_WIRE
                pos += 1
                continue

            tv = type(v)

            # ---- int / bool (inline if it fits in long long) ------------
            if tv is int or tv is bool:
                ival = PyLong_AsLongLongAndOverflow(v, &overflow)
                if overflow == 0:
                    pos += _pynuodb_emit_int(buf + pos, ival)
                    continue
                # else: bignum, fall through to fallback

            # ---- str (UTF-8 encode + tag) -------------------------------
            elif tv is str:
                utf8b = PyUnicode_AsUTF8String(v)
                slen = PyBytes_GET_SIZE(utf8b)
                sptr = PyBytes_AsString(utf8b)
                # Worst case: 1 tag byte + 4 length bytes + slen data bytes.
                # Re-grow if not enough headroom.
                if pos + 1 + 4 + slen > cap:
                    cap = pos + 1 + 4 + slen + max(4096,
                                                    (nrows - 1) * per_row_estimate)
                    if PyByteArray_Resize(out, cap) != 0:
                        raise MemoryError()
                    buf = PyByteArray_AS_STRING(out)
                    cap = PyByteArray_GET_SIZE(out)
                if slen < 40:
                    buf[pos] = <char>(UTF8LEN0_W + slen)
                    pos += 1
                else:
                    # Length-prefixed: emit UTF8COUNT0+nlb, then minimal
                    # big-endian unsigned length, then data.
                    if   slen <= 0xff:        lenbytes_n = 1
                    elif slen <= 0xffff:      lenbytes_n = 2
                    elif slen <= 0xffffff:    lenbytes_n = 3
                    else:                     lenbytes_n = 4
                    buf[pos] = <char>(UTF8COUNT0_W + lenbytes_n)
                    pos += 1
                    for i in range(lenbytes_n):
                        buf[pos + lenbytes_n - 1 - i] = <char>((slen >> (i * 8)) & 0xff)
                    pos += lenbytes_n
                memcpy(buf + pos, sptr, slen)
                pos += slen
                continue

            # ---- fallback: hand off to Python putValue ------------------
            # Commit the truncated size so session.putValue() sees the right
            # current length, append via the Python path, then re-read.
            if PyByteArray_Resize(out, pos) != 0:
                raise MemoryError()
            session.putValue(v)
            pos = PyByteArray_GET_SIZE(out)
            # Re-grow for the remaining rows / params in this row.
            headroom = row_marker_len + parameter_count * 9
            if pos + headroom > cap:
                cap = pos + headroom + max(4096,
                                            (nrows - 1) * per_row_estimate)
                if PyByteArray_Resize(out, cap) != 0:
                    raise MemoryError()
            buf = PyByteArray_AS_STRING(out)
            cap = PyByteArray_GET_SIZE(out)

    # Truncate to the actual final position.
    if PyByteArray_Resize(out, pos) != 0:
        raise MemoryError()
    return nrows
