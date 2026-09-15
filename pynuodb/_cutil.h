#ifndef PYNUODB_CUTIL_H
#define PYNUODB_CUTIL_H
/* C support for _fetch.pyx's decode loop: raw-PyObject* value builders for
   the row tuple, and big-endian integer/double readers for the wire
   protocol. */

#include <Python.h>
#include <string.h>
#include <stdint.h>

#if defined(_MSC_VER)
#include <stdlib.h>
#define NUODB_INLINE static __inline
#define NUODB_LIKELY(x) (x)
#define NUODB_UNLIKELY(x) (x)
#define NUODB_BSWAP16(x) _byteswap_ushort(x)
#define NUODB_BSWAP32(x) _byteswap_ulong(x)
#define NUODB_BSWAP64(x) _byteswap_uint64(x)
#else
#define NUODB_INLINE static inline
#define NUODB_LIKELY(x) __builtin_expect(!!(x), 1)
#define NUODB_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define NUODB_BSWAP16(x) __builtin_bswap16(x)
#define NUODB_BSWAP32(x) __builtin_bswap32(x)
#define NUODB_BSWAP64(x) __builtin_bswap64(x)
#endif

/* Build a value as a raw PyObject* for decode_next_batch to steal into a
   row tuple via _pynuodb_tuple_steal. Don't replace these with Cython's
   cpython.* pxd bindings: those take/return `object`, not `PyObject*`,
   which would route the value through Cython's own refcounting first. */
NUODB_INLINE void _pynuodb_tuple_steal(PyObject *t, Py_ssize_t i, PyObject *o)
{
    PyTuple_SET_ITEM(t, i, o);
}

NUODB_INLINE PyObject *_pynuodb_long_from_long(long v)
{
    return PyLong_FromLong(v);
}

NUODB_INLINE PyObject *_pynuodb_long_from_longlong(long long v)
{
    return PyLong_FromLongLong(v);
}

NUODB_INLINE PyObject *_pynuodb_decode_utf8(const char *s, Py_ssize_t n)
{
    return PyUnicode_DecodeUTF8(s, n, NULL);
}

NUODB_INLINE PyObject *_pynuodb_incref(PyObject *o)
{
    Py_INCREF(o);
    return o;
}

NUODB_INLINE PyObject *_pynuodb_none(void) { return Py_None; }
NUODB_INLINE PyObject *_pynuodb_true(void) { return Py_True; }
NUODB_INLINE PyObject *_pynuodb_false(void) { return Py_False; }

NUODB_INLINE unsigned long long _pynuodb_be_u64(const unsigned char *p, int n) 
{
    switch (n)
    {
        case 0: return 0;
        case 1: return p[0];
        case 2: {
            uint16_t x;
            memcpy(&x, p, 2);
            return NUODB_BSWAP16(x);
        }
        case 4: {
            uint32_t x;
            memcpy(&x, p, 4);
            return NUODB_BSWAP32(x);
        }
        case 8: {
            uint64_t x;
            memcpy(&x, p, 8);
            return NUODB_BSWAP64(x);
        }
        default: {
            unsigned long long v = 0;
            for (int i = 0; i < n; i++) {
                v = (v << 8) | p[i];
            }
            return v;
        }
    }
}

/* Sign-extends the n-byte big-endian unsigned value from _pynuodb_be_u64*/
NUODB_INLINE long long _pynuodb_be_i64(const unsigned char *p, int n)
{
    if (n <= 0) {
        return 0;
    }
    unsigned long long v = _pynuodb_be_u64(p, n);
    unsigned long long m = (n < 8) ? (1ULL << ((n << 3) - 1)) : 0x8000000000000000ULL;
    return (long long)((v ^ m) - m);
}

NUODB_INLINE double _pynuodb_be_double(const unsigned char *p, int n)
{
    unsigned char buf[8] = {0};
    if (n < 0) {
        n = 0;
    } else if (n > 8) {
        n = 8;
    }
    memcpy(buf, p, n);
    uint64_t v = _pynuodb_be_u64(buf, 8);
    double d;
    memcpy(&d, &v, 8);
    return d;
}

/* Try to make boundary checking fast through inline and marking the happy path as likely*/
NUODB_INLINE int _pynuodb_avail_ok(Py_ssize_t pos, Py_ssize_t need, Py_ssize_t n)
{
    return NUODB_LIKELY(need >= 0 && pos <= n - need);
}

NUODB_INLINE PyObject *_pynuodb_pylong_be_signed(const unsigned char *p, Py_ssize_t n)
{
    if (NUODB_UNLIKELY(n > 8)) {
        PyErr_SetString(PyExc_OverflowError,
            "_pynuodb_pylong_be_signed: width > 8 bytes not supported");
        return NULL;
    }
    return PyLong_FromLongLong(_pynuodb_be_i64(p, (int)n));
}

#endif /* PYNUODB_CUTIL_H */
