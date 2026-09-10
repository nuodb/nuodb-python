# Wire-protocol type codes, mirrored from protocol.py as C ints so
# decode_next_batch's per-cell range checks in _fetch.pyx don't pay for a
# Python attribute lookup on every column.
from . import protocol as _protocol

cdef int NULL_V                  = _protocol.NULL
cdef int TRUE_V                  = _protocol.TRUE
cdef int FALSE_V                 = _protocol.FALSE
cdef int INTMINUS10              = _protocol.INTMINUS10
cdef int INT0                    = _protocol.INT0  # inline value 0: end-of-batch marker
cdef int INT31                   = _protocol.INT31
cdef int INTLEN0                 = _protocol.INTLEN0  # == INT31; codes 52-59 carry 1-8 byte integers
cdef int INTLEN8                 = _protocol.INTLEN8
cdef int SCALEDLEN0              = _protocol.SCALEDLEN0  # base for 1-8 byte scaled decimals (61-68)
cdef int SCALEDLEN8              = _protocol.SCALEDLEN8
cdef int UTF8COUNT0              = _protocol.UTF8COUNT0  # base for length-prefixed strings (69-72)
cdef int UTF8COUNT1              = _protocol.UTF8COUNT1
cdef int UTF8COUNT4              = _protocol.UTF8COUNT4
cdef int OPAQUECOUNT0            = _protocol.OPAQUECOUNT0  # base for length-prefixed binary (73-76)
cdef int OPAQUECOUNT1            = _protocol.OPAQUECOUNT1
cdef int OPAQUECOUNT4            = _protocol.OPAQUECOUNT4
cdef int DOUBLELEN0              = _protocol.DOUBLELEN0  # 77 == double 0.0; 78-85 carry 1-8 byte doubles
cdef int DOUBLELEN8              = _protocol.DOUBLELEN8
cdef int MILLISECLEN0            = _protocol.MILLISECLEN0  # base for 1-8 byte millisecond timestamps
cdef int MILLISECLEN8            = _protocol.MILLISECLEN8
cdef int NANOSECLEN0             = _protocol.NANOSECLEN0  # base for 1-8 byte nanosecond timestamps
cdef int NANOSECLEN8             = _protocol.NANOSECLEN8
cdef int TIMELEN0                = _protocol.TIMELEN0  # base for 1-4 byte ms-since-midnight
cdef int TIMELEN4                = _protocol.TIMELEN4
cdef int UTF8LEN0                = _protocol.UTF8LEN0  # base for 0-39 byte inline-length strings
cdef int UTF8LEN39               = _protocol.UTF8LEN39
cdef int OPAQUELEN0              = _protocol.OPAQUELEN0  # base for 0-39 byte inline-length binary
cdef int OPAQUELEN39             = _protocol.OPAQUELEN39
cdef int BLOBLEN0                = _protocol.BLOBLEN0  # base for 0-4 byte length-prefixed BLOB
cdef int BLOBLEN4                = _protocol.BLOBLEN4
cdef int CLOBLEN0                = _protocol.CLOBLEN0  # base for 0-4 byte length-prefixed CLOB
cdef int CLOBLEN4                = _protocol.CLOBLEN4
cdef int UUID_C                  = _protocol.UUID
cdef int SCALEDDATELEN0          = _protocol.SCALEDDATELEN0  # 201-208 carry 1-8 byte scaled dates
cdef int SCALEDDATELEN8          = _protocol.SCALEDDATELEN8
cdef int SCALEDTIMELEN0          = _protocol.SCALEDTIMELEN0  # 209-216 carry 1-8 byte scaled times
cdef int SCALEDTIMELEN8          = _protocol.SCALEDTIMELEN8
cdef int SCALEDTIMESTAMPLEN0     = _protocol.SCALEDTIMESTAMPLEN0  # 217-224 carry 1-8 byte scaled timestamps
cdef int SCALEDTIMESTAMPLEN8     = _protocol.SCALEDTIMESTAMPLEN8
# No LEN1..LEN8 range: 234-240 belong to ARRAYLEN/SCALEDCOUNT3/DEBUGBARRIER.
cdef int SCALEDTIMESTAMPNOTZLEN0 = _protocol.SCALEDTIMESTAMPNOTZLEN0
cdef int SCALEDTIMESTAMPNOTZ     = _protocol.SCALEDTIMESTAMPNOTZ
