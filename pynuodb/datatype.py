"""A module for housing the datatype classes.

(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

Exported Classes:
Binary -- Class for a Binary object

Exported Functions:
DateFromTicks -- Converts ticks to a Date object.
TimeFromTicks -- Converts ticks to a Time object.
TimestampFromTicks -- Converts ticks to a Timestamp object.
DateToTicks -- Converts a Date object to ticks.
TimeToTicks -- Converts a Time object to ticks.
TimestampToTicks -- Converts a Timestamp object to ticks.
TypeObjectFromNuodb -- Converts a Nuodb column type name to a TypeObject variable.

TypeObject Variables:
STRING -- TypeObject(str)
BINARY -- TypeObject(str)
NUMBER -- TypeObject(int, decimal.Decimal)
DATETIME -- TypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID -- TypeObject()
"""

__all__ = ['Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
           'TimestampFromTicks', 'DateToTicks', 'TimeToTicks',
           'TimestampToTicks', 'Binary', 'STRING', 'BINARY', 'NUMBER',
           'DATETIME', 'ROWID', 'TypeObjectFromNuodb']

import sys
import decimal
import time

from datetime import datetime as Timestamp, date as Date, time as Time
from datetime import timedelta as TimeDelta

try:
    from typing import Tuple, Union  # pylint: disable=unused-import
except ImportError:
    pass

from .exception import DataError

isP2 = sys.version[0] == '2'


class Binary(bytes):
    """A binary string.

    If passed a string we assume it's encoded as LATIN-1, which ensures that
    the characters 0-255 are considered single-character sequences.
    """

    def __new__(cls, data):
        # type: (Union[str, bytes, bytearray]) -> Binary
        # I can't figure out how to get mypy to be OK with this.
        if isinstance(data, bytearray):
            return bytes.__new__(cls, data)  # type: ignore
        # In Python2 there's no distinction between str and bytes :(
        if isinstance(data, str) and not isP2:
            return bytes.__new__(cls, data.encode('latin-1'))  # type: ignore
        return bytes.__new__(cls, data)  # type: ignore

    def __str__(self):
        # type: () -> str
        # This is pretty terrible but it's what the old version did.
        # What does it really mean to run str(Binary)?  That should probably
        # be illegal, but I'm sure lots of code does "%s" % (Binary(x)) or
        # the equivalent.  In Python 3 we have to remove the 'b' prefix too.
        # I'll leave this for consideration at some future time.
        return repr(self)[1:-1] if isP2 else repr(self)[2:-1]

    @property
    def string(self):
        # type: () -> bytes
        """The old implementation of Binary provided this."""
        return self


def DateFromTicks(ticks):
    # type: (int) -> Date
    """Convert ticks to a Date object."""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks, micro=0):
    # type: (int, int) -> Time
    """Convert ticks to a Time object."""
    return Time(*time.localtime(ticks)[3:6] + (micro,))


def TimestampFromTicks(ticks, micro=0):
    # type: (int, int) -> Timestamp
    """Convert ticks to a Timestamp object."""
    return Timestamp(*time.localtime(ticks)[:6] + (micro,))


def DateToTicks(value):
    # type: (Date) -> int
    """Convert a Date object to ticks."""
    timeStruct = Date(value.year, value.month, value.day).timetuple()
    try:
        return int(time.mktime(timeStruct))
    except Exception:
        raise DataError("Year out of range")


def TimeToTicks(value):
    # type: (Time) -> Tuple[int, int]
    """Convert a Time object to ticks."""
    timeStruct = TimeDelta(hours=value.hour, minutes=value.minute,
                           seconds=value.second,
                           microseconds=value.microsecond)
    timeDec = decimal.Decimal(str(timeStruct.total_seconds()))
    return (int((timeDec + time.timezone) * 10**abs(timeDec.as_tuple()[2])),
            abs(timeDec.as_tuple()[2]))


def TimestampToTicks(value):
    # type: (Timestamp) -> Tuple[int, int]
    """Convert a Timestamp object to ticks."""
    timeStruct = Timestamp(value.year, value.month, value.day, value.hour,
                           value.minute, value.second).timetuple()
    try:
        if not value.microsecond:
            return (int(time.mktime(timeStruct)), 0)
        micro = decimal.Decimal(value.microsecond) / decimal.Decimal(1000000)
        t1 = decimal.Decimal(int(time.mktime(timeStruct))) + micro
        tlen = len(str(micro)) - 2
        return (int(t1 * decimal.Decimal(int(10**tlen))), tlen)
    except Exception:
        raise DataError("Year out of range")


class TypeObject(object):
    """A SQL type object."""

    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        return -1


STRING = TypeObject(str)
BINARY = TypeObject(str)
NUMBER = TypeObject(int, decimal.Decimal)
DATETIME = TypeObject(Timestamp, Date, Time)
ROWID = TypeObject()

TYPEMAP = {"<null>": None,
           "string": STRING,
           "char": STRING,
           "varchar": STRING,
           "smallint": NUMBER,
           "integer": NUMBER,
           "bigint": NUMBER,
           "float": NUMBER,
           "double": NUMBER,
           "date": DATETIME,
           "timestamp": DATETIME,
           "time": DATETIME,
           "clob": BINARY,
           "blob": BINARY,
           "numeric": NUMBER,
           "number": NUMBER,
           "bytes": BINARY,
           "binary": BINARY,
           "binary varying": BINARY,
           "boolean": NUMBER,
           "timestamp without time zone": DATETIME,
           "timestamp with time zone": DATETIME,
           "time without time zone": DATETIME,
           # Old types used by NuoDB <2.0.3
           "binarystring": BINARY,
           "binaryvaryingstring": BINARY,
           }


def TypeObjectFromNuodb(nuodb_type_name):
    # type: (str) -> TypeObject
    """Return a TypeObject based on the supplied NuoDB column type name."""
    name = nuodb_type_name.strip()
    obj = TYPEMAP.get(name)
    if obj is None:
        raise DataError('received unknown column type "%s"' % (name))
    return obj
