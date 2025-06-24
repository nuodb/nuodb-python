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
from datetime import datetime as Timestamp, date as Date, time as Time
from datetime import timedelta as TimeDelta
from datetime import tzinfo  # pylint: disable=unused-import

try:
    from typing import Tuple, Union  # pylint: disable=unused-import
except ImportError:
    pass

import tzlocal
from .exception import DataError
from .calendar import ymd2day, day2ymd

# zoneinfo.ZoneInfo is preferred but not introduced until python3.9
if sys.version_info >= (3, 9):
    # used for python>=3.9 with support for zoneinfo.ZoneInfo
    from zoneinfo import ZoneInfo  # pylint: disable=unused-import
    from datetime import timezone
    UTC = timezone.utc

    def utc_TimeStamp(year, month, day, hour=0, minute=0, second=0, microsecond=0):
        # type: (int, int, int, int, int, int, int) -> Timestamp
        """
        timezone aware datetime with UTC timezone.
        """
        return Timestamp(year=year, month=month, day=day,
                         hour=hour, minute=minute, second=second,
                         microsecond=microsecond, tzinfo=UTC)

    def timezone_aware(tstamp, tz_info):
        # type: (Timestamp, tzinfo) -> Timestamp
        return tstamp.replace(tzinfo=tz_info)

else:
    # used for python<3.9 without support for zoneinfo.ZoneInfo
    from pytz import utc as UTC

    def utc_TimeStamp(year, month, day, hour=0, minute=0, second=0, microsecond=0):
        # type: (int, int, int, int, int, int, int) -> Timestamp
        """
        timezone aware datetime with UTC timezone.
        """
        dt = Timestamp(year=year, month=month, day=day,
                       hour=hour, minute=minute, second=second, microsecond=microsecond)
        return UTC.localize(dt, is_dst=None)

    def timezone_aware(tstamp, tz_info):
        # type: (Timestamp, tzinfo) -> Timestamp
        return tz_info.localize(tstamp, is_dst=None)  # type: ignore[attr-defined]

isP2 = sys.version[0] == '2'
TICKSDAY = 86400
LOCALZONE = tzlocal.get_localzone()

if hasattr(tzlocal, 'get_localzone_name'):
    # tzlocal >= 3.0
    LOCALZONE_NAME = tzlocal.get_localzone_name()
else:
    # tzlocal < 3.0
    # local_tz is a pytz.tzinfo object.  should have zone attribute
    LOCALZONE_NAME = getattr(LOCALZONE, 'zone')


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
    y, m, d = day2ymd(ticks // TICKSDAY)
    return Date(year=y, month=m, day=d)


def TimeFromTicks(ticks, micro=0, zoneinfo=LOCALZONE):
    # type: (int, int, tzinfo) -> Time
    """Convert ticks to a Time object."""

    # NuoDB release <= 7.0, it's possible that ticks is
    # expressed as a Timestamp and not just a Time.
    # NuoDB release > 7.0,  ticks will be between (-TICKSDAY,2*TICKSDAY)

    if ticks < -TICKSDAY or ticks > 2 * TICKSDAY:
        dt = TimestampFromTicks(ticks, micro, zoneinfo)
        return dt.time()

    seconds = ticks % TICKSDAY
    hours = (seconds // 3600) % 24
    minutes = (seconds // 60) % 60
    seconds = seconds % 60
    tstamp = Timestamp.combine(Date(1970, 1, 1),
                               Time(hour=hours,
                                    minute=minutes,
                                    second=seconds,
                                    microsecond=micro)
                               )
    # remove offset that the engine added
    utcoffset = zoneinfo.utcoffset(tstamp)
    if utcoffset:
        tstamp += utcoffset
    # returns naive time , should a timezone-aware time be returned instead
    return tstamp.time()


def TimestampFromTicks(ticks, micro=0, zoneinfo=LOCALZONE):
    # type: (int, int, tzinfo) -> Timestamp
    """Convert ticks to a Timestamp object."""
    day = ticks // TICKSDAY
    y, m, d = day2ymd(day)
    timeticks = ticks % TICKSDAY
    hour = timeticks // 3600
    sec  = timeticks % 3600
    min  = sec // 60
    sec  %= 60

    # this requires both utc and current session to be between year 1 and year 9999 inclusive.
    # nuodb could store a timestamp that is east of utc where utc would be year 10000.
    if y < 10000:
        dt = utc_TimeStamp(year=y, month=m, day=d, hour=hour,
                           minute=min, second=sec, microsecond=micro)
        dt = dt.astimezone(zoneinfo)
    else:
        # shift one day.
        dt = utc_TimeStamp(year=9999, month=12, day=31, hour=hour,
                           minute=min, second=sec, microsecond=micro)
        dt = dt.astimezone(zoneinfo)
        # add day back.
        dt += TimeDelta(days=1)
    # returns timezone-aware datetime
    return dt


def DateToTicks(value):
    # type: (Date) -> int
    """Convert a Date object to ticks."""
    day = ymd2day(value.year, value.month, value.day)
    return day * TICKSDAY


def _packtime(seconds, microseconds):
    # type: (int, int) -> Tuple[int,int]
    if microseconds:
        ndiv = 0
        shiftr = 1000000
        shiftl = 1
        while (microseconds % shiftr):
            shiftr //= 10
            shiftl *= 10
            ndiv += 1
        return (seconds * shiftl + microseconds // shiftr, ndiv)
    else:
        return (seconds, 0)


def TimeToTicks(value, zoneinfo=LOCALZONE):
    # type: (Time, tzinfo) -> Tuple[int, int]
    """Convert a Time object to ticks."""
    epoch = Date(1970, 1, 1)
    tz_info = value.tzinfo
    if not tz_info:
        tz_info = zoneinfo

    my_time = Timestamp.combine(epoch, Time(hour=value.hour,
                                            minute=value.minute,
                                            second=value.second,
                                            microsecond=value.microsecond
                                            ))
    my_time = timezone_aware(my_time, tz_info)

    utc_time = Timestamp.combine(epoch, Time())
    utc_time = timezone_aware(utc_time, UTC)

    td = my_time - utc_time

    # fence time within a day range
    if td < TimeDelta(0):
        td = td + TimeDelta(days=1)
    if td > TimeDelta(days=1):
        td = td - TimeDelta(days=1)

    time_dec = decimal.Decimal(str(td.total_seconds()))
    exponent = time_dec.as_tuple()[2]
    if not isinstance(exponent, int):
        # this should not occur
        raise ValueError("Invalid exponent in Decimal: %r" % exponent)
    return (int(time_dec * 10**abs(exponent)), abs(exponent))


def TimestampToTicks(value, zoneinfo=LOCALZONE):
    # type: (Timestamp, tzinfo) -> Tuple[int, int]
    """Convert a Timestamp object to ticks."""
    # if naive timezone then leave date/time but change tzinfo to
    # be connection's timezone.
    if value.tzinfo is None:
        value = timezone_aware(value, zoneinfo)
    dt = value.astimezone(UTC)
    timesecs  = ymd2day(dt.year, dt.month, dt.day) * TICKSDAY
    timesecs += dt.hour * 3600
    timesecs += dt.minute * 60
    timesecs += dt.second
    packedtime = _packtime(timesecs, dt.microsecond)
    return packedtime


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
NULL = TypeObject(None)

TYPEMAP = {"<null>": NULL,
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
