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

try:
    from typing import Tuple, Union  # pylint: disable=unused-import
except ImportError:
    pass

from datetime import datetime as Timestamp, date as Date, time as Time, timezone as TimeZone
from datetime import timedelta as TimeDelta
import decimal
import time
import tzlocal
from .exception import DataError
from .calendar  import ymd2day, day2ymd

isP2 = sys.version[0] == '2'

localZoneInfo = tzlocal.get_localzone()

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

TICKSDAY=86400
def DateFromTicks(ticks):
    # type: (int) -> Date
    """Convert ticks to a Date object."""
    #return Date(*time.localtime(ticks)[:3])
    y,m,d = day2ymd(ticks//TICKSDAY)
    return Date(year=y,month=m,day=d)


def TimeFromTicks(ticks, micro=0, zoneinfo=localZoneInfo ):
    # type: (int, int) -> Time
    """Convert ticks to a Time object."""
    #return Time(*time.localtime(ticks)[3:6] + (micro,))

    print(f"TimeFromTicks: {ticks} {micro}")
    if ticks >= TICKSDAY or ticks <= -TICKSDAY:
        dt = TimestampFromTicks(ticks,micro,zoneinfo)
        _time = dt.time()
    else:
        if ticks < 0 and micro:
            ticks -= 1
        timeticks = ticks % TICKSDAY
        hour  = timeticks // 3600
        sec   = timeticks % 3600
        min   = (sec // 60 )
        sec  %= 60
        micro %= 1000000

        # convert time to standard time offset from utc for given timezone. Use
        # today's date for offset calculation.
        
        today  = Timestamp.now().date()
        time   = Time(hour=hour,minute=min,second=sec,microsecond=micro,tzinfo=TimeZone.utc)
        tstamp = Timestamp.combine(today,time).astimezone(zoneinfo)
        dst_offset = tstamp.dst()
        if dst_offset:
            tstamp -= dst_offset
        _time    = tstamp.time()
    return _time



def TimestampFromTicks(ticks, micro=0,zoneinfo=localZoneInfo):
    # type: (int, int) -> Timestamp
    """Convert ticks to a Timestamp object."""
    #return Timestamp(*time.localtime(ticks)[:6] + (micro,))
    day = ticks//TICKSDAY
    y,m,d = day2ymd(day)

    timeticks = ticks % TICKSDAY
    hour = timeticks // 3600
    sec  = timeticks % 3600
    min  =  sec // 60
    sec  %=  60

    dt = Timestamp(year=y,month=m,day=d,hour=hour,minute=min,second=sec,microsecond=micro,tzinfo=TimeZone.utc)
    dt = dt.astimezone(zoneinfo)
    #print(f"TimestampFromTicks: {ticks} {micro} - {dt}")
    return dt


def DateToTicks(value):
    # type: (Date) -> int
    """Convert a Date object to ticks."""
    # timeStruct = Date(value.year, value.month, value.day).timetuple()
    # try:
    #     return int(time.mktime(timeStruct))
    # except Exception:
    #     raise DataError("Year out of range")
    day = ymd2day(value.year, value.month, value.day)
    return day * TICKSDAY

def packtime(seconds: int, microseconds: int) -> (int, int):
    if microseconds:
        ndiv=0
        msecs  = microseconds
        shiftr = 1000000
        shiftl = 1
        while (microseconds % shiftr):
            shiftr //= 10
            shiftl *= 10
            ndiv +=1
        return ( seconds*shiftl + microseconds//shiftr, ndiv )
    else:
        return (seconds, 0)

# def packtime(seconds, microseconds):
#     if microseconds:
#         micro = decimal.Decimal(microseconds) / decimal.Decimal(1000000)
#         t1 = decimal.Decimal(str(seconds)) + micro
#         tlen = len(str(micro)) - 2
#         return (int(t1 * decimal.Decimal(int(10**tlen))), tlen)
#     else:
#         return (seconds, 0)

def TimeToTicks(value,zoneinfo = localZoneInfo):
    # type: (Time) -> Tuple[int, int]
    """Convert a Time object to ticks."""

    # convert time to time relative to connection timezone
    # using today as date.

    tstamp = Timestamp.combine(Date.today(),value)
    if tstamp.tzinfo is None:
        tstamp = tstamp.replace(tzinfo=zoneinfo)
    else:
        tstamp = tstamp.astimezone(zoneinfo)

    dst_offset = zoneinfo.dst(tstamp)
    utc_offset = zoneinfo.utcoffset(tstamp)
    std_offset = dst_offset - utc_offset
    
    timeStruct = TimeDelta(hours=tstamp.hour, minutes=tstamp.minute,
                           seconds=tstamp.second,
                           microseconds=tstamp.microsecond)
    timeDec = decimal.Decimal(str(timeStruct.total_seconds()))

    return (int((timeDec + std_offset) * 10**abs(timeDec.as_tuple()[2])),
            abs(timeDec.as_tuple()[2]))


def TimestampToTicks(value,zoneinfo  = localZoneInfo):
    # type: (Timestamp) -> Tuple[int, int]
    """Convert a Timestamp object to ticks."""
    # timeStruct = Timestamp(value.year, value.month, value.day, value.hour,
    #                        value.minute, value.second).timetuple()
    # try:
    #     if not value.microsecond:
    #         return (int(time.mktime(timeStruct)), 0)
    #     micro = decimal.Decimal(value.microsecond) / decimal.Decimal(1000000)
    #     t1 = decimal.Decimal(int(time.mktime(timeStruct))) + micro
    #     tlen = len(str(micro)) - 2
    #     return (int(t1 * decimal.Decimal(int(10**tlen))), tlen)
    # except Exception:
    #     raise DataError("Year out of range")

    # if naive timezone then leave date/time but change tzinfo to
    # be connection's timezone.    
    if value.tzinfo is None:
        value = value.replace(tzinfo=zoneinfo)
    dt = value.astimezone(TimeZone.utc)
    timesecs  = ymd2day(dt.year,dt.month,dt.day) * TICKSDAY
    timesecs += dt.hour * 3600
    timesecs += dt.minute * 60
    timesecs += dt.second
    packedtime = packtime(timesecs,dt.microsecond)
    #print(f'TimestampToTicks: {value} {timesecs} {dt.microsecond} =  {packedtime}')
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
