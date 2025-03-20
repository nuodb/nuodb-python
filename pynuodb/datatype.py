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
DATE -- TypeObject(datetime.date)
DATETIME -- TypeObject(datetime.datetime, datetime.time)
ROWID -- TypeObject()
"""

__all__ = ['Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
           'TimestampFromTicks', 'DateToTicks', 'TimeToTicks',
           'TimestampToTicks', 'Binary', 'STRING', 'BINARY', 'NUMBER',
           'DATETIME', 'DATE', 'ROWID', 'TypeObjectFromNuodb']

import sys

try:
    from typing import Tuple, Union  # pylint: disable=unused-import
except ImportError:
    pass

from .exception import DataError
from .calendar  import ymd2day, day2ymd
from datetime import datetime as Timestamp, date as Date, time as Time, timezone as TimeZone
from datetime import timedelta as TimeDelta
import decimal
import time
import tzlocal
from zoneinfo import ZoneInfo


Binary = bytes

TICKSDAY=86400

def DateFromTicks(ticks: int) -> Date:
    """ convert seconds from epoch to Date """

    # ticks is calculated from 1/1/1970 00:00:00 UTC
    day = ticks//TICKSDAY
    y,m,d = day2ymd(day)
    return Date(year=y,month=m,day=d)


def TimeFromTicks(ticks : int, micro : int = 0, zoneinfo: ZoneInfo = None ) -> Time:
    """
    Convert input to a Time object.
    Input:
       ticks  - number of seconds
       micro  - number of micoseconds
       as_gmt - whether to apply timezone offset
              - True - don't apply timezone offset.
              - False - apply standard time offset of the local timezone
                        to the time.
    """

    # ticks with timezone can be less than 0 and greater than TICKSDAY
    # add a day and mod.

    # time.timezone is used since Time object does not have
    # a date , nuodb uses time.timezone and not time.alttime for TIME.

    inticks = ticks
    ticks += TICKSDAY
    if zoneinfo:
        stdtime = Date.today().replace(month=1,day=1)
        dt_dst  = Timestamp.combine(stdtime,Time(),tzinfo=zoneinfo)
        ticks -= dt_dst.utcoffset() 
    ticks = ticks % TICKSDAY

    hour = ticks // 3600
    secs = ticks % 3600
    min  = (secs // 60 )
    sec  = secs % 60
    _time = Time(hour=hour,minute=min,second=sec,microsecond=micro)
    return _time


def TimestampFromTicks(ticks : int , micro : int = 0, zoneinfo : ZoneInfo = None) -> Timestamp:
    """
    Convert seconds, microseconds to a Timestamp object.

    ticks - number of seconds since epoch
    micro - microseconds of fractional second since epoch

    """

    # DateFromTicks is gmt.
    date = DateFromTicks(ticks)
    timeticks = ticks % TICKSDAY
    if ticks < 0 and micro:
        timeticks -= 1
    time = TimeFromTicks(timeticks, micro % 1000000)

    # combine date, time and convert to local timezone.
    dt = Timestamp.combine(date,time,TimeZone.utc).astimezone(zoneinfo)
    return dt


def DateToTicks(value : Date) -> int:
    """
    Convert Date (year,month,day) to number of seconds since epoch.
    """

    day = ymd2day(value.year, value.month, value.day)
    return day * TICKSDAY


def packtime(seconds: int, microseconds: int) -> (int, int):
    ndiv=0
    msecs  = microseconds
    shiftr = 1000000
    shiftl = 1
    while (microseconds % shiftr):
        shiftr //= 10
        shiftl *= 10
        ndiv +=1
    return ( seconds*shiftl + microseconds//shiftr, ndiv )


def TimeToTicks(value : Time) -> Tuple[int,int]:
    """
    Convert Time to packed integer.

    This assumes that the inputted time is naive.  that is without timezone.

    Input:
       value    - datetime.time - time object with microsecond precision to pack

    Output:
       (int , npos ) - npos defines number of positions (10**npos) that seconds was
                       switched to pack microseconds.

    Example:
      With local timezone (UTC):
       (hour=0,minute=0,second=10,microsecond=1) -> (10000001, 6)
       (hour=0,minute=0,second=10,microsecond=100000) -> (11, 1)
      With local timezone (America/New_York): (add offset (05:00))
       (hour=0,minute=0,second=10,microsecond=1) -> (180010000001, 6)
       (hour=0,minute=0,second=10,microsecond=100000) -> (180011, 1)
    """

    # Time can have an associated timezone or be naive.

    # do we use:
    #   local timezone standard time offset
    #   connection tzinfo standard time offset
    #   time tzinfo standard time offset

    # convert time to local timezone.
    today = Date.today()
    year = today.year

    dt = Timestamp.combine(today, value)
    dt = dt.astimezone(ZoneInfo(tzlocal.get_localzone_name()))
    ntime = dt.time()

    secs  = ntime.hour * 3600
    secs += ntime.minute * 60
    secs += ntime.second
    return packtime(secs,ntime.microsecond)

def TimestampToTicks(value : Timestamp) -> Tuple[int, int]:
    """
    Convert datetime.datetime to packed integer.

    The datetime.datetime is first converted to utc time and then packed.

    Input:
       value    - datetime.datetime - datetime object with microsecond precision to pack

    Output:
       (int , npos ) - npos defines number of positions (10**npos) that seconds was
                       switched to pack microseconds.

    Example:
    """

    dt = value.astimezone(TimeZone.utc)
    timesecs  = ymd2day(dt.year,dt.month,dt.day) * TICKSDAY
    timesecs += dt.hour * 3600
    timesecs += dt.minute * 60
    timesecs += dt.second
    return packtime(timesecs,dt.microsecond)

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
DATETIME = TypeObject(Timestamp)
DATE = TypeObject(Date)
TIME = TypeObject(Time)
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

