"""A module to calculate date from number of days from 1/1/1970.
This uses the Georgian Calendar for dates from 10/15/1582 and
the Julian Calendar fro dates before 10/4/1582.

(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

Calendar functions for computing year,month,day relative to number
of days from unix epoch (1/1/1970)
  - Georgian Calendar for dates from and including 10/15/1582.
  - Julian Calendar for dates before and including 10/4/1582.

10/5/1582 - 10/14/1582 are invalid dates.  These functions are needed
to map dates same as the calendar function in the nuodb server.  python
datetime uses a proleptic Gregorian calendar.

"""
from typing import Tuple  # pylint: disable=unused-import
import jdcal

JD_EPOCH = sum(jdcal.gcal2jd(1970, 1, 1))
GREGORIAN_START = (1582, 10, 15)
JULIAN_END = (1582, 10, 4)


def ymd2day(year, month, day):
    # type: (int, int, int) -> int
    """
    Converts given year , month, day to number of days since unix EPOCH.
      year  - between 0001-9999
      month - 1 - 12
      day   - 1 - 31 (depending upon month and year)
    The calculation will be based upon:
      - Georgian Calendar for dates from and including 10/15/1582.
      - Julian Calendar for dates before and including 10/4/1582.
    Dates between the Julian Calendar and Georgian Calendar don't exist a
    ValueError will be raised.
    """

    if (year, month, day) >= GREGORIAN_START:
        jd = sum(jdcal.gcal2jd(year, month, day))
    elif (year, month, day) <= JULIAN_END:
        jd = sum(jdcal.jcal2jd(year, month, day))
    else:
        raise ValueError("Invalid date: the range Oct 5-14, 1582 does not exist")

    daynum = int(jd - JD_EPOCH)
    if daynum < -719164:
        raise ValueError("Invalid date: before 1/1/1")
    if daynum > 2932896:
        raise ValueError("Invalid date: after 9999/12/31")
    return daynum


def day2ymd(daynum):
    # type: (int) -> Tuple[int, int, int]
    """
    Converts given day number relative to 1970-01-01 to a tuple (year,month,day).


    The calculation will be based upon:
      - Georgian Calendar for dates from and including 10/15/1582.
      - Julian Calendar for dates before and including 10/4/1582.

    Dates between the Julian Calendar and Georgian Calendar do not exist.

       +----------------------------+
       |  daynum | (year,month,day) |
       |---------+------------------|
       |       0 | (1970,1,1)       |
       | -141427 | (1582,10,15)     |
       | -141428 | (1582,10,4)      |
       | -719164 | (1,1,1)          |
       | 2932896 | (9999,12,31)     |
       +----------------------------+
    """
    if daynum >= -141427 and daynum <= 2932896:
        y, m, d, _ = jdcal.jd2gcal(daynum, JD_EPOCH)
    elif daynum < -141427 and daynum >= -719614:
        y, m, d, _ = jdcal.jd2jcal(daynum, JD_EPOCH)
    else:
        raise ValueError("Invalid daynum (not between 1/1/1 and 12/31/9999 inclusive).")

    return y, m, d
