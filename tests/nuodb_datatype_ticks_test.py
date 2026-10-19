# -*- coding: utf-8 -*-
"""
(C) Copyright 2026 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

The datetime fast paths in pynuodb.datatype must give exactly the same results
as the original calendar-based conversions (kept as the _*_slow functions),
including the timezone and the (ticks, scale) encoding sent to the server.
These tests need no database.
"""

import datetime
import random
from zoneinfo import ZoneInfo

import pytest

from pynuodb import datatype

TICKSDAY = datatype.TICKSDAY
GREGORIAN_START_DAY = -141427       # 1582-10-15
LAST_DAY = 2932896                  # 9999-12-31
FIRST_DAY = -719164                 # 0001-01-01

ZONES = [
    datatype.UTC,
    ZoneInfo('America/New_York'),
    ZoneInfo('Asia/Kolkata'),            # +05:30
    ZoneInfo('Australia/Lord_Howe'),     # +10:30, 30-minute DST
    ZoneInfo('Pacific/Kiritimati'),      # +14:00
    datetime.timezone(datetime.timedelta(hours=-7, seconds=13)),
    datatype.LOCALZONE,
]


def _outcome(fn, *args):
    try:
        result = fn(*args)
    except (ValueError, OverflowError) as e:
        return type(e)
    return (result, type(result), getattr(result, 'tzinfo', None))


def test_date_from_ticks_matches_calendar():
    days = list(range(GREGORIAN_START_DAY, LAST_DAY + 1, 13))
    days += [GREGORIAN_START_DAY, GREGORIAN_START_DAY - 1, LAST_DAY, 0, -1,
             FIRST_DAY, LAST_DAY + 1]
    rng = random.Random(1)
    days += [rng.randint(FIRST_DAY, GREGORIAN_START_DAY - 1) for _ in range(2000)]
    for day in days:
        for ticks in (day * TICKSDAY, day * TICKSDAY + TICKSDAY - 1):
            assert _outcome(datatype.DateFromTicks, ticks) == \
                _outcome(datatype._DateFromTicks_slow, ticks), day


def test_timestamp_from_ticks_matches_calendar():
    rng = random.Random(2)
    for _ in range(30000):
        day = rng.choice([rng.randint(FIRST_DAY, LAST_DAY), rng.randint(-1000, 30000),
                          LAST_DAY, GREGORIAN_START_DAY, GREGORIAN_START_DAY - 1])
        ticks = day * TICKSDAY + rng.randint(0, TICKSDAY - 1)
        micro = rng.choice([0, rng.randint(0, 999999)])
        tz = rng.choice(ZONES + [None])
        assert _outcome(datatype.TimestampFromTicks, ticks, micro, tz) == \
            _outcome(datatype._TimestampFromTicks_slow, ticks, micro, tz), \
            (ticks, micro, tz)


def test_time_to_ticks_matches_original_encoding():
    rng = random.Random(3)
    for _ in range(30000):
        micro = rng.choice([0, rng.randint(0, 999999),
                            rng.randint(1, 9) * 100000, rng.randint(1, 99) * 10])
        tzinfo = rng.choice([None, None, datatype.UTC, ZoneInfo('Asia/Kolkata'),
                             datetime.timezone(datetime.timedelta(hours=rng.randint(-12, 14)))])
        value = datetime.time(rng.randint(0, 23), rng.randint(0, 59),
                              rng.randint(0, 59), micro, tzinfo=tzinfo)
        tz = rng.choice(ZONES)
        fast = datatype.TimeToTicks(value, tz)
        assert fast == datatype._TimeToTicks_slow(value, tz), (value, tz)
        assert type(fast[0]) is int and type(fast[1]) is int


def test_time_to_ticks_whole_seconds_keep_scale_one():
    """The original Decimal(str(float)) encoding sent whole seconds with
    scale 1; changing that would change the bytes on the wire."""
    value = datetime.time(12, 0, 0)
    assert datatype.TimeToTicks(value, datatype.UTC) == (432000, 1)
    assert datatype.TimeToTicks(datetime.time(0, 0, 0), datatype.UTC) == (0, 1)
    assert datatype.TimeToTicks(datetime.time(0, 0, 0, 50), datatype.UTC) == (5, 5)


def test_time_to_ticks_without_utcoffset_falls_back():
    class NoOffset(datetime.tzinfo):
        def utcoffset(self, dt):
            return None

    # The original code can't subtract a naive datetime from an aware one;
    # the fast path must hand such tzinfos to it and raise the same error.
    value = datetime.time(1, 2, 3)
    with pytest.raises(TypeError):
        datatype._TimeToTicks_slow(value, NoOffset())
    with pytest.raises(TypeError):
        datatype.TimeToTicks(value, NoOffset())
