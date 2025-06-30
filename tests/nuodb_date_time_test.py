# -*- coding: utf-8 -*-
"""
(C) Copyright 2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import datetime
from contextlib import closing
import pytest
from pynuodb.exception import ProgrammingError
from .mock_tzs import localize, UTC, TimeZoneInfo

from . import nuodb_base


class TestNuoDBDateTime(nuodb_base.NuoBase):
    """Test datetime with timezone"""

    def test_connect_timezone(self):
        # type: () -> None
        """ test invalid TimeZone """
        with pytest.raises(ProgrammingError):
            self._connect(options={'TimeZone': 'XYZ'})

        # another invalid timezone
        # this would be handled okay by TE but, not understooded
        # by client
        with pytest.raises(ProgrammingError):
            self._connect(options={'TimeZone': 'PDT'})

    def test_nonnaive_timestamps(self):
        # type: () -> None
        """Test using different timezones with same connection"""

        dt = datetime.datetime(year=1990, month=1, day=1, hour=1, minute=30, second=10)

        # Local timezone is unknown, depends where we run from

        utc_dt = localize(dt, UTC)
        local_dt = localize(dt, TimeZoneInfo('America/New_York'))
        pst_dt = localize(dt, TimeZoneInfo('America/Los_Angeles'))

        with closing(self._connect(options={'TimeZone': 'America/Chicago'})) as con:
            cursor = con.cursor()
            cursor.execute("drop table if exists NONNAIVE")
            cursor.execute("create table NONNAIVE(tstamp datetime, dtstr string)")
            cursor.executemany("insert into NONNAIVE VALUES (?,?)", [
                (utc_dt, utc_dt.isoformat(),),
                (local_dt, local_dt.isoformat(),),
                (pst_dt, pst_dt.isoformat(),)
            ])
            con.commit()
            cursor.execute("select tstamp,dtstr from NONNAIVE")
            rows = cursor.fetchall()

            # given a timezone,  these should equal although the returned
            # row is actually connection timezone.
            assert rows[0][0] == utc_dt
            assert rows[1][0] == local_dt
            assert rows[2][0] == pst_dt
            assert rows[2][0] - rows[1][0] == datetime.timedelta(seconds=10800)

            assert rows[0][0].astimezone(UTC).isoformat() == rows[0][1]
            assert rows[1][0].astimezone(TimeZoneInfo('America/New_York')).isoformat() == rows[1][1]
            assert rows[2][0].astimezone(TimeZoneInfo('America/Los_Angeles')).isoformat() == rows[2][1]

            # timezone of return datetime should be same as timezone of connection
            tz_info = rows[0][0].tzinfo
            if hasattr(tz_info, 'key'):
                assert tz_info.key == 'America/Chicago'
            if hasattr(tz_info, 'zone'):
                assert tz_info.zone == 'America/Chicago'

    def test_pre_1900_date(self):
        # type: () -> None
        """Test insert and query of dates before 1900"""

        with closing(self._connect(options={'TimeZone': 'EST5EDT'})) as con:
            cursor = con.cursor()
            cursor.execute("create temporary table HISTORY(day date)")
            cursor.execute("insert into HISTORY VALUES ('November 19, 1863')")
            cursor.execute("select * from HISTORY")
            row = cursor.fetchone()

            assert row[0].year == 1863
            assert row[0].month == 11
            assert row[0].day   == 19

            cursor.execute("delete from HISTORY")
            cursor.execute("insert into HISTORY VALUES (?)",
                           (datetime.date(year=1865, month=4, day=14),))
            cursor.execute("select * from HISTORY")
            row = cursor.fetchone()

            assert row[0].year == 1865
            assert row[0].month == 4
            assert row[0].day   == 14

    def test_daylight_savings_time(self):
        # type: () -> None
        """Test read dates either in daylight saving time or not"""

        with closing(self._connect(options={'TimeZone': 'America/New_York'})) as con:
            cursor = con.cursor()

            tz = TimeZoneInfo('America/New_York')

            cursor.execute("select TIMESTAMP'2010-01-01 20:01:21' from DUAL")
            row = cursor.fetchone()
            nytime = row[0].astimezone(tz)
            assert nytime.hour == 20
            assert nytime.dst() == datetime.timedelta(seconds=0)

            cursor.execute("select TIMESTAMP'2010-06-01 20:01:21' from DUAL")
            row = cursor.fetchone()
            nytime = row[0].astimezone(tz)
            assert nytime.hour == 20

        with closing(self._connect(options={'TimeZone': 'Pacific/Auckland'})) as con:
            tz = TimeZoneInfo('Pacific/Auckland')
            cursor = con.cursor()
            cursor.execute("select TIMESTAMP'2010-01-01 20:01:21' from DUAL")
            row = cursor.fetchone()
            nztime = row[0]
            assert nztime.hour == 20

            cursor.execute("select TIMESTAMP'2010-06-01 20:01:21' from DUAL")
            row = cursor.fetchone()
            nztime = row[0]
            assert nztime.hour == 20
            assert nztime.dst() == datetime.timedelta(seconds=0)

    def test_gregorian_date(self):
        # type: () -> None
        """
        python datetime is based on the proleptic Gregorian
        calendar, which extends the Gregorian calendar backward before
        its actual adoption.  To handle same as NuoDB engine we need
        to use Julian calendar before Oct 5, 1582 and Gregorian
        calendar after Oct 14, 1582.  Note, Oct 5-14, 1582 are not
        valid dates per the switch over.
        """

        with closing(self._connect(options={'TimeZone': 'EST5EDT'})) as con:
            cursor = con.cursor()
            ddl = (
                "create temporary table HISTORY(day string, "
                "asdate DATE GENERATED ALWAYS AS (DATE(day)) PERSISTED)"
            )
            cursor.execute(ddl)
            cursor.execute("insert into HISTORY VALUES ('October 15, 1582')")
            cursor.execute("insert into HISTORY VALUES ('October 1, 1582')")
            cursor.execute("select DATE(day),asdate from HISTORY")
            row = cursor.fetchone()
            assert row[0].year == 1582
            assert row[0].month == 10
            assert row[0].day   == 15
            assert row[1].year == 1582
            assert row[1].month == 10
            assert row[1].day   == 15

            row = cursor.fetchone()
            assert row[0].year == 1582
            assert row[0].month == 10
            assert row[0].day   == 1
            assert row[1].year == 1582
            assert row[1].month == 10
            assert row[1].day   == 1

    def test_microseconds(self):
        # type: () -> None
        """Test timestamps with microseconds set before or after epoch"""

        with closing(self._connect(options={'TimeZone': 'EST5EDT'})) as con:
            cursor = con.cursor()

            dt = datetime.datetime(year=1990, month=1, day=1, hour=1,
                                   minute=30, second=10, microsecond=140)
            utc_dt = localize(dt, UTC)
            est_dt = utc_dt.astimezone(TimeZoneInfo('America/New_York'))

            cursor.execute("create temporary table T(ts TIMESTAMP)")
            cursor.execute("insert into T VALUES(?)", (utc_dt,))
            cursor.execute("select ts, EXTRACT(MICROSECOND FROM ts) from T")
            row = cursor.fetchone()

            assert row[0] == est_dt
            assert row[0] == utc_dt
            assert row[0].second == 10
            assert row[0].microsecond == 140
            assert row[1] == 140

            # less than datetime epoch
            dt = datetime.datetime(year=1969, month=1, day=1, hour=1,
                                   minute=30, second=10, microsecond=140)
            utc_dt = localize(dt, UTC)
            est_dt = utc_dt.astimezone(TimeZoneInfo('America/New_York'))

            cursor.execute("delete from T")
            cursor.execute("insert into T VALUES(?)", (utc_dt,))
            cursor.execute("select ts, EXTRACT(MICROSECOND FROM ts) from T")
            row = cursor.fetchone()
            assert row[0] == est_dt
            assert row[0] == utc_dt
            assert row[0].microsecond == 140
            assert row[0].second == 10
            # This fails but it's a bug with TE not driver
            # assert row[1] == 140

    def test_time_wraps_read(self):

        with closing(self._connect(options={'TimeZone': 'Pacific/Auckland'})) as con:
            cursor = con.cursor()
            # GMT is 1990-05-31 21:00:01.2
            cursor.execute("select CAST(TIMESTAMP'1990-06-01 9:00:01.2' AS TIME) from dual")
            row = cursor.fetchone()
            assert row[0].hour == 9
            assert row[0].minute == 0
            assert row[0].second == 1
            assert row[0].microsecond == 200000

        with closing(self._connect(options={'TimeZone': 'Pacific/Honolulu'})) as con:
            cursor = con.cursor()
            # GMT is 1990-06-02 05:00:01.2
            cursor.execute("select CAST(TIMESTAMP'1990-06-01 19:00:01.2' AS TIME) from dual")
            row = cursor.fetchone()
            assert row[0].hour == 19
            assert row[0].minute == 0
            assert row[0].second == 1
            assert row[0].microsecond == 200000

    def test_time_wraps_west_write(self):
        # saving time is very problematic.
        # time does not store the timezone and
        # the timezone is only useful when it's associated
        # with a date.  So that the offset for daylight savings
        # can be accounted for.
        #
        # in this test the conn timezone is in previous day (west) from
        # GMT. given a datetime we store that datetime (into a time field)
        # where the input is either.
        #   1. naive
        #   2. connection timezone aware
        #   3. gmt timezone aware
        #   4. east of gmt
        #
        # we store the time using:
        #   1. datetime
        #   2. string with timezone
        #   3. time
        #
        # case 1 -> te will map datetime to time
        # case 2 -> te will map string to time
        # case 3 -> client sends time object
        #
        # the test compares all of case 1 to see if returned
        # time is the same. (should be as date included with
        # time and timezone)
        #
        # then compare that what the client sent is same as what
        # the server computed (2 compared to 3).  These times
        # might be off.  as in both cases there is no date
        # assocated with the timezone and neither client nor
        # server can accurately account for daylight savings time.

        WESTTZ = "Pacific/Auckland"  # no dst in 1970, dst now
        CONNTZ = "America/Chicago"
        GMT    = "GMT"

        with closing(self._connect(options={'TimeZone': CONNTZ})) as con:
            cursor = con.cursor()
            cursor.execute('drop table WESTTZ if exists')
            cursor.execute("create table WESTTZ ( t TIME, t_as_string TIME, dt TIME)")

            dt = datetime.datetime(year=1990, month=1, day=31, hour=21,
                                   minute=0, second=1, microsecond=200000)
            for month in [ 1, 7 ]:
                dt = dt.replace(month=month)
                dt_time = dt.time()
                dt_time_str = dt_time.isoformat() + " " + CONNTZ

                conn_dt = localize(dt, TimeZoneInfo(CONNTZ))
                conn_time = conn_dt.timetz()
                conn_time_str = conn_time.isoformat() + " " + CONNTZ

                utc_dt = conn_dt.astimezone(TimeZoneInfo(GMT))
                utc_time = utc_dt.timetz()
                # isoformat for utc_time includes timezone info already
                utc_time_str = utc_time.isoformat()

                west_dt = utc_dt.astimezone(TimeZoneInfo(WESTTZ))
                west_time = west_dt.timetz()
                west_time_str = west_time.isoformat() + " " + WESTTZ

                cursor.execute('insert into WESTTZ VALUES (?,?,?)',
                               (dt_time, dt_time_str, dt,)) # naive time (dst northern)
                cursor.execute('insert into WESTTZ VALUES (?,?,?)',
                               (conn_time, conn_time_str, conn_dt,)) # connection time (dst northern)
                cursor.execute('insert into WESTTZ VALUES (?,?,?)',
                               (utc_time, utc_time_str, utc_dt,)) # gmt (no dst)
                cursor.execute('insert into WESTTZ VALUES (?,?,?)',
                               (west_time, west_time_str, west_dt,)) # new zealand (dst southern)

            cursor.execute('select t, t_as_string, dt from WESTTZ')
            rows = cursor.fetchall()
            con.commit()

            # using timestamp to initialize time no issues with daylight savings time
            assert rows[0][2] == rows[1][2]
            assert rows[2][2] == rows[1][2]
            assert rows[3][2] == rows[1][2]
            assert rows[4][2] == rows[1][2]
            assert rows[5][2] == rows[1][2]
            assert rows[6][2] == rows[1][2]
            assert rows[7][2] == rows[1][2]
            assert rows[0][2].hour == 21
            assert rows[0][2].minute == 0
            assert rows[0][2].second == 1
            assert rows[0][2].microsecond == 200000

            # confirm time created in client same as
            # time created in te via string assignment
            assert rows[0][0] == rows[0][1]
            assert rows[1][0] == rows[1][1]
            assert rows[2][0] == rows[2][1]
            assert rows[3][0] == rows[3][1]
            assert rows[4][0] == rows[4][1]
            assert rows[5][0] == rows[5][1]
            assert rows[6][0] == rows[6][1]
            assert rows[7][0] == rows[7][1]

            # naive time should be same as connection time
            assert rows[0][0] == rows[1][0]
            assert rows[4][0] == rows[5][0]
            assert rows[0][0].hour == 21
            assert rows[0][0].minute == 0
            assert rows[0][0].second == 1
            assert rows[0][0].microsecond == 200000

    def test_time_wraps_east_write(self):
        # same test as east but now connection timezone is east (next day) of GMT.

        CONNTZ = "Pacific/Auckland"
        EASTTZ = "America/Chicago"
        GMT    = "GMT"

        with closing(self._connect(options={'TimeZone': CONNTZ})) as con:
            cursor = con.cursor()
            cursor.execute('drop table EASTTZ if exists')
            cursor.execute("create table EASTTZ ( t TIME, t_as_string TIME, dt TIME)")

            dt = datetime.datetime(year=1990, month=1, day=31, hour=8,
                                   minute=0, second=1, microsecond=200000)

            for month in [ 1, 7 ]:
                dt = dt.replace(month=month)
                dt_time = dt.time()
                dt_time_str = dt_time.isoformat() + " " + CONNTZ

                conn_dt = localize(dt, TimeZoneInfo(CONNTZ))
                conn_time = conn_dt.timetz()
                conn_time_str = conn_time.isoformat() + " " + CONNTZ

                utc_dt = conn_dt.astimezone(TimeZoneInfo(GMT))
                utc_time = utc_dt.timetz()
                # isoformat for utc_time includes timezone info already
                utc_time_str = utc_time.isoformat()

                east_dt = utc_dt.astimezone(TimeZoneInfo(EASTTZ))
                east_time = east_dt.timetz()
                east_time_str = east_time.isoformat() + " " + EASTTZ

                cursor.execute('insert into EASTTZ VALUES (?,?,?)',
                               (dt_time, dt_time_str, dt,)) # naive time (dst southern)
                cursor.execute('insert into EASTTZ VALUES (?,?,?)',
                               (conn_time, conn_time_str, conn_dt,)) # connection time (dst southern)
                cursor.execute('insert into EASTTZ VALUES (?,?,?)',
                               (utc_time, utc_time_str, utc_dt,)) # gmt (no dst)
                cursor.execute('insert into EASTTZ VALUES (?,?,?)',
                               (east_time, east_time_str, east_dt,)) # chicago (dst northern)

            cursor.execute('select t, t_as_string, dt from EASTTZ')
            rows = cursor.fetchall()
            con.commit()

            # using timestamp to initialize time no issues with daylight savings time
            assert rows[0][2] == rows[1][2]
            assert rows[2][2] == rows[1][2]
            assert rows[3][2] == rows[1][2]
            assert rows[4][2] == rows[1][2]
            assert rows[5][2] == rows[1][2]
            assert rows[6][2] == rows[1][2]
            assert rows[7][2] == rows[1][2]
            assert rows[0][2].hour == 8
            assert rows[0][2].minute == 0
            assert rows[0][2].second == 1
            assert rows[0][2].microsecond == 200000

            # confirm time created in client same as
            # time created in te via string assignment
            assert rows[0][0] == rows[0][1]
            assert rows[1][0] == rows[1][1]
            assert rows[2][0] == rows[2][1]
            assert rows[3][0] == rows[3][1]
            assert rows[4][0] == rows[4][1]
            assert rows[5][0] == rows[5][1]
            assert rows[6][0] == rows[6][1]
            assert rows[7][0] == rows[7][1]

            # naive time should be same as connection time
            assert rows[0][0] == rows[1][0]
            assert rows[4][0] == rows[5][0]
            assert rows[0][0].hour == 8
            assert rows[0][0].minute == 0
            assert rows[0][0].second == 1
            assert rows[0][0].microsecond == 200000
