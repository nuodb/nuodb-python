
__doc__ = """
From: https://aa.usno.navy.mil/downloads/c15_usb_online.pdf

15.4.2 Rules for the Civil Use of the Gregorian Calendar

The Gregorian calendar uses the same months with the numbers of days
as it predecessor, the Julian calendar (see Table 15.5). Days are
counted from the first day of each month. Years are counted from the
initial epoch defined by Dionysius Exiguus (see § 15.1.8), and each
begins on January 1. A common year has 365 days but a leap year has
366, with an intercalary day, designated February 29, preceding March
1. Leap years are determined according to the following rule:

  Every year that is exactly divisible by 4 is a leap year, except for
  years that are exactly divisible by 100, but these centurial years
  are leap years if they are exactly divisible by 400.

As a result,  the year 2000 was a leap year, whereas 1900 and 2100 are
not.

The epoch of the Gregorian calendar, (1 January 1) was Monday, 1
January 3 in the Julian calendar or Julian Day Number 1721426.

The algorithm's for ymd2day and day2ymd are based off of.
- https://github.com/SETI/rms-julian/blob/main/julian/calendar.py
"""

#EPOCH 1/1/1970

# from: https://aa.usno.navy.mil/data/JulianDate
# Julian Dates
# Sunday	1 B.C. February 29	00:00:00.0	1721116.500000
# Monday	1 B.C. March 1	00:00:00.0	        1721117.500000
# Thursday	A.D. 1970 January 1	00:00:00.0	2440587.500000 (day 0 for our calculations)
# Thursday	A.D. 1582 October 4	00:00:00.0	2299159.500000 (last day of julian calendar)
# Friday	A.D. 1582 October 15	00:00:00.0	2299160.500000 (first day of gregorian calendar)

_FEB29_1BCE_JULIAN     = 1721116 - 2440587  # relative to JULIAN calendar
_FEB29_1BCE_GREGORIAN  = _FEB29_1BCE_JULIAN + 2 
_MAR01_1BCE            = - (_FEB29_1BCE_JULIAN + 1)
_GREGORIAN_DAY1        = 2299160 - 2440587


def ymd2day(year: int, month: int, day: int,validate: bool =False) -> int:

    """
    Converts given year , month, day to number of days since unix EPOCH.
      year  - assumed to be between 0001-9999
      month - 1 - 12
      day   - 1 - 31 (depending upon month and year)
    The calculation will be based upon:
      - Georgian Calendar for dates from and including 10/15/1582.
      - Julian Calendar for dates before and including 10/4/1582.
    Dates between the Julian Calendar and Georgian Calendar don't exist and None
    will be returned.
    
    If validate = true then None is returned if year,month,day is not a valid
    date.
    """
    
    mm = (month+9)%12
    yy = year - mm//10
    d  = day

    day_as_int = year*10000+month*100+day
    if day_as_int > 15821014:
        # use Georgian Calendar, leap year every 4 years except centuries that are not divisible by 400
        # 1900 - not yeap year
        # 2000 - yeap year
        daynum = (365*yy + yy//4 - yy//100 + yy//400) + (mm * 306 + 5)//10 + d + _FEB29_1BCE_GREGORIAN
    elif day_as_int < 15821005:
        # Julian Calendar, leap year ever 4 years
        daynum = (365*yy + yy//4) + (mm * 306 + 5)//10 + d + _FEB29_1BCE_JULIAN
    else:
        raise ValueError(f"Invalid date {year:04}-{month:02}-{day:02} not in Gregorian or Julian Calendar"))

    if validate:
        if day2ymd(daynum) != (year,month,day):
            raise ValueError(f"Invalid date {year:04}-{month:02}-{day:02}")
    return daynum

def day2ymd(daynum: int) -> (int,int,int):
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
       | -719614 | (1,1,1)          |
       | 2932896 | (9999,12,31)     |
       +----------------------------+
    
    """

    # before 1/1/1 or after 9999/12/25
    if daynum < -719164 or daynum > 2932896:
        raise ValueError(f"Invalid daynum {daynum} before 0001-01-01 or after 9999-12-31")

    # In Julian Calender 0001-01-03 is (JD 1721426).
    g = daynum + _MAR01_1BCE
    if daynum < _GREGORIAN_DAY1:
        y = (100 * g + 75) // 36525
        doy = g - (365*y + y//4)
    else:
        # In Georgian Calender 0001-01-01 is (JD 1721426).
        g -= 2
        y = (10000*g + 14780)//3652425
        doy = g - (365*y + y//4 - y//100 + y//400)
        if doy < 0:
            y -= 1
            doy = g - (365*y + y//4 - y//100 + y//400)
    m0 = (100 * doy + 52)//3060
    m = (m0+2)%12 + 1
    y += (m0+2)//12
    d = doy - (m0*306+5)//10 + 1
    return (y,m,d)

