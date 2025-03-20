#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import tzinfo
from datetime import datetime
import os

import pytz

import pynuodb

if os.path.exists('/etc/timezone'):
    with open('/etc/timezone') as tzf:
        Local = pytz.timezone(tzf.read().strip())
else:
    with open('/etc/localtime', 'rb') as tlf:
        Local = pytz.build_tzinfo('localtime', tlf)  # type: ignore

UTC = pytz.timezone('UTC')


class _MyOffset(tzinfo):
    '''
    A timezone class that uses the current offset for all times in the past and
    future. The database doesn't return an timezone offset to us, it just
    returns the timestamp it has, but cast into the client's current timezone.
    This class can be used to do exactly the same thing to the test val.
    '''
    def utcoffset(self, dt):
        return Local.localize(datetime.now()).utcoffset()


MyOffset = _MyOffset()


class EscapingTimestamp(pynuodb.Timestamp):
    '''
    An EscapingTimestamp is just like a regular pynuodb.Timestamp, except that
    it's string representation is a bit of executable SQL that constructs the
    correct timestamp on the server side.  This is necessary until [DB-2251] is
    fixed and we can interpret straight strings of the kind that
    pynuodb.Timestamp produces.
    '''
    py2sql = {
        '%Y': 'YYYY',
        '%m': 'MM',
        '%d': 'dd',
        '%H': 'HH',
        '%M': 'mm',
        '%S': 'ss',
        '%f000': 'SSSSSSSSS',
        '%z': 'ZZZZ'}

    def __str__(self):
        pyformat = '%Y-%m-%d %H:%M:%S.%f000 %z'
        sqlformat = pyformat
        for pyspec, sqlspec in self.py2sql.items():
            sqlformat = sqlformat.replace(pyspec, sqlspec)
        return "DATE_FROM_STR('%s', '%s')" % (self.strftime(pyformat), sqlformat)


if __name__ == '__main__':
    print(str(EscapingTimestamp(2014, 7, 15, 23, 59, 58, 72, Local)))
    print(repr(EscapingTimestamp(2014, 7, 15, 23, 59, 58, 72, Local)))
    print(str(EscapingTimestamp(2014, 12, 15, 23, 59, 58, 72, Local)))
