# -*- coding: utf-8 -*-
"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

from datetime import tzinfo, datetime
import sys
import typing
import tzlocal
import pytz
from pynuodb.datatype import LOCALZONE_NAME


try:
    from zoneinfo import ZoneInfo
    HAS_ZONEINFO = True
except ImportError:
    HAS_ZONEINFO = False

# Define a type for mypy/static typing
if typing.TYPE_CHECKING:
    if sys.version_info >= (3, 9):
        from zoneinfo import ZoneInfo as TZType
    else:
        from pytz.tzinfo import BaseTzInfo as TZType
else:
    TZType = tzinfo


# Timezone getter function
def get_timezone(name):
    # type: (str) -> TZType
    """ get tzinfo by name """
    if HAS_ZONEINFO:
        return ZoneInfo(name)  # type: ignore[return-value]
    return pytz.timezone(name)  # type: ignore[return-value]

UTC = get_timezone("UTC")
Local = get_timezone(LOCALZONE_NAME)
TimeZoneInfo = get_timezone

def localize(dt, tzinfo=Local):  # pylint: disable=redefined-outer-name
    # type: (datetime, TZType) -> datetime
    """ localize naive datetime with given timezone """
    if sys.version_info >= (3, 9):
        return dt.replace(tzinfo=tzinfo)
    return tzinfo.localize(dt, is_dst=None)
