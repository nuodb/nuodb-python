# -*- coding: utf-8 -*-
"""
(C) Copyright 2013-2026 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import sys
import datetime
import pytz

try:
    from zoneinfo import ZoneInfo
    HAS_ZONEINFO = True
except ImportError:
    HAS_ZONEINFO = False

try:
    import typing
    # Define a type for mypy/static typing
    if typing.TYPE_CHECKING:
        if HAS_ZONEINFO:
            TZType = ZoneInfo
        else:
            from pytz.tzinfo import BaseTzInfo as TZType
    else:
        TZType = datetime.tzinfo
except ImportError:
    pass

from pynuodb.datatype import LOCALZONE_NAME


def get_timezone(name):
    # type: (str) -> TZType
    """Return tzinfo for a given TZ name."""
    if HAS_ZONEINFO:
        return ZoneInfo(name)   # type: ignore[return-value]
    return pytz.timezone(name)  # type: ignore[return-value]


UTC = get_timezone("UTC")
Local = get_timezone(LOCALZONE_NAME)
TimeZoneInfo = get_timezone


def localize(dt, tzinfo=Local):
    # type: (datetime.datetime, TZType) -> datetime
    """Localize naive datetime with given timezone."""
    if sys.version_info >= (3, 9):
        return dt.replace(tzinfo=tzinfo)
    return tzinfo.localize(dt, is_dst=None)
