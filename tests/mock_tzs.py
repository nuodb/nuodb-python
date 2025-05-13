#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tzlocal
import pytz
try:
    from zoneinfo import ZoneInfo
    from datetime import timezone as TimeZone

    def localize(dt):
        return dt.astimezone(Local)

except ImportError:
    from pytz import timezone as ZoneInfo
    import pytz as TimeZone

    def localize(dt):
        return Local.localize(dt, is_dst=None)

if hasattr(tzlocal, 'get_localzone_name'):
    tzname = tzlocal.get_localzone_name()
else:
    tzname = tzlocal.get_localzone().zone

Local = ZoneInfo(tzname)
UTC = TimeZone.utc

