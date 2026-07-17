"""Unit tests for Peplink event-log timestamp parsing and regex."""

import datetime
from zoneinfo import ZoneInfo

from peplink_api import _LOG_WAN_RE, parse_log_ts


def test_parse_log_ts_eastern():
    # A fixed wall clock in America/New_York → known UTC unix time
    ts = parse_log_ts("Mar 24 11:44:34", "America/New_York")
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    # 11:44 EDT (UTC-4) or EST (UTC-5) depending on year; March 24 is typically EDT
    local = datetime.datetime.fromtimestamp(ts, tz=ZoneInfo("America/New_York"))
    assert local.month == 3 and local.day == 24
    assert local.hour == 11 and local.minute == 44 and local.second == 34
    # Must not treat the string as UTC (would be 4–5h off)
    as_utc = datetime.datetime(local.year, 3, 24, 11, 44, 34, tzinfo=datetime.timezone.utc)
    assert abs(ts - int(as_utc.timestamp())) >= 3 * 3600


def test_log_regex_connected():
    line = "Mar 24 11:44:34 WAN: Starlink (Priority 1) connected (100.75.195.71)"
    m = _LOG_WAN_RE.match(line)
    assert m is not None
    ts_str, wan, pri, ev, detail = m.groups()
    assert wan == "Starlink"
    assert pri == "1"
    assert ev == "connected"
    assert detail == "100.75.195.71"


def test_log_regex_disconnected():
    line = "Mar 24 11:44:14 WAN: Starlink (Priority 1) disconnected (WAN failed DNS test)"
    m = _LOG_WAN_RE.match(line)
    assert m is not None
    assert m.group(4) == "disconnected"
    assert "DNS" in m.group(5)
