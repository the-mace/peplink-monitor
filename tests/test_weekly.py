"""Weekly summary: drop intervals, collection gaps, windowing, and body format."""

import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import db
import report
import weekly

ET = ZoneInfo("America/New_York")


def _ev(wan, ts, old, new, msg="", source="log", eid=0):
    return {"id": eid, "wan_name": wan, "timestamp": ts, "old_status": old,
            "new_status": new, "message": msg, "source": source}


def test_drop_intervals_prefer_log_edges_and_ignore_duplicate_poll_recovery():
    events = [
        _ev("Starlink", 1000, "green", "red", "WAN failed DNS test", eid=1),
        _ev("Starlink", 1050, "red", "green", "100.75.195.71", eid=2),
        _ev("Starlink", 1200, "red", "green", "Connected", source="poll", eid=3),
    ]
    out = weekly.drop_intervals(events, 0, 5000)
    assert len(out) == 1
    d = out[0]
    assert (d["start"], d["end"], d["duration"]) == (1000, 1050, 50)
    assert d["reason"] == "WAN failed DNS test"
    assert d["precise"] and not d["ongoing"] and not d["carried"]


def test_drop_intervals_clamp_carried_and_ongoing():
    events = [
        _ev("Spectrum", 50, "green", "red", "No cable detected", eid=1),
        _ev("Spectrum", 150, "red", "green", eid=2),
        _ev("Starlink", 900, "green", "flash", "Connecting...", source="poll", eid=3),
    ]
    out = weekly.drop_intervals(events, 100, 1000)
    by_wan = {d["wan"]: d for d in out}
    assert by_wan["Spectrum"]["carried"] and by_wan["Spectrum"]["duration"] == 50
    assert by_wan["Starlink"]["ongoing"] and by_wan["Starlink"]["duration"] == 100
    assert not by_wan["Starlink"]["precise"]


def test_drop_downtime_matches_compute_availability():
    events = [
        _ev("Spectrum", 50, "green", "red", eid=1),
        _ev("Spectrum", 150, "red", "green", eid=2),
        _ev("Spectrum", 400, "green", "red", source="poll", eid=3),
        _ev("Spectrum", 460, "red", "green", eid=4),
        _ev("Spectrum", 500, "red", "green", source="poll", eid=5),
        _ev("Spectrum", 900, "green", "red", eid=6),
    ]
    avail = report.compute_availability(events, 100, 1000)
    total = sum(d["duration"] for d in weekly.drop_intervals(events, 100, 1000))
    assert total == avail["Spectrum"]["total_downtime_seconds"]


def test_collection_gaps_include_window_edges():
    gaps = weekly.collection_gaps([1000, 1300, 2500], 0, 3000, threshold=660)
    assert gaps == [(0, 1000), (1300, 2500)]
    assert weekly.collection_gaps([300, 600, 900], 0, 1200, threshold=660) == []


def test_default_window_is_whole_local_days_across_dst():
    now = int(datetime(2026, 11, 9, 5, 40, tzinfo=ET).timestamp())
    start, end = weekly.default_window(now, ET, 7, through_now=False)
    assert datetime.fromtimestamp(end, ET) == datetime(2026, 11, 9, tzinfo=ET)
    assert datetime.fromtimestamp(start, ET) == datetime(2026, 11, 2, tzinfo=ET)
    start, end = weekly.default_window(now, ET, 7, through_now=True)
    assert (start, end) == (now - 7 * 86400, now)


def _seed_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.init_db(conn)
    conn.executemany(
        "INSERT INTO interfaces (id, name, if_index, oid_hc_in, oid_hc_out, oid_status, label) "
        "VALUES (?, ?, ?, '', '', '', ?)",
        [(5, "Spectrum", 5, "WAN 1"), (6, "Starlink", 6, "WAN 2")],
    )
    return conn


def test_build_summary_lines_are_parseable():
    conn = _seed_db()
    start, end = 10_000_000, 10_000_000 + 7 * 86400
    for ts in range(start, end + 1, 300):
        for iface in (5, 6):
            conn.execute("INSERT INTO readings (interface_id, timestamp, bytes_in, bytes_out, "
                         "oper_status) VALUES (?, ?, 0, 0, 1)", (iface, ts))
    conn.execute("INSERT INTO throughput (interface_id, timestamp, mbps_in, mbps_out, "
                 "delta_bytes_in, delta_bytes_out, delta_seconds) "
                 "VALUES (5, ?, 250.5, 20.0, 2000000000, 100000000, 300)", (start + 600,))
    conn.execute("INSERT INTO wan_latency (timestamp, wan_name, latency_min, latency_avg, "
                 "latency_max, source) VALUES (?, 'Starlink', 20, 30, 90, 'api')", (start + 900,))
    for e in [
        _ev("Starlink", start + 3600, "green", "red", "WAN failed DNS test"),
        _ev("Spectrum", start + 3610, "green", "red", "No cable detected"),
        _ev("Starlink", start + 3640, "red", "green", "ip"),
        _ev("Spectrum", start + 3700, "red", "green", "ip"),
    ]:
        conn.execute("INSERT INTO health_events (timestamp, wan_id, wan_name, old_status, "
                     "new_status, message, source) VALUES (?, 1, ?, ?, ?, ?, ?)",
                     (e["timestamp"], e["wan_name"], e["old_status"], e["new_status"],
                      e["message"], e["source"]))
    cfg = {"alert_timezone": "America/New_York", "poll_interval_seconds": 300}
    subject, body = weekly.build_summary(conn, cfg, start, end, end + 60)

    assert "1 full outage(s), 2 drop(s)" in subject
    assert "COLLECTION ISSUE" not in subject
    lines = body.splitlines()
    assert lines[0].startswith("Summary: 1 full outage(s)")
    assert "format=peplink-weekly/1" in lines
    assert any(ln.startswith("[full_outages] count=1 total=30s total_s=30") for ln in lines)
    drop = next(ln for ln in lines if ln.startswith("drop wan=Starlink"))
    assert "dur_s=40" in drop and 'reason="WAN failed DNS test"' in drop
    link = next(ln for ln in lines if ln.startswith("link wan=Spectrum rx_gb="))
    assert "rx_gb=2.00" in link and "peak_rx_mbps=250.5" in link
    lat = next(ln for ln in lines if ln.startswith("link wan=Starlink lat_avg_ms="))
    assert "lat_avg_ms=30.0" in lat and "lat_max_ms=90.0" in lat
    assert any(ln.startswith("pattern wan=Starlink") and "change=new" in ln for ln in lines)
    assert any(ln.startswith("[collection]") and "gaps_over_11m=0" in ln and "stale=no" in ln
               for ln in lines)
