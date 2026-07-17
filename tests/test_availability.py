"""Unit tests for report.compute_availability open-outage handling."""

from report import compute_availability, detect_storms


def _ev(wan, ts, old, new, eid=1):
    return {
        "id": eid,
        "wan_name": wan,
        "timestamp": ts,
        "old_status": old,
        "new_status": new,
        "message": "",
        "source": "log",
    }


def test_paired_outage():
    # Down 100–200 within window 0–1000
    events = [_ev("A", 100, "green", "red", 1), _ev("A", 200, "red", "green", 2)]
    r = compute_availability(events, 0, 1000)["A"]
    assert r["event_count"] == 1
    assert r["total_downtime_seconds"] == 100
    assert r["longest_outage_seconds"] == 100
    assert abs(r["availability_pct"] - 90.0) < 1e-9


def test_open_outage_closed_at_end():
    events = [_ev("A", 100, "green", "red", 1)]
    r = compute_availability(events, 0, 1000)["A"]
    assert r["event_count"] == 1
    assert r["total_downtime_seconds"] == 900
    assert r["longest_outage_seconds"] == 900


def test_already_down_at_start_via_prewindow():
    # Went down before window, recovered mid-window
    events = [
        _ev("A", 50, "green", "red", 1),
        _ev("A", 200, "red", "green", 2),
    ]
    r = compute_availability(events, 100, 1000)["A"]
    assert r["event_count"] == 0  # down event was pre-window
    assert r["total_downtime_seconds"] == 100  # 100..200
    assert r["longest_outage_seconds"] == 100


def test_recovery_without_seen_down_assumes_down_from_start():
    events = [_ev("A", 200, "red", "green", 1)]
    r = compute_availability(events, 100, 1000)["A"]
    assert r["total_downtime_seconds"] == 100


def test_initial_status_seed():
    events = [_ev("A", 200, "red", "green", 1)]
    r = compute_availability(
        events, 100, 1000, initial_status_by_wan={"A": "red"}
    )["A"]
    assert r["total_downtime_seconds"] == 100


def test_detect_storms_window():
    events = [
        _ev("S", 1_000 + i * 10, "green", "red", i)
        for i in range(10)
    ]
    # All in same UTC day roughly
    storms = detect_storms(events, threshold=8, start_ts=0, end_ts=1_000_000)
    assert any(s["wan_name"] == "S" and s["count"] == 10 for s in storms)
    storms_empty = detect_storms(events, threshold=8, start_ts=9_999_999, end_ts=99_999_999)
    assert storms_empty == []
