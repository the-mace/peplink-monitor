"""Total-network-outage detection and alert email formatting."""

from alerts import (
    find_completed_total_outages,
    format_outage_email,
    process_total_outage_alerts,
    _fmt_duration,
    _fmt_local,
)
import db


def _ev(wan, ts, old, new, msg="", eid=1):
    return {
        "id": eid,
        "wan_name": wan,
        "timestamp": ts,
        "old_status": old,
        "new_status": new,
        "message": msg,
        "source": "log",
    }


def test_jul19_style_dual_outage_until_first_recovery():
    # Both down at t=100; Starlink up at 155; Spectrum up at 344 → total = 55s
    events = [
        _ev("Spectrum", 100, "green", "red", "No cable detected", 1),
        _ev("Starlink", 100, "green", "red", "No cable detected", 2),
        _ev("Starlink", 155, "red", "green", "206.214.239.194", 3),
        _ev("Spectrum", 344, "red", "green", "174.83.7.72", 4),
    ]
    out = find_completed_total_outages(events, {"Spectrum", "Starlink"})
    assert len(out) == 1
    assert out[0]["started_at"] == 100
    assert out[0]["ended_at"] == 155
    assert out[0]["duration_seconds"] == 55
    assert out[0]["start_messages"]["Spectrum"] == "No cable detected"
    assert out[0]["start_messages"]["Starlink"] == "No cable detected"


def test_single_wan_down_is_not_total_with_two_wans():
    events = [
        _ev("Starlink", 100, "green", "red", "DNS", 1),
        _ev("Starlink", 200, "red", "green", "ok", 2),
    ]
    out = find_completed_total_outages(events, {"Spectrum", "Starlink"})
    assert out == []


def test_single_monitored_wan_outage_counts():
    events = [
        _ev("Spectrum", 100, "green", "red", "DNS", 1),
        _ev("Spectrum", 400, "red", "green", "ok", 2),
    ]
    out = find_completed_total_outages(events, {"Spectrum"})
    assert len(out) == 1
    assert out[0]["duration_seconds"] == 300


def test_open_total_outage_not_reported():
    events = [
        _ev("Spectrum", 100, "green", "red", "x", 1),
        _ev("Starlink", 100, "green", "red", "y", 2),
    ]
    assert find_completed_total_outages(events, {"Spectrum", "Starlink"}) == []


def test_fmt_local_boston():
    # 2026-07-19 11:50:37 UTC = 7:50:37 AM EDT
    s = _fmt_local(1784461837, "America/New_York")
    assert "July 19, 2026" in s
    assert "7:50:37" in s
    assert "AM" in s


def test_fmt_duration():
    assert _fmt_duration(55) == "55s"
    assert _fmt_duration(244) == "4m 4s"
    assert _fmt_duration(3600) == "1h"


def test_format_outage_email_mentions_local_times():
    outage = {
        "started_at": 1784461837,
        "ended_at": 1784461892,
        "duration_seconds": 55,
        "wans": ["Spectrum", "Starlink"],
        "start_messages": {
            "Spectrum": "No cable detected",
            "Starlink": "No cable detected",
        },
    }
    subject, body = format_outage_email(outage, "America/New_York")
    assert "55s" in subject
    assert "7:50:37" in body
    assert "7:51:32" in body
    assert "No cable detected" in body


def test_monitored_wans_use_wan_labels_not_disabled_ports():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.save_interfaces(
        conn,
        [
            {
                "name": "Spectrum",
                "if_index": 5,
                "oid_hc_in": "x.5",
                "oid_hc_out": "y.5",
                "oid_status": "z.5",
                "label": "WAN 1",
            },
            {
                "name": "Starlink",
                "if_index": 6,
                "oid_hc_in": "x.6",
                "oid_hc_out": "y.6",
                "oid_status": "z.6",
                "label": "WAN 2",
            },
            {
                "name": "Eero",
                "if_index": 1,
                "oid_hc_in": "x.1",
                "oid_hc_out": "y.1",
                "oid_status": "z.1",
                "label": "LAN 1",
            },
        ],
    )
    db.upsert_wan_health_state(conn, 1, "Spectrum", "green", "ok", 0, 1)
    db.upsert_wan_health_state(conn, 2, "Starlink", "green", "ok", 0, 1)
    db.upsert_wan_health_state(conn, 3, "USB", "empty", "No Device", 0, 1)
    db.upsert_wan_health_state(conn, 4, "Wi-Fi WAN on 5 GHz", "gray", "Disabled", 0, 1)
    from alerts import monitored_wan_names

    assert monitored_wan_names(conn, {}) == {"Spectrum", "Starlink"}


def test_process_seeds_historical_without_email(monkeypatch):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.save_interfaces(
        conn,
        [
            {
                "name": "Spectrum",
                "if_index": 5,
                "oid_hc_in": "x.5",
                "oid_hc_out": "y.5",
                "oid_status": "z.5",
                "label": "WAN 1",
            },
            {
                "name": "Starlink",
                "if_index": 6,
                "oid_hc_in": "x.6",
                "oid_hc_out": "y.6",
                "oid_status": "z.6",
                "label": "WAN 2",
            },
        ],
    )
    # Two WANs in health state
    db.upsert_wan_health_state(conn, 1, "Spectrum", "green", "ok", 0, 10_000)
    db.upsert_wan_health_state(conn, 2, "Starlink", "green", "ok", 0, 10_000)
    # Historical dual outage long ago
    db.save_health_event(conn, 100, 1, "Spectrum", "green", "red", "down", "log")
    db.save_health_event(conn, 100, 2, "Starlink", "green", "red", "down", "log")
    db.save_health_event(conn, 200, 2, "Starlink", "red", "green", "up", "log")
    db.save_health_event(conn, 300, 1, "Spectrum", "red", "green", "up", "log")

    sent = []

    def fake_send(to, subject, body, **kw):
        sent.append((to, subject, body))

    monkeypatch.setattr("alerts.send_mail", fake_send)

    cfg = {
        "alert_email": "rob@example.com",
        "alert_timezone": "America/New_York",
        "poll_interval_seconds": 300,
        "alert_min_duration_seconds": 0,
    }
    # "now" far in the future → historical, no email
    mailed = process_total_outage_alerts(cfg, conn, now=1_000_000)
    assert mailed == []
    assert sent == []
    assert db.is_total_outage_notified(conn, 100, 200)

    # Re-run does nothing
    mailed2 = process_total_outage_alerts(cfg, conn, now=1_000_000)
    assert mailed2 == []
    assert sent == []


def test_process_emails_recent_total_outage(monkeypatch):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.save_interfaces(
        conn,
        [
            {
                "name": "Spectrum",
                "if_index": 5,
                "oid_hc_in": "x.5",
                "oid_hc_out": "y.5",
                "oid_status": "z.5",
                "label": "WAN 1",
            },
            {
                "name": "Starlink",
                "if_index": 6,
                "oid_hc_in": "x.6",
                "oid_hc_out": "y.6",
                "oid_status": "z.6",
                "label": "WAN 2",
            },
        ],
    )
    db.upsert_wan_health_state(conn, 1, "Spectrum", "green", "ok", 0, 10_000)
    db.upsert_wan_health_state(conn, 2, "Starlink", "green", "ok", 0, 10_000)

    now = 10_000
    start = now - 120
    end = now - 60
    db.save_health_event(conn, start, 1, "Spectrum", "green", "red", "No cable", "log")
    db.save_health_event(conn, start, 2, "Starlink", "green", "red", "No cable", "log")
    db.save_health_event(conn, end, 1, "Spectrum", "red", "green", "up", "log")
    db.save_health_event(conn, end + 30, 2, "Starlink", "red", "green", "up", "log")

    sent = []

    def fake_send(to, subject, body, **kw):
        sent.append((to, subject, body))

    monkeypatch.setattr("alerts.send_mail", fake_send)

    cfg = {
        "alert_email": "rob@example.com",
        "alert_timezone": "America/New_York",
        "poll_interval_seconds": 300,
        "alert_min_duration_seconds": 0,
    }
    mailed = process_total_outage_alerts(cfg, conn, now=now)
    assert len(mailed) == 1
    assert len(sent) == 1
    assert sent[0][0] == "rob@example.com"
    assert "60s" in sent[0][1] or "1m" in sent[0][1]
    assert db.is_total_outage_notified(conn, start, end)


def test_process_disabled_without_email():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    assert process_total_outage_alerts({}, conn, now=1000) == []
