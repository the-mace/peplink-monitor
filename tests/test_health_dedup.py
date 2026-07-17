"""Unit tests for health-event direction-based dedup."""

import db


def _conn():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    return conn


def test_log_dedup_same_status():
    conn = _conn()
    assert db.try_save_log_health_event(conn, 1000, "Starlink", "disconnected", "DNS")
    assert not db.try_save_log_health_event(conn, 1030, "Starlink", "disconnected", "DNS")
    n = conn.execute("SELECT COUNT(*) FROM health_events").fetchone()[0]
    assert n == 1


def test_dedup_yellow_vs_red_same_direction():
    conn = _conn()
    assert db.try_save_health_event(
        conn, 1000, 1, "Starlink", "green", "yellow", "degraded", "poll"
    )
    # Log maps disconnect → red; same down-direction within 60s → skip
    assert not db.try_save_log_health_event(
        conn, 1020, "Starlink", "disconnected", "WAN failed DNS test"
    )
    n = conn.execute("SELECT COUNT(*) FROM health_events").fetchone()[0]
    assert n == 1


def test_up_and_down_both_stored():
    conn = _conn()
    assert db.try_save_log_health_event(conn, 1000, "Starlink", "disconnected", "DNS")
    assert db.try_save_log_health_event(conn, 1030, "Starlink", "connected", "1.2.3.4")
    n = conn.execute("SELECT COUNT(*) FROM health_events").fetchone()[0]
    assert n == 2


def test_outside_window_not_deduped():
    conn = _conn()
    assert db.try_save_log_health_event(conn, 1000, "Starlink", "disconnected", "DNS")
    assert db.try_save_log_health_event(conn, 1100, "Starlink", "disconnected", "DNS")
    n = conn.execute("SELECT COUNT(*) FROM health_events").fetchone()[0]
    assert n == 2


def test_is_up_status():
    assert db.is_up_status("green")
    assert not db.is_up_status("red")
    assert not db.is_up_status("yellow")
    assert not db.is_up_status("flash")
