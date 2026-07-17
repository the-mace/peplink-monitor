"""Unit tests for rollup upsert idempotency and raw prune."""

import db


def test_prune_raw_samples():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    # Need an interface row for FK
    conn.execute(
        "INSERT INTO interfaces (name, if_index, oid_hc_in, oid_hc_out, oid_status, label) "
        "VALUES ('Spectrum', 5, 'a', 'b', 'c', 'WAN 1')"
    )
    iface_id = conn.execute("SELECT id FROM interfaces").fetchone()[0]
    conn.execute(
        "INSERT INTO readings (interface_id, timestamp, bytes_in, bytes_out, oper_status) "
        "VALUES (?, 100, 1, 1, 1), (?, 200, 2, 2, 1)",
        (iface_id, iface_id),
    )
    conn.execute(
        "INSERT INTO throughput "
        "(interface_id, timestamp, mbps_in, mbps_out, delta_bytes_in, delta_bytes_out, delta_seconds) "
        "VALUES (?, 100, 1, 1, 1, 1, 300), (?, 200, 1, 1, 1, 1, 300)",
        (iface_id, iface_id),
    )
    conn.execute(
        "INSERT INTO wan_latency (timestamp, wan_name, latency_min, latency_avg, latency_max, source) "
        "VALUES (100, 'Spectrum', 1, 1, 1, 'api'), (200, 'Spectrum', 2, 2, 2, 'api')"
    )
    conn.commit()

    deleted = db.prune_raw_samples(conn, older_than_ts=150)
    assert deleted["readings"] == 1
    assert deleted["throughput"] == 1
    assert deleted["wan_latency"] == 1
    assert conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 1
