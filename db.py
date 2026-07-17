"""SQLite persistence layer for peplink-monitor."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path


def get_connection(db_path: str) -> sqlite3.Connection:
    path = Path(db_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS interfaces (
            id        INTEGER PRIMARY KEY,
            name      TEXT    NOT NULL UNIQUE,
            if_index  INTEGER NOT NULL UNIQUE,
            oid_hc_in  TEXT   NOT NULL,
            oid_hc_out TEXT   NOT NULL,
            oid_status TEXT   NOT NULL
        );

        CREATE TABLE IF NOT EXISTS readings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            interface_id INTEGER NOT NULL REFERENCES interfaces(id),
            timestamp    INTEGER NOT NULL,
            bytes_in     INTEGER NOT NULL,
            bytes_out    INTEGER NOT NULL,
            oper_status  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS throughput (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            interface_id    INTEGER NOT NULL REFERENCES interfaces(id),
            timestamp       INTEGER NOT NULL,
            mbps_in         REAL    NOT NULL,
            mbps_out        REAL    NOT NULL,
            delta_bytes_in  INTEGER NOT NULL,
            delta_bytes_out INTEGER NOT NULL,
            delta_seconds   REAL    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_readings_iface_ts
            ON readings(interface_id, timestamp);

        CREATE INDEX IF NOT EXISTS idx_throughput_iface_ts
            ON throughput(interface_id, timestamp);

        CREATE TABLE IF NOT EXISTS wan_ping (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            isp       TEXT    NOT NULL,
            ping_ms   REAL    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_wan_ping_ts
            ON wan_ping(timestamp);

        CREATE TABLE IF NOT EXISTS wan_latency (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp    INTEGER NOT NULL,
            wan_name     TEXT    NOT NULL,
            latency_min  REAL    NOT NULL,
            latency_avg  REAL    NOT NULL,
            latency_max  REAL    NOT NULL,
            source       TEXT    NOT NULL DEFAULT 'api'
        );

        CREATE INDEX IF NOT EXISTS idx_wan_latency_ts
            ON wan_latency(timestamp);

        CREATE TABLE IF NOT EXISTS health_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp  INTEGER NOT NULL,
            wan_id     INTEGER NOT NULL,
            wan_name   TEXT    NOT NULL,
            old_status TEXT    NOT NULL,
            new_status TEXT    NOT NULL,
            message    TEXT    NOT NULL DEFAULT '',
            source     TEXT    NOT NULL DEFAULT 'poll'
        );

        CREATE INDEX IF NOT EXISTS idx_health_events_ts
            ON health_events(timestamp);

        CREATE INDEX IF NOT EXISTS idx_health_events_wan_ts
            ON health_events(wan_name, timestamp);

        CREATE INDEX IF NOT EXISTS idx_wan_latency_src_wan_ts
            ON wan_latency(source, wan_name, timestamp);

        CREATE TABLE IF NOT EXISTS wan_health_state (
            wan_id         INTEGER PRIMARY KEY,
            wan_name       TEXT    NOT NULL,
            status_led     TEXT    NOT NULL,
            message        TEXT    NOT NULL DEFAULT '',
            uptime_seconds INTEGER NOT NULL DEFAULT 0,
            last_seen      INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS throughput_daily (
            day           TEXT    NOT NULL,
            interface_id  INTEGER NOT NULL REFERENCES interfaces(id),
            peak_in       REAL    NOT NULL,
            peak_out      REAL    NOT NULL,
            avg_in        REAL    NOT NULL,
            avg_out       REAL    NOT NULL,
            total_in      INTEGER NOT NULL,
            total_out     INTEGER NOT NULL,
            sample_count  INTEGER NOT NULL,
            PRIMARY KEY (day, interface_id)
        );

        CREATE TABLE IF NOT EXISTS latency_daily (
            day           TEXT    NOT NULL,
            wan_name      TEXT    NOT NULL,
            min_latency   REAL    NOT NULL,
            avg_latency   REAL    NOT NULL,
            max_latency   REAL    NOT NULL,
            sample_count  INTEGER NOT NULL,
            PRIMARY KEY (day, wan_name)
        );
    """)
    # Migration: add label column to interfaces if not present
    cols = {row[1] for row in conn.execute("PRAGMA table_info(interfaces)")}
    if "label" not in cols:
        conn.execute("ALTER TABLE interfaces ADD COLUMN label TEXT NOT NULL DEFAULT ''")

    # Migration: add source column to health_events if not present; backfill as 'poll'
    he_cols = {row[1] for row in conn.execute("PRAGMA table_info(health_events)")}
    if "source" not in he_cols:
        conn.execute(
            "ALTER TABLE health_events ADD COLUMN source TEXT NOT NULL DEFAULT 'poll'"
        )

    # One-time migration: copy ping-sourced data from wan_ping into wan_latency
    latency_count = conn.execute("SELECT COUNT(*) FROM wan_latency").fetchone()[0]
    if latency_count == 0:
        ping_table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='wan_ping'"
        ).fetchone()[0]
        if ping_table_exists:
            conn.execute("""
                INSERT INTO wan_latency (timestamp, wan_name, latency_min, latency_avg, latency_max, source)
                SELECT timestamp,
                       upper(substr(isp, 1, 1)) || lower(substr(isp, 2)),
                       ping_ms, ping_ms, ping_ms,
                       'ping'
                FROM wan_ping
            """)

    # One-time migration: backfill throughput_daily / latency_daily from raw
    # history. Safe to re-run (upsert-based); only fires while the rollup
    # tables are empty.
    rollup_count = conn.execute("SELECT COUNT(*) FROM throughput_daily").fetchone()[0]
    if rollup_count == 0:
        rollup_range(conn, start_ts=0, end_ts=int(time.time()))

    conn.commit()


def rollup_range(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> None:
    """Compute and upsert throughput_daily / latency_daily rows covering
    [start_ts, end_ts]. Idempotent — safe to call repeatedly for the same
    range (e.g. re-rolling up 'today' on every run, or the one-time backfill
    over all history).
    """
    for row in get_throughput_daily(conn, start_ts, end_ts):
        upsert_throughput_daily(conn, row)
    for row in get_wan_latency_daily(conn, start_ts, end_ts):
        upsert_latency_daily(conn, row)
    conn.commit()


def upsert_throughput_daily(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO throughput_daily
            (day, interface_id, peak_in, peak_out, avg_in, avg_out,
             total_in, total_out, sample_count)
        VALUES (:day, :interface_id, :peak_in, :peak_out, :avg_in, :avg_out,
                :total_in, :total_out, :sample_count)
        ON CONFLICT (day, interface_id) DO UPDATE SET
            peak_in      = excluded.peak_in,
            peak_out     = excluded.peak_out,
            avg_in       = excluded.avg_in,
            avg_out      = excluded.avg_out,
            total_in     = excluded.total_in,
            total_out    = excluded.total_out,
            sample_count = excluded.sample_count
        """,
        {
            "day": row["day"],
            "interface_id": row["interface_id"],
            "peak_in": row["peak_in"],
            "peak_out": row["peak_out"],
            "avg_in": row["avg_in"],
            "avg_out": row["avg_out"],
            "total_in": row["total_in"],
            "total_out": row["total_out"],
            "sample_count": row["samples"],
        },
    )


def upsert_latency_daily(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO latency_daily
            (day, wan_name, min_latency, avg_latency, max_latency, sample_count)
        VALUES (:day, :wan_name, :min_latency, :avg_latency, :max_latency, :sample_count)
        ON CONFLICT (day, wan_name) DO UPDATE SET
            min_latency  = excluded.min_latency,
            avg_latency  = excluded.avg_latency,
            max_latency  = excluded.max_latency,
            sample_count = excluded.sample_count
        """,
        {
            "day": row["day"],
            "wan_name": row["wan_name"],
            "min_latency": row["min_latency"],
            "avg_latency": row["avg_latency"],
            "max_latency": row["max_latency"],
            "sample_count": row["samples"],
        },
    )


def get_throughput_rollup_range(
    conn: sqlite3.Connection,
    start_day: str,
    end_day: str,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT td.*, i.name, i.label, i.if_index
        FROM throughput_daily td
        JOIN interfaces i ON td.interface_id = i.id
        WHERE td.day >= ? AND td.day <= ?
        ORDER BY td.day, i.if_index
        """,
        (start_day, end_day),
    )
    return [dict(row) for row in cur.fetchall()]


def get_earliest_rollup_day(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MIN(day) FROM throughput_daily").fetchone()
    return row[0] if row and row[0] else None


def get_latency_rollup_range(
    conn: sqlite3.Connection,
    start_day: str,
    end_day: str,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT * FROM latency_daily
        WHERE day >= ? AND day <= ?
        ORDER BY day, wan_name
        """,
        (start_day, end_day),
    )
    return [dict(row) for row in cur.fetchall()]


def get_interfaces(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute("SELECT * FROM interfaces ORDER BY if_index")
    return [dict(row) for row in cur.fetchall()]


def save_interfaces(conn: sqlite3.Connection, interfaces: list[dict], *, commit: bool = True) -> None:
    """Upsert discovered interfaces by name; refresh index/OIDs/label on conflict."""
    conn.executemany(
        """
        INSERT INTO interfaces
            (name, if_index, oid_hc_in, oid_hc_out, oid_status, label)
        VALUES
            (:name, :if_index, :oid_hc_in, :oid_hc_out, :oid_status, :label)
        ON CONFLICT(name) DO UPDATE SET
            if_index   = excluded.if_index,
            oid_hc_in  = excluded.oid_hc_in,
            oid_hc_out = excluded.oid_hc_out,
            oid_status = excluded.oid_status,
            label      = excluded.label
        """,
        interfaces,
    )
    if commit:
        conn.commit()


def get_latest_reading(conn: sqlite3.Connection, interface_id: int) -> dict | None:
    cur = conn.execute(
        """
        SELECT * FROM readings
        WHERE interface_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (interface_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def save_reading(
    conn: sqlite3.Connection,
    interface_id: int,
    timestamp: int,
    bytes_in: int,
    bytes_out: int,
    oper_status: int,
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO readings
            (interface_id, timestamp, bytes_in, bytes_out, oper_status)
        VALUES (?, ?, ?, ?, ?)
        """,
        (interface_id, timestamp, bytes_in, bytes_out, oper_status),
    )
    if commit:
        conn.commit()


def save_throughput(
    conn: sqlite3.Connection,
    interface_id: int,
    timestamp: int,
    mbps_in: float,
    mbps_out: float,
    delta_bytes_in: int,
    delta_bytes_out: int,
    delta_seconds: float,
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO throughput
            (interface_id, timestamp, mbps_in, mbps_out,
             delta_bytes_in, delta_bytes_out, delta_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            interface_id, timestamp, mbps_in, mbps_out,
            delta_bytes_in, delta_bytes_out, delta_seconds,
        ),
    )
    if commit:
        conn.commit()


def get_latest_readings_all(conn: sqlite3.Connection) -> list[dict]:
    """Most recent reading row per interface, joined with interface name and label."""
    cur = conn.execute(
        """
        SELECT r.*, i.name, i.label
        FROM readings r
        JOIN interfaces i ON r.interface_id = i.id
        WHERE r.id IN (
            SELECT MAX(id) FROM readings GROUP BY interface_id
        )
        ORDER BY i.if_index
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_latest_throughput_all(conn: sqlite3.Connection) -> list[dict]:
    """Most recent throughput row per interface, joined with interface name."""
    cur = conn.execute(
        """
        SELECT t.*, i.name
        FROM throughput t
        JOIN interfaces i ON t.interface_id = i.id
        WHERE t.id IN (
            SELECT MAX(id) FROM throughput GROUP BY interface_id
        )
        ORDER BY i.if_index
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_throughput_in_period(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT t.*, i.name, i.label
        FROM throughput t
        JOIN interfaces i ON t.interface_id = i.id
        WHERE t.timestamp >= ? AND t.timestamp <= ?
        ORDER BY i.if_index, t.timestamp
        """,
        (start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


def get_interfaces_ever_up(conn: sqlite3.Connection) -> set[int]:
    """Return set of interface_ids that have had at least one oper_status=1 reading."""
    cur = conn.execute(
        "SELECT DISTINCT interface_id FROM readings WHERE oper_status = 1"
    )
    return {row["interface_id"] for row in cur.fetchall()}


def get_throughput_daily(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT
            date(t.timestamp, 'unixepoch') AS day,
            i.name,
            i.label,
            i.if_index,
            t.interface_id,
            MAX(t.mbps_in)         AS peak_in,
            MAX(t.mbps_out)        AS peak_out,
            AVG(t.mbps_in)         AS avg_in,
            AVG(t.mbps_out)        AS avg_out,
            SUM(t.delta_bytes_in)  AS total_in,
            SUM(t.delta_bytes_out) AS total_out,
            COUNT(*)               AS samples
        FROM throughput t
        JOIN interfaces i ON t.interface_id = i.id
        WHERE t.timestamp >= ? AND t.timestamp <= ?
        GROUP BY day, t.interface_id
        ORDER BY day DESC, i.if_index
        """,
        (start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


def save_wan_latency(
    conn: sqlite3.Connection,
    timestamp: int,
    wan_name: str,
    latency_min: float,
    latency_avg: float,
    latency_max: float,
    source: str = "api",
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO wan_latency
            (timestamp, wan_name, latency_min, latency_avg, latency_max, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (timestamp, wan_name, latency_min, latency_avg, latency_max, source),
    )
    if commit:
        conn.commit()


def get_latest_wan_latency_all(conn: sqlite3.Connection) -> list[dict]:
    """Most recent api-sourced latency sample per WAN."""
    cur = conn.execute(
        """
        SELECT * FROM wan_latency
        WHERE source = 'api'
          AND id IN (SELECT MAX(id) FROM wan_latency WHERE source = 'api' GROUP BY wan_name)
        ORDER BY wan_name
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_wan_latency_in_period(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT * FROM wan_latency
        WHERE source = 'api' AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
        """,
        (start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


def get_wan_latency_daily(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT
            date(timestamp, 'unixepoch') AS day,
            wan_name,
            MIN(latency_min) AS min_latency,
            AVG(latency_avg) AS avg_latency,
            MAX(latency_max) AS max_latency,
            COUNT(*) AS samples
        FROM wan_latency
        WHERE source = 'api' AND timestamp >= ? AND timestamp <= ?
        GROUP BY day, wan_name
        ORDER BY day DESC, wan_name
        """,
        (start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


def get_readings_in_period(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    wan_name: str | None = None,
) -> list[dict]:
    """Readings in a time range, for per-day failover counting."""
    if wan_name:
        cur = conn.execute(
            """
            SELECT r.*, i.name, i.label
            FROM readings r
            JOIN interfaces i ON r.interface_id = i.id
            WHERE r.timestamp >= ? AND r.timestamp <= ? AND i.name = ?
            ORDER BY i.if_index, r.timestamp
            """,
            (start_ts, end_ts, wan_name),
        )
    else:
        cur = conn.execute(
            """
            SELECT r.*, i.name, i.label
            FROM readings r
            JOIN interfaces i ON r.interface_id = i.id
            WHERE r.timestamp >= ? AND r.timestamp <= ?
            ORDER BY i.if_index, r.timestamp
            """,
            (start_ts, end_ts),
        )
    return [dict(row) for row in cur.fetchall()]


def get_wan_health_states(conn: sqlite3.Connection) -> dict[int, dict]:
    """Return last known API health state per WAN, keyed by wan_id."""
    cur = conn.execute("SELECT * FROM wan_health_state")
    return {row["wan_id"]: dict(row) for row in cur.fetchall()}


def upsert_wan_health_state(
    conn: sqlite3.Connection,
    wan_id: int,
    wan_name: str,
    status_led: str,
    message: str,
    uptime_seconds: int,
    last_seen: int,
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO wan_health_state
            (wan_id, wan_name, status_led, message, uptime_seconds, last_seen)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(wan_id) DO UPDATE SET
            wan_name       = excluded.wan_name,
            status_led     = excluded.status_led,
            message        = excluded.message,
            uptime_seconds = excluded.uptime_seconds,
            last_seen      = excluded.last_seen
        """,
        (wan_id, wan_name, status_led, message, uptime_seconds, last_seen),
    )
    if commit:
        conn.commit()


def is_up_status(status: str) -> bool:
    """True only for fully healthy green LED (not flash/yellow/red/empty/gray)."""
    return status == "green"


def has_near_duplicate_health_event(
    conn: sqlite3.Connection,
    wan_name: str,
    timestamp: int,
    new_status: str,
    window_seconds: int = 60,
) -> bool:
    """True if an event for this WAN already ends in the same up/down direction.

    Direction is normalized: any non-green ``new_status`` counts as down, so a
    poll-recorded ``yellow`` and a log-recorded ``red`` for the same flap are
    treated as the same transition within the window.
    """
    if is_up_status(new_status):
        row = conn.execute(
            """
            SELECT 1 FROM health_events
            WHERE wan_name = ? AND new_status = 'green'
              AND ABS(timestamp - ?) <= ?
            LIMIT 1
            """,
            (wan_name, timestamp, window_seconds),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT 1 FROM health_events
            WHERE wan_name = ? AND new_status != 'green'
              AND ABS(timestamp - ?) <= ?
            LIMIT 1
            """,
            (wan_name, timestamp, window_seconds),
        ).fetchone()
    return row is not None


def save_health_event(
    conn: sqlite3.Connection,
    timestamp: int,
    wan_id: int,
    wan_name: str,
    old_status: str,
    new_status: str,
    message: str,
    source: str = "poll",
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO health_events
            (timestamp, wan_id, wan_name, old_status, new_status, message, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (timestamp, wan_id, wan_name, old_status, new_status, message, source),
    )
    if commit:
        conn.commit()


def try_save_health_event(
    conn: sqlite3.Connection,
    timestamp: int,
    wan_id: int,
    wan_name: str,
    old_status: str,
    new_status: str,
    message: str,
    source: str = "poll",
    *,
    commit: bool = True,
) -> bool:
    """Insert a health event unless a near-duplicate direction already exists."""
    if has_near_duplicate_health_event(conn, wan_name, timestamp, new_status):
        return False
    save_health_event(
        conn, timestamp, wan_id, wan_name, old_status, new_status, message, source,
        commit=commit,
    )
    return True


def try_save_log_health_event(
    conn: sqlite3.Connection,
    timestamp: int,
    wan_name: str,
    event_type: str,
    detail: str,
    *,
    commit: bool = True,
) -> bool:
    """Insert a log-sourced WAN health event if no near-duplicate exists.

    Deduplication uses up/down *direction* (green vs non-green) within a
    60-second window so poll- and log-sourced captures of the same flap are
    not double-counted even when LED colors differ (e.g. yellow vs red).

    Returns True if the event was stored, False if it was skipped.
    """
    if event_type == "connected":
        old_status, new_status = "red", "green"
    else:
        old_status, new_status = "green", "red"

    if has_near_duplicate_health_event(conn, wan_name, timestamp, new_status):
        return False

    # Look up wan_id from the health state table; fall back to 0 if unknown.
    row = conn.execute(
        "SELECT wan_id FROM wan_health_state WHERE wan_name = ?",
        (wan_name,),
    ).fetchone()
    wan_id = row["wan_id"] if row else 0

    save_health_event(
        conn, timestamp, wan_id, wan_name, old_status, new_status, detail, "log",
        commit=commit,
    )
    return True


def get_health_events(
    conn: sqlite3.Connection,
    wan_name: str | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> list[dict]:
    """Health events in chronological order (timestamp, then id).

    Optional filters: WAN name and/or [start_ts, end_ts] inclusive.
    """
    clauses: list[str] = []
    params: list = []
    if wan_name:
        clauses.append("wan_name = ?")
        params.append(wan_name)
    if start_ts is not None:
        clauses.append("timestamp >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append("timestamp <= ?")
        params.append(end_ts)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    cur = conn.execute(
        f"SELECT * FROM health_events {where} ORDER BY timestamp, id",
        params,
    )
    return [dict(row) for row in cur.fetchall()]


def count_health_failovers_in_period(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> dict[str, int]:
    """Count green→non-green transitions per WAN name within the time window."""
    cur = conn.execute(
        """
        SELECT wan_name, COUNT(*) AS cnt
        FROM health_events
        WHERE timestamp >= ? AND timestamp <= ?
          AND old_status = 'green' AND new_status != 'green'
        GROUP BY wan_name
        """,
        (start_ts, end_ts),
    )
    return {row["wan_name"]: row["cnt"] for row in cur.fetchall()}


def count_health_failovers_daily(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> dict[tuple[str, str], int]:
    """Count green→non-green transitions per (date_str, wan_name)."""
    cur = conn.execute(
        """
        SELECT date(timestamp, 'unixepoch') AS day, wan_name, COUNT(*) AS cnt
        FROM health_events
        WHERE timestamp >= ? AND timestamp <= ?
          AND old_status = 'green' AND new_status != 'green'
        GROUP BY day, wan_name
        """,
        (start_ts, end_ts),
    )
    return {(row["day"], row["wan_name"]): row["cnt"] for row in cur.fetchall()}


def prune_raw_samples(conn: sqlite3.Connection, older_than_ts: int) -> dict[str, int]:
    """Delete raw readings/throughput/wan_latency older than older_than_ts.

    Does not touch health_events, rollup tables, or the wan_ping archive.
    Returns counts of deleted rows per table.
    """
    deleted = {}
    for table in ("readings", "throughput", "wan_latency"):
        cur = conn.execute(f"DELETE FROM {table} WHERE timestamp < ?", (older_than_ts,))
        deleted[table] = cur.rowcount
    conn.commit()
    return deleted
