"""SQLite persistence layer for peplink-monitor."""

import sqlite3
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
            message    TEXT    NOT NULL DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_health_events_ts
            ON health_events(timestamp);

        CREATE TABLE IF NOT EXISTS wan_health_state (
            wan_id         INTEGER PRIMARY KEY,
            wan_name       TEXT    NOT NULL,
            status_led     TEXT    NOT NULL,
            message        TEXT    NOT NULL DEFAULT '',
            uptime_seconds INTEGER NOT NULL DEFAULT 0,
            last_seen      INTEGER NOT NULL
        );
    """)
    # Migration: add label column if not present
    cols = {row[1] for row in conn.execute("PRAGMA table_info(interfaces)")}
    if "label" not in cols:
        conn.execute("ALTER TABLE interfaces ADD COLUMN label TEXT NOT NULL DEFAULT ''")

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

    conn.commit()


def get_interfaces(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute("SELECT * FROM interfaces ORDER BY if_index")
    return [dict(row) for row in cur.fetchall()]


def save_interfaces(conn: sqlite3.Connection, interfaces: list[dict]) -> None:
    """Insert discovered interfaces, ignoring duplicates to preserve existing IDs."""
    conn.executemany(
        """
        INSERT INTO interfaces
            (name, if_index, oid_hc_in, oid_hc_out, oid_status, label)
        VALUES
            (:name, :if_index, :oid_hc_in, :oid_hc_out, :oid_status, :label)
        ON CONFLICT(name) DO UPDATE SET label = excluded.label
        """,
        interfaces,
    )
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
) -> None:
    conn.execute(
        """
        INSERT INTO readings
            (interface_id, timestamp, bytes_in, bytes_out, oper_status)
        VALUES (?, ?, ?, ?, ?)
        """,
        (interface_id, timestamp, bytes_in, bytes_out, oper_status),
    )
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


def save_wan_ping(
    conn: sqlite3.Connection,
    timestamp: int,
    isp: str,
    ping_ms: float,
) -> None:
    conn.execute(
        "INSERT INTO wan_ping (timestamp, isp, ping_ms) VALUES (?, ?, ?)",
        (timestamp, isp, ping_ms),
    )
    conn.commit()


def get_latest_wan_ping_all(conn: sqlite3.Connection) -> list[dict]:
    """Most recent ping sample per ISP."""
    cur = conn.execute(
        """
        SELECT * FROM wan_ping
        WHERE id IN (SELECT MAX(id) FROM wan_ping GROUP BY isp)
        ORDER BY isp
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_wan_ping_in_period(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    cur = conn.execute(
        "SELECT * FROM wan_ping WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
        (start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


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
            SUM(t.delta_bytes_out) AS total_out
        FROM throughput t
        JOIN interfaces i ON t.interface_id = i.id
        WHERE t.timestamp >= ? AND t.timestamp <= ?
        GROUP BY day, t.interface_id
        ORDER BY day DESC, i.if_index
        """,
        (start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


def get_wan_ping_daily(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT
            date(timestamp, 'unixepoch') AS day,
            isp,
            MIN(ping_ms) AS min_ping,
            AVG(ping_ms) AS avg_ping,
            MAX(ping_ms) AS max_ping
        FROM wan_ping
        WHERE timestamp >= ? AND timestamp <= ?
        GROUP BY day, isp
        ORDER BY day DESC, isp
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
) -> None:
    conn.execute(
        """
        INSERT INTO wan_latency
            (timestamp, wan_name, latency_min, latency_avg, latency_max, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (timestamp, wan_name, latency_min, latency_avg, latency_max, source),
    )
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
    conn.commit()


def save_health_event(
    conn: sqlite3.Connection,
    timestamp: int,
    wan_id: int,
    wan_name: str,
    old_status: str,
    new_status: str,
    message: str,
) -> None:
    conn.execute(
        """
        INSERT INTO health_events
            (timestamp, wan_id, wan_name, old_status, new_status, message)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (timestamp, wan_id, wan_name, old_status, new_status, message),
    )
    conn.commit()


def get_health_events(
    conn: sqlite3.Connection,
    wan_name: str | None = None,
) -> list[dict]:
    """All health_events in chronological order, optionally filtered by WAN name."""
    if wan_name:
        cur = conn.execute(
            "SELECT * FROM health_events WHERE wan_name = ? ORDER BY timestamp",
            (wan_name,),
        )
    else:
        cur = conn.execute("SELECT * FROM health_events ORDER BY timestamp")
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


def get_readings_for_failovers(
    conn: sqlite3.Connection,
    wan_name: str | None = None,
) -> list[dict]:
    """All readings ordered by interface then timestamp, for failover detection."""
    if wan_name:
        cur = conn.execute(
            """
            SELECT r.*, i.name, i.label
            FROM readings r
            JOIN interfaces i ON r.interface_id = i.id
            WHERE i.name = ?
            ORDER BY i.if_index, r.timestamp
            """,
            (wan_name,),
        )
    else:
        cur = conn.execute(
            """
            SELECT r.*, i.name, i.label
            FROM readings r
            JOIN interfaces i ON r.interface_id = i.id
            ORDER BY i.if_index, r.timestamp
            """
        )
    return [dict(row) for row in cur.fetchall()]
