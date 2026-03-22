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
    """)
    # Migration: add label column if not present
    cols = {row[1] for row in conn.execute("PRAGMA table_info(interfaces)")}
    if "label" not in cols:
        conn.execute("ALTER TABLE interfaces ADD COLUMN label TEXT NOT NULL DEFAULT ''")
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
